import logging
from typing import Dict, List, Optional
import pandas as pd
import psycopg2
from contextlib import contextmanager

class DemoDatabaseMetadataManager:
    """
    Get metadata from the PostgreSQL demo database metadata_reference_table.
    Uses psycopg2 for database connectivity and retrieves metadata for agent initialization.
    Functions:
    - create_objects_documentation: Generate formatted documentation for tables and columns
    - get_database_content: Retrieve column values from metadata reference table
    """

    def __init__(self, connection_string: str):
        """
        Initialize the demo database metadata manager with PostgreSQL database connection.
        
        Args:
            connection_string: PostgreSQL connection string (e.g., 'host=localhost dbname=demo_db user=postgres password=pass')
        """
        self.connection_string = connection_string
        
        # logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    @contextmanager
    def get_connection(self):
        """Context manager for database connections with automatic cleanup."""
        conn = None
        try:
            conn = psycopg2.connect(self.connection_string)
            yield conn
        finally:
            if conn:
                conn.close()

    def _execute_sql(self, query: str, params: tuple = None) -> pd.DataFrame:
        """
        Execute SQL query and return DataFrame using psycopg2.
        
        Args:
            query: SQL query string
            params: Optional tuple of parameters for query
            
        Returns:
            pd.DataFrame: Query results
        """
        try:
            with self.get_connection() as conn:
                df = pd.read_sql(query, conn, params=params)
                return df
        except Exception as e:
            self.logger.error(f"Failed to execute SQL: {e}")
            self.logger.error(f"Query: {query}")
            raise

    def create_objects_documentation(self, schema: str = 'public', table_metadata_source: str = 'metadata_reference_table', tables: list = None) -> str:
        """
        Generate documentation for tables and columns based on the metadata reference table.
        
        Args:
            schema: Schema name for the metadata reference table (default: 'public')
            table_metadata_source: Table name for the metadata reference table (default: 'metadata_reference_table')
            tables: Optional list of dicts, each with keys:
                - 'table_schema', 'table_name', and optionally 'columns' (list of column names)
                If None, all tables and columns in the metadata_reference_table are used.
                
        Returns:
            str: Formatted documentation string ready for use in objects_documentation variable
        """
        try:
            query = f"SELECT * FROM {schema}.{table_metadata_source}"
            df = self._execute_sql(query)
            
            if df.empty:
                self.logger.warning("No metadata found in metadata_reference_table")
                return "No metadata found."

            doc_lines = []
            
            # Filter tables if provided
            if tables:
                # Build a set of (schema, table) for fast lookup
                table_set = set((t['table_schema'], t['table_name']) for t in tables)
                df = df[df.apply(lambda row: (row['table_schema'], row['table_name']) in table_set, axis=1)]
                
                # Optionally filter columns per table
                table_columns = {
                    (t['table_schema'], t['table_name']): set(t.get('columns', [])) 
                    for t in tables if 'columns' in t
                }
            else:
                table_columns = {}

            # Group by table
            table_groups = df.groupby(['table_schema', 'table_name'])
            
            for (schema_name, table_name), group in table_groups:
                # Get table-level documentation (where column_name is null)
                table_docs = group[group['column_name'].isnull()]
                table_comment = ""
                if not table_docs.empty:
                    table_comment = table_docs['table_column_documentation'].iloc[0] if pd.notnull(table_docs['table_column_documentation'].iloc[0]) else ''
                
                doc_lines.append(f"Table {schema_name}.{table_name}: {table_comment} Columns:")
                
                # Add column-level documentation
                column_docs = group[group['column_name'].notnull()].sort_values('column_ordinal_position')
                
                for _, row in column_docs.iterrows():
                    col_name = row['column_name']
                    
                    # Skip if specific columns were requested and this isn't one of them
                    if (schema_name, table_name) in table_columns and col_name not in table_columns[(schema_name, table_name)]:
                        continue
                    
                    comment = row['table_column_documentation'] if pd.notnull(row['table_column_documentation']) else ''
                    doc_lines.append(f"    {col_name}: {comment}")
                
                doc_lines.append("")  # Empty line after each table

            # Add relationships if present
            relationships = df.dropna(subset=['relationship_key1', 'relationship_key2'])
            if not relationships.empty:
                doc_lines.append("Relationships:")
                rel_pairs = set()
                
                for _, row in relationships.iterrows():
                    k1 = row['relationship_key1']
                    k2 = row['relationship_key2']
                    
                    # Avoid duplicate relationships (A->B and B->A)
                    if (k1, k2) not in rel_pairs and (k2, k1) not in rel_pairs:
                        doc_lines.append(f"{k1} relates to {k2}")
                        rel_pairs.add((k1, k2))

            result = '\n'.join(doc_lines)
            self.logger.info(f"Generated documentation for {len(table_groups)} tables")
            return result

        except Exception as e:
            self.logger.error(f"Failed to create objects documentation: {e}")
            return "Error retrieving metadata documentation."

    def get_database_content(self, schema: str = 'public', table_metadata_source: str = 'metadata_reference_table') -> str:
        """
        Retrieve column values from the metadata reference table and format them as a string.
        
        Args:
            schema: Schema name for the metadata reference table (default: 'public')
            table_metadata_source: Table name for the metadata reference table (default: 'metadata_reference_table')
            
        Returns:
            str: Formatted string with table.column and their values
        """
        try:
            # Query to get column values from the metadata_reference_table
            query = f"""
                SELECT table_name, column_name, column_values 
                FROM {schema}.{table_metadata_source}
                WHERE column_values IS NOT NULL 
                ORDER BY table_name, column_name
            """
            
            df = self._execute_sql(query)
            
            if df.empty:
                self.logger.info("No column values found in metadata_reference_table")
                return ""

            # Format the results as required
            content_lines = []
            for _, row in df.iterrows():
                table_name = row['table_name']
                column_name = row['column_name']
                column_values = row['column_values']
                
                if table_name and column_name and column_values:
                    content_lines.append(f"{table_name}.{column_name} column values: {column_values}.")

            result = '\n'.join(content_lines)
            self.logger.info(f"Generated database content for {len(content_lines)} columns")
            return result

        except Exception as e:
            self.logger.error(f"Failed to get database content: {e}")
            return ""

