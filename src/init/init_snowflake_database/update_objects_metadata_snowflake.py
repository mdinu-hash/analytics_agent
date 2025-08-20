# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit
import logging
from typing import Dict, List, Optional
import pandas as pd

class SnowflakeMetadataManager:
    """Get/Update tables metadata stored in Snowflake using Snowpark."""
    def __init__(self, session: snowpark.Session):
        self.session = session
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def get_table_metadata(self, catalog: str, schema: str, table: str, columns: Optional[List[str]] = None):
        full_table_name = f"{catalog}.{schema}.{table}"
        try:
            # Get table comment
            table_query = f"""
            SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COMMENT
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = '{catalog}'
                  AND TABLE_SCHEMA = '{schema}'
                  AND TABLE_NAME = '{table}'
            """
            table_df = self.session.sql(table_query)
            if table_df.count() == 0:
                self.logger.warning(f"Table {full_table_name} not found")
                return self.session.create_dataframe([{"error": "Table not found"}])
            table_comment = table_df.collect()[0]['COMMENT']
            # Get columns metadata
            columns_df = self.get_columns_metadata(catalog, schema, table, columns)
            # Build rows: one for table, then all columns
            rows = []
            # Table-level row
            rows.append({
                'TABLE_CATALOG': catalog,
                'TABLE_SCHEMA': schema,
                'TABLE_NAME': table,
                'COLUMN_NAME': None,
                'ORDINAL_POSITION': None,
                'COMMENT': table_comment
            })
            # Column-level rows
            for row in columns_df.collect():
                rows.append({
                    'TABLE_CATALOG': row['TABLE_CATALOG'],
                    'TABLE_SCHEMA': row['TABLE_SCHEMA'],
                    'TABLE_NAME': row['TABLE_NAME'],
                    'COLUMN_NAME': row['COLUMN_NAME'],
                    'ORDINAL_POSITION': row['ORDINAL_POSITION'],
                    'COMMENT': row['COMMENT']
                })
            return self.session.create_dataframe(rows)
        except Exception as e:
            self.logger.error(f"❌ Failed to get metadata for {full_table_name}: {e}")
            return self.session.create_dataframe([{"error": str(e)}])

    def get_columns_metadata(self, catalog: str, schema: str, table: str, columns: Optional[List[str]] = None):
        query = f"""
        SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = '{catalog}'
              AND TABLE_SCHEMA = '{schema}'
              AND TABLE_NAME = '{table}'
        """
        if columns:
            # Support both list of strings and list of dicts with 'name'
            column_names = [col['name'] if isinstance(col, dict) else col for col in columns]
            columns_list = ', '.join(["'" + col.replace("'", "''") + "'" for col in column_names])
            query += f" AND COLUMN_NAME IN ({columns_list})"
        query += " ORDER BY TABLE_NAME, ORDINAL_POSITION"
        try:
            df = self.session.sql(query)
            self.logger.info(f"Retrieved metadata for {df.count()} columns in {catalog}.{schema}.{table}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to get columns metadata for {catalog}.{schema}.{table}: {e}")
            return self.session.create_dataframe([{"error": str(e)}])

    def update_table_metadata(self, catalog: str, schema: str, table: str, comment: str, object_type: str = 'TABLE'):
        """Update metadata for a table or view."""
        full_object_name = f"{catalog}.{schema}.{table}"
        if not comment:
            self.logger.warning(f"⚠️  No comment provided for {full_object_name}")
            return self.session.create_dataframe([{"status": "No comment provided"}])
        escaped_comment = comment.replace("'", "''")
        query = f"COMMENT ON {object_type.upper()} {catalog}.{schema}.{table} IS '{escaped_comment}'"
        try:
            self.session.sql(query).collect()
            self.logger.info(f"✅ Updated {object_type.lower()} metadata for {full_object_name}")
            return self.session.create_dataframe([{"status": "success"}])
        except Exception as e:
            self.logger.error(f"❌ Failed to update {object_type.lower()} {full_object_name}: {e}")
            return self.session.create_dataframe([{"status": f"failure: {e}"}])

    def update_column_metadata(self, catalog: str, schema: str, table: str, column_updates: List[Dict[str, str]], object_type: str = 'TABLE'):
        """Update column metadata for a table or view."""
        full_object_name = f"{catalog}.{schema}.{table}"
        if not column_updates:
            self.logger.warning(f"No column updates provided for {full_object_name}")
            return self.session.create_dataframe([{"status": "No column updates provided"}])
        results = []
        for update in column_updates:
            column_name = update.get('name')
            comment = update.get('comment')
            if not column_name or not comment:
                self.logger.warning(f"Invalid column update: {update}")
                results.append({"column": column_name, "status": "invalid update"})
                continue
            escaped_comment = comment.replace("'", "''")
            if object_type.upper() == 'VIEW':
                query = f"ALTER VIEW {catalog}.{schema}.{table} ALTER COLUMN {column_name} COMMENT '{escaped_comment}'"
            else:
                query = f"COMMENT ON COLUMN {catalog}.{schema}.{table}.{column_name} IS '{escaped_comment}'"
            try:
                self.session.sql(query).collect()
                self.logger.info(f"✅ Updated column {full_object_name}.{column_name}")
                results.append({"column": column_name, "status": "success"})
            except Exception as e:
                self.logger.error(f"❌ Error updating column {full_object_name}.{column_name}: {e}")
                results.append({"column": column_name, "status": f"failure: {e}"})
        return self.session.create_dataframe(results)

    def bulk_update_metadata(self, metadata_updates: List[Dict]):
        """
        Bulk update metadata for multiple tables and columns. Does NOT update COLUMN_VALUES.
        Args:
            metadata_updates: List of update specifications.
        Returns:
            DataFrame with results for each update
        """
        results = []
        self.logger.info(f"Starting bulk update of {len(metadata_updates)} metadata operations")
        for i, update in enumerate(metadata_updates):
            update_type = update.get('type')
            catalog = update.get('catalog')
            schema = update.get('schema')
            table = update.get('table')
            object_type = update.get('object_type', 'TABLE')
            if update_type == 'table':
                res = self.update_table_metadata(catalog, schema, table, update['comment'], object_type)
                results.append({"update": f"{object_type.lower()}_{catalog}.{schema}.{table}", "status": res.collect()[0][0]})
            elif update_type == 'columns':
                res = self.update_column_metadata(catalog, schema, table, update['column_updates'], object_type)
                for row in res.collect():
                    if 'column' in row:
                        col_name = row['column']
                    elif 'COLUMN_NAME' in row:
                        col_name = row['COLUMN_NAME']
                    elif 'name' in row:
                        col_name = row['name']
                    else:
                        col_name = 'unknown'
                    if 'status' in row:
                        status_val = row['status']
                    elif 'STATUS' in row:
                        status_val = row['STATUS']
                    else:
                        status_val = 'unknown'
                    results.append({"update": f"columns_{catalog}.{schema}.{table}.{col_name}", "status": status_val})
            elif update_type == 'both':
                res_table = self.update_table_metadata(catalog, schema, table, update['table_comment'], object_type)
                results.append({"update": f"{object_type.lower()}_{catalog}.{schema}.{table}", "status": res_table.collect()[0][0]})
                res_cols = self.update_column_metadata(catalog, schema, table, update['column_updates'], object_type)
                for row in res_cols.collect():
                    if 'column' in row:
                        col_name = row['column']
                    elif 'COLUMN_NAME' in row:
                        col_name = row['COLUMN_NAME']
                    elif 'name' in row:
                        col_name = row['name']
                    else:
                        col_name = 'unknown'
                    if 'status' in row:
                        status_val = row['status']
                    elif 'STATUS' in row:
                        status_val = row['STATUS']
                    else:
                        status_val = 'unknown'
                    results.append({"update": f"columns_{catalog}.{schema}.{table}.{col_name}", "status": status_val})
            else:
                self.logger.error(f"Unknown update type: {update_type}")
                results.append({"update": f"unknown_{i+1}", "status": "unknown update type"})
        return self.session.create_dataframe(results)

    def create_metadata_reference_table(self, tables: List[Dict], relationships: List[Dict]):
        # Step 1: Validate and collect all columns metadata
        all_rows = []
        table_lookup = {}
        for t in tables:
            catalog = t['table_catalog']
            schema = t['table_schema']
            table = t['table_name']
            columns = t.get('columns', None)  # Optional list of column names to include
            column_metadata_map = {}
            # If columns is a list of dicts (with possible column_values), build a map
            if columns and isinstance(columns, list) and all(isinstance(col, dict) for col in columns):
                for col in columns:
                    col_name = col.get('name')
                    if col_name:
                        column_metadata_map[col_name] = col
            table_key = f"{catalog}.{schema}.{table}"
            table_lookup[table_key] = set([col['name'] if isinstance(col, dict) else col for col in columns]) if columns else None
            # Use get_table_metadata to retrieve both table and column metadata
            table_metadata_df = self.get_table_metadata(catalog, schema, table, [col['name'] if isinstance(col, dict) else col for col in columns] if columns else None)
            for row in table_metadata_df.collect():
                col_name = row['COLUMN_NAME'] if 'COLUMN_NAME' in row else None
                column_values = None
                # If this column has a custom query, execute it
                if col_name and col_name in column_metadata_map:
                    col_meta = column_metadata_map[col_name]
                    custom_query = col_meta.get('column_values')
                    if custom_query:
                        try:
                            result_df = self.session.sql(custom_query)
                            result_data = result_df.collect()
                            distinct_values = []
                            for r in result_data:
                                value = list(r.as_dict().values())[0]
                                if value is not None:
                                    distinct_values.append(str(value))
                            column_values = ', '.join(sorted(set(distinct_values)))
                        except Exception as e:
                            self.logger.error(f"Failed to execute custom query for {catalog}.{schema}.{table}.{col_name}: {e}")
                all_rows.append({
                    'TABLE_CATALOG': row['TABLE_CATALOG'],
                    'TABLE_SCHEMA': row['TABLE_SCHEMA'],
                    'TABLE_NAME': row['TABLE_NAME'],
                    'COLUMN_NAME': col_name,
                    'TABLE_COLUMN_DOCUMENTATION': row['COMMENT'],
                    'COLUMN_ORDINAL_POSITION': row['ORDINAL_POSITION'] if 'ORDINAL_POSITION' in row else None,
                    'RELATIONSHIP_KEY1': None,
                    'RELATIONSHIP_KEY2': None,
                    'COLUMN_VALUES': column_values
                })
        # Step 2: Map relationships to columns
        col_index = {f"{r['TABLE_CATALOG']}.{r['TABLE_SCHEMA']}.{r['TABLE_NAME']}.{r['COLUMN_NAME']}": r for r in all_rows if r['COLUMN_NAME'] is not None}
        for rel in relationships:
            key1 = rel.get('key1')
            key2 = rel.get('key2')
            # Validate keys
            for key in [key1, key2]:
                if key not in col_index:
                    self.logger.warning(f"Relationship key {key} not found in provided tables/columns.")
            # Attach relationship info
            if key1 in col_index:
                col_index[key1]['RELATIONSHIP_KEY1'] = key1
                col_index[key1]['RELATIONSHIP_KEY2'] = key2
            if key2 in col_index:
                col_index[key2]['RELATIONSHIP_KEY1'] = key1
                col_index[key2]['RELATIONSHIP_KEY2'] = key2
        # Step 3: Create or replace the reference table
        create_sql = '''
        CREATE OR REPLACE TABLE DATA_ANALYTICS.METADATA_REFERENCE_TABLE (
            TABLE_CATALOG STRING,
            TABLE_SCHEMA STRING,
            TABLE_NAME STRING,
            COLUMN_NAME STRING,
            TABLE_COLUMN_DOCUMENTATION STRING,
            COLUMN_ORDINAL_POSITION INT,
            RELATIONSHIP_KEY1 STRING,
            RELATIONSHIP_KEY2 STRING,
            COLUMN_VALUES STRING
        )'''
        self.session.sql(create_sql).collect()
        # Step 4: Insert all rows
        if all_rows:
            values_sql = ',\n'.join([
                (
                    f"('{r['TABLE_CATALOG']}', '{r['TABLE_SCHEMA']}', '{r['TABLE_NAME']}', "
                    + ("NULL" if r['COLUMN_NAME'] is None else f"'{r['COLUMN_NAME']}'") + ", "
                    + ("NULL" if r['TABLE_COLUMN_DOCUMENTATION'] is None else "'{}'".format(str(r['TABLE_COLUMN_DOCUMENTATION']).replace("'", "''"))) + ", "
                    + ("NULL" if r['COLUMN_ORDINAL_POSITION'] is None else str(r['COLUMN_ORDINAL_POSITION'])) + ", "
                    + ("NULL" if r['RELATIONSHIP_KEY1'] is None else "'{}'".format(r['RELATIONSHIP_KEY1'])) + ", "
                    + ("NULL" if r['RELATIONSHIP_KEY2'] is None else "'{}'".format(r['RELATIONSHIP_KEY2'])) + ", "
                    + ("NULL" if r.get('COLUMN_VALUES') is None else "'{}'".format(str(r['COLUMN_VALUES']).replace("'", "''")))
                    + ")"
                )
                for r in all_rows
            ])
            insert_sql = f"""
            INSERT INTO DATA_ANALYTICS.METADATA_REFERENCE_TABLE
            (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, TABLE_COLUMN_DOCUMENTATION, COLUMN_ORDINAL_POSITION, RELATIONSHIP_KEY1, RELATIONSHIP_KEY2, COLUMN_VALUES)
            VALUES
            {values_sql}
            """
            self.session.sql(insert_sql).collect()
        return self.session.create_dataframe([{"status": "success", "rows_inserted": len(all_rows)}])


def main(session: snowpark.Session): 
    manager = SnowflakeMetadataManager(session)
    metadata_updates = [
        # Dummy Table 1
        {
            'type': 'both',
            'catalog': 'CATALOG_NAME_1',
            'schema': 'SCHEMA_NAME_1', 
            'table': 'TABLE_NAME_1',
            'table_comment': '[TABLE_COMMENT_PLACEHOLDER_1]',
            'object_type': 'TABLE',
            'column_updates': [
                {'name': 'COLUMN_1_NAME', 'comment': '[COLUMN_1_COMMENT_PLACEHOLDER]'},
                {'name': 'COLUMN_2_NAME', 'comment': '[COLUMN_2_COMMENT_PLACEHOLDER]'},
                {'name': 'COLUMN_3_NAME', 'comment': '[COLUMN_3_COMMENT_PLACEHOLDER]', 'column_values': '[COLUMN_3_VALUES_QUERY_PLACEHOLDER]'},
                {'name': 'COLUMN_4_NAME', 'comment': '[COLUMN_4_COMMENT_PLACEHOLDER]'},
                {'name': 'COLUMN_5_NAME', 'comment': '[COLUMN_5_COMMENT_PLACEHOLDER]', 'column_values': '[COLUMN_5_VALUES_QUERY_PLACEHOLDER]'}
            ]
        },
        # Dummy Table 2
        {
            'type': 'both',
            'catalog': 'CATALOG_NAME_2',
            'schema': 'SCHEMA_NAME_2',
            'table': 'TABLE_NAME_2',
            'table_comment': '[TABLE_COMMENT_PLACEHOLDER_2]',
            'object_type': 'VIEW',
            'column_updates': [
                {'name': 'COLUMN_A_NAME', 'comment': '[COLUMN_A_COMMENT_PLACEHOLDER]'},
                {'name': 'COLUMN_B_NAME', 'comment': '[COLUMN_B_COMMENT_PLACEHOLDER]', 'column_values': '[COLUMN_B_VALUES_QUERY_PLACEHOLDER]'},
                {'name': 'COLUMN_C_NAME', 'comment': '[COLUMN_C_COMMENT_PLACEHOLDER]'},
                {'name': 'COLUMN_D_NAME', 'comment': '[COLUMN_D_COMMENT_PLACEHOLDER]'},
                {'name': 'COLUMN_E_NAME', 'comment': '[COLUMN_E_COMMENT_PLACEHOLDER]', 'column_values': '[COLUMN_E_VALUES_QUERY_PLACEHOLDER]'},
                {'name': 'COLUMN_F_NAME', 'comment': '[COLUMN_F_COMMENT_PLACEHOLDER]'}
            ]
        }
    ]
    
    # Step 1: Update metadata with custom queries
    update_result = manager.bulk_update_metadata(metadata_updates)
    
    # Step 2: Create metadata reference table
    # Extract table information from metadata_updates
    tables = []
    for update in metadata_updates:
        table_info = {
            'table_catalog': update['catalog'],
            'table_schema': update['schema'],
            'table_name': update['table'],
            'columns': update.get('column_updates', None)
        }
        tables.append(table_info)
    
    relationships = [
        {
            'key1': 'CATALOG_NAME_1.SCHEMA_NAME_1.TABLE_NAME_1.COLUMN_1_NAME',
            'key2': 'CATALOG_NAME_2.SCHEMA_NAME_2.TABLE_NAME_2.COLUMN_A_NAME'
        },
        {
            'key1': 'CATALOG_NAME_1.SCHEMA_NAME_1.TABLE_NAME_1.COLUMN_3_NAME',
            'key2': 'CATALOG_NAME_2.SCHEMA_NAME_2.TABLE_NAME_2.COLUMN_C_NAME'
        }
    ]
    
    reference_result = manager.create_metadata_reference_table(tables, relationships)
    
    # Return the reference table result
    return reference_result