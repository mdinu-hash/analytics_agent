import psycopg2
from contextlib import contextmanager

@contextmanager
def get_db_connection(connection_string):
    """Context manager for database connections."""
    conn = None
    try:
        conn = psycopg2.connect(connection_string)
        yield conn
    finally:
        if conn:
            conn.close()

def execute_query(query, connection_string):
    """
    Execute a SQL query and return results.

    Args:
        query: SQL query string to execute
        connection_string: PostgreSQL connection string

    Returns:
        List of results (each row as a tuple), or None if query fails
    """
    try:
        with get_db_connection(connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            return results
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

def create_objects_documentation(database_schema, table_relationships, key_terms, connection_string):
    """
    Build comprehensive database schema context string.

    Args:
        database_schema: List of table dictionaries with columns
        table_relationships: List of relationship dictionaries
        key_terms: List of business term dictionaries
        connection_string: PostgreSQL connection string

    Returns:
        str: Formatted documentation string
    """
    objects_documentation = []
    date_range_entries = []

    # Add all tables with all their columns AND values
    for table in database_schema:
        table_name = table['table_name']
        table_desc = table['table_description']

        # Start with table info
        table_text = f"Table {table_name}: {table_desc}\n"
        table_text += "Columns:\n"

        # Add all columns for this table
        for column_name, column_info in table['columns'].items():
            table_text += f"  - Column {column_name}: {column_info['description']}\n"

            # Check if column has a query to get values
            query = column_info.get('query_to_get_column_values', '')
            if query and query.strip():
                # Execute query to get column values
                results = execute_query(query, connection_string)
                if results:
                    # Extract values from results
                    column_values = []
                    for row in results:
                        if row and row[0] is not None:
                            column_values.append(str(row[0]))

                    # Add values as pipe-separated string
                    values_str = ' | '.join(column_values)
                    table_text += f"    Values in column {column_name}: {values_str}\n"

            # Check if column has a query to get date range
            date_query = column_info.get('query_to_get_date_range', '')
            if date_query and date_query.strip():
                # Execute query to get date range
                results = execute_query(date_query, connection_string)
                if results and results[0] and results[0][0] is not None:
                    date_info = str(results[0][0])
                    date_range_entries.append(f"  - Table {table_name}, column {column_name}: {date_info}\n")

        objects_documentation.append(table_text)

    # Add ALL table relationships
    relationships_text = "\nRelationships between Tables:\n"
    for rel in table_relationships:
        relationships_text += f"  {rel['key1']} -> {rel['key2']}\n"
    objects_documentation.append(relationships_text)

    # Add date range information
    if date_range_entries:
        date_range = "\nImportant considerations about dates available:\n"
        date_range += "".join(date_range_entries)
        objects_documentation.append(date_range)

    # Add key terms with query instructions
    key_terms_text = "\nQuery instructions for key terms:\n"
    for term in key_terms:
        term_name = term['name']
        term_definition = term['definition']
        query_instructions = term['query_instructions']

        if term_definition:
            key_terms_text += f"  - {term_name}: {term_definition}\n"
        else:
            key_terms_text += f"  - {term_name}\n"

        if query_instructions:
            key_terms_text += f"    {query_instructions}\n"

    objects_documentation.append(key_terms_text)

    # Join all parts
    return "\n".join(objects_documentation)
