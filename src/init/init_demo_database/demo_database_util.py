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
    with get_db_connection(connection_string) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

def fill_database_schema(database_schema, connection_string):
    """
    Fill column_values and date_range fields in database_schema using optimized batch queries.

    This function executes all queries from query_to_get_column_values and query_to_get_date_range
    using UNION ALL to minimize database round trips.

    Args:
        database_schema: List of table dictionaries with columns (will be modified in-place)
        connection_string: PostgreSQL connection string

    Returns:
        database_schema: The modified database_schema with filled column_values and date_range
    """
    # Collect all queries with metadata about their location
    column_value_queries = []
    date_range_queries = []

    for table_idx, table in enumerate(database_schema):
        table_name = table['table_name']
        for column_name, column_info in table['columns'].items():
            # Collect column value queries
            query = column_info.get('query_to_get_column_values', '').strip()
            if query:
                column_value_queries.append({
                    'table_idx': table_idx,
                    'column_name': column_name,
                    'query': query,
                    'identifier': f"{table_name}.{column_name}"
                })

            # Collect date range queries
            date_query = column_info.get('query_to_get_date_range', '').strip()
            if date_query:
                date_range_queries.append({
                    'table_idx': table_idx,
                    'column_name': column_name,
                    'query': date_query,
                    'identifier': f"{table_name}.{column_name}"
                })

    # Execute column value queries with UNION ALL
    if column_value_queries:
        union_parts = []
        for item in column_value_queries:
            # Wrap each query to add an identifier column
            union_parts.append(f"SELECT '{item['identifier']}' AS _identifier, value::TEXT AS value FROM ({item['query']}) AS subq(value)")

        batch_query = " UNION ALL ".join(union_parts)

        results = execute_query(batch_query, connection_string)

        if results:
            # Organize results by identifier
            results_dict = {}
            for row in results:
                identifier = row[0]
                value = row[1]
                if identifier not in results_dict:
                    results_dict[identifier] = []
                if value is not None:
                    results_dict[identifier].append(str(value))

            # Fill the database_schema with results
            for item in column_value_queries:
                identifier = item['identifier']
                if identifier in results_dict:
                    values_str = ' | '.join(results_dict[identifier])
                    database_schema[item['table_idx']]['columns'][item['column_name']]['column_values'] = values_str

    # Execute date range queries with UNION ALL
    if date_range_queries:
        union_parts = []
        for item in date_range_queries:
            # Wrap each query to add an identifier column
            union_parts.append(f"SELECT '{item['identifier']}' AS _identifier, value::TEXT AS value FROM ({item['query']}) AS subq(value)")

        batch_query = " UNION ALL ".join(union_parts)

        results = execute_query(batch_query, connection_string)

        if results:
            # Fill the database_schema with results
            for row in results:
                identifier = row[0]
                value = row[1]

                # Find the matching item
                for item in date_range_queries:
                    if item['identifier'] == identifier and value is not None:
                        database_schema[item['table_idx']]['columns'][item['column_name']]['date_range'] = str(value)
                        break

    return database_schema

def create_objects_documentation(database_schema, table_relationships, key_terms, connection_string=None):
    """
    Build comprehensive database schema context string.

    Note: This function now expects database_schema to have pre-filled column_values
    and date_range fields. Use fill_database_schema() before calling this function.

    Args:
        database_schema: List of table dictionaries with columns (with pre-filled values)
        table_relationships: List of relationship dictionaries
        key_terms: List of business term dictionaries

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

            # Use pre-filled column_values
            column_values = column_info.get('column_values', '').strip()
            if column_values:
                table_text += f"    Values in column {column_name}: {column_values}\n"

            # Use pre-filled date_range
            date_range = column_info.get('date_range', '').strip()
            if date_range:
                date_range_entries.append(f"  - Table {table_name}, column {column_name}: {date_range}\n")

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

    # Key terms section removed - will be added dynamically in reset_state based on search_terms

    # Join all parts
    return "\n".join(objects_documentation)
