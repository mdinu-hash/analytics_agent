import sqlite3
from datetime import date, datetime
from contextlib import contextmanager

# Register adapters so Python date/datetime objects are stored as ISO strings
sqlite3.register_adapter(date, lambda val: val.isoformat())
sqlite3.register_adapter(datetime, lambda val: val.isoformat())

@contextmanager
def get_db_connection(db_path):
    """Context manager for SQLite database connections."""
    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.close()

def execute_query(query, db_path):
    """
    Execute a SQL query and return results.

    Args:
        query: SQL query string to execute
        db_path: Path to the SQLite database file

    Returns:
        List of results (each row as a tuple), or None if query fails
    """
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

def create_objects_documentation(database_schema, table_relationships, key_terms):
    """
    Build comprehensive database schema context string.

    Args:
        database_schema: List of table dictionaries with columns
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

            # Use pre-filled sample_values
            sample_values = column_info.get('sample_values')
            if sample_values:
                table_text += f"    Sample values in column {column_name}: {sample_values}\n"

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

    # Join all parts
    return "\n".join(objects_documentation)
