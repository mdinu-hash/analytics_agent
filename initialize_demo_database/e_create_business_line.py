import sqlite3
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any
import os

sqlite3.register_adapter(date, lambda val: val.isoformat())
sqlite3.register_adapter(datetime, lambda val: val.isoformat())

class BusinessLineDataGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the business line data generator with SQLite database connection.

        Args:
            connection_string: Path to SQLite database file
            **db_params: Individual connection parameters (db_path)
        """
        self.db_path = connection_string or db_params.get('db_path', './demo.db')

        # Business lines from schema
        self.business_lines = [
            'Managed Portfolio',
            'Separately Managed Account',
            'Mutual Fund Wrap',
            'Annuity',
            'Cash'
        ]

    def generate_business_line_data(self) -> List[Dict[str, Any]]:
        """Generate business line data according to schema specifications."""
        print(f"Generating {len(self.business_lines)} business lines...")

        business_line_data = []
        for key, name in enumerate(self.business_lines, 1):
            business_line_data.append({
                'business_line_key': key,
                'business_line_name': name
            })

        print(f"Generated {len(business_line_data)} business line records")
        return business_line_data

    def create_business_line_table(self):
        """Create business_line table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS business_line (
            business_line_key INTEGER PRIMARY KEY,
            business_line_name TEXT NOT NULL UNIQUE
        );
        """

        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(create_table_sql)
            print("Business line table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating business line table: {e}")
            raise
        finally:
            conn.close()

    def clear_existing_data(self):
        """Clear existing business line data."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM business_line")
            conn.commit()
            print("Existing business line data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
        finally:
            conn.close()

    def insert_business_line_data(self, business_lines: List[Dict[str, Any]]):
        """Insert business line data into SQLite database."""
        print(f"Inserting {len(business_lines)} business line records...")

        insert_sql = """
        INSERT INTO business_line (business_line_key, business_line_name)
        VALUES (:business_line_key, :business_line_name)
        """

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.executemany(insert_sql, business_lines)
            conn.commit()
            print(f"Successfully inserted all {len(business_lines)} business line records")
        except Exception as e:
            print(f"Error inserting business line data: {e}")
            raise
        finally:
            conn.close()

    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Business line count': "SELECT COUNT(*) FROM business_line",
            'Business lines': "SELECT business_line_key, business_line_name FROM business_line ORDER BY business_line_key"
        }

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            print("\n" + "="*60)
            print("DATA VALIDATION RESULTS")
            print("="*60)

            for description, query in validation_queries.items():
                cursor.execute(query)
                results = cursor.fetchall()

                print(f"\n{description}:")
                if len(results) == 1 and len(results[0]) == 1:
                    print(f"  {results[0][0]}")
                else:
                    for row in results:
                        if description == 'Business lines':
                            print(f"  {row[0]}: {row[1]}")
                        else:
                            print(f"  {row}")

            print("\n" + "="*60)
        except Exception as e:
            print(f"Error during validation: {e}")
        finally:
            conn.close()

def main():
    """Main function to generate and insert business line data."""
    db_config = {
        'db_path': os.getenv('DB_PATH', './demo.db')
    }

    try:
        generator = BusinessLineDataGenerator(**db_config)

        generator.create_business_line_table()

        response = input("Do you want to clear existing business line data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()

        business_lines = generator.generate_business_line_data()
        generator.insert_business_line_data(business_lines)
        generator.validate_data()

        print(f"\nBusiness line data generation completed successfully!")
        print(f"Generated {len(business_lines)} business lines")
        print("Note: Tier fee data is managed separately by h_create_tier_fee.py")

    except Exception as e:
        print(f"Error: {e}")
        print("Please check your SQLite database path configuration.")
        print("Run h_create_tier_fee.py separately to create tier fee data.")

if __name__ == "__main__":
    main()
