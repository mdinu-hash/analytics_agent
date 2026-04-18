import sqlite3
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any
import os

sqlite3.register_adapter(date, lambda val: val.isoformat())
sqlite3.register_adapter(datetime, lambda val: val.isoformat())

class TierFeeDataGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the tier fee data generator with SQLite database connection.

        Args:
            connection_string: Path to SQLite database file
            **db_params: Individual connection parameters (db_path)
        """
        self.db_path = connection_string or db_params.get('db_path', './demo.db')

        # Tier fee structure from schema (in basis points converted to decimal)
        self.tier_fees_by_business_line = {
            'Managed Portfolio': [
                {'tier_min_aum': 0, 'tier_max_aum': 1000000, 'tier_fee_bps': 1},
                {'tier_min_aum': 1000000, 'tier_max_aum': 5000000, 'tier_fee_bps': 1},
                {'tier_min_aum': 5000000, 'tier_max_aum': 999999999999, 'tier_fee_bps': 1}
            ],
            'Separately Managed Account': [
                {'tier_min_aum': 0, 'tier_max_aum': 1000000, 'tier_fee_bps': 1},
                {'tier_min_aum': 1000000, 'tier_max_aum': 5000000, 'tier_fee_bps': 1},
                {'tier_min_aum': 5000000, 'tier_max_aum': 999999999999, 'tier_fee_bps': 1}
            ],
            'Mutual Fund Wrap': [
                {'tier_min_aum': 0, 'tier_max_aum': 1000000, 'tier_fee_bps': 1},
                {'tier_min_aum': 1000000, 'tier_max_aum': 5000000, 'tier_fee_bps': 1},
                {'tier_min_aum': 5000000, 'tier_max_aum': 999999999999, 'tier_fee_bps': 1}
            ],
            'Annuity': [
                {'tier_min_aum': 0, 'tier_max_aum': 999999999999, 'tier_fee_bps': 1}
            ],
            'Cash': [
                {'tier_min_aum': 0, 'tier_max_aum': 1000000, 'tier_fee_bps': 1},
                {'tier_min_aum': 1000000, 'tier_max_aum': 999999999999, 'tier_fee_bps': 1}
            ]
        }

    def _get_business_line_keys(self) -> Dict[str, int]:
        """Get business line keys from the database."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT business_line_key, business_line_name FROM business_line")
            business_lines = {row[1]: row[0] for row in cursor.fetchall()}

            if not business_lines:
                raise Exception("No business lines found. Please run e_create_business_line.py first.")

            print(f"Found {len(business_lines)} business lines")
            return business_lines

        except Exception as e:
            print(f"Error fetching business line keys: {e}")
            raise
        finally:
            conn.close()

    def generate_tier_fee_data(self) -> List[Dict[str, Any]]:
        """Generate tier fee data according to schema specifications."""
        business_line_keys = self._get_business_line_keys()

        tier_fees = []
        tier_fee_id = 1

        print("Generating tier fee structure...")

        for business_line_name, fee_tiers in self.tier_fees_by_business_line.items():
            business_line_key = business_line_keys.get(business_line_name)

            if not business_line_key:
                print(f"Warning: Business line '{business_line_name}' not found in database")
                continue

            print(f"Creating {len(fee_tiers)} fee tiers for {business_line_name}")

            for tier in fee_tiers:
                tier_fee = {
                    'tier_fee_id': tier_fee_id,
                    'business_line_key': business_line_key,
                    'business_line_name': business_line_name,
                    'tier_min_aum': tier['tier_min_aum'],
                    'tier_max_aum': tier['tier_max_aum'],
                    'tier_fee_bps': tier['tier_fee_bps']
                }

                tier_fees.append(tier_fee)
                tier_fee_id += 1

        print(f"Generated {len(tier_fees)} tier fee records")
        return tier_fees

    def create_tier_fee_table(self):
        """Create tier_fee table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS tier_fee (
            tier_fee_id INTEGER PRIMARY KEY,
            business_line_key INTEGER NOT NULL,
            tier_min_aum REAL NOT NULL,
            tier_max_aum REAL NOT NULL,
            tier_fee_bps INTEGER NOT NULL,
            FOREIGN KEY (business_line_key) REFERENCES business_line(business_line_key),
            CONSTRAINT check_tier_range CHECK (tier_min_aum <= tier_max_aum),
            CONSTRAINT check_fee_positive CHECK (tier_fee_bps >= 0)
        );
        CREATE INDEX IF NOT EXISTS ix_tier_fee_lookup
        ON tier_fee(business_line_key, tier_min_aum, tier_max_aum);
        CREATE INDEX IF NOT EXISTS ix_tier_fee_business_line
        ON tier_fee(business_line_key);
        """

        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(create_table_sql)
            print("Tier fee table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating tier fee table: {e}")
            raise
        finally:
            conn.close()

    def clear_existing_data(self):
        """Clear existing tier fee data."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM tier_fee")
            conn.commit()
            print("Existing tier fee data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
        finally:
            conn.close()

    def insert_tier_fee_data(self, tier_fees: List[Dict[str, Any]]):
        """Insert tier fee data into SQLite database."""
        print(f"Inserting {len(tier_fees)} tier fee records...")

        insert_sql = """
        INSERT INTO tier_fee (
            business_line_key, tier_min_aum, tier_max_aum, tier_fee_bps
        ) VALUES (
            :business_line_key, :tier_min_aum, :tier_max_aum, :tier_fee_bps
        )
        """

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.executemany(insert_sql, tier_fees)
            conn.commit()
            print(f"Successfully inserted all {len(tier_fees)} tier fee records")
        except Exception as e:
            print(f"Error inserting tier fee data: {e}")
            raise
        finally:
            conn.close()

    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total tier fee records': "SELECT COUNT(*) FROM tier_fee",
            'Tier fees by business line': """
                SELECT bl.business_line_name, COUNT(tf.tier_fee_id) as tier_count
                FROM business_line bl
                LEFT JOIN tier_fee tf ON bl.business_line_key = tf.business_line_key
                GROUP BY bl.business_line_name, bl.business_line_key
                ORDER BY bl.business_line_key
            """,
        }

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            print("\n" + "="*80)
            print("TIER FEE DATA VALIDATION RESULTS")
            print("="*80)

            for description, query in validation_queries.items():
                cursor.execute(query)
                results = cursor.fetchall()

                print(f"\n{description}:")

                if len(results) == 1 and len(results[0]) == 1:
                    print(f"  {results[0][0]}")
                elif not results:
                    print("  No issues found ✓")
                else:
                    for row in results:
                        if len(row) == 2:
                            print(f"  {row[0]}: {row[1]}")
                        else:
                            print(f"  {row}")

            print("\n" + "="*80)
        except Exception as e:
            print(f"Error during validation: {e}")
        finally:
            conn.close()

def main():
    """Main function to generate and insert tier fee data."""
    db_config = {
        'db_path': os.getenv('DB_PATH', './demo.db')
    }

    try:
        generator = TierFeeDataGenerator(**db_config)

        generator.create_tier_fee_table()

        response = input("Do you want to clear existing tier fee data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()

        tier_fees = generator.generate_tier_fee_data()
        generator.insert_tier_fee_data(tier_fees)
        generator.validate_data()

        print(f"\nTier fee data generation completed successfully!")
        print(f"Generated {len(tier_fees)} tier fee records across all business lines")
        print(f"Fee structure is ready for revenue calculations")

    except Exception as e:
        print(f"Error: {e}")
        print("Please check your SQLite database path configuration.")
        print("Ensure you have run e_create_business_line.py first to create business line lookup data.")

if __name__ == "__main__":
    main()
