import sqlite3
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any
import os

sqlite3.register_adapter(date, lambda val: val.isoformat())
sqlite3.register_adapter(datetime, lambda val: val.isoformat())

class AdvisorPayoutRateDataGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the advisor payout rate data generator with SQLite database connection.

        Args:
            connection_string: Path to SQLite database file
            **db_params: Individual connection parameters (db_path)
        """
        self.db_path = connection_string or db_params.get('db_path', './demo.db')

        # Advisor payout rates by firm affiliation model from schema
        self.payout_rates = {
            'RIA': 0.78,
            'Hybrid RIA': 0.70,
            'Independent BD': 0.85,
            'Broker-Dealer W-2': 0.45,
            'Wirehouse': 0.42,
            'Bank/Trust': 0.35,
            'Insurance BD': 0.75
        }

    def _get_advisor_firm_models(self) -> List[str]:
        """Get distinct firm affiliation models from advisors table."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT firm_affiliation_model FROM advisors ORDER BY firm_affiliation_model")
            firm_models = [row[0] for row in cursor.fetchall()]

            if not firm_models:
                print("Warning: No advisors found. Using all defined firm affiliation models from schema.")
                firm_models = list(self.payout_rates.keys())

            print(f"Found {len(firm_models)} distinct firm affiliation models")
            return firm_models

        except Exception as e:
            print(f"Error fetching firm affiliation models: {e}")
            return list(self.payout_rates.keys())
        finally:
            conn.close()

    def generate_payout_rate_data(self) -> List[Dict[str, Any]]:
        """Generate advisor payout rate data according to schema specifications."""
        firm_models = self._get_advisor_firm_models()

        payout_rate_data = []

        print("Generating advisor payout rates...")

        for firm_model in firm_models:
            payout_rate = self.payout_rates.get(firm_model)

            if payout_rate is None:
                print(f"Warning: No payout rate defined for firm model '{firm_model}'. Skipping...")
                continue

            payout_rate_record = {
                'firm_affiliation_model': firm_model,
                'advisor_payout_rate': payout_rate
            }

            payout_rate_data.append(payout_rate_record)
            print(f"  {firm_model}: {payout_rate:.1%}")

        print(f"Generated {len(payout_rate_data)} payout rate records")
        return payout_rate_data

    def generate_advisor_payout_rate_data(self) -> List[Dict[str, Any]]:
        """Alias for generate_payout_rate_data() for compatibility with notebook."""
        return self.generate_payout_rate_data()

    def create_table_if_not_exists(self):
        """Create advisor_payout_rate table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS advisor_payout_rate (
            firm_affiliation_model TEXT PRIMARY KEY,
            advisor_payout_rate REAL NOT NULL,
            CONSTRAINT check_payout_rate_valid CHECK (advisor_payout_rate >= 0 AND advisor_payout_rate <= 1)
        );
        CREATE INDEX IF NOT EXISTS ix_advisor_payout_firm
        ON advisor_payout_rate(firm_affiliation_model);
        """

        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(create_table_sql)
            print("Advisor payout rate table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating advisor payout rate table: {e}")
            raise
        finally:
            conn.close()

    def clear_existing_data(self):
        """Clear existing advisor payout rate data."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM advisor_payout_rate")
            conn.commit()
            print("Existing advisor payout rate data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
        finally:
            conn.close()

    def insert_payout_rate_data(self, payout_rates: List[Dict[str, Any]]):
        """Insert advisor payout rate data into SQLite database."""
        print(f"Inserting {len(payout_rates)} payout rate records...")

        insert_sql = """
        INSERT OR REPLACE INTO advisor_payout_rate (firm_affiliation_model, advisor_payout_rate)
        VALUES (:firm_affiliation_model, :advisor_payout_rate)
        """

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.executemany(insert_sql, payout_rates)
            conn.commit()
            print(f"Successfully inserted all {len(payout_rates)} payout rate records")
        except Exception as e:
            print(f"Error inserting payout rate data: {e}")
            raise
        finally:
            conn.close()

    def insert_advisor_payout_rate_data(self, payout_rates: List[Dict[str, Any]]):
        """Alias for insert_payout_rate_data() for compatibility with notebook."""
        return self.insert_payout_rate_data(payout_rates)

    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total payout rate records': "SELECT COUNT(*) FROM advisor_payout_rate",
            'All payout rates': """
                SELECT firm_affiliation_model,
                       advisor_payout_rate,
                       ROUND(advisor_payout_rate * 100, 1) as percentage
                FROM advisor_payout_rate
                ORDER BY advisor_payout_rate DESC
            """,
        }

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            print("\n" + "="*80)
            print("ADVISOR PAYOUT RATE DATA VALIDATION RESULTS")
            print("="*80)

            for description, query in validation_queries.items():
                cursor.execute(query)
                results = cursor.fetchall()

                print(f"\n{description}:")

                if len(results) == 1 and len(results[0]) == 1:
                    print(f"  {results[0][0]}")
                elif 'All payout rates' in description:
                    for row in results:
                        firm_model, rate, percentage = row
                        print(f"  {firm_model}: {rate:.4f} ({percentage:.1f}%)")
                elif not results:
                    print("  None found ✓")
                else:
                    for row in results:
                        if len(row) == 1:
                            print(f"  {row[0]}")
                        else:
                            print(f"  {row}")

            print("\n" + "="*80)
        except Exception as e:
            print(f"Error during validation: {e}")
        finally:
            conn.close()

def main():
    """Main function to generate and insert advisor payout rate data."""
    db_config = {
        'db_path': os.getenv('DB_PATH', './demo.db')
    }

    try:
        generator = AdvisorPayoutRateDataGenerator(**db_config)

        generator.create_table_if_not_exists()

        response = input("Do you want to clear existing advisor payout rate data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()

        payout_rates = generator.generate_payout_rate_data()
        generator.insert_payout_rate_data(payout_rates)
        generator.validate_data()

        print(f"\nAdvisor payout rate data generation completed successfully!")
        print(f"Generated {len(payout_rates)} payout rate records")
        print(f"Payout rates are ready for revenue calculations")

    except Exception as e:
        print(f"Error: {e}")
        print("Please check your SQLite database path configuration.")

if __name__ == "__main__":
    main()
