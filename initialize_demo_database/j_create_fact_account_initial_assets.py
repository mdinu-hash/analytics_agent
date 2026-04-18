import sqlite3
import random
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any, Tuple
import os
import numpy as np

sqlite3.register_adapter(date, lambda val: val.isoformat())
sqlite3.register_adapter(datetime, lambda val: val.isoformat())

class FactAccountInitialAssetsGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the fact account initial assets generator with SQLite database connection.

        Args:
            connection_string: Path to SQLite database file
            **db_params: Individual connection parameters (db_path)
        """
        self.db_path = connection_string or db_params.get('db_path', './demo.db')

        # Constants from schema
        self.snapshot_start_date = date(2024, 9, 30)

        self.asset_distributions = {
            'Taxable': {
                'median': 120000,
                'minimum': 10000,
                'std_dev': 60000
            },
            'IRA': {
                'median': 150000,
                'minimum': 5000,
                'std_dev': 75000
            },
            '401k': {
                'median': 80000,
                'minimum': 2000,
                'std_dev': 40000
            },
            'Trust': {
                'median': 400000,
                'minimum': 100000,
                'std_dev': 250000
            },
            'Custody': {
                'median': 600000,
                'minimum': 250000,
                'std_dev': 400000
            }
        }

    def _generate_normal_with_minimum(self, median: float, std_dev: float, minimum: float, maximum: float = 20000000) -> float:
        """Generate normally distributed value with enforced minimum and maximum."""
        mean = median

        attempts = 0
        while attempts < 100:
            value = np.random.normal(mean, std_dev)
            if minimum <= value <= maximum:
                return value
            attempts += 1

        return max(minimum, min(maximum, mean))

    def _get_eligible_accounts(self) -> List[Dict[str, Any]]:
        """Get accounts that were opened at snapshot_start_date."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT account_key, account_id, account_type, opened_date
                FROM account
                WHERE opened_date <= ?
                AND to_date = '9999-12-31'
                ORDER BY account_key
            """, (self.snapshot_start_date.isoformat(),))

            accounts = []
            for row in cursor.fetchall():
                accounts.append({
                    'account_key': row[0],
                    'account_id': row[1],
                    'account_type': row[2],
                    'opened_date': row[3]
                })

            if not accounts:
                raise Exception("No eligible accounts found. Please run f_create_account.py first.")

            print(f"Found {len(accounts)} accounts eligible for initial assets (opened by {self.snapshot_start_date})")
            return accounts

        except Exception as e:
            print(f"Error fetching eligible accounts: {e}")
            raise
        finally:
            conn.close()

    def generate_initial_assets_data(self) -> List[Dict[str, Any]]:
        """Generate initial assets data according to schema specifications."""
        accounts = self._get_eligible_accounts()

        print(f"Generating initial assets for {len(accounts)} accounts...")

        initial_assets_data = []

        accounts_by_type = {}
        for account in accounts:
            account_type = account['account_type']
            if account_type not in accounts_by_type:
                accounts_by_type[account_type] = []
            accounts_by_type[account_type].append(account)

        print("\nAccount type distribution:")
        for account_type, type_accounts in accounts_by_type.items():
            print(f"  {account_type}: {len(type_accounts)} accounts")

        processed = 0
        for account in accounts:
            if processed % 5000 == 0 and processed > 0:
                print(f"Generated initial assets for {processed} accounts...")

            account_type = account['account_type']
            distribution = self.asset_distributions.get(account_type)

            if not distribution:
                print(f"Warning: No asset distribution defined for account type '{account_type}'. Using Taxable defaults.")
                distribution = self.asset_distributions['Taxable']

            initial_assets = self._generate_normal_with_minimum(
                median=distribution['median'],
                std_dev=distribution['std_dev'],
                minimum=distribution['minimum']
            )

            initial_asset_record = {
                'account_key': account['account_key'],
                'account_initial_assets': round(initial_assets, 2)
            }

            initial_assets_data.append(initial_asset_record)
            processed += 1

        print(f"Generated {len(initial_assets_data)} initial asset records")
        return initial_assets_data

    def generate_fact_account_initial_assets_data(self) -> List[Dict[str, Any]]:
        """Alias for generate_initial_assets_data() for compatibility with notebook."""
        return self.generate_initial_assets_data()

    def create_table_if_not_exists(self):
        """Create fact_account_initial_assets table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fact_account_initial_assets (
            account_key INTEGER PRIMARY KEY,
            account_initial_assets REAL NOT NULL,
            FOREIGN KEY (account_key) REFERENCES account(account_key),
            CONSTRAINT check_initial_assets_positive CHECK (account_initial_assets > 0),
            CONSTRAINT check_initial_assets_max CHECK (account_initial_assets <= 20000000)
        );
        CREATE INDEX IF NOT EXISTS ix_fact_initial_assets_amount
        ON fact_account_initial_assets(account_initial_assets);
        """

        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(create_table_sql)
            print("Fact account initial assets table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating fact account initial assets table: {e}")
            raise
        finally:
            conn.close()

    def clear_existing_data(self):
        """Clear existing fact account initial assets data."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM fact_account_initial_assets")
            conn.commit()
            print("Existing fact account initial assets data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
        finally:
            conn.close()

    def insert_initial_assets_data(self, initial_assets: List[Dict[str, Any]], batch_size: int = 1000):
        """Insert initial assets data into SQLite database."""
        print(f"Inserting {len(initial_assets)} initial asset records...")

        insert_sql = """
        INSERT INTO fact_account_initial_assets (account_key, account_initial_assets)
        VALUES (:account_key, :account_initial_assets)
        """

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            for i in range(0, len(initial_assets), batch_size):
                batch = initial_assets[i:i + batch_size]
                cursor.executemany(insert_sql, batch)

                if (i // batch_size + 1) % 10 == 0:
                    print(f"Inserted {min(i + batch_size, len(initial_assets))} records...")

            conn.commit()
            print(f"Successfully inserted all {len(initial_assets)} initial asset records")
        except Exception as e:
            print(f"Error inserting initial assets data: {e}")
            raise
        finally:
            conn.close()

    def insert_fact_account_initial_assets_data(self, initial_assets: List[Dict[str, Any]], batch_size: int = 1000):
        """Alias for insert_initial_assets_data() for compatibility with notebook."""
        return self.insert_initial_assets_data(initial_assets, batch_size)

    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total initial asset records': "SELECT COUNT(*) FROM fact_account_initial_assets",
            'Total assets': """
                SELECT
                    SUM(account_initial_assets) as total_assets,
                    COUNT(*) as total_accounts,
                    AVG(account_initial_assets) as average_account_size
                FROM fact_account_initial_assets
            """
        }

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            print("\n" + "="*90)
            print("FACT ACCOUNT INITIAL ASSETS DATA VALIDATION RESULTS")
            print("="*90)

            for description, query in validation_queries.items():
                cursor.execute(query)
                results = cursor.fetchall()

                print(f"\n{description}:")

                if len(results) == 1 and len(results[0]) == 1:
                    print(f"  {results[0][0]}")
                elif 'Total assets' in description and results:
                    total_assets, total_accounts, average_size = results[0]
                    print(f"  Total Assets: ${total_assets:,.0f}")
                    print(f"  Total Accounts: {total_accounts:,}")
                    print(f"  Average Account Size: ${average_size:,.0f}")
                else:
                    for row in results:
                        print(f"  {row}")

            print("\n" + "="*90)
        except Exception as e:
            print(f"Error during validation: {e}")
        finally:
            conn.close()

def main():
    """Main function to generate and insert fact account initial assets data."""
    db_config = {
        'db_path': os.getenv('DB_PATH', './demo.db')
    }

    try:
        generator = FactAccountInitialAssetsGenerator(**db_config)

        generator.create_table_if_not_exists()

        response = input("Do you want to clear existing fact account initial assets data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()

        initial_assets = generator.generate_initial_assets_data()
        generator.insert_initial_assets_data(initial_assets)
        generator.validate_data()

        print(f"\nFact account initial assets data generation completed successfully!")
        print(f"Generated {len(initial_assets)} initial asset records")
        print(f"Data is ready for fact_account_monthly table generation")

    except Exception as e:
        print(f"Error: {e}")
        print("Please check your SQLite database path configuration.")
        print("Ensure you have run f_create_account.py first to create account data.")

# Alias for compatibility with notebook
FactAccountInitialAssetsDataGenerator = FactAccountInitialAssetsGenerator

if __name__ == "__main__":
    main()
