import sqlite3
import random
import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Tuple
import os
import numpy as np

sqlite3.register_adapter(date, lambda val: val.isoformat())
sqlite3.register_adapter(datetime, lambda val: val.isoformat())

class AccountDataGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the account data generator with SQLite database connection.

        Args:
            connection_string: Path to SQLite database file
            **db_params: Individual connection parameters (db_path)
        """
        self.db_path = connection_string or db_params.get('db_path', './demo.db')

        # Constants from schema
        self.current_date = date(2025, 9, 30)
        self.target_account_count = 800  # 50 advisors * 16 median = 800 accounts

        self.business_line_dist = {
            'Managed Portfolio': 0.45,
            'Separately Managed Account': 0.18,
            'Mutual Fund Wrap': 0.25,
            'Annuity': 0.06,
            'Cash': 0.06
        }

        self.account_type_dist = {
            'Taxable': 0.55,
            'IRA': 0.22,
            '401k': 0.10,
            'Trust': 0.10,
            'Custody': 0.03
        }

        self.custodian_dist = {
            'Schwab': 0.45,
            'Fidelity': 0.30,
            'Pershing': 0.15,
            'BankTrust': 0.06,
            'In-House': 0.04
        }

        self.status_dist = {
            'Open': 0.88,
            'Closed': 0.12
        }

        self.risk_profiles = ['Conservative', 'Moderate', 'Aggressive']

        self.min_accounts_per_advisor = 2
        self.median_accounts_per_advisor = 16
        self.max_accounts_per_advisor = 40

    def _weighted_choice(self, choices: Dict[str, float]) -> str:
        """Select a random choice based on weighted distribution."""
        choices_list = list(choices.keys())
        weights = list(choices.values())
        return np.random.choice(choices_list, p=weights)

    def _get_reference_data(self) -> Tuple[List[Dict], List[Dict], Dict[str, int]]:
        """Get advisor, household, and business line reference data from database."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT advisor_key, advisor_id
                FROM advisors
                WHERE to_date = '9999-12-31' AND advisor_status = 'Active'
            """)
            advisors = [{'advisor_key': row[0], 'advisor_id': row[1]} for row in cursor.fetchall()]

            cursor.execute("""
                SELECT household_key, household_id, household_advisor_id, household_registration_date
                FROM household
                WHERE to_date = '9999-12-31' AND household_status = 'Active'
            """)
            households = [{'household_key': row[0], 'household_id': row[1],
                          'household_advisor_id': row[2], 'household_registration_date': row[3]}
                         for row in cursor.fetchall()]

            cursor.execute("SELECT business_line_key, business_line_name FROM business_line")
            business_lines = {row[1]: row[0] for row in cursor.fetchall()}

            if not advisors:
                raise Exception("No active advisors found. Please run c_create_advisors.py first.")
            if not households:
                raise Exception("No active households found. Please run d_create_household.py first.")
            if not business_lines:
                raise Exception("No business lines found. Please run e_create_business_line.py first.")

            print(f"Found {len(advisors)} active advisors, {len(households)} active households, {len(business_lines)} business lines")
            return advisors, households, business_lines

        except Exception as e:
            print(f"Error fetching reference data: {e}")
            raise
        finally:
            conn.close()

    def _generate_accounts_per_advisor(self, total_accounts: int, num_advisors: int) -> Dict[int, int]:
        """Generate account distribution per advisor following schema constraints."""
        accounts_per_advisor = {}
        remaining_accounts = total_accounts

        advisor_keys = list(range(num_advisors))

        for i, advisor_idx in enumerate(advisor_keys):
            if i == len(advisor_keys) - 1:
                accounts_per_advisor[advisor_idx] = remaining_accounts
            else:
                if remaining_accounts <= self.min_accounts_per_advisor:
                    accounts = remaining_accounts
                else:
                    mean_accounts = min(self.median_accounts_per_advisor,
                                      remaining_accounts // (len(advisor_keys) - i))

                    variance = min(8, mean_accounts // 3)
                    accounts = max(self.min_accounts_per_advisor,
                                 min(self.max_accounts_per_advisor,
                                     int(np.random.normal(mean_accounts, variance))))

                    accounts = min(accounts, remaining_accounts - (len(advisor_keys) - i - 1) * self.min_accounts_per_advisor)

                accounts_per_advisor[advisor_idx] = accounts
                remaining_accounts -= accounts

        return accounts_per_advisor

    def _generate_opened_date(self, household_registration_date) -> date:
        """Generate account opened date between household registration and current date."""
        if isinstance(household_registration_date, str):
            household_registration_date = date.fromisoformat(household_registration_date)
        if household_registration_date >= self.current_date:
            return household_registration_date

        days_diff = (self.current_date - household_registration_date).days
        random_days = random.randint(0, days_diff)
        return household_registration_date + timedelta(days=random_days)

    def _generate_closed_date(self, opened_date: date) -> date:
        """Generate closed date after opened date."""
        days_since_open = (self.current_date - opened_date).days
        if days_since_open <= 0:
            return opened_date + timedelta(days=random.randint(1, 30))

        days_to_add = random.randint(1, min(days_since_open, 1095))
        return opened_date + timedelta(days=days_to_add)

    def generate_account_data(self) -> List[Dict[str, Any]]:
        """Generate account data according to schema specifications."""
        print(f"Generating {self.target_account_count} accounts...")

        advisors, households, business_lines = self._get_reference_data()

        accounts_per_advisor = self._generate_accounts_per_advisor(self.target_account_count, len(advisors))

        households_by_advisor = {}
        for household in households:
            advisor_id = household['household_advisor_id']
            if advisor_id not in households_by_advisor:
                households_by_advisor[advisor_id] = []
            households_by_advisor[advisor_id].append(household)

        accounts = []
        account_key = 1
        account_id = 1

        for advisor_idx, advisor in enumerate(advisors):
            num_accounts = accounts_per_advisor[advisor_idx]
            advisor_id = advisor['advisor_id']
            advisor_key = advisor['advisor_key']

            advisor_households = households_by_advisor.get(advisor_id, [])
            if not advisor_households:
                print(f"Warning: No households found for advisor {advisor_id}, skipping...")
                continue

            print(f"Generating {num_accounts} accounts for advisor {advisor_id}...")

            for _ in range(num_accounts):
                household = random.choice(advisor_households)

                business_line_name = self._weighted_choice(self.business_line_dist)
                business_line_key = business_lines[business_line_name]
                account_type = self._weighted_choice(self.account_type_dist)
                custodian = self._weighted_choice(self.custodian_dist)
                status = self._weighted_choice(self.status_dist)
                risk_profile = random.choice(self.risk_profiles)

                opened_date = self._generate_opened_date(household['household_registration_date'])
                closed_date = self._generate_closed_date(opened_date) if status == 'Closed' else None

                account = {
                    'account_key': account_key,
                    'account_id': account_id,
                    'advisor_key': advisor_key,
                    'household_key': household['household_key'],
                    'business_line_key': business_line_key,
                    'account_type': account_type,
                    'account_custodian': custodian,
                    'opened_date': opened_date,
                    'account_status': status,
                    'closed_date': closed_date,
                    'account_risk_profile': risk_profile,
                    'from_date': opened_date,
                    'to_date': closed_date if status == 'Closed' else date(9999, 12, 31)
                }

                accounts.append(account)
                account_key += 1
                account_id += 1

        print(f"Generated {len(accounts)} account records")
        return accounts

    def create_table_if_not_exists(self):
        """Create account table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS account (
            account_key INTEGER PRIMARY KEY,
            account_id INTEGER NOT NULL,
            advisor_key INTEGER NOT NULL,
            household_key INTEGER NOT NULL,
            business_line_key INTEGER NOT NULL,
            account_type TEXT NOT NULL,
            account_custodian TEXT NOT NULL,
            opened_date TEXT NOT NULL,
            account_status TEXT NOT NULL,
            closed_date TEXT,
            account_risk_profile TEXT NOT NULL,
            from_date TEXT NOT NULL,
            to_date TEXT NOT NULL,
            FOREIGN KEY (advisor_key) REFERENCES advisors(advisor_key),
            FOREIGN KEY (household_key) REFERENCES household(household_key),
            FOREIGN KEY (business_line_key) REFERENCES business_line(business_line_key)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ux_accounts_current
        ON account(account_id)
        WHERE to_date = '9999-12-31';
        CREATE INDEX IF NOT EXISTS ix_account_id_window
        ON account(account_id, from_date, to_date);
        CREATE INDEX IF NOT EXISTS ix_accounts_household
        ON account(household_key);
        CREATE INDEX IF NOT EXISTS ix_accounts_advisor
        ON account(advisor_key);
        """

        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(create_table_sql)
            print("Account table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating account table: {e}")
            raise
        finally:
            conn.close()

    def clear_existing_data(self):
        """Clear existing account data."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM account")
            conn.commit()
            print("Existing account data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
        finally:
            conn.close()

    def insert_account_data(self, accounts: List[Dict[str, Any]], batch_size: int = 1000):
        """Insert account data into SQLite database."""
        print(f"Inserting {len(accounts)} account records...")

        insert_sql = """
        INSERT INTO account (
            account_key, account_id, advisor_key, household_key, business_line_key,
            account_type, account_custodian, opened_date, account_status, closed_date,
            account_risk_profile, from_date, to_date
        ) VALUES (
            :account_key, :account_id, :advisor_key, :household_key, :business_line_key,
            :account_type, :account_custodian, :opened_date, :account_status, :closed_date,
            :account_risk_profile, :from_date, :to_date
        )
        """

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            for i in range(0, len(accounts), batch_size):
                batch = accounts[i:i + batch_size]
                cursor.executemany(insert_sql, batch)

                if (i // batch_size + 1) % 10 == 0:
                    print(f"Inserted {min(i + batch_size, len(accounts))} records...")

            conn.commit()
            print(f"Successfully inserted all {len(accounts)} account records")
        except Exception as e:
            print(f"Error inserting account data: {e}")
            raise
        finally:
            conn.close()

    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total account_id count': "SELECT COUNT(DISTINCT account_id) FROM account",
            'Current records count': "SELECT COUNT(*) FROM account WHERE to_date = '9999-12-31'",
            'Account type distribution': """
                SELECT account_type, COUNT(*) as count
                FROM account
                WHERE to_date = '9999-12-31'
                GROUP BY account_type
                ORDER BY count DESC
            """,
            'Status distribution': """
                SELECT account_status, COUNT(*) as count
                FROM account
                WHERE to_date = '9999-12-31'
                GROUP BY account_status
            """
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
                        if len(row) == 2:
                            print(f"  {row[0]}: {row[1]}")
                        else:
                            print(f"  {row}")

            print("\n" + "="*60)
        except Exception as e:
            print(f"Error during validation: {e}")
        finally:
            conn.close()

def main():
    """Main function to generate and insert account data."""
    db_config = {
        'db_path': os.getenv('DB_PATH', './demo.db')
    }

    try:
        generator = AccountDataGenerator(**db_config)

        generator.create_table_if_not_exists()

        response = input("Do you want to clear existing account data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()

        accounts = generator.generate_account_data()
        generator.insert_account_data(accounts)
        generator.validate_data()

        print(f"\nAccount data generation completed successfully!")
        print(f"Generated {len(accounts)} total account records")

    except Exception as e:
        print(f"Error: {e}")
        print("Please check your SQLite database path configuration.")
        print("Ensure you have run c_create_advisors.py, d_create_household.py, and e_create_business_line.py first.")

if __name__ == "__main__":
    main()
