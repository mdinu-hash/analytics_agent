import sqlite3
import random
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any, Tuple
import os
import numpy as np

sqlite3.register_adapter(date, lambda val: val.isoformat())
sqlite3.register_adapter(datetime, lambda val: val.isoformat())

class FactAccountMonthlyGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the fact account monthly generator with SQLite database connection.

        Args:
            connection_string: Path to SQLite database file
            **db_params: Individual connection parameters (db_path)
        """
        self.db_path = connection_string or db_params.get('db_path', './demo.db')

        # Constants from schema
        self.snapshot_start_date = date(2024, 9, 30)
        self.current_date = date(2025, 9, 30)

        # Generate all EOM dates between start and current
        self.snapshot_dates = self._generate_snapshot_dates()

        # Monthly return parameters by risk profile from schema
        self.risk_profile_returns = {
            'Conservative': {'median': 0.003, 'std_dev': 0.01},    # 0.30% ± 1.0%
            'Moderate': {'median': 0.0055, 'std_dev': 0.02},       # 0.55% ± 2.0%
            'Aggressive': {'median': 0.008, 'std_dev': 0.035}      # 0.80% ± 3.5%
        }

        # Net flow parameters by account type from schema
        self.net_flow_params = {
            '401k': {'median': 0.007, 'std_dev': 0.003},      # 0.7% ± 0.3%
            'IRA': {'median': 0.002, 'std_dev': 0.004},       # 0.2% ± 0.4%
            'Taxable': {'median': 0.0005, 'std_dev': 0.008},  # 0.05% ± 0.8%
            'Trust': {'median': -0.0025, 'std_dev': 0.0035},  # -0.25% ± 0.35%
            'Custody': {'median': 0.0, 'std_dev': 0.06}       # 0% ± 6%
        }

    def _generate_snapshot_dates(self) -> List[date]:
        """Generate all end-of-month dates between snapshot_start_date and current_date."""
        dates = []
        current = self.snapshot_start_date

        while current <= self.current_date:
            dates.append(current)
            # Move to next month end
            current = current + relativedelta(months=1)
            # Ensure it's end of month
            current = current.replace(day=1) + relativedelta(months=1) - relativedelta(days=1)
            # But keep original day if it was already end of month
            if current.month == 9 and current.day > 30:
                current = current.replace(day=30)

        return dates

    def _calculate_monthly_return(self, risk_profile: str, is_first_month: bool) -> float:
        """Calculate monthly return based on risk profile and schema rules."""
        if is_first_month:
            return 0.0  # First month return is 0%

        # Get base return parameters
        return_params = self.risk_profile_returns.get(risk_profile, self.risk_profile_returns['Moderate'])

        # Generate base return
        base_return = np.random.normal(return_params['median'], return_params['std_dev'])

        # Generate noise (0.25% ± 0.2%, limited to ±0.5%)
        noise = np.random.normal(0.0025, 0.002)
        noise = max(-0.005, min(0.005, noise))  # Limit to ±0.5%

        # Combine base return and noise
        total_return = base_return + noise

        # Limit total return to ±12%
        total_return = max(-0.12, min(0.12, total_return))

        return total_return

    def _calculate_net_flow(self, account_type: str, previous_assets: float) -> float:
        """Calculate net flow based on account type and previous month assets."""
        flow_params = self.net_flow_params.get(account_type, self.net_flow_params['Taxable'])

        # Generate flow percentage
        flow_percentage = np.random.normal(flow_params['median'], flow_params['std_dev'])

        # Calculate flow amount
        net_flow = previous_assets * flow_percentage

        # Limit net flow to 30% of previous assets (schema constraint)
        max_flow = previous_assets * 0.30
        net_flow = max(-max_flow, min(max_flow, net_flow))

        return net_flow

    def _get_accounts_with_initial_assets(self) -> List[Dict[str, Any]]:
        """Get accounts with their initial assets and metadata."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    a.account_key,
                    a.account_id,
                    a.account_type,
                    a.account_risk_profile,
                    a.advisor_key,
                    a.household_key,
                    a.business_line_key,
                    a.opened_date,
                    a.account_status,
                    a.closed_date,
                    faia.account_initial_assets
                FROM account a
                JOIN fact_account_initial_assets faia ON a.account_key = faia.account_key
                WHERE a.to_date = '9999-12-31'
                ORDER BY a.account_key
            """)

            accounts = []
            for row in cursor.fetchall():
                opened_date = date.fromisoformat(row[7]) if row[7] else None
                closed_date = date.fromisoformat(row[9]) if row[9] else None
                accounts.append({
                    'account_key': row[0],
                    'account_id': row[1],
                    'account_type': row[2],
                    'account_risk_profile': row[3],
                    'advisor_key': row[4],
                    'household_key': row[5],
                    'business_line_key': row[6],
                    'opened_date': opened_date,
                    'account_status': row[8],
                    'closed_date': closed_date,
                    'account_initial_assets': float(row[10])
                })

            if not accounts:
                raise Exception("No accounts with initial assets found. Please run create_fact_account_initial_assets.py first.")

            print(f"Found {len(accounts)} accounts with initial assets")
            return accounts

        except Exception as e:
            print(f"Error fetching accounts with initial assets: {e}")
            raise
        finally:
            conn.close()

    def _is_account_active_on_date(self, account: Dict[str, Any], snapshot_date: date) -> bool:
        """Check if account is active on the given snapshot date."""
        opened_date = account['opened_date']
        closed_date = account['closed_date']

        # Account must be opened by snapshot date
        if opened_date > snapshot_date:
            return False

        # If account is closed, it must be closed after snapshot date
        if closed_date and closed_date <= snapshot_date:
            return False

        return True

    def generate_monthly_data(self) -> List[Dict[str, Any]]:
        """Generate fact account monthly data according to schema specifications."""
        accounts = self._get_accounts_with_initial_assets()

        print(f"Generating monthly data for {len(self.snapshot_dates)} months...")
        print(f"Date range: {self.snapshot_dates[0]} to {self.snapshot_dates[-1]}")

        monthly_data = []

        # Track previous month assets for each account
        previous_assets_by_account = {}

        for i, snapshot_date in enumerate(self.snapshot_dates):
            is_first_month = (i == 0)
            print(f"Processing {snapshot_date} (month {i+1}/{len(self.snapshot_dates)})...")

            month_records = 0

            for account in accounts:
                # Skip if account is not active on this date
                if not self._is_account_active_on_date(account, snapshot_date):
                    continue

                account_key = account['account_key']

                # Get previous month assets
                if is_first_month:
                    previous_assets = account['account_initial_assets']
                else:
                    previous_assets = previous_assets_by_account.get(account_key, account['account_initial_assets'])

                # Calculate monthly return
                monthly_return = self._calculate_monthly_return(
                    account['account_risk_profile'],
                    is_first_month
                )

                # Calculate net flow
                net_flow = self._calculate_net_flow(account['account_type'], previous_assets)

                # Calculate current month assets
                current_assets = previous_assets * (1 + monthly_return) + net_flow

                # Ensure assets remain positive and within constraints
                current_assets = max(0, min(20000000, current_assets))

                # Create monthly record
                monthly_record = {
                    'snapshot_date': snapshot_date,
                    'account_key': account_key,
                    'account_monthly_return': monthly_return,
                    'account_net_flow': net_flow,
                    'account_assets_previous_month': previous_assets,
                    'account_assets': current_assets,
                    'advisor_key': account['advisor_key'],
                    'household_key': account['household_key'],
                    'business_line_key': account['business_line_key']
                }

                monthly_data.append(monthly_record)

                # Store current assets for next month
                previous_assets_by_account[account_key] = current_assets
                month_records += 1

            print(f"  Generated {month_records} records for {snapshot_date}")

        print(f"Generated {len(monthly_data)} total monthly records")
        return monthly_data

    def generate_fact_account_monthly_data(self) -> List[Dict[str, Any]]:
        """Alias for generate_monthly_data() for compatibility with notebook."""
        return self.generate_monthly_data()

    def create_table_if_not_exists(self):
        """Create fact_account_monthly table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fact_account_monthly (
            snapshot_date TEXT NOT NULL,
            account_key INTEGER NOT NULL,
            account_monthly_return REAL NOT NULL,
            account_net_flow REAL NOT NULL,
            account_assets_previous_month REAL NOT NULL,
            account_assets REAL NOT NULL,
            advisor_key INTEGER NOT NULL,
            household_key INTEGER NOT NULL,
            business_line_key INTEGER NOT NULL,
            PRIMARY KEY (snapshot_date, account_key),
            FOREIGN KEY (account_key) REFERENCES account(account_key),
            FOREIGN KEY (advisor_key) REFERENCES advisors(advisor_key),
            FOREIGN KEY (household_key) REFERENCES household(household_key),
            FOREIGN KEY (business_line_key) REFERENCES business_line(business_line_key),
            CONSTRAINT check_monthly_return_range CHECK (account_monthly_return >= -0.12 AND account_monthly_return <= 0.12),
            CONSTRAINT check_assets_positive CHECK (account_assets >= 0),
            CONSTRAINT check_assets_max CHECK (account_assets <= 20000000),
            CONSTRAINT check_previous_assets_positive CHECK (account_assets_previous_month >= 0)
        );

        CREATE INDEX IF NOT EXISTS ix_fact_monthly_snapshot
        ON fact_account_monthly(snapshot_date);

        CREATE INDEX IF NOT EXISTS ix_fact_monthly_account
        ON fact_account_monthly(account_key);

        CREATE INDEX IF NOT EXISTS ix_fact_monthly_advisor
        ON fact_account_monthly(advisor_key);

        CREATE INDEX IF NOT EXISTS ix_fact_monthly_household
        ON fact_account_monthly(household_key);

        CREATE INDEX IF NOT EXISTS ix_fact_monthly_business_line
        ON fact_account_monthly(business_line_key);
        """

        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(create_table_sql)
            print("Fact account monthly table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating fact account monthly table: {e}")
            raise
        finally:
            conn.close()

    def clear_existing_data(self):
        """Clear existing fact account monthly data."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM fact_account_monthly")
            conn.commit()
            print("Existing fact account monthly data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
        finally:
            conn.close()

    def insert_monthly_data(self, monthly_data: List[Dict[str, Any]], batch_size: int = 1000):
        """Insert monthly data into SQLite database."""
        print(f"Inserting {len(monthly_data)} monthly records...")

        insert_sql = """
        INSERT INTO fact_account_monthly (
            snapshot_date, account_key, account_monthly_return, account_net_flow,
            account_assets_previous_month, account_assets, advisor_key, household_key, business_line_key
        ) VALUES (
            :snapshot_date, :account_key, :account_monthly_return, :account_net_flow,
            :account_assets_previous_month, :account_assets, :advisor_key, :household_key, :business_line_key
        )
        """

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            for i in range(0, len(monthly_data), batch_size):
                batch = monthly_data[i:i + batch_size]
                cursor.executemany(insert_sql, batch)

                if (i // batch_size + 1) % 10 == 0:
                    print(f"Inserted {min(i + batch_size, len(monthly_data))} records...")

            conn.commit()
            print(f"Successfully inserted all {len(monthly_data)} monthly records")
        except Exception as e:
            print(f"Error inserting monthly data: {e}")
            raise
        finally:
            conn.close()

    def insert_fact_account_monthly_data(self, monthly_data: List[Dict[str, Any]], batch_size: int = 1000):
        """Alias for insert_monthly_data() for compatibility with notebook."""
        return self.insert_monthly_data(monthly_data, batch_size)

    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total monthly records': "SELECT COUNT(*) FROM fact_account_monthly",
            'Distinct snapshot dates': "SELECT COUNT(DISTINCT snapshot_date) FROM fact_account_monthly",
            'Date range': """
                SELECT MIN(snapshot_date) as min_date, MAX(snapshot_date) as max_date
                FROM fact_account_monthly
            """,
            'Records per month': """
                SELECT snapshot_date, COUNT(*) as record_count
                FROM fact_account_monthly
                GROUP BY snapshot_date
                ORDER BY snapshot_date
            """,
            'Asset calculation validation': """
                SELECT COUNT(*) as calculation_errors
                FROM fact_account_monthly
                WHERE ABS(account_assets - (account_assets_previous_month * (1 + account_monthly_return) + account_net_flow)) > 0.01
            """,
            'Constraint violations': """
                SELECT
                    'Return out of range' as violation_type,
                    COUNT(*) as count
                FROM fact_account_monthly
                WHERE account_monthly_return < -0.12 OR account_monthly_return > 0.12
                UNION ALL
                SELECT
                    'Assets negative' as violation_type,
                    COUNT(*) as count
                FROM fact_account_monthly
                WHERE account_assets < 0
                UNION ALL
                SELECT
                    'Assets too high' as violation_type,
                    COUNT(*) as count
                FROM fact_account_monthly
                WHERE account_assets > 20000000
                UNION ALL
                SELECT
                    'First month non-zero return' as violation_type,
                    COUNT(*) as count
                FROM fact_account_monthly
                WHERE snapshot_date = '2024-09-30' AND account_monthly_return != 0
            """
        }

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            print("\n" + "="*100)
            print("FACT ACCOUNT MONTHLY DATA VALIDATION RESULTS")
            print("="*100)

            for description, query in validation_queries.items():
                cursor.execute(query)
                results = cursor.fetchall()

                print(f"\n{description}:")

                if len(results) == 1 and len(results[0]) <= 2:
                    if len(results[0]) == 1:
                        print(f"  {results[0][0]}")
                    else:
                        print(f"  {results[0][0]} to {results[0][1]}")
                elif 'Records per month' in description:
                    for row in results:
                        print(f"  {row[0]}: {row[1]:,} records")
                elif 'Constraint violations' in description:
                    violations_found = False
                    for row in results:
                        violation_type, count = row
                        if count > 0:
                            print(f"  {violation_type}: {count} violations")
                            violations_found = True
                    if not violations_found:
                        print("  No constraint violations found")
                else:
                    for row in results:
                        print(f"  {row}")

            print("\n" + "="*100)
        except Exception as e:
            print(f"Error during validation: {e}")
        finally:
            conn.close()

def main():
    """Main function to generate and insert fact account monthly data."""
    db_config = {
        'db_path': os.getenv('DB_PATH', './demo.db')
    }

    try:
        generator = FactAccountMonthlyGenerator(**db_config)

        generator.create_table_if_not_exists()

        response = input("Do you want to clear existing fact account monthly data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()

        monthly_data = generator.generate_monthly_data()
        generator.insert_monthly_data(monthly_data)
        generator.validate_data()

        print(f"\nFact account monthly data generation completed successfully!")
        print(f"Generated {len(monthly_data)} monthly records across {len(generator.snapshot_dates)} months")
        print(f"Data is ready for revenue calculations and household aggregations")

    except Exception as e:
        print(f"Error: {e}")
        print("Please check your SQLite database path configuration.")
        print("Ensure you have run create_account.py and create_fact_account_initial_assets.py first.")

# Alias for compatibility with notebook
FactAccountMonthlyDataGenerator = FactAccountMonthlyGenerator

if __name__ == "__main__":
    main()
