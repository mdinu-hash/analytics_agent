import sqlite3
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any, Tuple
import os
import numpy as np

sqlite3.register_adapter(date, lambda val: val.isoformat())
sqlite3.register_adapter(datetime, lambda val: val.isoformat())

class FactRevenueMonthlyGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the fact revenue monthly generator with SQLite database connection.

        Args:
            connection_string: Path to SQLite database file
            **db_params: Individual connection parameters (db_path)
        """
        self.db_path = connection_string or db_params.get('db_path', './demo.db')

        # Third party fee parameters from schema
        self.third_party_fee_params = {
            'median': 0.10,     # 10% of gross fee amount
            'std_dev': 0.05     # 5% standard deviation
        }

    def _calculate_third_party_fee_percentage(self) -> float:
        """Calculate third party fee percentage using normal distribution."""
        # Generate percentage normally with median 10% and std dev 5%
        percentage = np.random.normal(
            self.third_party_fee_params['median'],
            self.third_party_fee_params['std_dev']
        )

        # Ensure percentage is within reasonable bounds (0% to 25%)
        percentage = max(0.0, min(0.25, percentage))

        return percentage

    def _get_account_monthly_data(self) -> List[Dict[str, Any]]:
        """Get all data from fact_account_monthly to generate revenue records."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    fam.snapshot_date,
                    fam.account_key,
                    fam.advisor_key,
                    fam.household_key,
                    fam.business_line_key,
                    fam.account_assets
                FROM fact_account_monthly fam
                ORDER BY fam.snapshot_date, fam.account_key
            """)

            account_data = []
            for row in cursor.fetchall():
                account_data.append({
                    'snapshot_date': row[0],
                    'account_key': row[1],
                    'advisor_key': row[2],
                    'household_key': row[3],
                    'business_line_key': row[4],
                    'account_assets': float(row[5])
                })

            if not account_data:
                raise Exception("No account monthly data found. Please run create_fact_account_monthly.py first.")

            print(f"Found {len(account_data)} account monthly records for revenue calculation")
            return account_data

        except Exception as e:
            print(f"Error fetching account monthly data: {e}")
            raise
        finally:
            conn.close()

    def _get_tier_fees(self) -> Dict[int, List[Dict[str, Any]]]:
        """Get tier fee structures by business line."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    business_line_key,
                    tier_min_aum,
                    tier_max_aum,
                    tier_fee_bps
                FROM tier_fee
                ORDER BY business_line_key, tier_min_aum
            """)

            tier_fees = {}
            for row in cursor.fetchall():
                business_line_key = row[0]
                tier_data = {
                    'tier_min_aum': float(row[1]),
                    'tier_max_aum': float(row[2]) if row[2] is not None else float('inf'),
                    'tier_fee': float(row[3])
                }

                if business_line_key not in tier_fees:
                    tier_fees[business_line_key] = []
                tier_fees[business_line_key].append(tier_data)

            if not tier_fees:
                raise Exception("No tier fee data found. Please run create_tier_fee.py first.")

            print(f"Loaded tier fees for {len(tier_fees)} business lines")
            return tier_fees

        except Exception as e:
            print(f"Error fetching tier fees: {e}")
            raise
        finally:
            conn.close()

    def _get_advisor_payout_rates(self) -> Dict[str, float]:
        """Get advisor payout rates by firm affiliation model."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    firm_affiliation_model,
                    advisor_payout_rate
                FROM advisor_payout_rate
            """)

            payout_rates = {}
            for row in cursor.fetchall():
                payout_rates[row[0]] = float(row[1])

            if not payout_rates:
                raise Exception("No advisor payout rates found. Please run create_advisor_payout_rate.py first.")

            print(f"Loaded payout rates for {len(payout_rates)} firm affiliation models")
            return payout_rates

        except Exception as e:
            print(f"Error fetching advisor payout rates: {e}")
            raise
        finally:
            conn.close()

    def _get_advisor_firm_models(self) -> Dict[int, str]:
        """Get firm affiliation model for each advisor."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    advisor_key,
                    firm_affiliation_model
                FROM advisors
                WHERE to_date = '9999-12-31'
            """)

            advisor_firm_models = {}
            for row in cursor.fetchall():
                advisor_firm_models[row[0]] = row[1]

            if not advisor_firm_models:
                raise Exception("No active advisors found. Please run create_advisors.py first.")

            print(f"Loaded firm affiliation models for {len(advisor_firm_models)} advisors")
            return advisor_firm_models

        except Exception as e:
            print(f"Error fetching advisor firm models: {e}")
            raise
        finally:
            conn.close()

    def _calculate_fee_percentage(self, account_assets: float, business_line_key: int, tier_fees: Dict[int, List[Dict[str, Any]]]) -> float:
        """Calculate fee percentage based on account assets and business line tier structure."""
        if business_line_key not in tier_fees:
            print(f"Warning: No tier fees found for business_line_key {business_line_key}. Using 0.75% default.")
            return 0.0075  # 75 bps default

        # Find appropriate tier for this account's assets
        for tier in tier_fees[business_line_key]:
            if tier['tier_min_aum'] <= account_assets < tier['tier_max_aum']:
                return tier['tier_fee'] / 100  # Convert to decimal

        # If no tier found, use the highest tier (should be the last one)
        highest_tier = tier_fees[business_line_key][-1]
        return highest_tier['tier_fee'] / 100

    def generate_revenue_monthly_data(self) -> List[Dict[str, Any]]:
        """Generate revenue monthly data based on fact_account_monthly and fee structures."""
        print("Loading supporting data...")
        account_data = self._get_account_monthly_data()
        tier_fees = self._get_tier_fees()
        payout_rates = self._get_advisor_payout_rates()
        advisor_firm_models = self._get_advisor_firm_models()

        print(f"Generating revenue data for {len(account_data)} account monthly records...")

        revenue_data = []
        processed = 0

        for account_record in account_data:
            if processed % 10000 == 0 and processed > 0:
                print(f"Processed {processed} revenue records...")

            # Extract data from account record
            snapshot_date = account_record['snapshot_date']
            account_key = account_record['account_key']
            advisor_key = account_record['advisor_key']
            household_key = account_record['household_key']
            business_line_key = account_record['business_line_key']
            account_assets = account_record['account_assets']

            # Calculate fee percentage based on tier structure
            fee_percentage = self._calculate_fee_percentage(account_assets, business_line_key, tier_fees)

            # Calculate gross fee amount
            gross_fee_amount = account_assets * fee_percentage

            # Calculate third party fee percentage and amount
            third_party_fee_percentage = self._calculate_third_party_fee_percentage()
            third_party_fee = gross_fee_amount * third_party_fee_percentage

            # Get advisor's firm affiliation model and payout rate
            firm_model = advisor_firm_models.get(advisor_key)
            if firm_model and firm_model in payout_rates:
                advisor_payout_rate = payout_rates[firm_model]
            else:
                print(f"Warning: No payout rate found for advisor {advisor_key} with firm model {firm_model}. Using 50% default.")
                advisor_payout_rate = 0.50

            # Calculate advisor payout amount
            advisor_payout_amount = (gross_fee_amount - third_party_fee) * advisor_payout_rate

            # Calculate net revenue
            net_revenue = gross_fee_amount - third_party_fee - advisor_payout_amount

            # Create revenue record
            revenue_record = {
                'snapshot_date': snapshot_date,
                'account_key': account_key,
                'advisor_key': advisor_key,
                'household_key': household_key,
                'business_line_key': business_line_key,
                'account_assets': account_assets,
                'fee_percentage': fee_percentage,
                'gross_fee_amount': gross_fee_amount,
                'third_party_fee': third_party_fee,
                'advisor_payout_rate': advisor_payout_rate,
                'advisor_payout_amount': advisor_payout_amount,
                'net_revenue': net_revenue
            }

            revenue_data.append(revenue_record)
            processed += 1

        print(f"Generated {len(revenue_data)} revenue records")
        return revenue_data

    def generate_fact_revenue_monthly_data(self) -> List[Dict[str, Any]]:
        """Alias for generate_revenue_monthly_data() for compatibility with notebook."""
        return self.generate_revenue_monthly_data()

    def create_table_if_not_exists(self):
        """Create fact_revenue_monthly table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fact_revenue_monthly (
            snapshot_date TEXT NOT NULL,
            account_key INTEGER NOT NULL,
            advisor_key INTEGER NOT NULL,
            household_key INTEGER NOT NULL,
            business_line_key INTEGER NOT NULL,
            account_assets REAL NOT NULL,
            fee_percentage REAL NOT NULL,
            gross_fee_amount REAL NOT NULL,
            third_party_fee REAL NOT NULL,
            advisor_payout_rate REAL NOT NULL,
            advisor_payout_amount REAL NOT NULL,
            net_revenue REAL NOT NULL,
            PRIMARY KEY (snapshot_date, account_key),
            FOREIGN KEY (advisor_key) REFERENCES advisors(advisor_key),
            FOREIGN KEY (household_key) REFERENCES household(household_key),
            FOREIGN KEY (business_line_key) REFERENCES business_line(business_line_key),
            CONSTRAINT check_fee_percentage_positive CHECK (fee_percentage >= 0),
            CONSTRAINT check_gross_fee_positive CHECK (gross_fee_amount >= 0),
            CONSTRAINT check_third_party_fee_positive CHECK (third_party_fee >= 0),
            CONSTRAINT check_advisor_payout_rate_valid CHECK (advisor_payout_rate >= 0 AND advisor_payout_rate <= 1),
            CONSTRAINT check_advisor_payout_positive CHECK (advisor_payout_amount >= 0)
        );

        CREATE INDEX IF NOT EXISTS ix_fact_revenue_monthly_snapshot
        ON fact_revenue_monthly(snapshot_date);

        CREATE INDEX IF NOT EXISTS ix_fact_revenue_monthly_advisor
        ON fact_revenue_monthly(advisor_key);

        CREATE INDEX IF NOT EXISTS ix_fact_revenue_monthly_household
        ON fact_revenue_monthly(household_key);

        CREATE INDEX IF NOT EXISTS ix_fact_revenue_monthly_business_line
        ON fact_revenue_monthly(business_line_key);

        CREATE INDEX IF NOT EXISTS ix_fact_revenue_monthly_net_revenue
        ON fact_revenue_monthly(net_revenue);
        """

        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(create_table_sql)
            print("Fact revenue monthly table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating fact revenue monthly table: {e}")
            raise
        finally:
            conn.close()

    def clear_existing_data(self):
        """Clear existing fact revenue monthly data."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM fact_revenue_monthly")
            conn.commit()
            print("Existing fact revenue monthly data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
        finally:
            conn.close()

    def insert_revenue_monthly_data(self, revenue_data: List[Dict[str, Any]], batch_size: int = 1000):
        """Insert revenue monthly data into SQLite database."""
        print(f"Inserting {len(revenue_data)} revenue records...")

        insert_sql = """
        INSERT INTO fact_revenue_monthly (
            snapshot_date, account_key, advisor_key, household_key, business_line_key,
            account_assets, fee_percentage, gross_fee_amount, third_party_fee,
            advisor_payout_rate, advisor_payout_amount, net_revenue
        ) VALUES (
            :snapshot_date, :account_key, :advisor_key, :household_key, :business_line_key,
            :account_assets, :fee_percentage, :gross_fee_amount, :third_party_fee,
            :advisor_payout_rate, :advisor_payout_amount, :net_revenue
        )
        """

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            for i in range(0, len(revenue_data), batch_size):
                batch = revenue_data[i:i + batch_size]
                cursor.executemany(insert_sql, batch)

                if (i // batch_size + 1) % 20 == 0:
                    print(f"Inserted {min(i + batch_size, len(revenue_data))} records...")

            conn.commit()
            print(f"Successfully inserted all {len(revenue_data)} revenue records")
        except Exception as e:
            print(f"Error inserting revenue monthly data: {e}")
            raise
        finally:
            conn.close()

    def insert_fact_revenue_monthly_data(self, revenue_data: List[Dict[str, Any]], batch_size: int = 1000):
        """Alias for insert_revenue_monthly_data() for compatibility with notebook."""
        return self.insert_revenue_monthly_data(revenue_data, batch_size)

    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total revenue records': "SELECT COUNT(*) FROM fact_revenue_monthly",
            'Revenue calculation validation': """
                SELECT COUNT(*) as calculation_errors
                FROM fact_revenue_monthly
                WHERE ABS(net_revenue - (gross_fee_amount - third_party_fee - advisor_payout_amount)) > 0.01
            """,
            'Gross fee calculation validation': """
                SELECT COUNT(*) as calculation_errors
                FROM fact_revenue_monthly
                WHERE ABS(gross_fee_amount - (account_assets * fee_percentage)) > 0.01
            """,
            'Revenue statistics by business line': """
                SELECT
                    bl.business_line_name,
                    COUNT(*) as account_count,
                    SUM(frm.gross_fee_amount) as total_gross_revenue,
                    AVG(frm.fee_percentage * 10000) as avg_fee_bps,
                    SUM(frm.net_revenue) as total_net_revenue,
                    AVG(frm.advisor_payout_rate) as avg_payout_rate
                FROM fact_revenue_monthly frm
                JOIN business_line bl ON frm.business_line_key = bl.business_line_key
                GROUP BY bl.business_line_name, bl.business_line_key
                ORDER BY total_gross_revenue DESC
            """,
            'Monthly revenue trends': """
                SELECT
                    snapshot_date,
                    COUNT(*) as account_count,
                    SUM(gross_fee_amount) as total_gross_revenue,
                    SUM(third_party_fee) as total_third_party_fees,
                    SUM(advisor_payout_amount) as total_advisor_payouts,
                    SUM(net_revenue) as total_net_revenue
                FROM fact_revenue_monthly
                GROUP BY snapshot_date
                ORDER BY snapshot_date
            """,
            'Constraint violations': """
                SELECT
                    'Negative fee percentage' as violation_type,
                    COUNT(*) as count
                FROM fact_revenue_monthly
                WHERE fee_percentage < 0
                UNION ALL
                SELECT
                    'Negative gross fee' as violation_type,
                    COUNT(*) as count
                FROM fact_revenue_monthly
                WHERE gross_fee_amount < 0
                UNION ALL
                SELECT
                    'Invalid payout rate' as violation_type,
                    COUNT(*) as count
                FROM fact_revenue_monthly
                WHERE advisor_payout_rate < 0 OR advisor_payout_rate > 1
                UNION ALL
                SELECT
                    'Negative net revenue (potential issue)' as violation_type,
                    COUNT(*) as count
                FROM fact_revenue_monthly
                WHERE net_revenue < 0
            """
        }

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            print("\n" + "="*120)
            print("FACT REVENUE MONTHLY DATA VALIDATION RESULTS")
            print("="*120)

            for description, query in validation_queries.items():
                cursor.execute(query)
                results = cursor.fetchall()

                print(f"\n{description}:")

                if len(results) == 1 and len(results[0]) == 1:
                    print(f"  {results[0][0]}")
                elif 'Revenue statistics by business line' in description:
                    print(f"  {'Business Line':<25} {'Accounts':<10} {'Gross Rev':<15} {'Avg Fee':<10} {'Net Rev':<15} {'Avg Payout':<12}")
                    print(f"  {'-'*25} {'-'*10} {'-'*15} {'-'*10} {'-'*15} {'-'*12}")
                    for row in results:
                        bl_name, count, gross_rev, avg_fee_bps, net_rev, avg_payout = row
                        print(f"  {bl_name:<25} {count:<10,} ${gross_rev:<14,.0f} {avg_fee_bps:<9.0f}bp ${net_rev:<14,.0f} {avg_payout:<11.1%}")
                elif 'Monthly revenue trends' in description:
                    print(f"  {'Month':<12} {'Accounts':<10} {'Gross Rev':<15} {'3rd Party':<15} {'Payouts':<15} {'Net Rev':<15}")
                    print(f"  {'-'*12} {'-'*10} {'-'*15} {'-'*15} {'-'*15} {'-'*15}")
                    for row in results:
                        snap_date, count, gross_rev, third_party, payouts, net_rev = row
                        print(f"  {snap_date!s:<12} {count:<10,} ${gross_rev:<14,.0f} ${third_party:<14,.0f} ${payouts:<14,.0f} ${net_rev:<14,.0f}")
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

            print("\n" + "="*120)
        except Exception as e:
            print(f"Error during validation: {e}")
        finally:
            conn.close()

def main():
    """Main function to generate and insert fact revenue monthly data."""
    db_config = {
        'db_path': os.getenv('DB_PATH', './demo.db')
    }

    try:
        generator = FactRevenueMonthlyGenerator(**db_config)

        generator.create_table_if_not_exists()

        response = input("Do you want to clear existing fact revenue monthly data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()

        revenue_data = generator.generate_revenue_monthly_data()
        generator.insert_revenue_monthly_data(revenue_data)
        generator.validate_data()

        print(f"\nFact revenue monthly data generation completed successfully!")
        print(f"Generated {len(revenue_data)} revenue records")
        print(f"Data includes gross fees, third party fees, advisor payouts, and net revenue")

    except Exception as e:
        print(f"Error: {e}")
        print("Please check your SQLite database path configuration.")
        print("Ensure you have run the prerequisite scripts:")
        print("- create_fact_account_monthly.py")
        print("- create_tier_fee.py (or create_business_line.py)")
        print("- create_advisor_payout_rate.py (or create_advisors.py)")

# Alias for compatibility with notebook
FactRevenueMonthlyDataGenerator = FactRevenueMonthlyGenerator

if __name__ == "__main__":
    main()
