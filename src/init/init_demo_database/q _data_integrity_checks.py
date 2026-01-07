import psycopg2
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Tuple
import os
from urllib.parse import urlparse

class DataIntegrityChecker:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the data integrity checker with PostgreSQL database connection.
        
        Args:
            connection_string: PostgreSQL connection string (e.g., 'postgresql://user:pass@host:port/db')
            **db_params: Individual connection parameters (host, port, database, user, password)
        """
        if connection_string:
            self.connection_params = self._parse_connection_string(connection_string)
        else:
            self.connection_params = {
                'host': db_params.get('host', 'localhost'),
                'port': db_params.get('port', 5432),
                'database': db_params.get('database', 'demo_db'),
                'user': db_params.get('user', 'postgres'),
                'password': db_params.get('password', '')
            }
        self.results = []
        
    def _parse_connection_string(self, connection_string: str) -> dict:
        """Parse PostgreSQL connection string into connection parameters."""
        parsed = urlparse(connection_string)
        return {
            'host': parsed.hostname,
            'port': parsed.port or 5432,
            'database': parsed.path.lstrip('/'),
            'user': parsed.username,
            'password': parsed.password
        }
        
    def add_result(self, test_name: str, test_details: str, test_result: str):
        """Add a test result to the results list."""
        self.results.append({
            'test_name': test_name,
            'test_details': test_details,
            'test_result': test_result
        })
    
    def execute_query(self, query: str) -> List[Tuple]:
        """Execute a query and return results."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    return cursor.fetchall()
        except Exception as e:
            print(f"Query error: {str(e)}")
            return []
    
    def check_household_table(self):
        """Check household table integrity."""
        
        # Check 1.1: No duplicated household_key values
        query = """
        SELECT household_key, COUNT(*) as count
        FROM household
        GROUP BY household_key
        HAVING COUNT(*) > 1
        """
        duplicates = self.execute_query(query)
        if duplicates:
            result = "; ".join([f"household_key {row[0]} appears {row[1]} times" for row in duplicates])
            self.add_result("Check 1.1", "No duplicated household_key values", result)
        
        # Check 1.2: Exactly one record per household_id where to_date = '9999-12-31'
        query = """
        SELECT household_id, COUNT(*) as count
        FROM household
        WHERE to_date = '9999-12-31'
        GROUP BY household_id
        HAVING COUNT(*) != 1
        """
        current_records = self.execute_query(query)
        if current_records:
            result = "; ".join([f"household_id {row[0]} has {row[1]} current records" for row in current_records])
            self.add_result("Check 1.2", "Exactly one record per household_id where to_date = '9999-12-31'", result)
        
        # Check 1.3: Target count: Exactly 45,000 distinct household_id values
        query = "SELECT COUNT(DISTINCT household_id) FROM household"
        count_result = self.execute_query(query)
        if count_result and count_result[0][0] != 45000:
            self.add_result("Check 1.3", "Target count: Exactly 45,000 distinct household_id values", 
                          f"Found {count_result[0][0]} distinct household_id values")
        
        # Check 1.4: No conflicting dates: from_date < to_date for all records
        query = """
        SELECT household_key, household_id, from_date, to_date
        FROM household
        WHERE from_date >= to_date
        """
        date_conflicts = self.execute_query(query)
        if date_conflicts:
            result = "; ".join([f"household_key {row[0]} (id {row[1]}): from_date {row[2]} >= to_date {row[3]}" for row in date_conflicts])
            self.add_result("Check 1.4", "No conflicting dates: from_date < to_date for all records", result)
        
        # Check 1.5: No gaps in SCD2 history
        query = """
        WITH next_records AS (
            SELECT household_id, to_date,
                   LEAD(from_date) OVER (PARTITION BY household_id ORDER BY from_date) as next_from_date
            FROM household
        )
        SELECT household_id, to_date, next_from_date
        FROM next_records
        WHERE next_from_date IS NOT NULL AND to_date != next_from_date
        """
        gaps = self.execute_query(query)
        if gaps:
            result = "; ".join([f"household_id {row[0]}: gap between to_date {row[1]} and next from_date {row[2]}" for row in gaps])
            self.add_result("Check 1.5", "No gaps in SCD2 history", result)
        
        # Check 1.6: household_registration_date alignment with tenure
        query = """
        SELECT household_key, household_id, household_registration_date, household_tenure,
               DATE '2025-09-30' - INTERVAL '1 day' * (household_tenure * 365) as calculated_base,
               ABS(EXTRACT(DAYS FROM (household_registration_date - (DATE '2025-09-30' - INTERVAL '1 day' * (household_tenure * 365))))) as days_diff
        FROM household
        WHERE to_date = '9999-12-31'
        AND days_diff > 180
        """
        tenure_misalign = self.execute_query(query)
        if tenure_misalign:
            result = "; ".join([f"household_key {row[0]} (id {row[1]}): {row[5]:.0f} days difference" for row in tenure_misalign])
            self.add_result("Check 1.6", "household_registration_date alignment with tenure (±180 days)", result)
        
        # Check 1.7: All household_advisor_id exist in advisors.advisor_id
        query = """
        SELECT DISTINCT h.household_advisor_id
        FROM household h
        LEFT JOIN advisors a ON h.household_advisor_id = a.advisor_id
        WHERE a.advisor_id IS NULL
        """
        missing_advisors = self.execute_query(query)
        if missing_advisors:
            result = "; ".join([f"advisor_id {row[0]}" for row in missing_advisors])
            self.add_result("Check 1.7", "All household_advisor_id exist in advisors.advisor_id", result)
        
        # Check 1.9: household_tenure is between 1 and 40 years
        query = """
        SELECT household_key, household_id, household_tenure
        FROM household
        WHERE household_tenure < 1 OR household_tenure > 40
        """
        tenure_invalid = self.execute_query(query)
        if tenure_invalid:
            result = "; ".join([f"household_key {row[0]} (id {row[1]}): tenure {row[2]}" for row in tenure_invalid])
            self.add_result("Check 1.9", "household_tenure is between 1 and 40 years", result)
        
        # Check 1.10: Terminated households should have to_date != '9999-12-31'
        query = """
        SELECT household_key, household_id
        FROM household
        WHERE household_status = 'Terminated' AND to_date = '9999-12-31'
        """
        terminated_current = self.execute_query(query)
        if terminated_current:
            result = "; ".join([f"household_key {row[0]} (id {row[1]})" for row in terminated_current])
            self.add_result("Check 1.10", "Terminated households should have to_date != '9999-12-31'", result)
    
    def check_advisors_table(self):
        """Check advisors table integrity."""
        
        # Check 2.1: No duplicated advisor_key values
        query = """
        SELECT advisor_key, COUNT(*) as count
        FROM advisors
        GROUP BY advisor_key
        HAVING COUNT(*) > 1
        """
        duplicates = self.execute_query(query)
        if duplicates:
            result = "; ".join([f"advisor_key {row[0]} appears {row[1]} times" for row in duplicates])
            self.add_result("Check 2.1", "No duplicated advisor_key values", result)
        
        # Check 2.2: Exactly one record per advisor_id where to_date = '9999-12-31'
        query = """
        SELECT advisor_id, COUNT(*) as count
        FROM advisors
        WHERE to_date = '9999-12-31'
        GROUP BY advisor_id
        HAVING COUNT(*) != 1
        """
        current_records = self.execute_query(query)
        if current_records:
            result = "; ".join([f"advisor_id {row[0]} has {row[1]} current records" for row in current_records])
            self.add_result("Check 2.2", "Exactly one record per advisor_id where to_date = '9999-12-31'", result)
        
        # Check 2.3: Target count: Exactly 500 distinct advisor_id values
        query = "SELECT COUNT(DISTINCT advisor_id) FROM advisors"
        count_result = self.execute_query(query)
        if count_result and count_result[0][0] != 500:
            self.add_result("Check 2.3", "Target count: Exactly 500 distinct advisor_id values", 
                          f"Found {count_result[0][0]} distinct advisor_id values")
        
        # Check 2.4: No conflicting dates: from_date < to_date for all records
        query = """
        SELECT advisor_key, advisor_id, from_date, to_date
        FROM advisors
        WHERE from_date >= to_date
        """
        date_conflicts = self.execute_query(query)
        if date_conflicts:
            result = "; ".join([f"advisor_key {row[0]} (id {row[1]}): from_date {row[2]} >= to_date {row[3]}" for row in date_conflicts])
            self.add_result("Check 2.4", "No conflicting dates: from_date < to_date for all records", result)
        
        # Check 2.5: No gaps in SCD2 history
        query = """
        WITH next_records AS (
            SELECT advisor_id, to_date,
                   LEAD(from_date) OVER (PARTITION BY advisor_id ORDER BY from_date) as next_from_date
            FROM advisors
        )
        SELECT advisor_id, to_date, next_from_date
        FROM next_records
        WHERE next_from_date IS NOT NULL AND to_date != next_from_date
        """
        gaps = self.execute_query(query)
        if gaps:
            result = "; ".join([f"advisor_id {row[0]}: gap between to_date {row[1]} and next from_date {row[2]}" for row in gaps])
            self.add_result("Check 2.5", "No gaps in SCD2 history", result)
        
        # Check 2.6: advisor_tenure is between 1 and 40 years
        query = """
        SELECT advisor_key, advisor_id, advisor_tenure
        FROM advisors
        WHERE advisor_tenure < 1 OR advisor_tenure > 40
        """
        tenure_invalid = self.execute_query(query)
        if tenure_invalid:
            result = "; ".join([f"advisor_key {row[0]} (id {row[1]}): tenure {row[2]}" for row in tenure_invalid])
            self.add_result("Check 2.6", "advisor_tenure is between 1 and 40 years", result)
        
        # Check 2.7: Terminated advisors should have to_date != '9999-12-31'
        query = """
        SELECT advisor_key, advisor_id
        FROM advisors
        WHERE advisor_status = 'Terminated' AND to_date = '9999-12-31'
        """
        terminated_current = self.execute_query(query)
        if terminated_current:
            result = "; ".join([f"advisor_key {row[0]} (id {row[1]})" for row in terminated_current])
            self.add_result("Check 2.7", "Terminated advisors should have to_date != '9999-12-31'", result)
    
    def check_account_table(self):
        """Check account table integrity."""
        
        # Check 3.1: No duplicated account_key values
        query = """
        SELECT account_key, COUNT(*) as count
        FROM account
        GROUP BY account_key
        HAVING COUNT(*) > 1
        """
        duplicates = self.execute_query(query)
        if duplicates:
            result = "; ".join([f"account_key {row[0]} appears {row[1]} times" for row in duplicates])
            self.add_result("Check 3.1", "No duplicated account_key values", result)
        
        # Check 3.2: Exactly one record per account_id where to_date = '9999-12-31'
        query = """
        SELECT account_id, COUNT(*) as count
        FROM account
        WHERE to_date = '9999-12-31'
        GROUP BY account_id
        HAVING COUNT(*) != 1
        """
        current_records = self.execute_query(query)
        if current_records:
            result = "; ".join([f"account_id {row[0]} has {row[1]} current records" for row in current_records])
            self.add_result("Check 3.2", "Exactly one record per account_id where to_date = '9999-12-31'", result)
        
        # Check 3.3: Target count: Exactly 72,000 distinct account_id values
        query = "SELECT COUNT(DISTINCT account_id) FROM account"
        count_result = self.execute_query(query)
        if count_result and count_result[0][0] != 72000:
            self.add_result("Check 3.3", "Target count: Exactly 72,000 distinct account_id values", 
                          f"Found {count_result[0][0]} distinct account_id values")
        
        # Check 3.4: No conflicting dates: from_date < to_date for all records
        query = """
        SELECT account_key, account_id, from_date, to_date
        FROM account
        WHERE from_date >= to_date
        """
        date_conflicts = self.execute_query(query)
        if date_conflicts:
            result = "; ".join([f"account_key {row[0]} (id {row[1]}): from_date {row[2]} >= to_date {row[3]}" for row in date_conflicts])
            self.add_result("Check 3.4", "No conflicting dates: from_date < to_date for all records", result)
        
        # Check 3.7: All closed accounts have closed_date > opened_date
        query = """
        SELECT account_key, account_id, opened_date, closed_date
        FROM account
        WHERE account_status = 'Closed' AND closed_date <= opened_date
        """
        invalid_closed = self.execute_query(query)
        if invalid_closed:
            result = "; ".join([f"account_key {row[0]} (id {row[1]}): opened {row[2]}, closed {row[3]}" for row in invalid_closed])
            self.add_result("Check 3.7", "All closed accounts have closed_date > opened_date", result)
        
        # Check 3.8: Open accounts have closed_date IS NULL
        query = """
        SELECT account_key, account_id, closed_date
        FROM account
        WHERE account_status = 'Open' AND closed_date IS NOT NULL
        """
        open_with_closed = self.execute_query(query)
        if open_with_closed:
            result = "; ".join([f"account_key {row[0]} (id {row[1]}): closed_date {row[2]}" for row in open_with_closed])
            self.add_result("Check 3.8", "Open accounts have closed_date IS NULL", result)
        
        # Check 3.9: Closed accounts have closed_date IS NOT NULL
        query = """
        SELECT account_key, account_id
        FROM account
        WHERE account_status = 'Closed' AND closed_date IS NULL
        """
        closed_without_date = self.execute_query(query)
        if closed_without_date:
            result = "; ".join([f"account_key {row[0]} (id {row[1]})" for row in closed_without_date])
            self.add_result("Check 3.9", "Closed accounts have closed_date IS NOT NULL", result)
        
        # Check 3.15: Accounts per advisor: min 15, max 400
        query = """
        SELECT advisor_key, COUNT(*) as account_count
        FROM account
        WHERE to_date = '9999-12-31'
        GROUP BY advisor_key
        HAVING account_count < 15 OR account_count > 400
        """
        advisor_account_issues = self.execute_query(query)
        if advisor_account_issues:
            result = "; ".join([f"advisor_key {row[0]}: {row[1]} accounts" for row in advisor_account_issues])
            self.add_result("Check 3.15", "Accounts per advisor: min 15, max 400", result)
    
    def check_product_table(self):
        """Check product table integrity."""
        
        # Check 4.1: No duplicated product_id values
        query = """
        SELECT product_id, COUNT(*) as count
        FROM product
        GROUP BY product_id
        HAVING COUNT(*) > 1
        """
        duplicates = self.execute_query(query)
        if duplicates:
            result = "; ".join([f"product_id {row[0]} appears {row[1]} times" for row in duplicates])
            self.add_result("Check 4.1", "No duplicated product_id values", result)
        
        # Check 4.2: Target count: Exactly 350 distinct product_id values
        query = "SELECT COUNT(DISTINCT product_id) FROM product"
        count_result = self.execute_query(query)
        if count_result and count_result[0][0] != 350:
            self.add_result("Check 4.2", "Target count: Exactly 350 distinct product_id values", 
                          f"Found {count_result[0][0]} distinct product_id values")
        
        # Check 4.6: product_name is not null and not empty
        query = """
        SELECT product_id
        FROM product
        WHERE product_name IS NULL OR product_name = '' OR TRIM(product_name) = ''
        """
        empty_names = self.execute_query(query)
        if empty_names:
            result = "; ".join([f"product_id {row[0]}" for row in empty_names])
            self.add_result("Check 4.6", "product_name is not null and not empty", result)
    
    def check_advisor_payout_rate_table(self):
        """Check advisor_payout_rate table integrity."""
        
        # Check 6.1: All firm_affiliation_model values from advisors table exist
        query = """
        SELECT DISTINCT a.firm_affiliation_model
        FROM advisors a
        LEFT JOIN advisor_payout_rate apr ON a.firm_affiliation_model = apr.firm_affiliation_model
        WHERE apr.firm_affiliation_model IS NULL
        """
        missing_payout_rates = self.execute_query(query)
        if missing_payout_rates:
            result = "; ".join([f"firm_affiliation_model '{row[0]}'" for row in missing_payout_rates])
            self.add_result("Check 6.1", "All firm_affiliation_model values from advisors table exist", result)
    
    def check_fact_account_initial_assets_table(self):
        """Check fact_account_initial_assets table integrity."""
        
        # Check 7.3: account_initial_assets are positive (> 0)
        query = """
        SELECT account_key
        FROM fact_account_initial_assets
        WHERE account_initial_assets <= 0
        """
        non_positive = self.execute_query(query)
        if non_positive:
            result = "; ".join([f"account_key {row[0]}" for row in non_positive])
            self.add_result("Check 7.3", "account_initial_assets are positive (> 0)", result)
        
        # Check 7.4: No account should exceed $20M in initial assets
        query = """
        SELECT account_key, account_initial_assets
        FROM fact_account_initial_assets
        WHERE account_initial_assets > 20000000
        """
        excessive_assets = self.execute_query(query)
        if excessive_assets:
            result = "; ".join([f"account_key {row[0]}: ${row[1]:,.0f}" for row in excessive_assets])
            self.add_result("Check 7.4", "No account should exceed $20M in initial assets", result)
    
    def check_fact_account_monthly_table(self):
        """Check fact_account_monthly table integrity."""
        
        # Check 8.1: Exactly 12 distinct snapshot_date values (end-of-month dates)
        query = "SELECT COUNT(DISTINCT snapshot_date) FROM fact_account_monthly"
        count_result = self.execute_query(query)
        if count_result and count_result[0][0] != 12:
            self.add_result("Check 8.1", "Exactly 12 distinct snapshot_date values (end-of-month dates)", 
                          f"Found {count_result[0][0]} distinct snapshot_date values")
        
        # Check 8.10: account_monthly_return is between -12% and +12%
        query = """
        SELECT snapshot_date, account_key, account_monthly_return
        FROM fact_account_monthly
        WHERE account_monthly_return < -0.12 OR account_monthly_return > 0.12
        """
        invalid_returns = self.execute_query(query)
        if invalid_returns:
            result = "; ".join([f"account_key {row[1]} on {row[0]}: {row[2]:.2%}" for row in invalid_returns])
            self.add_result("Check 8.10", "account_monthly_return is between -12% and +12%", result)
        
        # Check 8.11: For snapshot_date = snapshot_start_date, account_monthly_return = 0%
        query = """
        SELECT account_key, account_monthly_return
        FROM fact_account_monthly
        WHERE snapshot_date = '2024-09-30' AND account_monthly_return != 0
        """
        first_month_returns = self.execute_query(query)
        if first_month_returns:
            result = "; ".join([f"account_key {row[0]}: {row[1]:.2%}" for row in first_month_returns])
            self.add_result("Check 8.11", "For snapshot_date = snapshot_start_date, account_monthly_return = 0%", result)
        
        # Check 8.13: account_assets is positive and ≤ $20M
        query = """
        SELECT snapshot_date, account_key, account_assets
        FROM fact_account_monthly
        WHERE account_assets <= 0 OR account_assets > 20000000
        """
        invalid_assets = self.execute_query(query)
        if invalid_assets:
            result = "; ".join([f"account_key {row[1]} on {row[0]}: ${row[2]:,.0f}" for row in invalid_assets])
            self.add_result("Check 8.13", "account_assets is positive and ≤ $20M", result)
    
    def check_fact_account_product_monthly_table(self):
        """Check fact_account_product_monthly table integrity."""
        
        # Check 9.3: Sum of product_allocation_pct per (snapshot_date, account_key) equals 100%
        query = """
        SELECT snapshot_date, account_key, SUM(product_allocation_pct) as total_allocation
        FROM fact_account_product_monthly
        GROUP BY snapshot_date, account_key
        HAVING ABS(total_allocation - 100.0) > 0.01
        """
        allocation_issues = self.execute_query(query)
        if allocation_issues:
            result = "; ".join([f"account_key {row[1]} on {row[0]}: {row[2]:.2f}%" for row in allocation_issues])
            self.add_result("Check 9.3", "Sum of product_allocation_pct per (snapshot_date, account_key) equals 100%", result)
        
        # Check 9.4: Each account has between 2 and 5 products
        query = """
        SELECT snapshot_date, account_key, COUNT(*) as product_count
        FROM fact_account_product_monthly
        GROUP BY snapshot_date, account_key
        HAVING product_count < 2 OR product_count > 5
        """
        product_count_issues = self.execute_query(query)
        if product_count_issues:
            result = "; ".join([f"account_key {row[1]} on {row[0]}: {row[2]} products" for row in product_count_issues])
            self.add_result("Check 9.4", "Each account has between 2 and 5 products", result)
        
        # Check 9.5: product_allocation_pct is between 0 and 100
        query = """
        SELECT snapshot_date, account_key, product_id, product_allocation_pct
        FROM fact_account_product_monthly
        WHERE product_allocation_pct < 0 OR product_allocation_pct > 100
        """
        invalid_allocations = self.execute_query(query)
        if invalid_allocations:
            result = "; ".join([f"account_key {row[1]}, product_id {row[2]} on {row[0]}: {row[3]:.2f}%" for row in invalid_allocations])
            self.add_result("Check 9.5", "product_allocation_pct is between 0 and 100", result)
    
    def check_fact_household_monthly_table(self):
        """Check fact_household_monthly table integrity."""
        
        # Check 10.5: high_net_worth_flag is TRUE if and only if household_assets >= $1M
        query = """
        SELECT snapshot_date, household_key, household_assets, high_net_worth_flag
        FROM fact_household_monthly
        WHERE (household_assets >= 1000000 AND high_net_worth_flag = false)
           OR (household_assets < 1000000 AND high_net_worth_flag = true)
        """
        hnw_flag_issues = self.execute_query(query)
        if hnw_flag_issues:
            result = "; ".join([f"household_key {row[1]} on {row[0]}: ${row[2]:,.0f}, flag={row[3]}" for row in hnw_flag_issues])
            self.add_result("Check 10.5", "high_net_worth_flag is TRUE if and only if household_assets >= $1M", result)
    
    def check_fact_revenue_monthly_table(self):
        """Check fact_revenue_monthly table integrity."""
        
        # Check 11.6: All monetary amounts are non-negative
        query = """
        SELECT snapshot_date, account_key, 'gross_fee_amount' as field, gross_fee_amount as amount
        FROM fact_revenue_monthly
        WHERE gross_fee_amount < 0
        UNION ALL
        SELECT snapshot_date, account_key, 'third_party_fee', third_party_fee
        FROM fact_revenue_monthly
        WHERE third_party_fee < 0
        UNION ALL
        SELECT snapshot_date, account_key, 'advisor_payout_amount', advisor_payout_amount
        FROM fact_revenue_monthly
        WHERE advisor_payout_amount < 0
        """
        negative_amounts = self.execute_query(query)
        if negative_amounts:
            result = "; ".join([f"account_key {row[1]} on {row[0]}: {row[2]} = ${row[3]:,.2f}" for row in negative_amounts])
            self.add_result("Check 11.6", "All monetary amounts are non-negative", result)
        
        # Check 11.7: net_revenue should be positive
        query = """
        SELECT snapshot_date, account_key, net_revenue
        FROM fact_revenue_monthly
        WHERE net_revenue <= 0
        """
        non_positive_revenue = self.execute_query(query)
        if non_positive_revenue:
            result = "; ".join([f"account_key {row[1]} on {row[0]}: ${row[2]:,.2f}" for row in non_positive_revenue])
            self.add_result("Check 11.7", "net_revenue should be positive", result)
    
    def check_fact_customer_feedback_table(self):
        """Check fact_customer_feedback table integrity."""
        
        # Check 13.5: feedback_date is between January 1st of previous year and current_date
        query = """
        SELECT feedback_id, feedback_date
        FROM fact_customer_feedback
        WHERE feedback_date < '2024-01-01' OR feedback_date > '2025-09-30'
        """
        invalid_dates = self.execute_query(query)
        if invalid_dates:
            result = "; ".join([f"feedback_id {row[0]}: {row[1]}" for row in invalid_dates])
            self.add_result("Check 13.5", "feedback_date is between January 1st of previous year and current_date", result)
        
        # Check 13.6: satisfaction_score is between 0 and 100
        query = """
        SELECT feedback_id, satisfaction_score
        FROM fact_customer_feedback
        WHERE satisfaction_score < 0 OR satisfaction_score > 100
        """
        invalid_scores = self.execute_query(query)
        if invalid_scores:
            result = "; ".join([f"feedback_id {row[0]}: {row[1]}" for row in invalid_scores])
            self.add_result("Check 13.6", "satisfaction_score is between 0 and 100", result)
    
    def run_all_checks(self):
        """Run all data integrity checks."""
        print("Starting data integrity checks...")
        
        try:
            self.check_household_table()
            print("✓ Household table checks completed")
        except Exception as e:
            print(f"✗ Error checking household table: {e}")
        
        try:
            self.check_advisors_table()
            print("✓ Advisors table checks completed")
        except Exception as e:
            print(f"✗ Error checking advisors table: {e}")
        
        try:
            self.check_account_table()
            print("✓ Account table checks completed")
        except Exception as e:
            print(f"✗ Error checking account table: {e}")
        
        try:
            self.check_product_table()
            print("✓ Product table checks completed")
        except Exception as e:
            print(f"✗ Error checking product table: {e}")
        
        try:
            self.check_advisor_payout_rate_table()
            print("✓ Advisor payout rate table checks completed")
        except Exception as e:
            print(f"✗ Error checking advisor payout rate table: {e}")
        
        try:
            self.check_fact_account_initial_assets_table()
            print("✓ Fact account initial assets table checks completed")
        except Exception as e:
            print(f"✗ Error checking fact account initial assets table: {e}")
        
        try:
            self.check_fact_account_monthly_table()
            print("✓ Fact account monthly table checks completed")
        except Exception as e:
            print(f"✗ Error checking fact account monthly table: {e}")
        
        try:
            self.check_fact_account_product_monthly_table()
            print("✓ Fact account product monthly table checks completed")
        except Exception as e:
            print(f"✗ Error checking fact account product monthly table: {e}")
        
        try:
            self.check_fact_household_monthly_table()
            print("✓ Fact household monthly table checks completed")
        except Exception as e:
            print(f"✗ Error checking fact household monthly table: {e}")
        
        try:
            self.check_fact_revenue_monthly_table()
            print("✓ Fact revenue monthly table checks completed")
        except Exception as e:
            print(f"✗ Error checking fact revenue monthly table: {e}")
        
        try:
            self.check_fact_customer_feedback_table()
            print("✓ Fact customer feedback table checks completed")
        except Exception as e:
            print(f"✗ Error checking fact customer feedback table: {e}")
        
        print(f"\nData integrity checks completed. Found {len(self.results)} issues.")
        return self.results
    
    def export_to_csv(self, filename: str = None):
        """Export results to CSV file."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data_integrity_checks_result_{timestamp}.csv"
        
        df = pd.DataFrame(self.results)
        
        # Only export if there are issues found
        if len(self.results) > 0:
            df.to_csv(filename, index=False)
            print(f"Results exported to {filename}")
        else:
            print("No issues found - no CSV file created")
        
        return filename

def main():
    """Main function to run data integrity checks."""
    # PostgreSQL connection configuration
    # Option 1: Use connection string
    # connection_string = "postgresql://username:password@localhost:5432/demo_db"
    # checker = DataIntegrityChecker(connection_string=connection_string)
    
    # Option 2: Use individual parameters (recommended)
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'demo_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    
    try:
        checker = DataIntegrityChecker(**db_config)
    except Exception as e:
        print(f"Failed to connect to PostgreSQL database: {e}")
        print("Please check your database configuration and ensure PostgreSQL is running.")
        print("Set environment variables: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")
        return
    results = checker.run_all_checks()
    
    if results:
        print(f"\nFound {len(results)} integrity issues:")
        for i, result in enumerate(results, 1):
            print(f"{i}. {result['test_name']}: {result['test_details']}")
            print(f"   Issues: {result['test_result']}")
            print()
        
        # Export to CSV
        csv_filename = checker.export_to_csv()
        print(f"Detailed results saved to: {csv_filename}")
    else:
        print("\nAll data integrity checks passed! ✓")

if __name__ == "__main__":
    main()