import psycopg2
import random
import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Tuple
import os
from urllib.parse import urlparse
import numpy as np

class AccountDataGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the account data generator with PostgreSQL database connection.
        
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
        
        # Constants from schema
        self.current_date = date(2025, 9, 30)
        self.target_account_count = 72000
        
        # Distribution configurations
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
        
        # Account distribution per advisor: min 15, median 145, max 400
        self.min_accounts_per_advisor = 15
        self.median_accounts_per_advisor = 145
        self.max_accounts_per_advisor = 400
        
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
    
    def _weighted_choice(self, choices: Dict[str, float]) -> str:
        """Select a random choice based on weighted distribution."""
        choices_list = list(choices.keys())
        weights = list(choices.values())
        return np.random.choice(choices_list, p=weights)
    
    def _get_reference_data(self) -> Tuple[List[Dict], List[Dict], Dict[str, int]]:
        """Get advisor, household, and business line reference data from database."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    # Get current advisors
                    cursor.execute("""
                        SELECT advisor_key, advisor_id 
                        FROM advisors 
                        WHERE to_date = '9999-12-31' AND advisor_status = 'Active'
                    """)
                    advisors = [{'advisor_key': row[0], 'advisor_id': row[1]} for row in cursor.fetchall()]
                    
                    # Get current households with their advisor
                    cursor.execute("""
                        SELECT household_key, household_id, household_advisor_id, household_registration_date
                        FROM household 
                        WHERE to_date = '9999-12-31' AND household_status = 'Active'
                    """)
                    households = [{'household_key': row[0], 'household_id': row[1], 
                                 'household_advisor_id': row[2], 'household_registration_date': row[3]} 
                                for row in cursor.fetchall()]
                    
                    # Get business lines
                    cursor.execute("SELECT business_line_key, business_line_name FROM business_line")
                    business_lines = {row[1]: row[0] for row in cursor.fetchall()}
                    
                    if not advisors:
                        raise Exception("No active advisors found. Please run create_advisors.py first.")
                    if not households:
                        raise Exception("No active households found. Please run create_household.py first.")
                    if not business_lines:
                        raise Exception("No business lines found. Please run create_business_line.py first.")
                    
                    print(f"Found {len(advisors)} active advisors, {len(households)} active households, {len(business_lines)} business lines")
                    return advisors, households, business_lines
                    
        except Exception as e:
            print(f"Error fetching reference data: {e}")
            raise
    
    def _generate_accounts_per_advisor(self, total_accounts: int, num_advisors: int) -> Dict[int, int]:
        """Generate account distribution per advisor following schema constraints."""
        accounts_per_advisor = {}
        remaining_accounts = total_accounts
        
        # Sort advisors to ensure consistent distribution
        advisor_keys = list(range(num_advisors))
        
        for i, advisor_idx in enumerate(advisor_keys):
            if i == len(advisor_keys) - 1:
                # Last advisor gets remaining accounts
                accounts_per_advisor[advisor_idx] = remaining_accounts
            else:
                # Generate number of accounts for this advisor
                # Use a skewed distribution to approximate median = 145
                if remaining_accounts <= self.min_accounts_per_advisor:
                    accounts = remaining_accounts
                else:
                    # Generate using log-normal distribution approximating median = 145
                    mean_accounts = min(self.median_accounts_per_advisor, 
                                      remaining_accounts // (len(advisor_keys) - i))
                    
                    # Add some randomness while respecting constraints
                    variance = min(50, mean_accounts // 3)
                    accounts = max(self.min_accounts_per_advisor,
                                 min(self.max_accounts_per_advisor,
                                     int(np.random.normal(mean_accounts, variance))))
                    
                    accounts = min(accounts, remaining_accounts - (len(advisor_keys) - i - 1) * self.min_accounts_per_advisor)
                
                accounts_per_advisor[advisor_idx] = accounts
                remaining_accounts -= accounts
        
        return accounts_per_advisor
    
    def _generate_opened_date(self, household_registration_date: date) -> date:
        """Generate account opened date between household registration and current date."""
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
        
        days_to_add = random.randint(1, min(days_since_open, 1095))  # Max 3 years
        return opened_date + timedelta(days=days_to_add)
    
    def generate_account_data(self) -> List[Dict[str, Any]]:
        """Generate account data according to schema specifications."""
        print(f"Generating {self.target_account_count} accounts...")
        
        # Get reference data
        advisors, households, business_lines = self._get_reference_data()
        
        # Generate account distribution per advisor
        accounts_per_advisor = self._generate_accounts_per_advisor(self.target_account_count, len(advisors))
        
        # Group households by advisor for proper relationships
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
            
            # Get households for this advisor
            advisor_households = households_by_advisor.get(advisor_id, [])
            if not advisor_households:
                print(f"Warning: No households found for advisor {advisor_id}, skipping...")
                continue
            
            print(f"Generating {num_accounts} accounts for advisor {advisor_id}...")
            
            for _ in range(num_accounts):
                # Select random household for this advisor
                household = random.choice(advisor_households)
                
                # Generate account attributes
                business_line_name = self._weighted_choice(self.business_line_dist)
                business_line_key = business_lines[business_line_name]
                account_type = self._weighted_choice(self.account_type_dist)
                custodian = self._weighted_choice(self.custodian_dist)
                status = self._weighted_choice(self.status_dist)
                risk_profile = random.choice(self.risk_profiles)
                
                # Generate dates
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
            account_key SERIAL PRIMARY KEY,
            account_id INTEGER NOT NULL,
            advisor_key INTEGER NOT NULL,
            household_key INTEGER NOT NULL,
            business_line_key INTEGER NOT NULL,
            account_type VARCHAR(20) NOT NULL,
            account_custodian VARCHAR(20) NOT NULL,
            opened_date DATE NOT NULL,
            account_status VARCHAR(10) NOT NULL,
            closed_date DATE,
            account_risk_profile VARCHAR(20) NOT NULL,
            from_date DATE NOT NULL,
            to_date DATE NOT NULL,
            FOREIGN KEY (advisor_key) REFERENCES advisors(advisor_key),
            FOREIGN KEY (household_key) REFERENCES household(household_key),
            FOREIGN KEY (business_line_key) REFERENCES business_line(business_line_key)
        );
        
        -- Create indexes as specified in schema
        CREATE UNIQUE INDEX IF NOT EXISTS ux_accounts_current 
        ON account(account_id) 
        WHERE to_date = DATE '9999-12-31';
        
        CREATE INDEX IF NOT EXISTS ix_account_id_window 
        ON account(account_id, from_date, to_date);
        
        CREATE INDEX IF NOT EXISTS ix_accounts_household 
        ON account(household_key);
        
        CREATE INDEX IF NOT EXISTS ix_accounts_advisor 
        ON account(advisor_key);
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    conn.commit()
                    print("Account table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating account table: {e}")
            raise
    
    def clear_existing_data(self):
        """Clear existing account data."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM account")
                    conn.commit()
                    print("Existing account data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
    
    def insert_account_data(self, accounts: List[Dict[str, Any]], batch_size: int = 1000):
        """Insert account data into PostgreSQL database."""
        print(f"Inserting {len(accounts)} account records...")
        
        insert_sql = """
        INSERT INTO account (
            account_key, account_id, advisor_key, household_key, business_line_key,
            account_type, account_custodian, opened_date, account_status, closed_date,
            account_risk_profile, from_date, to_date
        ) VALUES (
            %(account_key)s, %(account_id)s, %(advisor_key)s, %(household_key)s, %(business_line_key)s,
            %(account_type)s, %(account_custodian)s, %(opened_date)s, %(account_status)s, %(closed_date)s,
            %(account_risk_profile)s, %(from_date)s, %(to_date)s
        )
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
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
    
    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total account_id count': "SELECT COUNT(DISTINCT account_id) FROM account",
            'Current records count': "SELECT COUNT(*) FROM account WHERE to_date = '9999-12-31'",
            'Accounts per advisor (min/max/avg)': """
                SELECT MIN(account_count) as min_accounts, 
                       MAX(account_count) as max_accounts,
                       AVG(account_count)::INTEGER as avg_accounts
                FROM (
                    SELECT advisor_key, COUNT(*) as account_count
                    FROM account 
                    WHERE to_date = '9999-12-31'
                    GROUP BY advisor_key
                ) advisor_counts
            """,
            'Account type distribution': """
                SELECT account_type, COUNT(*) as count
                FROM account 
                WHERE to_date = '9999-12-31'
                GROUP BY account_type 
                ORDER BY count DESC
            """,
            'Business line distribution': """
                SELECT bl.business_line_name, COUNT(*) as count
                FROM account a
                JOIN business_line bl ON a.business_line_key = bl.business_line_key
                WHERE a.to_date = '9999-12-31'
                GROUP BY bl.business_line_name 
                ORDER BY count DESC
            """,
            'Custodian distribution': """
                SELECT account_custodian, COUNT(*) as count
                FROM account 
                WHERE to_date = '9999-12-31'
                GROUP BY account_custodian 
                ORDER BY count DESC
            """,
            'Status distribution': """
                SELECT account_status, COUNT(*) as count
                FROM account 
                WHERE to_date = '9999-12-31'
                GROUP BY account_status
            """
        }
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    print("\n" + "="*60)
                    print("DATA VALIDATION RESULTS")
                    print("="*60)
                    
                    for description, query in validation_queries.items():
                        cursor.execute(query)
                        results = cursor.fetchall()
                        
                        print(f"\n{description}:")
                        if 'min/max/avg' in description and results:
                            min_val, max_val, avg_val = results[0]
                            print(f"  Min: {min_val}, Max: {max_val}, Avg: {avg_val}")
                        elif len(results) == 1 and len(results[0]) == 1:
                            print(f"  {results[0][0]}")
                        else:
                            for row in results:
                                if len(row) == 2:
                                    percentage = (row[1] / sum(r[1] for r in results)) * 100 if len(results) > 1 else 0
                                    print(f"  {row[0]}: {row[1]} ({percentage:.1f}%)")
                                else:
                                    print(f"  {row}")
                    
                    print("\n" + "="*60)
        except Exception as e:
            print(f"Error during validation: {e}")

def main():
    """Main function to generate and insert account data."""
    # PostgreSQL connection configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'demo_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    
    try:
        generator = AccountDataGenerator(**db_config)
        
        # Create table if not exists
        generator.create_table_if_not_exists()
        
        # Ask user if they want to clear existing data
        response = input("Do you want to clear existing account data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()
        
        # Generate account data
        accounts = generator.generate_account_data()
        
        # Insert data into database
        generator.insert_account_data(accounts)
        
        # Validate inserted data
        generator.validate_data()
        
        print(f"\nAccount data generation completed successfully!")
        print(f"Generated {len(accounts)} total account records")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Please check your database configuration and ensure PostgreSQL is running.")
        print("Ensure you have run create_advisors.py, create_household.py, and create_business_line.py first.")

if __name__ == "__main__":
    main()