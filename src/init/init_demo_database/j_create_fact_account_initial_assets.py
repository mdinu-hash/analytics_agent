import psycopg2
import random
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any, Tuple
import os
from urllib.parse import urlparse
import numpy as np

class FactAccountInitialAssetsGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the fact account initial assets generator with PostgreSQL database connection.
        
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
        self.snapshot_start_date = date(2024, 9, 30)
        
        # Asset distribution parameters by account type from schema
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
    
    def _generate_normal_with_minimum(self, median: float, std_dev: float, minimum: float, maximum: float = 20000000) -> float:
        """Generate normally distributed value with enforced minimum and maximum."""
        # Convert median to mean for normal distribution
        # For log-normal distribution, we need to adjust
        mean = median
        
        # Generate normal distribution and enforce constraints
        attempts = 0
        while attempts < 100:  # Prevent infinite loops
            value = np.random.normal(mean, std_dev)
            if minimum <= value <= maximum:
                return value
            attempts += 1
        
        # Fallback: return constrained value
        return max(minimum, min(maximum, mean))
    
    def _get_eligible_accounts(self) -> List[Dict[str, Any]]:
        """Get accounts that were opened at snapshot_start_date."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    # Get accounts opened at snapshot start date
                    cursor.execute("""
                        SELECT account_key, account_id, account_type, opened_date
                        FROM account 
                        WHERE opened_date <= %s
                        AND to_date = '9999-12-31'
                        ORDER BY account_key
                    """, (self.snapshot_start_date,))
                    
                    accounts = []
                    for row in cursor.fetchall():
                        accounts.append({
                            'account_key': row[0],
                            'account_id': row[1],
                            'account_type': row[2],
                            'opened_date': row[3]
                        })
                    
                    if not accounts:
                        raise Exception("No eligible accounts found. Please run create_account.py first.")
                    
                    print(f"Found {len(accounts)} accounts eligible for initial assets (opened by {self.snapshot_start_date})")
                    return accounts
                    
        except Exception as e:
            print(f"Error fetching eligible accounts: {e}")
            raise
    
    def generate_initial_assets_data(self) -> List[Dict[str, Any]]:
        """Generate initial assets data according to schema specifications."""
        accounts = self._get_eligible_accounts()
        
        print(f"Generating initial assets for {len(accounts)} accounts...")
        
        initial_assets_data = []
        
        # Group accounts by type for statistics
        accounts_by_type = {}
        for account in accounts:
            account_type = account['account_type']
            if account_type not in accounts_by_type:
                accounts_by_type[account_type] = []
            accounts_by_type[account_type].append(account)
        
        # Display account type distribution
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
            
            # Generate initial asset amount using normal distribution
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
    
    def create_table_if_not_exists(self):
        """Create fact_account_initial_assets table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fact_account_initial_assets (
            account_key INTEGER PRIMARY KEY,
            account_initial_assets DECIMAL(15,2) NOT NULL,
            FOREIGN KEY (account_key) REFERENCES account(account_key),
            CONSTRAINT check_initial_assets_positive CHECK (account_initial_assets > 0),
            CONSTRAINT check_initial_assets_max CHECK (account_initial_assets <= 20000000)
        );
        
        -- Create index for efficient lookups
        CREATE INDEX IF NOT EXISTS ix_fact_initial_assets_amount 
        ON fact_account_initial_assets(account_initial_assets);
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    conn.commit()
                    print("Fact account initial assets table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating fact account initial assets table: {e}")
            raise
    
    def clear_existing_data(self):
        """Clear existing fact account initial assets data."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM fact_account_initial_assets")
                    conn.commit()
                    print("Existing fact account initial assets data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
    
    def insert_initial_assets_data(self, initial_assets: List[Dict[str, Any]], batch_size: int = 1000):
        """Insert initial assets data into PostgreSQL database."""
        print(f"Inserting {len(initial_assets)} initial asset records...")
        
        insert_sql = """
        INSERT INTO fact_account_initial_assets (account_key, account_initial_assets)
        VALUES (%(account_key)s, %(account_initial_assets)s)
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
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
    
    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total initial asset records': "SELECT COUNT(*) FROM fact_account_initial_assets",
            'Asset statistics by account type': """
                SELECT a.account_type,
                       COUNT(*) as account_count,
                       MIN(faia.account_initial_assets) as min_assets,
                       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY faia.account_initial_assets) as median_assets,
                       AVG(faia.account_initial_assets) as avg_assets,
                       MAX(faia.account_initial_assets) as max_assets,
                       STDDEV(faia.account_initial_assets) as stddev_assets
                FROM fact_account_initial_assets faia
                JOIN account a ON faia.account_key = a.account_key
                WHERE a.to_date = '9999-12-31'
                GROUP BY a.account_type
                ORDER BY median_assets DESC
            """,
            'Asset range distribution': """
                SELECT 
                    CASE 
                        WHEN account_initial_assets < 50000 THEN 'Under $50K'
                        WHEN account_initial_assets < 100000 THEN '$50K - $100K'
                        WHEN account_initial_assets < 250000 THEN '$100K - $250K'
                        WHEN account_initial_assets < 500000 THEN '$250K - $500K'
                        WHEN account_initial_assets < 1000000 THEN '$500K - $1M'
                        WHEN account_initial_assets < 5000000 THEN '$1M - $5M'
                        ELSE '$5M+'
                    END as asset_range,
                    COUNT(*) as account_count,
                    ROUND(AVG(account_initial_assets), 0) as avg_amount
                FROM fact_account_initial_assets
                GROUP BY 
                    CASE 
                        WHEN account_initial_assets < 50000 THEN 'Under $50K'
                        WHEN account_initial_assets < 100000 THEN '$50K - $100K'
                        WHEN account_initial_assets < 250000 THEN '$100K - $250K'
                        WHEN account_initial_assets < 500000 THEN '$250K - $500K'
                        WHEN account_initial_assets < 1000000 THEN '$500K - $1M'
                        WHEN account_initial_assets < 5000000 THEN '$1M - $5M'
                        ELSE '$5M+'
                    END
                ORDER BY MIN(account_initial_assets)
            """,
            'Constraint violations': """
                SELECT 
                    'Below minimum' as violation_type,
                    COUNT(*) as count
                FROM fact_account_initial_assets 
                WHERE account_initial_assets <= 0
                UNION ALL
                SELECT 
                    'Above maximum' as violation_type,
                    COUNT(*) as count
                FROM fact_account_initial_assets 
                WHERE account_initial_assets > 20000000
            """,
            'Total assets': """
                SELECT 
                    SUM(account_initial_assets) as total_assets,
                    COUNT(*) as total_accounts,
                    AVG(account_initial_assets) as average_account_size
                FROM fact_account_initial_assets
            """
        }
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    print("\n" + "="*90)
                    print("FACT ACCOUNT INITIAL ASSETS DATA VALIDATION RESULTS")
                    print("="*90)
                    
                    for description, query in validation_queries.items():
                        cursor.execute(query)
                        results = cursor.fetchall()
                        
                        print(f"\n{description}:")
                        
                        if len(results) == 1 and len(results[0]) == 1:
                            print(f"  {results[0][0]}")
                        elif 'Asset statistics by account type' in description:
                            print(f"  {'Type':<8} {'Count':<8} {'Min':<12} {'Median':<12} {'Avg':<12} {'Max':<12} {'StdDev':<12}")
                            print(f"  {'-'*8} {'-'*8} {'-'*12} {'-'*12} {'-'*12} {'-'*12} {'-'*12}")
                            for row in results:
                                acc_type, count, min_val, median_val, avg_val, max_val, stddev_val = row
                                print(f"  {acc_type:<8} {count:<8} ${min_val:<11,.0f} ${median_val:<11,.0f} ${avg_val:<11,.0f} ${max_val:<11,.0f} ${stddev_val:<11,.0f}")
                        elif 'Asset range distribution' in description:
                            total_accounts = sum(row[1] for row in results)
                            for row in results:
                                asset_range, count, avg_amount = row
                                percentage = (count / total_accounts) * 100 if total_accounts > 0 else 0
                                print(f"  {asset_range:<15}: {count:>6} accounts ({percentage:>5.1f}%) - Avg: ${avg_amount:>10,.0f}")
                        elif 'Total assets' in description and results:
                            total_assets, total_accounts, average_size = results[0]
                            print(f"  Total Assets: ${total_assets:,.0f}")
                            print(f"  Total Accounts: {total_accounts:,}")
                            print(f"  Average Account Size: ${average_size:,.0f}")
                        elif 'Constraint violations' in description:
                            violations_found = False
                            for row in results:
                                violation_type, count = row
                                if count > 0:
                                    print(f"  {violation_type}: {count} violations")
                                    violations_found = True
                            if not violations_found:
                                print("  No constraint violations found ✓")
                        else:
                            for row in results:
                                print(f"  {row}")
                    
                    print("\n" + "="*90)
        except Exception as e:
            print(f"Error during validation: {e}")

def main():
    """Main function to generate and insert fact account initial assets data."""
    # PostgreSQL connection configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'demo_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    
    try:
        generator = FactAccountInitialAssetsGenerator(**db_config)
        
        # Create table if not exists
        generator.create_table_if_not_exists()
        
        # Ask user if they want to clear existing data
        response = input("Do you want to clear existing fact account initial assets data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()
        
        # Generate initial assets data
        initial_assets = generator.generate_initial_assets_data()
        
        # Insert data into database
        generator.insert_initial_assets_data(initial_assets)
        
        # Validate inserted data
        generator.validate_data()
        
        print(f"\nFact account initial assets data generation completed successfully!")
        print(f"Generated {len(initial_assets)} initial asset records")
        print(f"Data is ready for fact_account_monthly table generation")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Please check your database configuration and ensure PostgreSQL is running.")
        print("Ensure you have run create_account.py first to create account data.")

if __name__ == "__main__":
    main()