import psycopg2
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any
import os
from urllib.parse import urlparse

class FactHouseholdMonthlyGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the fact household monthly generator with PostgreSQL database connection.
        
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
        
        # Asset range buckets from schema
        self.asset_ranges = [
            {'name': '$0 – $100k', 'min': 0, 'max': 100000},
            {'name': '$100k – $250k', 'min': 100000, 'max': 250000},
            {'name': '$250k – $500k', 'min': 250000, 'max': 500000},
            {'name': '$500k – $1M', 'min': 500000, 'max': 1000000},
            {'name': '$1M – $5M', 'min': 1000000, 'max': 5000000},
            {'name': '$5M – $10M', 'min': 5000000, 'max': 10000000},
            {'name': '$10M+', 'min': 10000000, 'max': float('inf')}
        ]
        
        # High net worth threshold from schema
        self.hnw_threshold = 1000000  # $1M
        
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
    
    def _determine_asset_range_bucket(self, household_assets: float) -> str:
        """Determine asset range bucket based on household assets."""
        for asset_range in self.asset_ranges:
            if asset_range['min'] <= household_assets < asset_range['max']:
                return asset_range['name']
        
        # Fallback to highest bucket if assets exceed all ranges
        return self.asset_ranges[-1]['name']
    
    def _calculate_high_net_worth_flag(self, household_assets: float) -> bool:
        """Calculate high net worth flag based on assets >= $1M."""
        return household_assets >= self.hnw_threshold
    
    def generate_household_monthly_data(self) -> List[Dict[str, Any]]:
        """Generate household monthly data by aggregating from fact_account_monthly."""
        print("Aggregating household data from fact_account_monthly...")
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    # Aggregate account data by household and snapshot date
                    aggregation_query = """
                    SELECT 
                        fam.snapshot_date,
                        fam.household_key,
                        SUM(fam.account_assets) as household_assets,
                        SUM(fam.account_net_flow) as household_net_flow,
                        COUNT(*) as account_count
                    FROM fact_account_monthly fam
                    GROUP BY fam.snapshot_date, fam.household_key
                    ORDER BY fam.snapshot_date, fam.household_key
                    """
                    
                    cursor.execute(aggregation_query)
                    results = cursor.fetchall()
                    
                    if not results:
                        raise Exception("No data found in fact_account_monthly. Please run create_fact_account_monthly.py first.")
                    
                    household_monthly_data = []
                    
                    print(f"Processing {len(results)} household-month combinations...")
                    
                    for i, row in enumerate(results):
                        if i % 5000 == 0 and i > 0:
                            print(f"Processed {i} household-month records...")
                        
                        snapshot_date, household_key, household_assets, household_net_flow, account_count = row
                        
                        # Convert to float for calculations
                        household_assets = float(household_assets) if household_assets else 0.0
                        household_net_flow = float(household_net_flow) if household_net_flow else 0.0
                        
                        # Determine asset range bucket
                        asset_range_bucket = self._determine_asset_range_bucket(household_assets)
                        
                        # Calculate high net worth flag
                        high_net_worth_flag = self._calculate_high_net_worth_flag(household_assets)
                        
                        household_record = {
                            'snapshot_date': snapshot_date,
                            'household_key': household_key,
                            'household_assets': household_assets,
                            'asset_range_bucket': asset_range_bucket,
                            'high_net_worth_flag': high_net_worth_flag,
                            'household_net_flow': household_net_flow
                        }
                        
                        household_monthly_data.append(household_record)
                    
                    print(f"Generated {len(household_monthly_data)} household monthly records")
                    return household_monthly_data
                    
        except Exception as e:
            print(f"Error generating household monthly data: {e}")
            raise
    
    def generate_fact_household_monthly_data(self) -> List[Dict[str, Any]]:
        """Alias for generate_household_monthly_data() for compatibility with notebook."""
        return self.generate_household_monthly_data()
    
    def create_table_if_not_exists(self):
        """Create fact_household_monthly table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fact_household_monthly (
            snapshot_date DATE NOT NULL,
            household_key INTEGER NOT NULL,
            household_assets DECIMAL(15,2) NOT NULL,
            asset_range_bucket VARCHAR(20) NOT NULL,
            high_net_worth_flag BOOLEAN NOT NULL,
            household_net_flow DECIMAL(15,2) NOT NULL,
            PRIMARY KEY (snapshot_date, household_key),
            FOREIGN KEY (household_key) REFERENCES household(household_key),
            CONSTRAINT check_household_assets_positive CHECK (household_assets >= 0)
        );
        
        -- Create indexes for efficient queries
        CREATE INDEX IF NOT EXISTS ix_fact_household_monthly_snapshot 
        ON fact_household_monthly(snapshot_date);
        
        CREATE INDEX IF NOT EXISTS ix_fact_household_monthly_household 
        ON fact_household_monthly(household_key);
        
        CREATE INDEX IF NOT EXISTS ix_fact_household_monthly_assets 
        ON fact_household_monthly(household_assets);
        
        CREATE INDEX IF NOT EXISTS ix_fact_household_monthly_range 
        ON fact_household_monthly(asset_range_bucket);
        
        CREATE INDEX IF NOT EXISTS ix_fact_household_monthly_hnw 
        ON fact_household_monthly(high_net_worth_flag);
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    conn.commit()
                    print("Fact household monthly table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating fact household monthly table: {e}")
            raise
    
    def clear_existing_data(self):
        """Clear existing fact household monthly data."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM fact_household_monthly")
                    conn.commit()
                    print("Existing fact household monthly data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
    
    def insert_household_monthly_data(self, household_data: List[Dict[str, Any]], batch_size: int = 1000):
        """Insert household monthly data into PostgreSQL database."""
        print(f"Inserting {len(household_data)} household monthly records...")
        
        insert_sql = """
        INSERT INTO fact_household_monthly (
            snapshot_date, household_key, household_assets, asset_range_bucket, 
            high_net_worth_flag, household_net_flow
        ) VALUES (
            %(snapshot_date)s, %(household_key)s, %(household_assets)s, %(asset_range_bucket)s,
            %(high_net_worth_flag)s, %(household_net_flow)s
        )
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    for i in range(0, len(household_data), batch_size):
                        batch = household_data[i:i + batch_size]
                        cursor.executemany(insert_sql, batch)
                        
                        if (i // batch_size + 1) % 10 == 0:
                            print(f"Inserted {min(i + batch_size, len(household_data))} records...")
                    
                    conn.commit()
                    print(f"Successfully inserted all {len(household_data)} household monthly records")
        except Exception as e:
            print(f"Error inserting household monthly data: {e}")
            raise
    
    def insert_fact_household_monthly_data(self, household_data: List[Dict[str, Any]], batch_size: int = 1000):
        """Alias for insert_household_monthly_data() for compatibility with notebook."""
        return self.insert_household_monthly_data(household_data, batch_size)
    
    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total household monthly records': "SELECT COUNT(*) FROM fact_household_monthly",
            'Distinct snapshot dates': "SELECT COUNT(DISTINCT snapshot_date) FROM fact_household_monthly",
            'Distinct households': "SELECT COUNT(DISTINCT household_key) FROM fact_household_monthly",
            'Records per month': """
                SELECT snapshot_date, COUNT(*) as household_count
                FROM fact_household_monthly
                GROUP BY snapshot_date
                ORDER BY snapshot_date
            """,
            'Asset range distribution': """
                SELECT 
                    asset_range_bucket,
                    COUNT(*) as household_count,
                    ROUND(AVG(household_assets), 0) as avg_assets,
                    ROUND(SUM(household_assets), 0) as total_assets
                FROM fact_household_monthly
                GROUP BY asset_range_bucket
                ORDER BY MIN(household_assets)
            """,
            'High net worth statistics': """
                SELECT 
                    high_net_worth_flag,
                    COUNT(*) as household_count,
                    ROUND(AVG(household_assets), 0) as avg_assets,
                    ROUND(MIN(household_assets), 0) as min_assets,
                    ROUND(MAX(household_assets), 0) as max_assets
                FROM fact_household_monthly
                GROUP BY high_net_worth_flag
                ORDER BY high_net_worth_flag
            """,
            'Asset statistics': """
                SELECT 
                    COUNT(*) as total_records,
                    ROUND(SUM(household_assets), 0) as total_aum,
                    ROUND(AVG(household_assets), 0) as avg_household_assets,
                    ROUND(MIN(household_assets), 0) as min_household_assets,
                    ROUND(MAX(household_assets), 0) as max_household_assets
                FROM fact_household_monthly
            """,
            'Net flow statistics': """
                SELECT 
                    ROUND(SUM(household_net_flow), 0) as total_net_flow,
                    ROUND(AVG(household_net_flow), 0) as avg_net_flow,
                    ROUND(MIN(household_net_flow), 0) as min_net_flow,
                    ROUND(MAX(household_net_flow), 0) as max_net_flow,
                    COUNT(CASE WHEN household_net_flow > 0 THEN 1 END) as positive_flow_count,
                    COUNT(CASE WHEN household_net_flow < 0 THEN 1 END) as negative_flow_count
                FROM fact_household_monthly
            """,
            'High net worth flag validation': """
                SELECT 
                    'Incorrect HNW flag' as validation_type,
                    COUNT(*) as count
                FROM fact_household_monthly
                WHERE (household_assets >= 1000000 AND high_net_worth_flag = false)
                   OR (household_assets < 1000000 AND high_net_worth_flag = true)
            """,
            'Asset aggregation validation': """
                SELECT 
                    fhm.snapshot_date,
                    fhm.household_key,
                    fhm.household_assets as household_total,
                    SUM(fam.account_assets) as account_sum,
                    ABS(fhm.household_assets - SUM(fam.account_assets)) as difference
                FROM fact_household_monthly fhm
                JOIN fact_account_monthly fam ON fhm.snapshot_date = fam.snapshot_date 
                                              AND fhm.household_key = fam.household_key
                GROUP BY fhm.snapshot_date, fhm.household_key, fhm.household_assets
                HAVING ABS(fhm.household_assets - SUM(fam.account_assets)) > 0.01
                LIMIT 10
            """
        }
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    print("\n" + "="*100)
                    print("FACT HOUSEHOLD MONTHLY DATA VALIDATION RESULTS")
                    print("="*100)
                    
                    for description, query in validation_queries.items():
                        cursor.execute(query)
                        results = cursor.fetchall()
                        
                        print(f"\n{description}:")
                        
                        if len(results) == 1 and len(results[0]) <= 3:
                            if len(results[0]) == 1:
                                print(f"  {results[0][0]}")
                            else:
                                for val in results[0]:
                                    print(f"  {val}")
                        elif 'Records per month' in description:
                            for row in results:
                                print(f"  {row[0]}: {row[1]:,} households")
                        elif 'Asset range distribution' in description:
                            total_households = sum(row[1] for row in results)
                            total_assets = sum(row[3] for row in results)
                            print(f"  {'Range':<15} {'Count':<10} {'%':<6} {'Avg Assets':<15} {'Total Assets':<15} {'% of AUM':<8}")
                            print(f"  {'-'*15} {'-'*10} {'-'*6} {'-'*15} {'-'*15} {'-'*8}")
                            for row in results:
                                asset_range, count, avg_assets, range_total = row
                                count_pct = (count / total_households) * 100 if total_households > 0 else 0
                                aum_pct = (range_total / total_assets) * 100 if total_assets > 0 else 0
                                print(f"  {asset_range:<15} {count:<10,} {count_pct:<5.1f}% ${avg_assets:<14,} ${range_total:<14,} {aum_pct:<7.1f}%")
                        elif 'High net worth statistics' in description:
                            for row in results:
                                hnw_flag, count, avg_assets, min_assets, max_assets = row
                                status = "HNW" if hnw_flag else "Non-HNW"
                                print(f"  {status}: {count:,} households, avg ${avg_assets:,}, range ${min_assets:,} - ${max_assets:,}")
                        elif 'Asset statistics' in description and results:
                            total_records, total_aum, avg_assets, min_assets, max_assets = results[0]
                            print(f"  Total Records: {total_records:,}")
                            print(f"  Total AUM: ${total_aum:,}")
                            print(f"  Average Household Assets: ${avg_assets:,}")
                            print(f"  Min Household Assets: ${min_assets:,}")
                            print(f"  Max Household Assets: ${max_assets:,}")
                        elif 'Net flow statistics' in description and results:
                            total_flow, avg_flow, min_flow, max_flow, pos_count, neg_count = results[0]
                            print(f"  Total Net Flow: ${total_flow:,}")
                            print(f"  Average Net Flow: ${avg_flow:,}")
                            print(f"  Flow Range: ${min_flow:,} to ${max_flow:,}")
                            print(f"  Positive Flows: {pos_count:,} households")
                            print(f"  Negative Flows: {neg_count:,} households")
                        elif 'validation' in description:
                            if not results or (len(results) == 1 and results[0][1] == 0):
                                print("  No validation errors found ✓")
                            else:
                                for row in results:
                                    if len(row) >= 2:
                                        print(f"  {row[0]}: {row[1]} issues")
                                    else:
                                        print(f"  {row}")
                        else:
                            for row in results:
                                print(f"  {row}")
                    
                    print("\n" + "="*100)
        except Exception as e:
            print(f"Error during validation: {e}")

def main():
    """Main function to generate and insert fact household monthly data."""
    # PostgreSQL connection configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'demo_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    
    try:
        generator = FactHouseholdMonthlyGenerator(**db_config)
        
        # Create table if not exists
        generator.create_table_if_not_exists()
        
        # Ask user if they want to clear existing data
        response = input("Do you want to clear existing fact household monthly data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()
        
        # Generate household monthly data
        household_data = generator.generate_household_monthly_data()
        
        # Insert data into database
        generator.insert_household_monthly_data(household_data)
        
        # Validate inserted data
        generator.validate_data()
        
        print(f"\nFact household monthly data generation completed successfully!")
        print(f"Generated {len(household_data)} household monthly records")
        print(f"Data aggregated from fact_account_monthly table")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Please check your database configuration and ensure PostgreSQL is running.")
        print("Ensure you have run create_fact_account_monthly.py first.")

# Alias for compatibility with notebook
FactHouseholdMonthlyDataGenerator = FactHouseholdMonthlyGenerator

if __name__ == "__main__":
    main()