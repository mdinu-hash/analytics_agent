import psycopg2
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any
import os
from urllib.parse import urlparse

class AdvisorPayoutRateDataGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the advisor payout rate data generator with PostgreSQL database connection.
        
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
        
        # Advisor payout rates by firm affiliation model from schema
        self.payout_rates = {
            'RIA': 0.78,                    # 78%
            'Hybrid RIA': 0.70,             # 70%
            'Independent BD': 0.85,         # 85%
            'Broker-Dealer W-2': 0.45,      # 45%
            'Wirehouse': 0.42,              # 42%
            'Bank/Trust': 0.35,             # 35%
            'Insurance BD': 0.75            # 75%
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
    
    def _get_advisor_firm_models(self) -> List[str]:
        """Get distinct firm affiliation models from advisors table."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT DISTINCT firm_affiliation_model FROM advisors ORDER BY firm_affiliation_model")
                    firm_models = [row[0] for row in cursor.fetchall()]
                    
                    if not firm_models:
                        print("Warning: No advisors found. Using all defined firm affiliation models from schema.")
                        firm_models = list(self.payout_rates.keys())
                    
                    print(f"Found {len(firm_models)} distinct firm affiliation models")
                    return firm_models
                    
        except Exception as e:
            print(f"Error fetching firm affiliation models: {e}")
            # Fallback to all models from schema
            return list(self.payout_rates.keys())
    
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
            firm_affiliation_model VARCHAR(30) PRIMARY KEY,
            advisor_payout_rate DECIMAL(5,4) NOT NULL,
            CONSTRAINT check_payout_rate_valid CHECK (advisor_payout_rate >= 0 AND advisor_payout_rate <= 1)
        );
        
        -- Create index for efficient lookups during revenue calculations
        CREATE INDEX IF NOT EXISTS ix_advisor_payout_firm 
        ON advisor_payout_rate(firm_affiliation_model);
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    conn.commit()
                    print("Advisor payout rate table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating advisor payout rate table: {e}")
            raise
    
    def clear_existing_data(self):
        """Clear existing advisor payout rate data."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM advisor_payout_rate")
                    conn.commit()
                    print("Existing advisor payout rate data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
    
    def insert_payout_rate_data(self, payout_rates: List[Dict[str, Any]]):
        """Insert advisor payout rate data into PostgreSQL database."""
        print(f"Inserting {len(payout_rates)} payout rate records...")
        
        insert_sql = """
        INSERT INTO advisor_payout_rate (firm_affiliation_model, advisor_payout_rate)
        VALUES (%(firm_affiliation_model)s, %(advisor_payout_rate)s)
        ON CONFLICT (firm_affiliation_model) DO UPDATE 
        SET advisor_payout_rate = EXCLUDED.advisor_payout_rate
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(insert_sql, payout_rates)
                    conn.commit()
                    print(f"Successfully inserted all {len(payout_rates)} payout rate records")
        except Exception as e:
            print(f"Error inserting payout rate data: {e}")
            raise
    
    def insert_advisor_payout_rate_data(self, payout_rates: List[Dict[str, Any]]):
        """Alias for insert_payout_rate_data() for compatibility with notebook."""
        return self.insert_payout_rate_data(payout_rates)
    
    def test_payout_calculation(self, test_revenues: List[float] = None):
        """Test payout calculation logic with sample revenue amounts."""
        if test_revenues is None:
            test_revenues = [1000, 5000, 10000, 25000, 50000, 100000]
        
        print(f"\nTesting advisor payout calculation for sample gross revenues...")
        print("=" * 90)
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    # Get all payout rates
                    cursor.execute("""
                        SELECT firm_affiliation_model, advisor_payout_rate 
                        FROM advisor_payout_rate 
                        ORDER BY advisor_payout_rate DESC
                    """)
                    
                    payout_rates = cursor.fetchall()
                    
                    print(f"{'Firm Affiliation Model':<25} {'Payout Rate':<12} {'Sample Calculations'}")
                    print("-" * 90)
                    
                    for firm_model, payout_rate in payout_rates:
                        sample_revenue = 10000  # $10K sample
                        advisor_payout = sample_revenue * payout_rate
                        firm_retention = sample_revenue * (1 - payout_rate)
                        
                        print(f"{firm_model:<25} {payout_rate:.1%:<12} ${sample_revenue:,} gross → ${advisor_payout:,.0f} advisor, ${firm_retention:,.0f} firm")
                    
                    print("\nDetailed payout examples:")
                    print("-" * 90)
                    
                    for revenue in test_revenues:
                        print(f"\nGross Revenue: ${revenue:,}")
                        for firm_model, payout_rate in payout_rates:
                            advisor_payout = revenue * payout_rate
                            print(f"  {firm_model}: ${advisor_payout:,.0f} ({payout_rate:.1%})")
                    
        except Exception as e:
            print(f"Error during payout calculation test: {e}")
    
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
            'Payout rate statistics': """
                SELECT 
                    MIN(advisor_payout_rate) as min_rate,
                    MAX(advisor_payout_rate) as max_rate,
                    AVG(advisor_payout_rate) as avg_rate,
                    STDDEV(advisor_payout_rate) as stddev_rate
                FROM advisor_payout_rate
            """,
            'Coverage check - advisors without payout rates': """
                SELECT DISTINCT a.firm_affiliation_model
                FROM advisors a
                LEFT JOIN advisor_payout_rate apr ON a.firm_affiliation_model = apr.firm_affiliation_model
                WHERE apr.firm_affiliation_model IS NULL
            """,
            'Coverage check - unused payout rates': """
                SELECT apr.firm_affiliation_model
                FROM advisor_payout_rate apr
                LEFT JOIN advisors a ON apr.firm_affiliation_model = a.firm_affiliation_model
                WHERE a.firm_affiliation_model IS NULL
            """,
            'Advisor count by firm affiliation': """
                SELECT a.firm_affiliation_model, 
                       COUNT(*) as advisor_count,
                       apr.advisor_payout_rate
                FROM advisors a
                JOIN advisor_payout_rate apr ON a.firm_affiliation_model = apr.firm_affiliation_model
                WHERE a.to_date = '9999-12-31'
                GROUP BY a.firm_affiliation_model, apr.advisor_payout_rate
                ORDER BY apr.advisor_payout_rate DESC
            """
        }
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
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
                        elif 'statistics' in description and results:
                            min_rate, max_rate, avg_rate, stddev_rate = results[0]
                            print(f"  Min: {min_rate:.1%}, Max: {max_rate:.1%}, Avg: {avg_rate:.1%}, StdDev: {stddev_rate:.3f}")
                        elif 'Advisor count by firm' in description:
                            total_advisors = sum(row[1] for row in results)
                            print(f"  Total active advisors: {total_advisors}")
                            for row in results:
                                firm_model, count, payout_rate = row
                                percentage = (count / total_advisors) * 100 if total_advisors > 0 else 0
                                print(f"  {firm_model}: {count} advisors ({percentage:.1f}%) at {payout_rate:.1%} payout")
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

def main():
    """Main function to generate and insert advisor payout rate data."""
    # PostgreSQL connection configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'demo_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    
    try:
        generator = AdvisorPayoutRateDataGenerator(**db_config)
        
        # Create table if not exists
        generator.create_table_if_not_exists()
        
        # Ask user if they want to clear existing data
        response = input("Do you want to clear existing advisor payout rate data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()
        
        # Generate payout rate data
        payout_rates = generator.generate_payout_rate_data()
        
        # Insert data into database
        generator.insert_payout_rate_data(payout_rates)
        
        # Validate inserted data
        generator.validate_data()
        
        # Test payout calculation logic
        generator.test_payout_calculation()
        
        print(f"\nAdvisor payout rate data generation completed successfully!")
        print(f"Generated {len(payout_rates)} payout rate records")
        print(f"Payout rates are ready for revenue calculations")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Please check your database configuration and ensure PostgreSQL is running.")
        print("You may want to run create_advisors.py first to populate firm affiliation models.")

if __name__ == "__main__":
    main()