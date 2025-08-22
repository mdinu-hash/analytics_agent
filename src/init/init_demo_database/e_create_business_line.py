import psycopg2
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any
import os
from urllib.parse import urlparse

class BusinessLineDataGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the business line data generator with PostgreSQL database connection.
        
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
        
        # Business lines from schema
        self.business_lines = [
            'Managed Portfolio',
            'Separately Managed Account', 
            'Mutual Fund Wrap',
            'Annuity',
            'Cash'
        ]
        
        # Tier fee structure from schema
        self.tier_fees = [
            # Managed Portfolio
            {'business_line_name': 'Managed Portfolio', 'tier_min_aum': 0, 'tier_max_aum': 1000000, 'tier_fee_pct': 0.90},
            {'business_line_name': 'Managed Portfolio', 'tier_min_aum': 1000000, 'tier_max_aum': 5000000, 'tier_fee_pct': 0.75},
            {'business_line_name': 'Managed Portfolio', 'tier_min_aum': 5000000, 'tier_max_aum': 999999999999, 'tier_fee_pct': 0.55},
            
            # Separately Managed Account (SMA)
            {'business_line_name': 'Separately Managed Account', 'tier_min_aum': 0, 'tier_max_aum': 1000000, 'tier_fee_pct': 1.10},
            {'business_line_name': 'Separately Managed Account', 'tier_min_aum': 1000000, 'tier_max_aum': 5000000, 'tier_fee_pct': 0.90},
            {'business_line_name': 'Separately Managed Account', 'tier_min_aum': 5000000, 'tier_max_aum': 999999999999, 'tier_fee_pct': 0.70},
            
            # Mutual Fund Wrap
            {'business_line_name': 'Mutual Fund Wrap', 'tier_min_aum': 0, 'tier_max_aum': 1000000, 'tier_fee_pct': 0.75},
            {'business_line_name': 'Mutual Fund Wrap', 'tier_min_aum': 1000000, 'tier_max_aum': 5000000, 'tier_fee_pct': 0.60},
            {'business_line_name': 'Mutual Fund Wrap', 'tier_min_aum': 5000000, 'tier_max_aum': 999999999999, 'tier_fee_pct': 0.45},
            
            # Annuity
            {'business_line_name': 'Annuity', 'tier_min_aum': 0, 'tier_max_aum': 999999999999, 'tier_fee_pct': 0.25},
            
            # Cash (sweep programs are low and often flat/near-flat)
            {'business_line_name': 'Cash', 'tier_min_aum': 0, 'tier_max_aum': 1000000, 'tier_fee_pct': 0.10},
            {'business_line_name': 'Cash', 'tier_min_aum': 1000000, 'tier_max_aum': 999999999999, 'tier_fee_pct': 0.05}
        ]
        
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
    
    def generate_business_line_data(self) -> List[Dict[str, Any]]:
        """Generate business line data according to schema specifications."""
        print(f"Generating {len(self.business_lines)} business lines...")
        
        business_line_data = []
        for key, name in enumerate(self.business_lines, 1):
            business_line_data.append({
                'business_line_key': key,
                'business_line_name': name
            })
        
        print(f"Generated {len(business_line_data)} business line records")
        return business_line_data
    
    def create_business_line_table(self):
        """Create business_line table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS business_line (
            business_line_key SERIAL PRIMARY KEY,
            business_line_name VARCHAR(50) NOT NULL UNIQUE
        );
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    conn.commit()
                    print("Business line table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating business line table: {e}")
            raise
    
    def create_tier_fee_table(self):
        """Create tier_fee table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS tier_fee (
            tier_fee_id SERIAL PRIMARY KEY,
            business_line_key INTEGER NOT NULL,
            tier_min_aum DECIMAL(15,2) NOT NULL,
            tier_max_aum DECIMAL(15,2) NOT NULL,
            tier_fee_pct DECIMAL(5,4) NOT NULL,
            FOREIGN KEY (business_line_key) REFERENCES business_line(business_line_key)
        );
        
        -- Create index for efficient lookups
        CREATE INDEX IF NOT EXISTS ix_tier_fee_lookup 
        ON tier_fee(business_line_key, tier_min_aum, tier_max_aum);
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    conn.commit()
                    print("Tier fee table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating tier fee table: {e}")
            raise
    
    def clear_existing_data(self):
        """Clear existing business line and tier fee data."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    # Clear in correct order due to foreign key constraints
                    cursor.execute("DELETE FROM tier_fee")
                    cursor.execute("DELETE FROM business_line")
                    conn.commit()
                    print("Existing business line and tier fee data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
    
    def insert_business_line_data(self, business_lines: List[Dict[str, Any]]):
        """Insert business line data into PostgreSQL database."""
        print(f"Inserting {len(business_lines)} business line records...")
        
        insert_sql = """
        INSERT INTO business_line (business_line_key, business_line_name)
        VALUES (%(business_line_key)s, %(business_line_name)s)
        ON CONFLICT (business_line_name) DO UPDATE 
        SET business_line_key = EXCLUDED.business_line_key
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(insert_sql, business_lines)
                    conn.commit()
                    print(f"Successfully inserted all {len(business_lines)} business line records")
        except Exception as e:
            print(f"Error inserting business line data: {e}")
            raise
    
    def insert_tier_fee_data(self):
        """Insert tier fee data into PostgreSQL database."""
        print(f"Inserting {len(self.tier_fees)} tier fee records...")
        
        # First get business line keys
        get_keys_sql = "SELECT business_line_key, business_line_name FROM business_line"
        business_line_keys = {}
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(get_keys_sql)
                    for key, name in cursor.fetchall():
                        business_line_keys[name] = key
        except Exception as e:
            print(f"Error getting business line keys: {e}")
            raise
        
        # Prepare tier fee data with business line keys
        tier_fee_data = []
        for fee in self.tier_fees:
            business_line_key = business_line_keys.get(fee['business_line_name'])
            if business_line_key:
                tier_fee_data.append({
                    'business_line_key': business_line_key,
                    'tier_min_aum': fee['tier_min_aum'],
                    'tier_max_aum': fee['tier_max_aum'],
                    'tier_fee_pct': fee['tier_fee_pct']
                })
        
        insert_sql = """
        INSERT INTO tier_fee (business_line_key, tier_min_aum, tier_max_aum, tier_fee_pct)
        VALUES (%(business_line_key)s, %(tier_min_aum)s, %(tier_max_aum)s, %(tier_fee_pct)s)
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(insert_sql, tier_fee_data)
                    conn.commit()
                    print(f"Successfully inserted all {len(tier_fee_data)} tier fee records")
        except Exception as e:
            print(f"Error inserting tier fee data: {e}")
            raise
    
    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Business line count': "SELECT COUNT(*) FROM business_line",
            'Business lines': "SELECT business_line_key, business_line_name FROM business_line ORDER BY business_line_key",
            'Tier fee count': "SELECT COUNT(*) FROM tier_fee",
            'Tier fees by business line': """
                SELECT bl.business_line_name, COUNT(tf.tier_fee_id) as tier_count
                FROM business_line bl
                LEFT JOIN tier_fee tf ON bl.business_line_key = tf.business_line_key
                GROUP BY bl.business_line_name, bl.business_line_key
                ORDER BY bl.business_line_key
            """,
            'Sample tier fee structure': """
                SELECT bl.business_line_name, tf.tier_min_aum, tf.tier_max_aum, tf.tier_fee_pct
                FROM tier_fee tf
                JOIN business_line bl ON tf.business_line_key = bl.business_line_key
                ORDER BY bl.business_line_name, tf.tier_min_aum
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
                        if len(results) == 1 and len(results[0]) == 1:
                            print(f"  {results[0][0]}")
                        else:
                            for row in results:
                                if description == 'Business lines':
                                    print(f"  {row[0]}: {row[1]}")
                                elif description == 'Tier fees by business line':
                                    print(f"  {row[0]}: {row[1]} tiers")
                                elif description == 'Sample tier fee structure':
                                    min_aum = f"${row[1]:,.0f}" if row[1] < 999999999999 else "No limit"
                                    max_aum = f"${row[2]:,.0f}" if row[2] < 999999999999 else "No limit"
                                    print(f"  {row[0]}: {min_aum} - {max_aum} = {row[3]:.2f}%")
                                else:
                                    print(f"  {row}")
                    
                    print("\n" + "="*60)
        except Exception as e:
            print(f"Error during validation: {e}")

def main():
    """Main function to generate and insert business line data."""
    # PostgreSQL connection configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'demo_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    
    try:
        generator = BusinessLineDataGenerator(**db_config)
        
        # Create tables if not exists
        generator.create_business_line_table()
        generator.create_tier_fee_table()
        
        # Ask user if they want to clear existing data
        response = input("Do you want to clear existing business line and tier fee data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()
        
        # Generate and insert business line data
        business_lines = generator.generate_business_line_data()
        generator.insert_business_line_data(business_lines)
        
        # Insert tier fee data
        generator.insert_tier_fee_data()
        
        # Validate inserted data
        generator.validate_data()
        
        print(f"\nBusiness line and tier fee data generation completed successfully!")
        print(f"Generated {len(business_lines)} business lines and {len(generator.tier_fees)} tier fee records")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Please check your database configuration and ensure PostgreSQL is running.")
        print("Set environment variables: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")

if __name__ == "__main__":
    main()