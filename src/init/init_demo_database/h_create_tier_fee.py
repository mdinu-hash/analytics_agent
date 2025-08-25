import psycopg2
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any
import os
from urllib.parse import urlparse

class TierFeeDataGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the tier fee data generator with PostgreSQL database connection.
        
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
        
        # Tier fee structure from schema (in basis points converted to decimal)
        self.tier_fees_by_business_line = {
            'Managed Portfolio': [
                {'tier_min_aum': 0, 'tier_max_aum': 1000000, 'tier_fee_pct': 1},        # 1%
                {'tier_min_aum': 1000000, 'tier_max_aum': 5000000, 'tier_fee_pct': 1},   # 1%
                {'tier_min_aum': 5000000, 'tier_max_aum': 999999999999, 'tier_fee_pct': 1}  # 1%
            ],
            'Separately Managed Account': [
                {'tier_min_aum': 0, 'tier_max_aum': 1000000, 'tier_fee_pct': 1},        # 1%
                {'tier_min_aum': 1000000, 'tier_max_aum': 5000000, 'tier_fee_pct': 1},   # 1%
                {'tier_min_aum': 5000000, 'tier_max_aum': 999999999999, 'tier_fee_pct': 1}  # 1%
            ],
            'Mutual Fund Wrap': [
                {'tier_min_aum': 0, 'tier_max_aum': 1000000, 'tier_fee_pct': 1},        # 1%
                {'tier_min_aum': 1000000, 'tier_max_aum': 5000000, 'tier_fee_pct': 1},   # 1%
                {'tier_min_aum': 5000000, 'tier_max_aum': 999999999999, 'tier_fee_pct': 1}  # 1%
            ],
            'Annuity': [
                {'tier_min_aum': 0, 'tier_max_aum': 999999999999, 'tier_fee_pct': 1}   # 1%
            ],
            'Cash': [
                {'tier_min_aum': 0, 'tier_max_aum': 1000000, 'tier_fee_pct': 1},        # 1%
                {'tier_min_aum': 1000000, 'tier_max_aum': 999999999999, 'tier_fee_pct': 1}  # 1%
            ]
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
    
    def _get_business_line_keys(self) -> Dict[str, int]:
        """Get business line keys from the database."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT business_line_key, business_line_name FROM business_line")
                    business_lines = {row[1]: row[0] for row in cursor.fetchall()}
                    
                    if not business_lines:
                        raise Exception("No business lines found. Please run create_business_line.py first.")
                    
                    print(f"Found {len(business_lines)} business lines")
                    return business_lines
                    
        except Exception as e:
            print(f"Error fetching business line keys: {e}")
            raise
    
    def generate_tier_fee_data(self) -> List[Dict[str, Any]]:
        """Generate tier fee data according to schema specifications."""
        business_line_keys = self._get_business_line_keys()
        
        tier_fees = []
        tier_fee_id = 1
        
        print("Generating tier fee structure...")
        
        for business_line_name, fee_tiers in self.tier_fees_by_business_line.items():
            business_line_key = business_line_keys.get(business_line_name)
            
            if not business_line_key:
                print(f"Warning: Business line '{business_line_name}' not found in database")
                continue
            
            print(f"Creating {len(fee_tiers)} fee tiers for {business_line_name}")
            
            for tier in fee_tiers:
                tier_fee = {
                    'tier_fee_id': tier_fee_id,
                    'business_line_key': business_line_key,
                    'business_line_name': business_line_name,  # For display purposes
                    'tier_min_aum': tier['tier_min_aum'],
                    'tier_max_aum': tier['tier_max_aum'],
                    'tier_fee_pct': tier['tier_fee_pct']
                }
                
                tier_fees.append(tier_fee)
                tier_fee_id += 1
        
        print(f"Generated {len(tier_fees)} tier fee records")
        return tier_fees
    
    def create_tier_fee_table(self):
        """Create tier_fee table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS tier_fee (
            tier_fee_id SERIAL PRIMARY KEY,
            business_line_key INTEGER NOT NULL,
            tier_min_aum DECIMAL(15,2) NOT NULL,
            tier_max_aum DECIMAL(15,2) NOT NULL,
            tier_fee_pct INTEGER NOT NULL,
            FOREIGN KEY (business_line_key) REFERENCES business_line(business_line_key),
            CONSTRAINT check_tier_range CHECK (tier_min_aum <= tier_max_aum),
            CONSTRAINT check_fee_positive CHECK (tier_fee_pct >= 0)
        );
        
        -- Create indexes for efficient lookups during fee calculations
        CREATE INDEX IF NOT EXISTS ix_tier_fee_lookup 
        ON tier_fee(business_line_key, tier_min_aum, tier_max_aum);
        
        CREATE INDEX IF NOT EXISTS ix_tier_fee_business_line 
        ON tier_fee(business_line_key);
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
        """Clear existing tier fee data."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM tier_fee")
                    conn.commit()
                    print("Existing tier fee data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
    
    def insert_tier_fee_data(self, tier_fees: List[Dict[str, Any]]):
        """Insert tier fee data into PostgreSQL database."""
        print(f"Inserting {len(tier_fees)} tier fee records...")
        
        insert_sql = """
        INSERT INTO tier_fee (
            business_line_key, tier_min_aum, tier_max_aum, tier_fee_pct
        ) VALUES (
            %(business_line_key)s, %(tier_min_aum)s, %(tier_max_aum)s, %(tier_fee_pct)s
        )
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(insert_sql, tier_fees)
                    conn.commit()
                    print(f"Successfully inserted all {len(tier_fees)} tier fee records")
        except Exception as e:
            print(f"Error inserting tier fee data: {e}")
            raise
    
    def test_fee_calculation(self, test_amounts: List[float] = None):
        """Test fee calculation logic with sample AUM amounts."""
        if test_amounts is None:
            test_amounts = [50000, 500000, 1500000, 3000000, 7500000, 15000000]
        
        print(f"\nTesting fee calculation for sample AUM amounts...")
        print("=" * 80)
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    # Get business line names for testing
                    cursor.execute("SELECT business_line_key, business_line_name FROM business_line ORDER BY business_line_key")
                    business_lines = cursor.fetchall()
                    
                    for bl_key, bl_name in business_lines:
                        print(f"\n{bl_name}:")
                        
                        for amount in test_amounts:
                            # Find applicable fee tier
                            cursor.execute("""
                                SELECT tier_fee_pct 
                                FROM tier_fee 
                                WHERE business_line_key = %s 
                                AND %s >= tier_min_aum 
                                AND %s < tier_max_aum
                                LIMIT 1
                            """, (bl_key, amount, amount))
                            
                            result = cursor.fetchone()
                            if result:
                                fee_pct = result[0]
                                annual_fee = amount * (fee_pct / 100)
                                print(f"  ${amount:,.0f} AUM → {fee_pct}% fee = ${annual_fee:,.0f} annual")
                            else:
                                print(f"  ${amount:,.0f} AUM → No fee tier found")
                    
        except Exception as e:
            print(f"Error during fee calculation test: {e}")
    
    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total tier fee records': "SELECT COUNT(*) FROM tier_fee",
            'Tier fees by business line': """
                SELECT bl.business_line_name, COUNT(tf.tier_fee_id) as tier_count
                FROM business_line bl
                LEFT JOIN tier_fee tf ON bl.business_line_key = tf.business_line_key
                GROUP BY bl.business_line_name, bl.business_line_key
                ORDER BY bl.business_line_key
            """,
            'Fee structure validation': """
                SELECT bl.business_line_name, 
                       tf.tier_min_aum, 
                       tf.tier_max_aum, 
                       tf.tier_fee_pct,
                       CASE 
                           WHEN tf.tier_max_aum = 999999999999 THEN 'No limit'
                           ELSE '$' || TO_CHAR(tf.tier_max_aum, 'FM999,999,999')
                       END as max_display
                FROM tier_fee tf
                JOIN business_line bl ON tf.business_line_key = bl.business_line_key
                ORDER BY bl.business_line_name, tf.tier_min_aum
            """,
            'Gap detection': """
                WITH tier_gaps AS (
                    SELECT bl.business_line_name,
                           tf1.tier_max_aum as current_max,
                           tf2.tier_min_aum as next_min,
                           CASE WHEN tf1.tier_max_aum != tf2.tier_min_aum THEN 'GAP' ELSE 'OK' END as gap_status
                    FROM tier_fee tf1
                    JOIN tier_fee tf2 ON tf1.business_line_key = tf2.business_line_key 
                                     AND tf1.tier_min_aum < tf2.tier_min_aum
                    JOIN business_line bl ON tf1.business_line_key = bl.business_line_key
                    WHERE tf1.tier_max_aum < 999999999999
                )
                SELECT business_line_name, COUNT(*) as gaps
                FROM tier_gaps 
                WHERE gap_status = 'GAP'
                GROUP BY business_line_name
            """,
            'Overlapping tiers check': """
                SELECT bl.business_line_name, COUNT(*) as overlaps
                FROM tier_fee tf1
                JOIN tier_fee tf2 ON tf1.business_line_key = tf2.business_line_key 
                                 AND tf1.tier_fee_id != tf2.tier_fee_id
                                 AND tf1.tier_min_aum < tf2.tier_max_aum 
                                 AND tf1.tier_max_aum > tf2.tier_min_aum
                JOIN business_line bl ON tf1.business_line_key = bl.business_line_key
                GROUP BY bl.business_line_name
            """
        }
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    print("\n" + "="*80)
                    print("TIER FEE DATA VALIDATION RESULTS")
                    print("="*80)
                    
                    for description, query in validation_queries.items():
                        cursor.execute(query)
                        results = cursor.fetchall()
                        
                        print(f"\n{description}:")
                        
                        if len(results) == 1 and len(results[0]) == 1:
                            print(f"  {results[0][0]}")
                        elif 'Fee structure validation' in description:
                            current_bl = None
                            for row in results:
                                bl_name, min_aum, max_aum, fee_pct, max_display = row
                                if bl_name != current_bl:
                                    print(f"\n  {bl_name}:")
                                    current_bl = bl_name
                                min_display = f"${min_aum:,.0f}" if min_aum > 0 else "$0"
                                print(f"    {min_display} - {max_display}: {fee_pct}%")
                        elif not results:
                            print("  No issues found ✓")
                        else:
                            for row in results:
                                if len(row) == 2:
                                    print(f"  {row[0]}: {row[1]}")
                                else:
                                    print(f"  {row}")
                    
                    print("\n" + "="*80)
        except Exception as e:
            print(f"Error during validation: {e}")

def main():
    """Main function to generate and insert tier fee data."""
    # PostgreSQL connection configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'demo_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    
    try:
        generator = TierFeeDataGenerator(**db_config)
        
        # Create table if not exists
        generator.create_tier_fee_table()
        
        # Ask user if they want to clear existing data
        response = input("Do you want to clear existing tier fee data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()
        
        # Generate tier fee data
        tier_fees = generator.generate_tier_fee_data()
        
        # Insert data into database
        generator.insert_tier_fee_data(tier_fees)
        
        # Validate inserted data
        generator.validate_data()
        
        # Test fee calculation logic
        generator.test_fee_calculation()
        
        print(f"\nTier fee data generation completed successfully!")
        print(f"Generated {len(tier_fees)} tier fee records across all business lines")
        print(f"Fee structure is ready for revenue calculations")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Please check your database configuration and ensure PostgreSQL is running.")
        print("Ensure you have run create_business_line.py first to create business line lookup data.")

if __name__ == "__main__":
    main()