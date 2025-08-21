import psycopg2
import random
import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
import os
from urllib.parse import urlparse
import numpy as np

class AdvisorDataGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the advisor data generator with PostgreSQL database connection.
        
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
        self.target_advisor_count = 500
        
        # Firm name generation patterns
        self.firm_prefixes = [
            'Summit', 'Harbor', 'Granite', 'Cedar', 'Crescent',
            'Atlas', 'Pioneer', 'Ridge', 'Oak', 'River'
        ]
        
        self.firm_suffixes = [
            'Capital', 'Advisors', 'Partners', 'Wealth', 'Financial'
        ]
        
        # Distribution configurations
        self.affiliation_model_dist = {
            'RIA': 0.35,
            'Hybrid RIA': 0.20,
            'Independent BD': 0.18,
            'Broker-Dealer W-2': 0.12,
            'Wirehouse': 0.07,
            'Bank/Trust': 0.05,
            'Insurance BD': 0.03
        }
        
        self.advisor_role_dist = {
            'Lead Advisor': 0.45,
            'Associate Advisor': 0.20,
            'Relationship Manager': 0.15,
            'Portfolio Manager': 0.10,
            'Client Service Associate': 0.10
        }
        
        self.status_dist = {
            'Active': 0.92,
            'Terminated': 0.08
        }
        
        self.practice_segment_dist = {
            'Solo Practice': 0.22,
            'Small Team': 0.36,
            'Ensemble': 0.28,
            'Enterprise': 0.14
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
    
    def _weighted_choice(self, choices: Dict[str, float]) -> str:
        """Select a random choice based on weighted distribution."""
        choices_list = list(choices.keys())
        weights = list(choices.values())
        return np.random.choice(choices_list, p=weights)
    
    def _generate_firm_name(self) -> str:
        """Generate fake firm name according to schema pattern."""
        prefix = random.choice(self.firm_prefixes)
        suffix = random.choice(self.firm_suffixes)
        return f"{prefix} {suffix} LLC"
    
    def _generate_advisor_tenure(self) -> int:
        """Generate advisor tenure (1-40 years, uniform distribution)."""
        return random.randint(1, 40)
    
    def _should_terminate_recently(self) -> bool:
        """Determine if advisor should be terminated in last 24 months (10% chance)."""
        return random.random() < 0.10
    
    def _generate_termination_date(self) -> date:
        """Generate termination date within last 24 months."""
        twenty_four_months_ago = self.current_date - timedelta(days=730)  # 24 months
        days_since = random.randint(0, 730)
        return twenty_four_months_ago + timedelta(days=days_since)
    
    def _generate_from_date(self, tenure: int) -> date:
        """Generate from_date based on tenure."""
        return self.current_date - timedelta(days=tenure * 365)
    
    def generate_advisor_data(self) -> List[Dict[str, Any]]:
        """Generate advisor data according to schema specifications."""
        print(f"Generating {self.target_advisor_count} advisors...")
        
        advisors = []
        advisor_key = 1
        
        for advisor_id in range(1, self.target_advisor_count + 1):
            if advisor_id % 100 == 0:
                print(f"Generated {advisor_id} advisors...")
            
            # Generate basic advisor attributes
            tenure = self._generate_advisor_tenure()
            firm_name = self._generate_firm_name()
            affiliation_model = self._weighted_choice(self.affiliation_model_dist)
            advisor_role = self._weighted_choice(self.advisor_role_dist)
            practice_segment = self._weighted_choice(self.practice_segment_dist)
            from_date = self._generate_from_date(tenure)
            
            # Determine if this advisor will be terminated recently
            will_terminate = self._should_terminate_recently()
            
            if will_terminate:
                # Create initial active record
                active_advisor = {
                    'advisor_key': advisor_key,
                    'advisor_id': advisor_id,
                    'advisor_tenure': tenure,
                    'firm_name': firm_name,
                    'firm_affiliation_model': affiliation_model,
                    'advisor_role': advisor_role,
                    'advisor_status': 'Active',
                    'practice_segment': practice_segment,
                    'from_date': from_date,
                    'to_date': None  # Will be set to termination date
                }
                
                # Generate termination date and create terminated record
                termination_date = self._generate_termination_date()
                active_advisor['to_date'] = termination_date
                
                terminated_advisor = active_advisor.copy()
                terminated_advisor['advisor_key'] = advisor_key + 1
                terminated_advisor['advisor_status'] = 'Terminated'
                terminated_advisor['from_date'] = termination_date
                terminated_advisor['to_date'] = date(9999, 12, 31)
                
                advisors.extend([active_advisor, terminated_advisor])
                advisor_key += 2
            else:
                # Create only one record (active or terminated)
                status = self._weighted_choice(self.status_dist)
                
                if status == 'Terminated':
                    # For older terminations, create a terminated record with earlier date
                    termination_date = self._generate_termination_date()
                    to_date = termination_date
                    from_date = min(from_date, termination_date - timedelta(days=365))  # At least 1 year before termination
                else:
                    to_date = date(9999, 12, 31)
                
                advisor = {
                    'advisor_key': advisor_key,
                    'advisor_id': advisor_id,
                    'advisor_tenure': tenure,
                    'firm_name': firm_name,
                    'firm_affiliation_model': affiliation_model,
                    'advisor_role': advisor_role,
                    'advisor_status': status,
                    'practice_segment': practice_segment,
                    'from_date': from_date,
                    'to_date': to_date
                }
                
                advisors.append(advisor)
                advisor_key += 1
        
        print(f"Generated {len(advisors)} advisor records (including SCD2 history)")
        return advisors
    
    def create_table_if_not_exists(self):
        """Create advisors table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS advisors (
            advisor_key SERIAL PRIMARY KEY,
            advisor_id INTEGER NOT NULL,
            advisor_tenure INTEGER NOT NULL,
            firm_name VARCHAR(100) NOT NULL,
            firm_affiliation_model VARCHAR(30) NOT NULL,
            advisor_role VARCHAR(30) NOT NULL,
            advisor_status VARCHAR(20) NOT NULL,
            practice_segment VARCHAR(20) NOT NULL,
            from_date DATE NOT NULL,
            to_date DATE NOT NULL
        );
        
        -- Create indexes as specified in schema
        CREATE UNIQUE INDEX IF NOT EXISTS ux_advisors_current 
        ON advisors(advisor_id) 
        WHERE to_date = DATE '9999-12-31';
        
        CREATE INDEX IF NOT EXISTS ix_advisor_id_window 
        ON advisors(advisor_id, from_date, to_date);
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    conn.commit()
                    print("Advisors table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating advisors table: {e}")
            raise
    
    def create_advisor_payout_rate_table(self):
        """Create advisor_payout_rate table with data from schema."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS advisor_payout_rate (
            firm_affiliation_model VARCHAR(30) PRIMARY KEY,
            advisor_payout_rate DECIMAL(5,4) NOT NULL
        );
        """
        
        # Payout rates from schema
        payout_rates = [
            ('RIA', 0.78),
            ('Hybrid RIA', 0.70),
            ('Independent BD', 0.85),
            ('Broker-Dealer W-2', 0.45),
            ('Wirehouse', 0.42),
            ('Bank/Trust', 0.35),
            ('Insurance BD', 0.75)
        ]
        
        insert_sql = """
        INSERT INTO advisor_payout_rate (firm_affiliation_model, advisor_payout_rate)
        VALUES (%s, %s)
        ON CONFLICT (firm_affiliation_model) DO UPDATE 
        SET advisor_payout_rate = EXCLUDED.advisor_payout_rate
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    cursor.executemany(insert_sql, payout_rates)
                    conn.commit()
                    print("Advisor payout rate table created and populated successfully")
        except Exception as e:
            print(f"Error creating advisor payout rate table: {e}")
            raise
    
    def clear_existing_data(self):
        """Clear existing advisor data."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM advisors")
                    conn.commit()
                    print("Existing advisor data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
    
    def insert_advisor_data(self, advisors: List[Dict[str, Any]], batch_size: int = 1000):
        """Insert advisor data into PostgreSQL database."""
        print(f"Inserting {len(advisors)} advisor records...")
        
        insert_sql = """
        INSERT INTO advisors (
            advisor_key, advisor_id, advisor_tenure, firm_name, firm_affiliation_model,
            advisor_role, advisor_status, practice_segment, from_date, to_date
        ) VALUES (
            %(advisor_key)s, %(advisor_id)s, %(advisor_tenure)s, %(firm_name)s, %(firm_affiliation_model)s,
            %(advisor_role)s, %(advisor_status)s, %(practice_segment)s, %(from_date)s, %(to_date)s
        )
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    for i in range(0, len(advisors), batch_size):
                        batch = advisors[i:i + batch_size]
                        cursor.executemany(insert_sql, batch)
                        
                        if (i // batch_size + 1) % 5 == 0:
                            print(f"Inserted {min(i + batch_size, len(advisors))} records...")
                    
                    conn.commit()
                    print(f"Successfully inserted all {len(advisors)} advisor records")
        except Exception as e:
            print(f"Error inserting advisor data: {e}")
            raise
    
    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total advisor_id count': "SELECT COUNT(DISTINCT advisor_id) FROM advisors",
            'Current records count': "SELECT COUNT(*) FROM advisors WHERE to_date = '9999-12-31'",
            'Terminated in last 24 months': """
                SELECT COUNT(DISTINCT advisor_id) 
                FROM advisors 
                WHERE advisor_status = 'Terminated' 
                AND from_date >= %s
            """,
            'Firm affiliation distribution': """
                SELECT firm_affiliation_model, COUNT(*) as count
                FROM advisors 
                WHERE to_date = '9999-12-31'
                GROUP BY firm_affiliation_model 
                ORDER BY count DESC
            """,
            'Advisor role distribution': """
                SELECT advisor_role, COUNT(*) as count
                FROM advisors 
                WHERE to_date = '9999-12-31'
                GROUP BY advisor_role 
                ORDER BY count DESC
            """,
            'Status distribution': """
                SELECT advisor_status, COUNT(*) as count
                FROM advisors 
                WHERE to_date = '9999-12-31'
                GROUP BY advisor_status
            """,
            'Practice segment distribution': """
                SELECT practice_segment, COUNT(*) as count
                FROM advisors 
                WHERE to_date = '9999-12-31'
                GROUP BY practice_segment 
                ORDER BY count DESC
            """,
            'Sample firm names': """
                SELECT DISTINCT firm_name 
                FROM advisors 
                WHERE to_date = '9999-12-31'
                LIMIT 10
            """
        }
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    print("\n" + "="*50)
                    print("DATA VALIDATION RESULTS")
                    print("="*50)
                    
                    for description, query in validation_queries.items():
                        if 'last 24 months' in description:
                            twenty_four_months_ago = self.current_date - timedelta(days=730)
                            cursor.execute(query, (twenty_four_months_ago,))
                        else:
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
                                    print(f"  {row[0]}")
                    
                    print("\n" + "="*50)
        except Exception as e:
            print(f"Error during validation: {e}")

def main():
    """Main function to generate and insert advisor data."""
    # PostgreSQL connection configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'demo_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    
    try:
        generator = AdvisorDataGenerator(**db_config)
        
        # Create tables if not exists
        generator.create_table_if_not_exists()
        generator.create_advisor_payout_rate_table()
        
        # Ask user if they want to clear existing data
        response = input("Do you want to clear existing advisor data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()
        
        # Generate advisor data
        advisors = generator.generate_advisor_data()
        
        # Insert data into database
        generator.insert_advisor_data(advisors)
        
        # Validate inserted data
        generator.validate_data()
        
        print(f"\nAdvisor data generation completed successfully!")
        print(f"Generated {len(advisors)} total records for {generator.target_advisor_count} advisors")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Please check your database configuration and ensure PostgreSQL is running.")
        print("Set environment variables: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")

if __name__ == "__main__":
    main()