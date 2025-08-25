import psycopg2
import random
import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
import os
from urllib.parse import urlparse
import numpy as np

class HouseholdDataGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the household data generator with PostgreSQL database connection.
        
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
        self.snapshot_start_date = date(2024, 9, 30)
        self.target_household_count = 5000
        
        # Distribution configurations
        self.registration_type_dist = {
            'Individual': 0.64,
            'Joint': 0.22,
            'Trust': 0.09,
            'Institutional': 0.05
        }
        
        self.segment_dist = {
            'Self-Directed': 0.15,
            'Advice-Seeking': 0.25,
            'Discretionary Managed': 0.30,
            'Retirement Income': 0.15,
            'Business/Institutional': 0.10,
            'Active Trader': 0.05
        }
        
        self.status_dist = {
            'Active': 0.90,
            'Terminated': 0.10
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
    
    def _get_advisor_ids(self) -> List[int]:
        """Get list of advisor IDs from the database."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT DISTINCT advisor_id FROM advisors")
                    advisor_ids = [row[0] for row in cursor.fetchall()]
                    if not advisor_ids:
                        # If no advisors exist, create sample advisor IDs
                        print("Warning: No advisors found in database. Using sample advisor IDs 1-500.")
                        advisor_ids = list(range(1, 501))
                    return advisor_ids
        except Exception as e:
            print(f"Error fetching advisor IDs: {e}")
            # Fallback to sample advisor IDs
            return list(range(1, 501))
    
    def _generate_household_tenure(self) -> int:
        """Generate household tenure (1-40 years, uniform distribution)."""
        return random.randint(1, 40)
    
    def _generate_registration_date(self, tenure: int) -> date:
        """Generate household registration date based on tenure with ±180 day variance."""
        base_date = self.current_date - timedelta(days=tenure * 365)
        variance_days = random.randint(-180, 180)
        return base_date + timedelta(days=variance_days)
    
    def _should_terminate_recently(self) -> bool:
        """Determine if household should be terminated in last 3 months (12% chance)."""
        return random.random() < 0.12
    
    def _generate_termination_date(self) -> date:
        """Generate termination date within last 3 months."""
        three_months_ago = self.current_date - timedelta(days=90)
        days_since = random.randint(0, 90)
        return three_months_ago + timedelta(days=days_since)
    
    def generate_household_data(self) -> List[Dict[str, Any]]:
        """Generate household data according to schema specifications."""
        print(f"Generating {self.target_household_count} households...")
        
        # Cache advisor IDs once
        advisor_ids = self._get_advisor_ids()
        print(f"Using {len(advisor_ids)} advisor IDs for household generation")
        
        # Pre-generate all random values in batches for MAXIMUM performance
        import numpy as np
        from datetime import timedelta, date
        
        print("Pre-generating all random values...")
        
        # Generate ALL random values at once (no loops!)
        tenures = np.random.randint(1, 41, self.target_household_count)
        advisor_assignments = np.random.choice(advisor_ids, self.target_household_count)
        termination_flags = np.random.random(self.target_household_count) < 0.12
        
        # Pre-generate weighted choices
        reg_types = list(self.registration_type_dist.keys())
        reg_weights = list(self.registration_type_dist.values())
        seg_types = list(self.segment_dist.keys())
        seg_weights = list(self.segment_dist.values())
        status_types = list(self.status_dist.keys())
        status_weights = list(self.status_dist.values())
        
        registration_types = np.random.choice(reg_types, self.target_household_count, p=reg_weights)
        segments = np.random.choice(seg_types, self.target_household_count, p=seg_weights)
        
        # Pre-generate ALL dates at once (major bottleneck elimination)
        variance_days = np.random.randint(-180, 181, self.target_household_count)
        termination_days = np.random.randint(0, 91, self.target_household_count)
        
        # Calculate base dates vectorized
        current_date_ordinal = self.current_date.toordinal()
        base_dates = current_date_ordinal - (tenures * 365)
        registration_dates = [date.fromordinal(int(bd + vd)) for bd, vd in zip(base_dates, variance_days)]
        
        # Pre-calculate termination dates
        three_months_ago_ordinal = (self.current_date - timedelta(days=90)).toordinal()
        termination_dates = [date.fromordinal(three_months_ago_ordinal + td) for td in termination_days]
        
        # Pre-generate status choices for non-terminated households
        non_terminated_statuses = np.random.choice(status_types, self.target_household_count, p=status_weights)
        
        print("Pre-generation complete. Creating household records...")
        
        # Vectorized record creation
        final_households = []
        household_key = 1
        far_future = date(9999, 12, 31)
        
        for i in range(self.target_household_count):
            household_id = i + 1
            
            if household_id % 5000 == 0:
                print(f"Generated {household_id} households...")
            
            # All values are pre-calculated
            tenure = int(tenures[i])
            advisor_id = int(advisor_assignments[i])
            registration_type = registration_types[i]
            segment = segments[i]
            will_terminate = termination_flags[i]
            registration_date = registration_dates[i]
            
            if will_terminate:
                # Create SCD2 records for terminated household
                termination_date = termination_dates[i]
                
                # Active record (ends on termination date)
                final_households.append({
                    'household_key': household_key,
                    'household_id': household_id,
                    'household_tenure': tenure,
                    'household_registration_type': registration_type,
                    'household_registration_date': registration_date,
                    'household_segment': segment,
                    'household_status': 'Active',
                    'household_advisor_id': advisor_id,
                    'from_date': registration_date,
                    'to_date': termination_date
                })
                
                # Terminated record (starts day after termination)
                final_households.append({
                    'household_key': household_key + 1,
                    'household_id': household_id,
                    'household_tenure': tenure,
                    'household_registration_type': registration_type,
                    'household_registration_date': registration_date,
                    'household_segment': segment,
                    'household_status': 'Terminated',
                    'household_advisor_id': advisor_id,
                    'from_date': termination_date + timedelta(days=1),
                    'to_date': far_future
                })
                
                household_key += 2
            else:
                # Single record household (active or old termination)
                status = non_terminated_statuses[i]
                to_date = far_future
                from_date = registration_date
                
                if status == 'Terminated':
                    # For older terminations, set termination date
                    termination_date = termination_dates[i]
                    to_date = termination_date
                    from_date = min(registration_date, termination_date - timedelta(days=365))
                
                final_households.append({
                    'household_key': household_key,
                    'household_id': household_id,
                    'household_tenure': tenure,
                    'household_registration_type': registration_type,
                    'household_registration_date': registration_date,
                    'household_segment': segment,
                    'household_status': status,
                    'household_advisor_id': advisor_id,
                    'from_date': from_date,
                    'to_date': to_date
                })
                household_key += 1
        
        print(f"Generated {len(final_households)} household records (including SCD2 history)")
        return final_households
    
    def create_table_if_not_exists(self):
        """Create household table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS household (
            household_key SERIAL PRIMARY KEY,
            household_id INTEGER NOT NULL,
            household_tenure INTEGER NOT NULL,
            household_registration_type VARCHAR(20) NOT NULL,
            household_registration_date DATE NOT NULL,
            household_segment VARCHAR(30) NOT NULL,
            household_status VARCHAR(20) NOT NULL,
            household_advisor_id INTEGER NOT NULL,
            from_date DATE NOT NULL,
            to_date DATE NOT NULL
        );
        
        -- Create indexes as specified in schema
        CREATE UNIQUE INDEX IF NOT EXISTS ux_household_current 
        ON household(household_id) 
        WHERE to_date = DATE '9999-12-31';
        
        CREATE INDEX IF NOT EXISTS ix_household_id_window 
        ON household(household_id, from_date, to_date);
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    conn.commit()
                    print("Household table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating household table: {e}")
            raise
    
    def clear_existing_data(self):
        """Clear existing household data."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM household")
                    conn.commit()
                    print("Existing household data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
    
    def insert_household_data(self, households: List[Dict[str, Any]], batch_size: int = 1000):
        """Insert household data into PostgreSQL database."""
        print(f"Inserting {len(households)} household records...")
        
        insert_sql = """
        INSERT INTO household (
            household_key, household_id, household_tenure, household_registration_type,
            household_registration_date, household_segment, household_status,
            household_advisor_id, from_date, to_date
        ) VALUES (
            %(household_key)s, %(household_id)s, %(household_tenure)s, %(household_registration_type)s,
            %(household_registration_date)s, %(household_segment)s, %(household_status)s,
            %(household_advisor_id)s, %(from_date)s, %(to_date)s
        )
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    for i in range(0, len(households), batch_size):
                        batch = households[i:i + batch_size]
                        cursor.executemany(insert_sql, batch)
                        
                        if (i // batch_size + 1) % 10 == 0:
                            print(f"Inserted {min(i + batch_size, len(households))} records...")
                    
                    conn.commit()
                    print(f"Successfully inserted all {len(households)} household records")
        except Exception as e:
            print(f"Error inserting household data: {e}")
            raise
    
    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total household_id count': "SELECT COUNT(DISTINCT household_id) FROM household",
            'Current records count': "SELECT COUNT(*) FROM household WHERE to_date = '9999-12-31'",
            'Terminated in last 3 months': """
                SELECT COUNT(DISTINCT household_id) 
                FROM household 
                WHERE household_status = 'Terminated' 
                AND from_date >= %s
            """,
            'Registration type distribution': """
                SELECT household_registration_type, COUNT(*) as count
                FROM household 
                WHERE to_date = '9999-12-31'
                GROUP BY household_registration_type 
                ORDER BY count DESC
            """,
            'Status distribution': """
                SELECT household_status, COUNT(*) as count
                FROM household 
                WHERE to_date = '9999-12-31'
                GROUP BY household_status
            """
        }
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    print("\n" + "="*50)
                    print("DATA VALIDATION RESULTS")
                    print("="*50)
                    
                    for description, query in validation_queries.items():
                        if 'last 3 months' in description:
                            three_months_ago = self.current_date - timedelta(days=90)
                            cursor.execute(query, (three_months_ago,))
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
                                    print(f"  {row}")
                    
                    print("\n" + "="*50)
        except Exception as e:
            print(f"Error during validation: {e}")

def main():
    """Main function to generate and insert household data."""
    # PostgreSQL connection configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'demo_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    
    try:
        generator = HouseholdDataGenerator(**db_config)
        
        # Create table if not exists
        generator.create_table_if_not_exists()
        
        # Ask user if they want to clear existing data
        response = input("Do you want to clear existing household data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()
        
        # Generate household data
        households = generator.generate_household_data()
        
        # Insert data into database
        generator.insert_household_data(households)
        
        # Validate inserted data
        generator.validate_data()
        
        print(f"\nHousehold data generation completed successfully!")
        print(f"Generated {len(households)} total records for {generator.target_household_count} households")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Please check your database configuration and ensure PostgreSQL is running.")
        print("Set environment variables: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")

if __name__ == "__main__":
    main()