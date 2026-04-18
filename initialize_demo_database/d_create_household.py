import sqlite3
import random
import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
import os
import numpy as np

sqlite3.register_adapter(date, lambda val: val.isoformat())
sqlite3.register_adapter(datetime, lambda val: val.isoformat())

class HouseholdDataGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the household data generator with SQLite database connection.

        Args:
            connection_string: Path to SQLite database file
            **db_params: Individual connection parameters (db_path)
        """
        self.db_path = connection_string or db_params.get('db_path', './demo.db')

        # Constants from schema
        self.current_date = date(2025, 9, 30)
        self.snapshot_start_date = date(2024, 9, 30)
        self.target_household_count = 5000

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

    def _weighted_choice(self, choices: Dict[str, float]) -> str:
        """Select a random choice based on weighted distribution."""
        choices_list = list(choices.keys())
        weights = list(choices.values())
        return np.random.choice(choices_list, p=weights)

    def _get_advisor_ids(self) -> List[int]:
        """Get list of advisor IDs from the database."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT advisor_id FROM advisors")
            advisor_ids = [row[0] for row in cursor.fetchall()]
            if not advisor_ids:
                print("Warning: No advisors found in database. Using sample advisor IDs 1-500.")
                advisor_ids = list(range(1, 501))
            return advisor_ids
        except Exception as e:
            print(f"Error fetching advisor IDs: {e}")
            return list(range(1, 501))
        finally:
            conn.close()

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

        advisor_ids = self._get_advisor_ids()
        print(f"Using {len(advisor_ids)} advisor IDs for household generation")

        import numpy as np
        from datetime import timedelta, date

        print("Pre-generating all random values...")

        tenures = np.random.randint(1, 41, self.target_household_count)
        advisor_assignments = np.random.choice(advisor_ids, self.target_household_count)
        termination_flags = np.random.random(self.target_household_count) < 0.12

        reg_types = list(self.registration_type_dist.keys())
        reg_weights = list(self.registration_type_dist.values())
        seg_types = list(self.segment_dist.keys())
        seg_weights = list(self.segment_dist.values())
        status_types = list(self.status_dist.keys())
        status_weights = list(self.status_dist.values())

        registration_types = np.random.choice(reg_types, self.target_household_count, p=reg_weights)
        segments = np.random.choice(seg_types, self.target_household_count, p=seg_weights)

        variance_days = np.random.randint(-180, 181, self.target_household_count)
        termination_days = np.random.randint(0, 91, self.target_household_count)

        current_date_ordinal = self.current_date.toordinal()
        base_dates = current_date_ordinal - (tenures * 365)
        registration_dates = [date.fromordinal(int(bd + vd)) for bd, vd in zip(base_dates, variance_days)]

        three_months_ago_ordinal = (self.current_date - timedelta(days=90)).toordinal()
        termination_dates = [date.fromordinal(three_months_ago_ordinal + td) for td in termination_days]

        non_terminated_statuses = np.random.choice(status_types, self.target_household_count, p=status_weights)

        print("Pre-generation complete. Creating household records...")

        final_households = []
        household_key = 1
        far_future = date(9999, 12, 31)

        for i in range(self.target_household_count):
            household_id = i + 1

            if household_id % 5000 == 0:
                print(f"Generated {household_id} households...")

            tenure = int(tenures[i])
            advisor_id = int(advisor_assignments[i])
            registration_type = registration_types[i]
            segment = segments[i]
            will_terminate = termination_flags[i]
            registration_date = registration_dates[i]

            if will_terminate:
                termination_date = termination_dates[i]

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
                status = non_terminated_statuses[i]
                to_date = far_future
                from_date = registration_date

                if status == 'Terminated':
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
            household_key INTEGER PRIMARY KEY,
            household_id INTEGER NOT NULL,
            household_tenure INTEGER NOT NULL,
            household_registration_type TEXT NOT NULL,
            household_registration_date TEXT NOT NULL,
            household_segment TEXT NOT NULL,
            household_status TEXT NOT NULL,
            household_advisor_id INTEGER NOT NULL,
            from_date TEXT NOT NULL,
            to_date TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ux_household_current
        ON household(household_id)
        WHERE to_date = '9999-12-31';
        CREATE INDEX IF NOT EXISTS ix_household_id_window
        ON household(household_id, from_date, to_date);
        """

        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(create_table_sql)
            print("Household table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating household table: {e}")
            raise
        finally:
            conn.close()

    def clear_existing_data(self):
        """Clear existing household data."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM household")
            conn.commit()
            print("Existing household data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
        finally:
            conn.close()

    def insert_household_data(self, households: List[Dict[str, Any]], batch_size: int = 1000):
        """Insert household data into SQLite database."""
        print(f"Inserting {len(households)} household records...")

        insert_sql = """
        INSERT INTO household (
            household_key, household_id, household_tenure, household_registration_type,
            household_registration_date, household_segment, household_status,
            household_advisor_id, from_date, to_date
        ) VALUES (
            :household_key, :household_id, :household_tenure, :household_registration_type,
            :household_registration_date, :household_segment, :household_status,
            :household_advisor_id, :from_date, :to_date
        )
        """

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
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
        finally:
            conn.close()

    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total household_id count': "SELECT COUNT(DISTINCT household_id) FROM household",
            'Current records count': "SELECT COUNT(*) FROM household WHERE to_date = '9999-12-31'",
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

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            print("\n" + "="*50)
            print("DATA VALIDATION RESULTS")
            print("="*50)

            for description, query in validation_queries.items():
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
        finally:
            conn.close()

def main():
    """Main function to generate and insert household data."""
    db_config = {
        'db_path': os.getenv('DB_PATH', './demo.db')
    }

    try:
        generator = HouseholdDataGenerator(**db_config)

        generator.create_table_if_not_exists()

        response = input("Do you want to clear existing household data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()

        households = generator.generate_household_data()
        generator.insert_household_data(households)
        generator.validate_data()

        print(f"\nHousehold data generation completed successfully!")
        print(f"Generated {len(households)} total records for {generator.target_household_count} households")

    except Exception as e:
        print(f"Error: {e}")
        print("Please check your SQLite database path configuration.")

if __name__ == "__main__":
    main()
