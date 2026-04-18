import sqlite3
import random
import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
import os
import numpy as np

sqlite3.register_adapter(date, lambda val: val.isoformat())
sqlite3.register_adapter(datetime, lambda val: val.isoformat())

class AdvisorDataGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the advisor data generator with SQLite database connection.

        Args:
            connection_string: Path to SQLite database file
            **db_params: Individual connection parameters (db_path)
        """
        self.db_path = connection_string or db_params.get('db_path', './demo.db')

        # Constants from schema
        self.current_date = date(2025, 9, 30)
        self.target_advisor_count = 50

        self.firm_prefixes = [
            'Summit', 'Harbor', 'Granite', 'Cedar', 'Crescent',
            'Atlas', 'Pioneer', 'Ridge', 'Oak', 'River'
        ]

        self.firm_suffixes = [
            'Capital', 'Advisors', 'Partners', 'Wealth', 'Financial'
        ]

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
        twenty_four_months_ago = self.current_date - timedelta(days=730)
        days_since = random.randint(0, 730)
        return twenty_four_months_ago + timedelta(days=days_since)

    def _generate_from_date(self, tenure: int) -> date:
        """Generate from_date based on tenure."""
        return self.current_date - timedelta(days=tenure * 365)

    def generate_advisor_data(self) -> List[Dict[str, Any]]:
        """Generate advisor data according to schema specifications."""
        print(f"Generating {self.target_advisor_count} advisors...")

        initial_advisors = []
        advisors_to_terminate = []

        for advisor_id in range(1, self.target_advisor_count + 1):
            if advisor_id % 100 == 0:
                print(f"Generated {advisor_id} advisors...")

            tenure = self._generate_advisor_tenure()
            firm_name = self._generate_firm_name()
            affiliation_model = self._weighted_choice(self.affiliation_model_dist)
            advisor_role = self._weighted_choice(self.advisor_role_dist)
            practice_segment = self._weighted_choice(self.practice_segment_dist)
            from_date = self._generate_from_date(tenure)

            advisor = {
                'advisor_id': advisor_id,
                'advisor_tenure': tenure,
                'firm_name': firm_name,
                'firm_affiliation_model': affiliation_model,
                'advisor_role': advisor_role,
                'advisor_status': 'Active',
                'practice_segment': practice_segment,
                'from_date': from_date,
                'to_date': date(9999, 12, 31)
            }

            initial_advisors.append(advisor)

            if self._should_terminate_recently():
                advisors_to_terminate.append(advisor_id)

        print(f"Processing {len(advisors_to_terminate)} advisor terminations...")
        final_advisors = []
        advisor_key = 1

        for advisor in initial_advisors:
            advisor_id = advisor['advisor_id']

            if advisor_id in advisors_to_terminate:
                termination_date = self._generate_termination_date()

                active_record = advisor.copy()
                active_record['advisor_key'] = advisor_key
                active_record['to_date'] = termination_date
                final_advisors.append(active_record)

                terminated_record = advisor.copy()
                terminated_record['advisor_key'] = advisor_key + 1
                terminated_record['advisor_status'] = 'Terminated'
                terminated_record['from_date'] = termination_date + timedelta(days=1)
                terminated_record['to_date'] = date(9999, 12, 31)
                final_advisors.append(terminated_record)

                advisor_key += 2
            else:
                status = self._weighted_choice(self.status_dist)
                advisor['advisor_key'] = advisor_key
                advisor['advisor_status'] = status

                if status == 'Terminated':
                    termination_date = self._generate_termination_date()
                    advisor['to_date'] = termination_date
                    advisor['from_date'] = min(advisor['from_date'], termination_date - timedelta(days=365))

                final_advisors.append(advisor)
                advisor_key += 1

        print(f"Generated {len(final_advisors)} advisor records (including SCD2 history)")
        return final_advisors

    def create_table_if_not_exists(self):
        """Create advisors table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS advisors (
            advisor_key INTEGER PRIMARY KEY,
            advisor_id INTEGER NOT NULL,
            advisor_tenure INTEGER NOT NULL,
            firm_name TEXT NOT NULL,
            firm_affiliation_model TEXT NOT NULL,
            advisor_role TEXT NOT NULL,
            advisor_status TEXT NOT NULL,
            practice_segment TEXT NOT NULL,
            from_date TEXT NOT NULL,
            to_date TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ux_advisors_current
        ON advisors(advisor_id)
        WHERE to_date = '9999-12-31';
        CREATE INDEX IF NOT EXISTS ix_advisor_id_window
        ON advisors(advisor_id, from_date, to_date);
        """

        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(create_table_sql)
            print("Advisors table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating advisors table: {e}")
            raise
        finally:
            conn.close()

    def create_advisor_payout_rate_table(self):
        """Create advisor_payout_rate table with data from schema."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS advisor_payout_rate (
            firm_affiliation_model TEXT PRIMARY KEY,
            advisor_payout_rate REAL NOT NULL
        );
        """

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
        INSERT OR REPLACE INTO advisor_payout_rate (firm_affiliation_model, advisor_payout_rate)
        VALUES (?, ?)
        """

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            conn.executescript(create_table_sql)
            cursor.executemany(insert_sql, payout_rates)
            conn.commit()
            print("Advisor payout rate table created and populated successfully")
        except Exception as e:
            print(f"Error creating advisor payout rate table: {e}")
            raise
        finally:
            conn.close()

    def clear_existing_data(self):
        """Clear existing advisor data."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM advisors")
            conn.commit()
            print("Existing advisor data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
        finally:
            conn.close()

    def insert_advisor_data(self, advisors: List[Dict[str, Any]], batch_size: int = 1000):
        """Insert advisor data into SQLite database."""
        print(f"Inserting {len(advisors)} advisor records...")

        insert_sql = """
        INSERT INTO advisors (
            advisor_key, advisor_id, advisor_tenure, firm_name, firm_affiliation_model,
            advisor_role, advisor_status, practice_segment, from_date, to_date
        ) VALUES (
            :advisor_key, :advisor_id, :advisor_tenure, :firm_name, :firm_affiliation_model,
            :advisor_role, :advisor_status, :practice_segment, :from_date, :to_date
        )
        """

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
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
        finally:
            conn.close()

    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total advisor_id count': "SELECT COUNT(DISTINCT advisor_id) FROM advisors",
            'Current records count': "SELECT COUNT(*) FROM advisors WHERE to_date = '9999-12-31'",
            'Firm affiliation distribution': """
                SELECT firm_affiliation_model, COUNT(*) as count
                FROM advisors
                WHERE to_date = '9999-12-31'
                GROUP BY firm_affiliation_model
                ORDER BY count DESC
            """,
            'Status distribution': """
                SELECT advisor_status, COUNT(*) as count
                FROM advisors
                WHERE to_date = '9999-12-31'
                GROUP BY advisor_status
            """,
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
                            print(f"  {row[0]}")

            print("\n" + "="*50)
        except Exception as e:
            print(f"Error during validation: {e}")
        finally:
            conn.close()

def main():
    """Main function to generate and insert advisor data."""
    db_config = {
        'db_path': os.getenv('DB_PATH', './demo.db')
    }

    try:
        generator = AdvisorDataGenerator(**db_config)

        generator.create_table_if_not_exists()
        generator.create_advisor_payout_rate_table()

        response = input("Do you want to clear existing advisor data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()

        advisors = generator.generate_advisor_data()
        generator.insert_advisor_data(advisors)
        generator.validate_data()

        print(f"\nAdvisor data generation completed successfully!")
        print(f"Generated {len(advisors)} total records for {generator.target_advisor_count} advisors")

    except Exception as e:
        print(f"Error: {e}")
        print("Please check your SQLite database path configuration.")

if __name__ == "__main__":
    main()
