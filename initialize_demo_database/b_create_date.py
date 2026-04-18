import sqlite3
import pandas as pd
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any
import os
import calendar

sqlite3.register_adapter(date, lambda val: val.isoformat())
sqlite3.register_adapter(datetime, lambda val: val.isoformat())

class DateDimensionGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the date dimension generator with SQLite database connection.

        Args:
            connection_string: Path to SQLite database file
            **db_params: Individual connection parameters (db_path)
        """
        self.db_path = connection_string or db_params.get('db_path', './demo.db')

        # Date range from schema: >= last 10y + next 2y
        self.current_date = date(2025, 9, 30)  # Reference date from schema
        self.start_date = date(self.current_date.year - 10, 1, 1)  # 10 years back from current
        self.end_date = date(self.current_date.year + 2, 12, 31)   # 2 years forward from current

        # Month names
        self.month_names = [
            'January', 'February', 'March', 'April', 'May', 'June',
            'July', 'August', 'September', 'October', 'November', 'December'
        ]

        # Quarter names
        self.quarter_names = ['Q1', 'Q2', 'Q3', 'Q4']

    def _get_quarter(self, calendar_day: date) -> int:
        """Get quarter number for a given date (1-4)."""
        month = calendar_day.month
        if month in [1, 2, 3]:
            return 1
        elif month in [4, 5, 6]:
            return 2
        elif month in [7, 8, 9]:
            return 3
        else:  # [10, 11, 12]
            return 4

    def _get_quarter_start_date(self, calendar_day: date) -> date:
        """Get the start date of the quarter for a given date."""
        quarter = self._get_quarter(calendar_day)
        year = calendar_day.year

        if quarter == 1:
            return date(year, 1, 1)
        elif quarter == 2:
            return date(year, 4, 1)
        elif quarter == 3:
            return date(year, 7, 1)
        else:  # quarter == 4
            return date(year, 10, 1)

    def _get_quarter_end_date(self, calendar_day: date) -> date:
        """Get the end date of the quarter for a given date."""
        quarter = self._get_quarter(calendar_day)
        year = calendar_day.year

        if quarter == 1:
            return date(year, 3, 31)
        elif quarter == 2:
            return date(year, 6, 30)
        elif quarter == 3:
            return date(year, 9, 30)
        else:  # quarter == 4
            return date(year, 12, 31)

    def _get_month_start_date(self, calendar_day: date) -> date:
        """Get the start date of the month for a given date."""
        return date(calendar_day.year, calendar_day.month, 1)

    def _get_month_end_date(self, calendar_day: date) -> date:
        """Get the end date of the month for a given date."""
        last_day = calendar.monthrange(calendar_day.year, calendar_day.month)[1]
        return date(calendar_day.year, calendar_day.month, last_day)

    def _is_weekend(self, calendar_day: date) -> bool:
        """Check if the date falls on a weekend (Saturday or Sunday)."""
        return calendar_day.weekday() in [5, 6]  # Saturday=5, Sunday=6

    def generate_date_dimension_data(self) -> List[Dict[str, Any]]:
        """Generate date dimension data according to schema specifications."""
        print(f"Generating date dimension from {self.start_date} to {self.end_date}...")

        date_data = []
        current = self.start_date
        processed = 0

        while current <= self.end_date:
            if processed % 1000 == 0:
                print(f"Generated {processed:,} date records...")

            month_name = self.month_names[current.month - 1]
            month = current.month
            day_of_month = current.day
            month_start_date = self._get_month_start_date(current)
            month_end_date = self._get_month_end_date(current)

            quarter = self._get_quarter(current)
            quarter_name = self.quarter_names[quarter - 1]
            quarter_start_date = self._get_quarter_start_date(current)
            quarter_end_date = self._get_quarter_end_date(current)

            year = current.year
            is_weekend = self._is_weekend(current)

            date_record = {
                'calendar_day': current,
                'month_name': month_name,
                'month': month,
                'day_of_month': day_of_month,
                'month_start_date': month_start_date,
                'month_end_date': month_end_date,
                'quarter': quarter,
                'quarter_name': quarter_name,
                'quarter_start_date': quarter_start_date,
                'quarter_end_date': quarter_end_date,
                'year': year,
                'is_weekend': is_weekend
            }

            date_data.append(date_record)

            current += timedelta(days=1)
            processed += 1

        print(f"Generated {len(date_data):,} date records")
        print(f"Date range: {self.start_date} to {self.end_date}")
        print(f"Total span: {(self.end_date - self.start_date).days + 1:,} days")

        return date_data

    def create_table_if_not_exists(self):
        """Create date table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS date (
            calendar_day TEXT PRIMARY KEY,
            month_name TEXT NOT NULL,
            month INTEGER NOT NULL,
            day_of_month INTEGER NOT NULL,
            month_start_date TEXT NOT NULL,
            month_end_date TEXT NOT NULL,
            quarter INTEGER NOT NULL,
            quarter_name TEXT NOT NULL,
            quarter_start_date TEXT NOT NULL,
            quarter_end_date TEXT NOT NULL,
            year INTEGER NOT NULL,
            is_weekend INTEGER NOT NULL,
            CONSTRAINT check_month_valid CHECK (month >= 1 AND month <= 12),
            CONSTRAINT check_day_valid CHECK (day_of_month >= 1 AND day_of_month <= 31),
            CONSTRAINT check_quarter_valid CHECK (quarter >= 1 AND quarter <= 4),
            CONSTRAINT check_year_valid CHECK (year >= 2000 AND year <= 3000)
        );
        CREATE INDEX IF NOT EXISTS ix_date_year ON date(year);
        CREATE INDEX IF NOT EXISTS ix_date_quarter ON date(year, quarter);
        CREATE INDEX IF NOT EXISTS ix_date_month ON date(year, month);
        CREATE INDEX IF NOT EXISTS ix_date_is_weekend ON date(is_weekend);
        CREATE INDEX IF NOT EXISTS ix_date_month_start ON date(month_start_date);
        CREATE INDEX IF NOT EXISTS ix_date_quarter_start ON date(quarter_start_date);
        """

        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(create_table_sql)
            print("Date dimension table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating date table: {e}")
            raise
        finally:
            conn.close()

    def clear_existing_data(self):
        """Clear existing date dimension data."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM date")
            conn.commit()
            print("Existing date dimension data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
        finally:
            conn.close()

    def insert_date_dimension_data(self, date_data: List[Dict[str, Any]], batch_size: int = 1000):
        """Insert date dimension data into SQLite database."""
        print(f"Inserting {len(date_data):,} date records...")

        insert_sql = """
        INSERT INTO date (
            calendar_day, month_name, month, day_of_month, month_start_date, month_end_date,
            quarter, quarter_name, quarter_start_date, quarter_end_date, year, is_weekend
        ) VALUES (
            :calendar_day, :month_name, :month, :day_of_month, :month_start_date, :month_end_date,
            :quarter, :quarter_name, :quarter_start_date, :quarter_end_date, :year, :is_weekend
        )
        """

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            for i in range(0, len(date_data), batch_size):
                batch = date_data[i:i + batch_size]
                cursor.executemany(insert_sql, batch)

                if (i // batch_size + 1) % 10 == 0:
                    print(f"Inserted {min(i + batch_size, len(date_data)):,} records...")

            conn.commit()
            print(f"Successfully inserted all {len(date_data):,} date records")
        except Exception as e:
            print(f"Error inserting date dimension data: {e}")
            raise
        finally:
            conn.close()

    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total date records': "SELECT COUNT(*) FROM date",
            'Date range coverage': """
                SELECT
                    MIN(calendar_day) as earliest_date,
                    MAX(calendar_day) as latest_date
                FROM date
            """,
            'Year distribution': """
                SELECT
                    year,
                    COUNT(*) as day_count,
                    MIN(calendar_day) as year_start,
                    MAX(calendar_day) as year_end
                FROM date
                GROUP BY year
                ORDER BY year
            """,
        }

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            print("\n" + "="*100)
            print("DATE DIMENSION DATA VALIDATION RESULTS")
            print("="*100)

            for description, query in validation_queries.items():
                cursor.execute(query)
                results = cursor.fetchall()

                print(f"\n{description}:")

                if len(results) == 1 and len(results[0]) == 1:
                    print(f"  {results[0][0]:,}")
                elif 'Year distribution' in description:
                    print(f"  {'Year':<6} {'Days':<6} {'Start':<12} {'End':<12}")
                    print(f"  {'-'*6} {'-'*6} {'-'*12} {'-'*12}")
                    for row in results:
                        year, day_count, year_start, year_end = row
                        print(f"  {year:<6} {day_count:<6} {year_start!s:<12} {year_end!s:<12}")
                else:
                    for row in results:
                        print(f"  {row}")

            print("\n" + "="*100)
        except Exception as e:
            print(f"Error during validation: {e}")
        finally:
            conn.close()

def main():
    """Main function to generate and insert date dimension data."""
    db_config = {
        'db_path': os.getenv('DB_PATH', './demo.db')
    }

    try:
        generator = DateDimensionGenerator(**db_config)

        generator.create_table_if_not_exists()

        response = input("Do you want to clear existing date dimension data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()

        date_data = generator.generate_date_dimension_data()
        generator.insert_date_dimension_data(date_data)
        generator.validate_data()

        print(f"\nDate dimension data generation completed successfully!")
        print(f"Generated {len(date_data):,} date records")
        print(f"Coverage: {generator.start_date} to {generator.end_date}")

    except Exception as e:
        print(f"Error: {e}")
        print("Please check your SQLite database path configuration.")

if __name__ == "__main__":
    main()
