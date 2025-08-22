import psycopg2
import pandas as pd
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any
import os
from urllib.parse import urlparse
import calendar

class DateDimensionGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the date dimension generator with PostgreSQL database connection.
        
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
        
        # Date range from schema: ≥ last 10y + next 2y
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
        # Get the last day of the month
        last_day = calendar.monthrange(calendar_day.year, calendar_day.month)[1]
        return date(calendar_day.year, calendar_day.month, last_day)
    
    def _is_weekend(self, calendar_day: date) -> bool:
        """Check if the date falls on a weekend (Saturday or Sunday)."""
        # weekday() returns 0=Monday, 1=Tuesday, ..., 6=Sunday
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
            
            # Calculate all derived attributes
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
            
            # Create date record
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
            
            # Move to next day
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
            calendar_day DATE PRIMARY KEY,
            month_name VARCHAR(20) NOT NULL,
            month INTEGER NOT NULL,
            day_of_month INTEGER NOT NULL,
            month_start_date DATE NOT NULL,
            month_end_date DATE NOT NULL,
            quarter INTEGER NOT NULL,
            quarter_name VARCHAR(10) NOT NULL,
            quarter_start_date DATE NOT NULL,
            quarter_end_date DATE NOT NULL,
            year INTEGER NOT NULL,
            is_weekend BOOLEAN NOT NULL,
            CONSTRAINT check_month_valid CHECK (month >= 1 AND month <= 12),
            CONSTRAINT check_day_valid CHECK (day_of_month >= 1 AND day_of_month <= 31),
            CONSTRAINT check_quarter_valid CHECK (quarter >= 1 AND quarter <= 4),
            CONSTRAINT check_year_valid CHECK (year >= 2000 AND year <= 3000)
        );
        
        -- Create indexes for efficient queries
        CREATE INDEX IF NOT EXISTS ix_date_year 
        ON date(year);
        
        CREATE INDEX IF NOT EXISTS ix_date_quarter 
        ON date(year, quarter);
        
        CREATE INDEX IF NOT EXISTS ix_date_month 
        ON date(year, month);
        
        CREATE INDEX IF NOT EXISTS ix_date_is_weekend 
        ON date(is_weekend);
        
        CREATE INDEX IF NOT EXISTS ix_date_month_start 
        ON date(month_start_date);
        
        CREATE INDEX IF NOT EXISTS ix_date_quarter_start 
        ON date(quarter_start_date);
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    conn.commit()
                    print("Date dimension table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating date table: {e}")
            raise
    
    def clear_existing_data(self):
        """Clear existing date dimension data."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM date")
                    conn.commit()
                    print("Existing date dimension data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
    
    def insert_date_dimension_data(self, date_data: List[Dict[str, Any]], batch_size: int = 1000):
        """Insert date dimension data into PostgreSQL database."""
        print(f"Inserting {len(date_data):,} date records...")
        
        insert_sql = """
        INSERT INTO date (
            calendar_day, month_name, month, day_of_month, month_start_date, month_end_date,
            quarter, quarter_name, quarter_start_date, quarter_end_date, year, is_weekend
        ) VALUES (
            %(calendar_day)s, %(month_name)s, %(month)s, %(day_of_month)s, %(month_start_date)s, %(month_end_date)s,
            %(quarter)s, %(quarter_name)s, %(quarter_start_date)s, %(quarter_end_date)s, %(year)s, %(is_weekend)s
        )
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
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
    
    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total date records': "SELECT COUNT(*) FROM date",
            'Date range coverage': """
                SELECT 
                    MIN(calendar_day) as earliest_date,
                    MAX(calendar_day) as latest_date,
                    MAX(calendar_day) - MIN(calendar_day) + 1 as total_days
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
            'Quarter distribution (sample year)': """
                SELECT 
                    year,
                    quarter,
                    quarter_name,
                    COUNT(*) as day_count,
                    MIN(quarter_start_date) as q_start,
                    MIN(quarter_end_date) as q_end
                FROM date
                WHERE year = 2025
                GROUP BY year, quarter, quarter_name
                ORDER BY quarter
            """,
            'Month distribution (sample year)': """
                SELECT 
                    month,
                    month_name,
                    COUNT(*) as day_count,
                    MIN(month_start_date) as m_start,
                    MIN(month_end_date) as m_end
                FROM date
                WHERE year = 2025
                GROUP BY month, month_name
                ORDER BY month
            """,
            'Weekend vs Weekday distribution': """
                SELECT 
                    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
                    COUNT(*) as day_count,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM date), 1) as percentage
                FROM date
                GROUP BY is_weekend
                ORDER BY is_weekend
            """,
            'Date consistency validation': """
                SELECT 
                    'Month start/end mismatch' as validation_type,
                    COUNT(*) as count
                FROM date
                WHERE month_start_date != DATE_TRUNC('month', calendar_day)::date
                   OR month_end_date != (DATE_TRUNC('month', calendar_day) + INTERVAL '1 month - 1 day')::date
                UNION ALL
                SELECT 
                    'Quarter start/end mismatch' as validation_type,
                    COUNT(*) as count
                FROM date
                WHERE quarter_start_date != DATE_TRUNC('quarter', calendar_day)::date
                   OR quarter_end_date != (DATE_TRUNC('quarter', calendar_day) + INTERVAL '3 months - 1 day')::date
                UNION ALL
                SELECT 
                    'Month name mismatch' as validation_type,
                    COUNT(*) as count
                FROM date
                WHERE month_name != TO_CHAR(calendar_day, 'Month')
                UNION ALL
                SELECT 
                    'Day of month mismatch' as validation_type,
                    COUNT(*) as count
                FROM date
                WHERE day_of_month != EXTRACT(DAY FROM calendar_day)
                UNION ALL
                SELECT 
                    'Quarter mismatch' as validation_type,
                    COUNT(*) as count
                FROM date
                WHERE quarter != EXTRACT(QUARTER FROM calendar_day)
                UNION ALL
                SELECT 
                    'Year mismatch' as validation_type,
                    COUNT(*) as count
                FROM date
                WHERE year != EXTRACT(YEAR FROM calendar_day)
            """,
            'Sample weekend validation': """
                SELECT 
                    calendar_day,
                    TO_CHAR(calendar_day, 'Day') as day_name,
                    is_weekend
                FROM date
                WHERE calendar_day BETWEEN '2025-01-01' AND '2025-01-07'
                ORDER BY calendar_day
            """,
            'Leap year validation': """
                SELECT 
                    year,
                    COUNT(*) as days_in_year,
                    CASE 
                        WHEN COUNT(*) = 366 THEN 'Leap Year'
                        WHEN COUNT(*) = 365 THEN 'Regular Year'
                        ELSE 'Invalid Year'
                    END as year_type
                FROM date
                WHERE year IN (2020, 2021, 2024, 2025, 2028)  -- Sample years including leap years
                GROUP BY year
                ORDER BY year
            """
        }
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    print("\n" + "="*100)
                    print("DATE DIMENSION DATA VALIDATION RESULTS")
                    print("="*100)
                    
                    for description, query in validation_queries.items():
                        cursor.execute(query)
                        results = cursor.fetchall()
                        
                        print(f"\n{description}:")
                        
                        if len(results) == 1 and len(results[0]) <= 3:
                            if len(results[0]) == 1:
                                print(f"  {results[0][0]:,}")
                            elif len(results[0]) == 3:
                                earliest, latest, total = results[0]
                                print(f"  From {earliest} to {latest} ({total:,} days)")
                            else:
                                print(f"  From {results[0][0]} to {results[0][1]}")
                        elif 'Year distribution' in description:
                            print(f"  {'Year':<6} {'Days':<6} {'Start':<12} {'End':<12}")
                            print(f"  {'-'*6} {'-'*6} {'-'*12} {'-'*12}")
                            for row in results:
                                year, day_count, year_start, year_end = row
                                print(f"  {year:<6} {day_count:<6} {year_start!s:<12} {year_end!s:<12}")
                        elif 'Quarter distribution' in description:
                            print(f"  {'Q#':<3} {'Name':<4} {'Days':<6} {'Start':<12} {'End':<12}")
                            print(f"  {'-'*3} {'-'*4} {'-'*6} {'-'*12} {'-'*12}")
                            for row in results:
                                year, quarter, q_name, day_count, q_start, q_end = row
                                print(f"  {quarter:<3} {q_name:<4} {day_count:<6} {q_start!s:<12} {q_end!s:<12}")
                        elif 'Month distribution' in description:
                            print(f"  {'#':<3} {'Month':<12} {'Days':<6} {'Start':<12} {'End':<12}")
                            print(f"  {'-'*3} {'-'*12} {'-'*6} {'-'*12} {'-'*12}")
                            for row in results:
                                month, m_name, day_count, m_start, m_end = row
                                print(f"  {month:<3} {m_name:<12} {day_count:<6} {m_start!s:<12} {m_end!s:<12}")
                        elif 'Weekend vs Weekday' in description:
                            for row in results:
                                day_type, count, percentage = row
                                print(f"  {day_type}: {count:,} days ({percentage}%)")
                        elif 'validation' in description.lower():
                            violations_found = False
                            for row in results:
                                validation_type, count = row
                                if count > 0:
                                    print(f"  {validation_type}: {count} violations")
                                    violations_found = True
                            if not violations_found:
                                print("  No validation errors found ✓")
                        elif 'Sample weekend validation' in description:
                            print(f"  {'Date':<12} {'Day Name':<12} {'Weekend?':<10}")
                            print(f"  {'-'*12} {'-'*12} {'-'*10}")
                            for row in results:
                                cal_day, day_name, is_weekend = row
                                weekend_str = 'Yes' if is_weekend else 'No'
                                print(f"  {cal_day!s:<12} {day_name.strip():<12} {weekend_str:<10}")
                        elif 'Leap year validation' in description:
                            print(f"  {'Year':<6} {'Days':<6} {'Type':<12}")
                            print(f"  {'-'*6} {'-'*6} {'-'*12}")
                            for row in results:
                                year, days, year_type = row
                                print(f"  {year:<6} {days:<6} {year_type:<12}")
                        else:
                            for row in results:
                                print(f"  {row}")
                    
                    print("\n" + "="*100)
        except Exception as e:
            print(f"Error during validation: {e}")

def main():
    """Main function to generate and insert date dimension data."""
    # PostgreSQL connection configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'demo_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    
    try:
        generator = DateDimensionGenerator(**db_config)
        
        # Create table if not exists
        generator.create_table_if_not_exists()
        
        # Ask user if they want to clear existing data
        response = input("Do you want to clear existing date dimension data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()
        
        # Generate date dimension data
        date_data = generator.generate_date_dimension_data()
        
        # Insert data into database
        generator.insert_date_dimension_data(date_data)
        
        # Validate inserted data
        generator.validate_data()
        
        print(f"\nDate dimension data generation completed successfully!")
        print(f"Generated {len(date_data):,} date records")
        print(f"Coverage: {generator.start_date} to {generator.end_date}")
        print(f"Span: Last 10 years + Next 2 years from current date")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Please check your database configuration and ensure PostgreSQL is running.")

if __name__ == "__main__":
    main()