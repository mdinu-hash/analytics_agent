import psycopg2
import random
import pandas as pd
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any, Tuple
import os
from urllib.parse import urlparse
import numpy as np

class FactCustomerFeedbackGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the fact customer feedback generator with PostgreSQL database connection.
        
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
        self.min_feedback_date = date(2024, 1, 1)  # January 1st of year before current_date
        self.target_monthly_feedback = 1000  # 1k feedback per month from schema
        
        # Satisfaction score parameters from schema
        self.satisfaction_params = {
            'median': 90,     # Median satisfaction score of 90
            'std_dev': 12     # Reasonable standard deviation to create realistic distribution
        }
        
        # Sample feedback texts (≤ 200 chars, ≤ 2 sentences per schema)
        self.positive_feedback_templates = [
            "Excellent service from my advisor. Very responsive and knowledgeable about investment strategies.",
            "Great communication throughout the quarter. My advisor always explains complex concepts clearly.",
            "Outstanding portfolio performance this year. I appreciate the personalized attention and regular updates.",
            "Professional and trustworthy advisor. Always available to answer questions and provide guidance.",
            "Highly satisfied with the investment recommendations. My advisor understands my risk tolerance perfectly.",
            "Fantastic support during market volatility. Clear explanations helped me stay confident in my strategy.",
            "My advisor goes above and beyond expectations. Regular check-ins and proactive market insights are valuable.",
            "Impressed with the comprehensive financial planning. My advisor takes a holistic approach to my goals.",
            "Excellent customer service and attention to detail. I feel confident about my financial future.",
            "Great advisor who listens to my concerns. Always provides thoughtful and well-researched recommendations."
        ]
        
        self.neutral_feedback_templates = [
            "Service is adequate but could improve communication frequency. Generally satisfied with performance.",
            "Advisor is knowledgeable but sometimes slow to respond. Overall experience has been acceptable.",
            "Portfolio performance meets expectations. Would like more frequent updates on market conditions.",
            "Good advisor but limited availability for meetings. Investment strategy seems sound so far.",
            "Reasonable service quality overall. Some room for improvement in proactive communication.",
            "Advisor is professional but interactions feel routine. Would appreciate more personalized approach.",
            "Investment performance is okay, meeting basic expectations. Communication could be more timely.",
            "Service is satisfactory but not exceptional. Advisor handles basic needs adequately.",
            "Generally positive experience with room for improvement. Would like more detailed explanations.",
            "Adequate support for my investment needs. Advisor is competent but not particularly proactive."
        ]
        
        self.negative_feedback_templates = [
            "Disappointed with communication frequency. Advisor rarely initiates contact or provides market updates.",
            "Poor responsiveness to calls and emails. Investment performance has been below expectations.",
            "Advisor seems disengaged and provides generic advice. Not feeling valued as a client.",
            "Frequent delays in returning calls. Portfolio strategy doesn't align with my stated objectives.",
            "Unsatisfied with lack of proactive communication. Advisor only contacts me when prompted.",
            "Investment recommendations seem one-size-fits-all. Not receiving the personalized service expected.",
            "Poor performance during market downturns. Advisor failed to adjust strategy appropriately.",
            "Limited availability for consultations. Feel like I'm not getting adequate attention for my assets.",
            "Advisor lacks depth in market knowledge. Recommendations seem conservative without explanation.",
            "Frustrated with slow response times. Investment strategy changes are not well communicated."
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
    
    def _generate_satisfaction_score(self) -> int:
        """Generate satisfaction score using normal distribution with median 90."""
        # Generate score normally distributed around median 90
        score = np.random.normal(self.satisfaction_params['median'], self.satisfaction_params['std_dev'])
        
        # Ensure score is within bounds 0-100
        score = max(0, min(100, int(round(score))))
        
        return score
    
    def _select_feedback_text(self, satisfaction_score: int) -> str:
        """Select appropriate feedback text based on satisfaction score."""
        if satisfaction_score >= 80:
            # High satisfaction - positive feedback
            return random.choice(self.positive_feedback_templates)
        elif satisfaction_score >= 60:
            # Medium satisfaction - neutral feedback
            return random.choice(self.neutral_feedback_templates)
        else:
            # Low satisfaction - negative feedback
            return random.choice(self.negative_feedback_templates)
    
    def _generate_feedback_dates(self) -> List[date]:
        """Generate random feedback dates between min_feedback_date and current_date."""
        # Calculate total months between start and current date
        total_months = (self.current_date.year - self.min_feedback_date.year) * 12 + \
                      (self.current_date.month - self.min_feedback_date.month)
        
        # Target total feedback count
        total_feedback_target = total_months * self.target_monthly_feedback
        
        print(f"Generating feedback dates for {total_months} months ({total_feedback_target:,} total feedback)")
        
        feedback_dates = []
        current = self.min_feedback_date
        
        while current <= self.current_date:
            # Generate random number of feedback entries for this month (around target with variation)
            monthly_count = max(1, int(np.random.normal(self.target_monthly_feedback, 100)))
            
            # Generate random dates within this month
            if current.month == 12:
                next_month = date(current.year + 1, 1, 1)
            else:
                next_month = date(current.year, current.month + 1, 1)
            
            # Don't go beyond current_date
            month_end = min(next_month - timedelta(days=1), self.current_date)
            
            for _ in range(monthly_count):
                # Generate random day within the month
                day_offset = random.randint(0, (month_end - current).days)
                feedback_date = current + timedelta(days=day_offset)
                
                if feedback_date <= self.current_date:
                    feedback_dates.append(feedback_date)
            
            # Move to next month
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)
        
        # Shuffle to randomize order
        random.shuffle(feedback_dates)
        
        print(f"Generated {len(feedback_dates):,} feedback dates")
        return feedback_dates
    
    def _get_active_households_and_advisors(self) -> List[Dict[str, Any]]:
        """Get active households with their primary advisors for feedback generation."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    # Get active households with their advisors
                    cursor.execute("""
                        SELECT DISTINCT
                            h.household_key,
                            h.household_id,
                            h.household_advisor_id,
                            a.advisor_key,
                            h.from_date as household_from_date,
                            h.to_date as household_to_date,
                            a.from_date as advisor_from_date,
                            a.to_date as advisor_to_date
                        FROM household h
                        JOIN advisors a ON h.household_advisor_id = a.advisor_id
                        WHERE h.household_status = 'Active'
                        ORDER BY h.household_key
                    """)
                    
                    household_advisor_pairs = []
                    for row in cursor.fetchall():
                        household_advisor_pairs.append({
                            'household_key': row[0],
                            'household_id': row[1],
                            'household_advisor_id': row[2],
                            'advisor_key': row[3],
                            'household_from_date': row[4],
                            'household_to_date': row[5],
                            'advisor_from_date': row[6],
                            'advisor_to_date': row[7]
                        })
                    
                    if not household_advisor_pairs:
                        raise Exception("No active household-advisor pairs found. Please run create_household.py and create_advisors.py first.")
                    
                    print(f"Found {len(household_advisor_pairs)} household-advisor pairs for feedback generation")
                    return household_advisor_pairs
                    
        except Exception as e:
            print(f"Error fetching household-advisor pairs: {e}")
            raise
    
    def _is_valid_feedback_pair(self, household_data: Dict[str, Any], feedback_date: date) -> bool:
        """Check if household and advisor are valid for the feedback date per schema requirements."""
        # Check household validity
        # Household must be active and from_date >= feedback_date (per schema)
        if household_data['household_to_date'] != date(9999, 12, 31):
            # Household is terminated, check if termination is after feedback date
            if household_data['household_to_date'] <= feedback_date:
                return False
        
        if household_data['household_from_date'] > feedback_date:
            return False
        
        # Check advisor validity
        # Advisor must be active during feedback period (from_date <= feedback_date < to_date)
        if household_data['advisor_from_date'] > feedback_date:
            return False
        
        if household_data['advisor_to_date'] != date(9999, 12, 31):
            if household_data['advisor_to_date'] <= feedback_date:
                return False
        
        return True
    
    def generate_customer_feedback_data(self) -> List[Dict[str, Any]]:
        """Generate customer feedback data according to schema specifications."""
        print("Loading household-advisor relationships...")
        household_advisor_pairs = self._get_active_households_and_advisors()
        
        print("Generating feedback dates...")
        feedback_dates = self._generate_feedback_dates()
        
        print(f"Generating customer feedback for {len(feedback_dates):,} feedback entries...")
        
        feedback_data = []
        feedback_id = 1
        processed = 0
        
        for feedback_date in feedback_dates:
            if processed % 5000 == 0 and processed > 0:
                print(f"Generated {processed:,} feedback records...")
            
            # Randomly select a household-advisor pair
            max_attempts = 50  # Limit attempts to prevent infinite loops
            attempts = 0
            valid_pair = None
            
            while attempts < max_attempts:
                household_advisor = random.choice(household_advisor_pairs)
                
                # Check if this pair is valid for the feedback date
                if self._is_valid_feedback_pair(household_advisor, feedback_date):
                    valid_pair = household_advisor
                    break
                
                attempts += 1
            
            if valid_pair is None:
                # Skip this feedback date if no valid pair found
                continue
            
            # Generate satisfaction score
            satisfaction_score = self._generate_satisfaction_score()
            
            # Select appropriate feedback text based on score
            feedback_text = self._select_feedback_text(satisfaction_score)
            
            # Create feedback record
            feedback_record = {
                'feedback_date': feedback_date,
                'feedback_id': feedback_id,
                'household_key': valid_pair['household_key'],
                'advisor_key': valid_pair['advisor_key'],
                'feedback_text': feedback_text,
                'satisfaction_score': satisfaction_score
            }
            
            feedback_data.append(feedback_record)
            feedback_id += 1
            processed += 1
        
        print(f"Generated {len(feedback_data):,} customer feedback records")
        return feedback_data
    
    def generate_fact_customer_feedback_data(self) -> List[Dict[str, Any]]:
        """Alias for generate_customer_feedback_data() for compatibility with notebook."""
        return self.generate_customer_feedback_data()
    
    def create_table_if_not_exists(self):
        """Create fact_customer_feedback table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fact_customer_feedback (
            feedback_date DATE NOT NULL,
            feedback_id INTEGER NOT NULL,
            household_key INTEGER NOT NULL,
            advisor_key INTEGER NOT NULL,
            feedback_text TEXT NOT NULL,
            satisfaction_score INTEGER NOT NULL,
            PRIMARY KEY (feedback_id),
            FOREIGN KEY (household_key) REFERENCES household(household_key),
            FOREIGN KEY (advisor_key) REFERENCES advisors(advisor_key),
            CONSTRAINT check_satisfaction_score_range CHECK (satisfaction_score >= 0 AND satisfaction_score <= 100),
            CONSTRAINT check_feedback_text_length CHECK (LENGTH(feedback_text) <= 200),
            CONSTRAINT check_feedback_date_range CHECK (feedback_date >= DATE '2024-01-01' AND feedback_date <= DATE '2025-09-30')
        );
        
        -- Create indexes per schema requirements
        CREATE INDEX IF NOT EXISTS ix_fb_house_date 
        ON fact_customer_feedback(household_key, feedback_date);
        
        CREATE INDEX IF NOT EXISTS ix_fb_adv_date 
        ON fact_customer_feedback(advisor_key, feedback_date);
        
        CREATE INDEX IF NOT EXISTS ix_fb_satisfaction 
        ON fact_customer_feedback(satisfaction_score);
        
        CREATE INDEX IF NOT EXISTS ix_fb_date 
        ON fact_customer_feedback(feedback_date);
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    conn.commit()
                    print("Fact customer feedback table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating fact customer feedback table: {e}")
            raise
    
    def clear_existing_data(self):
        """Clear existing fact customer feedback data."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM fact_customer_feedback")
                    conn.commit()
                    print("Existing fact customer feedback data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
    
    def insert_customer_feedback_data(self, feedback_data: List[Dict[str, Any]], batch_size: int = 1000):
        """Insert customer feedback data into PostgreSQL database."""
        print(f"Inserting {len(feedback_data):,} feedback records...")
        
        insert_sql = """
        INSERT INTO fact_customer_feedback (
            feedback_date, feedback_id, household_key, advisor_key, feedback_text, satisfaction_score
        ) VALUES (
            %(feedback_date)s, %(feedback_id)s, %(household_key)s, %(advisor_key)s, %(feedback_text)s, %(satisfaction_score)s
        )
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    for i in range(0, len(feedback_data), batch_size):
                        batch = feedback_data[i:i + batch_size]
                        cursor.executemany(insert_sql, batch)
                        
                        if (i // batch_size + 1) % 20 == 0:
                            print(f"Inserted {min(i + batch_size, len(feedback_data)):,} records...")
                    
                    conn.commit()
                    print(f"Successfully inserted all {len(feedback_data):,} feedback records")
        except Exception as e:
            print(f"Error inserting customer feedback data: {e}")
            raise
    
    def insert_fact_customer_feedback_data(self, feedback_data: List[Dict[str, Any]], batch_size: int = 1000):
        """Alias for insert_customer_feedback_data() for compatibility with notebook."""
        return self.insert_customer_feedback_data(feedback_data, batch_size)
    
    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total feedback records': "SELECT COUNT(*) FROM fact_customer_feedback",
            'Date range validation': """
                SELECT 
                    MIN(feedback_date) as earliest_feedback,
                    MAX(feedback_date) as latest_feedback
                FROM fact_customer_feedback
            """,
            'Monthly feedback distribution': """
                SELECT 
                    DATE_TRUNC('month', feedback_date) as feedback_month,
                    COUNT(*) as feedback_count
                FROM fact_customer_feedback
                GROUP BY DATE_TRUNC('month', feedback_date)
                ORDER BY feedback_month
                LIMIT 12
            """,
            'Satisfaction score distribution': """
                SELECT 
                    CASE 
                        WHEN satisfaction_score >= 90 THEN 'Excellent (90-100)'
                        WHEN satisfaction_score >= 80 THEN 'Good (80-89)'
                        WHEN satisfaction_score >= 70 THEN 'Fair (70-79)'
                        WHEN satisfaction_score >= 60 THEN 'Poor (60-69)'
                        ELSE 'Very Poor (0-59)'
                    END as satisfaction_range,
                    COUNT(*) as feedback_count,
                    ROUND(AVG(satisfaction_score), 1) as avg_score
                FROM fact_customer_feedback
                GROUP BY 
                    CASE 
                        WHEN satisfaction_score >= 90 THEN 'Excellent (90-100)'
                        WHEN satisfaction_score >= 80 THEN 'Good (80-89)'
                        WHEN satisfaction_score >= 70 THEN 'Fair (70-79)'
                        WHEN satisfaction_score >= 60 THEN 'Poor (60-69)'
                        ELSE 'Very Poor (0-59)'
                    END
                ORDER BY MIN(satisfaction_score) DESC
            """,
            'Top and bottom rated advisors': """
                SELECT 
                    advisor_key,
                    COUNT(*) as feedback_count,
                    ROUND(AVG(satisfaction_score), 1) as avg_satisfaction,
                    MIN(satisfaction_score) as min_score,
                    MAX(satisfaction_score) as max_score
                FROM fact_customer_feedback
                GROUP BY advisor_key
                HAVING COUNT(*) >= 5
                ORDER BY avg_satisfaction DESC
                LIMIT 10
            """,
            'Warning signals - Low satisfaction advisors': """
                SELECT 
                    advisor_key,
                    COUNT(*) as low_satisfaction_feedback,
                    ROUND(AVG(satisfaction_score), 1) as avg_score_for_low_ratings
                FROM fact_customer_feedback
                WHERE satisfaction_score <= 60 
                  AND feedback_date >= CURRENT_DATE - INTERVAL '90 days'
                GROUP BY advisor_key
                HAVING COUNT(*) >= 2
                ORDER BY COUNT(*) DESC, AVG(satisfaction_score) ASC
            """,
            'Feedback text length validation': """
                SELECT 
                    'Text too long' as validation_type,
                    COUNT(*) as count
                FROM fact_customer_feedback 
                WHERE LENGTH(feedback_text) > 200
                UNION ALL
                SELECT 
                    'Empty feedback text' as validation_type,
                    COUNT(*) as count
                FROM fact_customer_feedback 
                WHERE feedback_text IS NULL OR feedback_text = ''
            """,
            'Referential integrity validation': """
                SELECT 
                    'Invalid household_key' as validation_type,
                    COUNT(*) as count
                FROM fact_customer_feedback fcf
                LEFT JOIN household h ON fcf.household_key = h.household_key
                WHERE h.household_key IS NULL
                UNION ALL
                SELECT 
                    'Invalid advisor_key' as validation_type,
                    COUNT(*) as count
                FROM fact_customer_feedback fcf
                LEFT JOIN advisors a ON fcf.advisor_key = a.advisor_key
                WHERE a.advisor_key IS NULL
                UNION ALL
                SELECT 
                    'Satisfaction score out of range' as validation_type,
                    COUNT(*) as count
                FROM fact_customer_feedback 
                WHERE satisfaction_score < 0 OR satisfaction_score > 100
            """,
            'Overall satisfaction statistics': """
                SELECT 
                    COUNT(*) as total_feedback,
                    ROUND(AVG(satisfaction_score), 1) as overall_avg_satisfaction,
                    ROUND(STDDEV(satisfaction_score), 1) as satisfaction_stddev,
                    MIN(satisfaction_score) as min_satisfaction,
                    MAX(satisfaction_score) as max_satisfaction,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY satisfaction_score) as median_satisfaction
                FROM fact_customer_feedback
            """
        }
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    print("\n" + "="*120)
                    print("FACT CUSTOMER FEEDBACK DATA VALIDATION RESULTS")
                    print("="*120)
                    
                    for description, query in validation_queries.items():
                        cursor.execute(query)
                        results = cursor.fetchall()
                        
                        print(f"\n{description}:")
                        
                        if len(results) == 1 and len(results[0]) <= 2:
                            if len(results[0]) == 1:
                                print(f"  {results[0][0]:,}")
                            else:
                                print(f"  From {results[0][0]} to {results[0][1]}")
                        elif 'Monthly feedback distribution' in description:
                            print(f"  {'Month':<12} {'Count':<10}")
                            print(f"  {'-'*12} {'-'*10}")
                            for row in results:
                                month, count = row
                                print(f"  {str(month)[:7]:<12} {count:<10,}")
                        elif 'Satisfaction score distribution' in description:
                            total_feedback = sum(row[1] for row in results)
                            for row in results:
                                range_name, count, avg_score = row
                                percentage = (count / total_feedback) * 100 if total_feedback > 0 else 0
                                print(f"  {range_name}: {count:,} feedback ({percentage:.1f}%) - Avg: {avg_score}")
                        elif 'Top and bottom rated advisors' in description:
                            print(f"  {'Advisor':<8} {'Count':<8} {'Avg Score':<10} {'Min':<5} {'Max':<5}")
                            print(f"  {'-'*8} {'-'*8} {'-'*10} {'-'*5} {'-'*5}")
                            for row in results:
                                advisor_key, count, avg_score, min_score, max_score = row
                                print(f"  {advisor_key:<8} {count:<8} {avg_score:<10} {min_score:<5} {max_score:<5}")
                        elif 'Warning signals' in description:
                            if not results:
                                print("  No advisors with warning signals found ✓")
                            else:
                                print(f"  {'Advisor':<8} {'Low Ratings':<12} {'Avg Score':<10}")
                                print(f"  {'-'*8} {'-'*12} {'-'*10}")
                                for row in results:
                                    advisor_key, low_count, avg_score = row
                                    print(f"  {advisor_key:<8} {low_count:<12} {avg_score:<10}")
                        elif 'validation' in description:
                            violations_found = False
                            for row in results:
                                validation_type, count = row
                                if count > 0:
                                    print(f"  {validation_type}: {count} violations")
                                    violations_found = True
                            if not violations_found:
                                print("  No validation errors found ✓")
                        elif 'Overall satisfaction statistics' in description and results:
                            total, avg, stddev, min_score, max_score, median = results[0]
                            print(f"  Total Feedback: {total:,}")
                            print(f"  Average Satisfaction: {avg}")
                            print(f"  Standard Deviation: {stddev}")
                            print(f"  Score Range: {min_score} - {max_score}")
                            print(f"  Median Score: {median}")
                        else:
                            for row in results:
                                print(f"  {row}")
                    
                    print("\n" + "="*120)
        except Exception as e:
            print(f"Error during validation: {e}")

def main():
    """Main function to generate and insert fact customer feedback data."""
    # PostgreSQL connection configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'demo_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    
    try:
        generator = FactCustomerFeedbackGenerator(**db_config)
        
        # Create table if not exists
        generator.create_table_if_not_exists()
        
        # Ask user if they want to clear existing data
        response = input("Do you want to clear existing fact customer feedback data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()
        
        # Generate customer feedback data
        feedback_data = generator.generate_customer_feedback_data()
        
        # Insert data into database
        generator.insert_customer_feedback_data(feedback_data)
        
        # Validate inserted data
        generator.validate_data()
        
        print(f"\nFact customer feedback data generation completed successfully!")
        print(f"Generated {len(feedback_data):,} feedback records")
        print(f"Data includes realistic feedback text and satisfaction scores")
        print(f"Target: ~1,000 feedback entries per month with median satisfaction of 90")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Please check your database configuration and ensure PostgreSQL is running.")
        print("Ensure you have run the prerequisite scripts:")
        print("- create_household.py")
        print("- create_advisors.py")

# Alias for compatibility with notebook
FactCustomerFeedbackDataGenerator = FactCustomerFeedbackGenerator

if __name__ == "__main__":
    main()