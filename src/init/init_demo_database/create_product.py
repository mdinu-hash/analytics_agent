import psycopg2
import random
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any
import os
from urllib.parse import urlparse
import numpy as np

class ProductDataGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the product data generator with PostgreSQL database connection.
        
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
        self.target_product_count = 350
        
        # Asset category distribution from schema
        self.asset_category_dist = {
            'Equity': 0.50,
            'Fixed Income': 0.35,
            'Multi-Asset': 0.12,
            'Cash': 0.03
        }
        
        # Asset subcategory distribution from schema
        self.asset_subcategory_dist = {
            'Common Stock': 0.20,
            'Preferred Stock': 0.05,
            'Equity Mutual Fund': 0.25,
            'Balanced Fund (60/40)': 0.07,
            'Target-Date Fund': 0.05,
            'U.S. Treasury Bill': 0.04,
            'U.S. Treasury Note': 0.07,
            'Investment-Grade Corporate Bond': 0.12,
            'Municipal Bond': 0.12,
            'Money Market Fund': 0.03
        }
        
        # Product line distribution from schema
        self.product_line_dist = {
            'Mutual Fund': 0.40,
            'ETF': 0.25,
            'Separately Managed Account Strategy': 0.20,
            'Annuity Contract': 0.10,
            'Money Market': 0.05
        }
        
        # Mapping subcategories to categories
        self.subcategory_to_category = {
            'Common Stock': 'Equity',
            'Preferred Stock': 'Equity',
            'Equity Mutual Fund': 'Equity',
            'Balanced Fund (60/40)': 'Multi-Asset',
            'Target-Date Fund': 'Multi-Asset',
            'U.S. Treasury Bill': 'Fixed Income',
            'U.S. Treasury Note': 'Fixed Income',
            'Investment-Grade Corporate Bond': 'Fixed Income',
            'Municipal Bond': 'Fixed Income',
            'Money Market Fund': 'Cash'
        }
        
        # Product name components for generation
        self.equity_prefixes = [
            'Growth', 'Value', 'Dividend', 'Large Cap', 'Small Cap', 'Mid Cap',
            'International', 'Emerging Markets', 'Technology', 'Healthcare',
            'Financial', 'Energy', 'Consumer', 'Industrial', 'Real Estate'
        ]
        
        self.fixed_income_prefixes = [
            'Corporate Bond', 'Government Bond', 'Municipal Bond', 'High Yield',
            'Investment Grade', 'Treasury', 'Inflation Protected', 'Short Term',
            'Intermediate Term', 'Long Term', 'Global Bond', 'Emerging Market Bond'
        ]
        
        self.multi_asset_prefixes = [
            'Balanced', 'Conservative', 'Moderate', 'Aggressive', 'Target Date',
            'Asset Allocation', 'Multi-Strategy', 'Global Allocation',
            'Income', 'Growth & Income', 'Tactical'
        ]
        
        self.fund_suffixes = [
            'Fund', 'Portfolio', 'Trust', 'Index', 'Strategy', 'Select',
            'Opportunities', 'Plus', 'Prime', 'Advantage', 'Core', 'Enhanced'
        ]
        
        self.company_names = [
            'Vanguard', 'Fidelity', 'BlackRock', 'American Funds', 'T. Rowe Price',
            'Franklin Templeton', 'Invesco', 'PIMCO', 'Schwab', 'JPMorgan',
            'Goldman Sachs', 'Morgan Stanley', 'Wells Fargo', 'Northern Trust',
            'State Street', 'Nuveen', 'Columbia', 'Principal', 'TIAA-CREF',
            'MFS', 'John Hancock', 'Prudential', 'MetLife', 'AIG'
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
    
    def _weighted_choice(self, choices: Dict[str, float]) -> str:
        """Select a random choice based on weighted distribution."""
        choices_list = list(choices.keys())
        weights = list(choices.values())
        return np.random.choice(choices_list, p=weights)
    
    def _generate_product_name(self, asset_category: str, asset_subcategory: str, product_line: str) -> str:
        """Generate realistic product name based on category and type."""
        company = random.choice(self.company_names)
        
        if asset_category == 'Equity':
            prefix = random.choice(self.equity_prefixes)
        elif asset_category == 'Fixed Income':
            prefix = random.choice(self.fixed_income_prefixes)
        elif asset_category == 'Multi-Asset':
            prefix = random.choice(self.multi_asset_prefixes)
        else:  # Cash
            prefix = 'Money Market'
        
        if product_line == 'ETF':
            suffix = 'ETF'
        elif product_line == 'Separately Managed Account Strategy':
            suffix = 'SMA'
        elif product_line == 'Annuity Contract':
            suffix = 'Annuity'
        elif product_line == 'Money Market':
            suffix = 'Money Market'
        else:  # Mutual Fund
            suffix = random.choice(self.fund_suffixes)
        
        # Special cases for specific subcategories
        if asset_subcategory == 'Target-Date Fund':
            year = random.choice([2030, 2035, 2040, 2045, 2050, 2055, 2060, 2065])
            return f"{company} Target Date {year} {suffix}"
        elif asset_subcategory == 'Balanced Fund (60/40)':
            return f"{company} Balanced {suffix}"
        elif 'Treasury' in asset_subcategory:
            duration = random.choice(['Short-Term', 'Intermediate-Term', 'Long-Term'])
            return f"{company} {duration} Treasury {suffix}"
        elif asset_subcategory == 'Money Market Fund':
            return f"{company} Prime Money Market {suffix}"
        
        return f"{company} {prefix} {suffix}"
    
    def _distribute_products_by_subcategory(self) -> List[str]:
        """Distribute products according to asset subcategory percentages."""
        products_by_subcategory = []
        
        for subcategory, percentage in self.asset_subcategory_dist.items():
            count = int(self.target_product_count * percentage)
            products_by_subcategory.extend([subcategory] * count)
        
        # Fill remaining spots with random subcategories to reach exact target
        remaining = self.target_product_count - len(products_by_subcategory)
        if remaining > 0:
            subcategories = list(self.asset_subcategory_dist.keys())
            for _ in range(remaining):
                products_by_subcategory.append(random.choice(subcategories))
        
        # Shuffle to randomize order
        random.shuffle(products_by_subcategory)
        return products_by_subcategory[:self.target_product_count]
    
    def generate_product_data(self) -> List[Dict[str, Any]]:
        """Generate product data according to schema specifications."""
        print(f"Generating {self.target_product_count} products...")
        
        products = []
        
        # Distribute products by subcategory first to ensure exact percentages
        subcategory_assignments = self._distribute_products_by_subcategory()
        
        for product_id in range(1, self.target_product_count + 1):
            if product_id % 50 == 0:
                print(f"Generated {product_id} products...")
            
            # Get assigned subcategory
            asset_subcategory = subcategory_assignments[product_id - 1]
            asset_category = self.subcategory_to_category[asset_subcategory]
            
            # Generate product line based on distribution
            product_line = self._weighted_choice(self.product_line_dist)
            
            # Adjust product line based on asset category for realism
            if asset_category == 'Cash':
                product_line = 'Money Market'
            elif asset_subcategory == 'Common Stock' and random.random() < 0.7:
                product_line = random.choice(['ETF', 'Separately Managed Account Strategy'])
            elif asset_subcategory in ['U.S. Treasury Bill', 'U.S. Treasury Note'] and random.random() < 0.5:
                product_line = 'ETF'
            
            # Generate product name
            product_name = self._generate_product_name(asset_category, asset_subcategory, product_line)
            
            product = {
                'product_id': product_id,
                'asset_category': asset_category,
                'asset_subcategory': asset_subcategory,
                'product_line': product_line,
                'product_name': product_name
            }
            
            products.append(product)
        
        print(f"Generated {len(products)} product records")
        return products
    
    def create_table_if_not_exists(self):
        """Create product table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS product (
            product_id INTEGER PRIMARY KEY,
            asset_category VARCHAR(20) NOT NULL,
            asset_subcategory VARCHAR(50) NOT NULL,
            product_line VARCHAR(50) NOT NULL,
            product_name VARCHAR(100) NOT NULL
        );
        
        -- Create indexes for efficient lookups
        CREATE INDEX IF NOT EXISTS ix_product_category 
        ON product(asset_category);
        
        CREATE INDEX IF NOT EXISTS ix_product_subcategory 
        ON product(asset_subcategory);
        
        CREATE INDEX IF NOT EXISTS ix_product_line 
        ON product(product_line);
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    conn.commit()
                    print("Product table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating product table: {e}")
            raise
    
    def clear_existing_data(self):
        """Clear existing product data."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM product")
                    conn.commit()
                    print("Existing product data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
    
    def insert_product_data(self, products: List[Dict[str, Any]], batch_size: int = 1000):
        """Insert product data into PostgreSQL database."""
        print(f"Inserting {len(products)} product records...")
        
        insert_sql = """
        INSERT INTO product (
            product_id, asset_category, asset_subcategory, product_line, product_name
        ) VALUES (
            %(product_id)s, %(asset_category)s, %(asset_subcategory)s, %(product_line)s, %(product_name)s
        )
        """
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    for i in range(0, len(products), batch_size):
                        batch = products[i:i + batch_size]
                        cursor.executemany(insert_sql, batch)
                        
                        if (i // batch_size + 1) % 5 == 0:
                            print(f"Inserted {min(i + batch_size, len(products))} records...")
                    
                    conn.commit()
                    print(f"Successfully inserted all {len(products)} product records")
        except Exception as e:
            print(f"Error inserting product data: {e}")
            raise
    
    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total product count': "SELECT COUNT(*) FROM product",
            'Asset category distribution': """
                SELECT asset_category, COUNT(*) as count,
                       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM product), 1) as percentage
                FROM product 
                GROUP BY asset_category 
                ORDER BY count DESC
            """,
            'Asset subcategory distribution': """
                SELECT asset_subcategory, COUNT(*) as count,
                       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM product), 1) as percentage
                FROM product 
                GROUP BY asset_subcategory 
                ORDER BY count DESC
            """,
            'Product line distribution': """
                SELECT product_line, COUNT(*) as count,
                       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM product), 1) as percentage
                FROM product 
                GROUP BY product_line 
                ORDER BY count DESC
            """,
            'Sample product names': """
                SELECT product_name 
                FROM product 
                ORDER BY RANDOM()
                LIMIT 10
            """,
            'Products with empty names': """
                SELECT COUNT(*) 
                FROM product 
                WHERE product_name IS NULL OR product_name = '' OR TRIM(product_name) = ''
            """
        }
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    print("\n" + "="*70)
                    print("DATA VALIDATION RESULTS")
                    print("="*70)
                    
                    for description, query in validation_queries.items():
                        cursor.execute(query)
                        results = cursor.fetchall()
                        
                        print(f"\n{description}:")
                        if len(results) == 1 and len(results[0]) == 1:
                            print(f"  {results[0][0]}")
                        elif 'distribution' in description:
                            for row in results:
                                print(f"  {row[0]}: {row[1]} ({row[2]}%)")
                        elif 'Sample product names' in description:
                            for row in results:
                                print(f"  {row[0]}")
                        else:
                            for row in results:
                                print(f"  {row}")
                    
                    print("\n" + "="*70)
        except Exception as e:
            print(f"Error during validation: {e}")

def main():
    """Main function to generate and insert product data."""
    # PostgreSQL connection configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'demo_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    
    try:
        generator = ProductDataGenerator(**db_config)
        
        # Create table if not exists
        generator.create_table_if_not_exists()
        
        # Ask user if they want to clear existing data
        response = input("Do you want to clear existing product data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()
        
        # Generate product data
        products = generator.generate_product_data()
        
        # Insert data into database
        generator.insert_product_data(products)
        
        # Validate inserted data
        generator.validate_data()
        
        print(f"\nProduct data generation completed successfully!")
        print(f"Generated {len(products)} total product records")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Please check your database configuration and ensure PostgreSQL is running.")

if __name__ == "__main__":
    main()