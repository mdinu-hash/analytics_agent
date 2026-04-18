import sqlite3
import random
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any
import os
import numpy as np

sqlite3.register_adapter(date, lambda val: val.isoformat())
sqlite3.register_adapter(datetime, lambda val: val.isoformat())

class ProductDataGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the product data generator with SQLite database connection.

        Args:
            connection_string: Path to SQLite database file
            **db_params: Individual connection parameters (db_path)
        """
        self.db_path = connection_string or db_params.get('db_path', './demo.db')

        self.target_product_count = 350

        self.asset_category_dist = {
            'Equity': 0.50,
            'Fixed Income': 0.35,
            'Multi-Asset': 0.12,
            'Cash': 0.03
        }

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

        self.product_line_dist = {
            'Mutual Fund': 0.40,
            'ETF': 0.25,
            'Separately Managed Account Strategy': 0.20,
            'Annuity Contract': 0.10,
            'Money Market': 0.05
        }

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

        remaining = self.target_product_count - len(products_by_subcategory)
        if remaining > 0:
            subcategories = list(self.asset_subcategory_dist.keys())
            for _ in range(remaining):
                products_by_subcategory.append(random.choice(subcategories))

        random.shuffle(products_by_subcategory)
        return products_by_subcategory[:self.target_product_count]

    def generate_product_data(self) -> List[Dict[str, Any]]:
        """Generate product data according to schema specifications."""
        print(f"Generating {self.target_product_count} products...")

        products = []

        subcategory_assignments = self._distribute_products_by_subcategory()

        for product_id in range(1, self.target_product_count + 1):
            if product_id % 50 == 0:
                print(f"Generated {product_id} products...")

            asset_subcategory = subcategory_assignments[product_id - 1]
            asset_category = self.subcategory_to_category[asset_subcategory]

            product_line = self._weighted_choice(self.product_line_dist)

            if asset_category == 'Cash':
                product_line = 'Money Market'
            elif asset_subcategory == 'Common Stock' and random.random() < 0.7:
                product_line = random.choice(['ETF', 'Separately Managed Account Strategy'])
            elif asset_subcategory in ['U.S. Treasury Bill', 'U.S. Treasury Note'] and random.random() < 0.5:
                product_line = 'ETF'

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
            asset_category TEXT NOT NULL,
            asset_subcategory TEXT NOT NULL,
            product_line TEXT NOT NULL,
            product_name TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_product_category ON product(asset_category);
        CREATE INDEX IF NOT EXISTS ix_product_subcategory ON product(asset_subcategory);
        CREATE INDEX IF NOT EXISTS ix_product_line ON product(product_line);
        """

        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(create_table_sql)
            print("Product table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating product table: {e}")
            raise
        finally:
            conn.close()

    def clear_existing_data(self):
        """Clear existing product data."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM product")
            conn.commit()
            print("Existing product data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
        finally:
            conn.close()

    def insert_product_data(self, products: List[Dict[str, Any]], batch_size: int = 1000):
        """Insert product data into SQLite database."""
        print(f"Inserting {len(products)} product records...")

        insert_sql = """
        INSERT INTO product (
            product_id, asset_category, asset_subcategory, product_line, product_name
        ) VALUES (
            :product_id, :asset_category, :asset_subcategory, :product_line, :product_name
        )
        """

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
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
        finally:
            conn.close()

    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total product count': "SELECT COUNT(*) FROM product",
            'Asset category distribution': """
                SELECT asset_category, COUNT(*) as count
                FROM product
                GROUP BY asset_category
                ORDER BY count DESC
            """,
        }

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            print("\n" + "="*70)
            print("DATA VALIDATION RESULTS")
            print("="*70)

            for description, query in validation_queries.items():
                cursor.execute(query)
                results = cursor.fetchall()

                print(f"\n{description}:")
                if len(results) == 1 and len(results[0]) == 1:
                    print(f"  {results[0][0]}")
                else:
                    for row in results:
                        print(f"  {row}")

            print("\n" + "="*70)
        except Exception as e:
            print(f"Error during validation: {e}")
        finally:
            conn.close()

def main():
    """Main function to generate and insert product data."""
    db_config = {
        'db_path': os.getenv('DB_PATH', './demo.db')
    }

    try:
        generator = ProductDataGenerator(**db_config)

        generator.create_table_if_not_exists()

        response = input("Do you want to clear existing product data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()

        products = generator.generate_product_data()
        generator.insert_product_data(products)
        generator.validate_data()

        print(f"\nProduct data generation completed successfully!")
        print(f"Generated {len(products)} total product records")

    except Exception as e:
        print(f"Error: {e}")
        print("Please check your SQLite database path configuration.")

if __name__ == "__main__":
    main()
