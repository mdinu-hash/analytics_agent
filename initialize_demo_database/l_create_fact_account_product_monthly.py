import sqlite3
import random
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any, Tuple
import os
import numpy as np

sqlite3.register_adapter(date, lambda val: val.isoformat())
sqlite3.register_adapter(datetime, lambda val: val.isoformat())

class FactAccountProductMonthlyGenerator:
    def __init__(self, connection_string: str = None, **db_params):
        """Initialize the fact account product monthly generator with SQLite database connection.

        Args:
            connection_string: Path to SQLite database file
            **db_params: Individual connection parameters (db_path)
        """
        self.db_path = connection_string or db_params.get('db_path', './demo.db')

        # Asset category weights by business line from schema
        self.business_line_weights = {
            'Managed Portfolio': {
                'Equity': 0.45,
                'Fixed Income': 0.35,
                'Multi-Asset': 0.18,
                'Cash': 0.02
            },
            'Separately Managed Account': {
                'Equity': 0.65,
                'Fixed Income': 0.20,
                'Multi-Asset': 0.13,
                'Cash': 0.02
            },
            'Mutual Fund Wrap': {
                'Equity': 0.55,
                'Fixed Income': 0.30,
                'Multi-Asset': 0.13,
                'Cash': 0.02
            },
            'Annuity': {
                'Multi-Asset': 0.70,
                'Fixed Income': 0.30,
                'Equity': 0.00,
                'Cash': 0.00
            },
            'Cash': {
                'Cash': 1.00,
                'Equity': 0.00,
                'Fixed Income': 0.00,
                'Multi-Asset': 0.00
            }
        }

        # Products by category (will be loaded from database)
        self.products_by_category = {}

    def _weighted_choice(self, choices: Dict[str, float]) -> str:
        """Select a random choice based on weighted distribution."""
        # Filter out zero weights
        valid_choices = {k: v for k, v in choices.items() if v > 0}
        if not valid_choices:
            return None

        choices_list = list(valid_choices.keys())
        weights = list(valid_choices.values())
        return np.random.choice(choices_list, p=weights)

    def _load_products_by_category(self):
        """Load products from database grouped by asset category."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT product_id, asset_category, asset_subcategory, product_line, product_name
                FROM product
                ORDER BY product_id
            """)

            for row in cursor.fetchall():
                product_id, asset_category, asset_subcategory, product_line, product_name = row

                if asset_category not in self.products_by_category:
                    self.products_by_category[asset_category] = []

                self.products_by_category[asset_category].append({
                    'product_id': product_id,
                    'asset_category': asset_category,
                    'asset_subcategory': asset_subcategory,
                    'product_line': product_line,
                    'product_name': product_name
                })

            if not self.products_by_category:
                raise Exception("No products found. Please run create_product.py first.")

            # Display product counts by category
            print("Products available by asset category:")
            for category, products in self.products_by_category.items():
                print(f"  {category}: {len(products)} products")

        except Exception as e:
            print(f"Error loading products: {e}")
            raise
        finally:
            conn.close()

    def _get_account_monthly_data(self) -> List[Dict[str, Any]]:
        """Get distinct (snapshot_date, account_key) combinations from fact_account_monthly."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    fam.snapshot_date,
                    fam.account_key,
                    a.account_id,
                    bl.business_line_name
                FROM fact_account_monthly fam
                JOIN account a ON fam.account_key = a.account_key
                JOIN business_line bl ON fam.business_line_key = bl.business_line_key
                WHERE a.to_date = '9999-12-31'
                ORDER BY fam.snapshot_date, fam.account_key
            """)

            account_data = []
            for row in cursor.fetchall():
                account_data.append({
                    'snapshot_date': row[0],
                    'account_key': row[1],
                    'account_id': row[2],
                    'business_line_name': row[3]
                })

            if not account_data:
                raise Exception("No account monthly data found. Please run create_fact_account_monthly.py first.")

            print(f"Found {len(account_data)} account-month combinations")
            return account_data

        except Exception as e:
            print(f"Error fetching account monthly data: {e}")
            raise
        finally:
            conn.close()

    def _select_products_for_account(self, business_line_name: str, num_products: int) -> List[int]:
        """Select products for an account based on business line weights."""
        weights = self.business_line_weights.get(business_line_name, self.business_line_weights['Managed Portfolio'])

        selected_products = []

        for _ in range(num_products):
            # Select asset category based on weights
            asset_category = self._weighted_choice(weights)

            if asset_category and asset_category in self.products_by_category:
                # Select random product from this category
                available_products = self.products_by_category[asset_category]
                if available_products:
                    product = random.choice(available_products)
                    selected_products.append(product['product_id'])

        # Remove duplicates while preserving order and ensuring we have enough products
        unique_products = []
        for product_id in selected_products:
            if product_id not in unique_products:
                unique_products.append(product_id)

        # If we don't have enough unique products, add more
        while len(unique_products) < num_products:
            asset_category = self._weighted_choice(weights)
            if asset_category and asset_category in self.products_by_category:
                available_products = self.products_by_category[asset_category]
                if available_products:
                    product = random.choice(available_products)
                    if product['product_id'] not in unique_products:
                        unique_products.append(product['product_id'])

        return unique_products[:num_products]

    def _generate_allocation_percentages(self, num_products: int) -> List[float]:
        """Generate allocation percentages that sum to 100%."""
        if num_products == 1:
            return [100.0]

        # Generate random weights for all products except the last
        allocations = []
        remaining = 100.0

        for i in range(num_products - 1):
            # Generate allocation between 20 and min(100, remaining - (num_products - i - 1) * 20)
            # This ensures we can still allocate at least 20% to remaining products
            min_allocation = 20.0
            max_allocation = min(100.0, remaining - (num_products - i - 1) * min_allocation)

            if max_allocation <= min_allocation:
                allocation = min_allocation
            else:
                allocation = random.uniform(min_allocation, max_allocation)

            allocations.append(allocation)
            remaining -= allocation

        # Last product gets the remainder
        allocations.append(remaining)

        # Ensure all allocations are between 0 and 100
        allocations = [max(0.0, min(100.0, alloc)) for alloc in allocations]

        # Normalize to ensure sum equals 100%
        total = sum(allocations)
        if total > 0:
            allocations = [alloc * 100.0 / total for alloc in allocations]

        return allocations

    def generate_product_monthly_data(self) -> List[Dict[str, Any]]:
        """Generate product allocation data according to schema specifications."""
        print("Loading products by category...")
        self._load_products_by_category()

        print("Getting account monthly data...")
        account_data = self._get_account_monthly_data()

        print(f"Generating product allocations for {len(account_data)} account-month combinations...")

        product_monthly_data = []

        # Group by account_key to ensure consistent product allocation across months
        accounts_by_key = {}
        for data in account_data:
            account_key = data['account_key']
            if account_key not in accounts_by_key:
                accounts_by_key[account_key] = []
            accounts_by_key[account_key].append(data)

        # Track progress
        processed_accounts = 0
        total_accounts = len(accounts_by_key)

        for account_key, account_months in accounts_by_key.items():
            if processed_accounts % 1000 == 0:
                print(f"Processed {processed_accounts}/{total_accounts} accounts...")

            # Use first month's business line for product selection
            business_line_name = account_months[0]['business_line_name']

            # Determine number of products for this account (2-5)
            num_products = random.randint(2, 5)

            # Select products for this account (consistent across all months)
            selected_products = self._select_products_for_account(business_line_name, num_products)

            # Generate allocation percentages (consistent across all months)
            allocations = self._generate_allocation_percentages(num_products)

            # Create records for all months for this account
            for account_month in account_months:
                for i, product_id in enumerate(selected_products):
                    product_record = {
                        'snapshot_date': account_month['snapshot_date'],
                        'account_key': account_key,
                        'product_id': product_id,
                        'product_allocation_pct': round(allocations[i], 2)
                    }

                    product_monthly_data.append(product_record)

            processed_accounts += 1

        print(f"Generated {len(product_monthly_data)} product allocation records")
        return product_monthly_data

    def generate_fact_account_product_monthly_data(self) -> List[Dict[str, Any]]:
        """Alias for generate_product_monthly_data() for compatibility with notebook."""
        return self.generate_product_monthly_data()

    def create_table_if_not_exists(self):
        """Create fact_account_product_monthly table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fact_account_product_monthly (
            snapshot_date TEXT NOT NULL,
            account_key INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            product_allocation_pct REAL NOT NULL,
            PRIMARY KEY (snapshot_date, account_key, product_id),
            FOREIGN KEY (product_id) REFERENCES product(product_id),
            CONSTRAINT check_allocation_range CHECK (product_allocation_pct >= 0 AND product_allocation_pct <= 100)
        );

        CREATE INDEX IF NOT EXISTS ix_fact_product_monthly_snapshot
        ON fact_account_product_monthly(snapshot_date);

        CREATE INDEX IF NOT EXISTS ix_fact_product_monthly_account
        ON fact_account_product_monthly(account_key);

        CREATE INDEX IF NOT EXISTS ix_fact_product_monthly_product
        ON fact_account_product_monthly(product_id);
        """

        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(create_table_sql)
            print("Fact account product monthly table created successfully (if not existed)")
        except Exception as e:
            print(f"Error creating fact account product monthly table: {e}")
            raise
        finally:
            conn.close()

    def clear_existing_data(self):
        """Clear existing fact account product monthly data."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM fact_account_product_monthly")
            conn.commit()
            print("Existing fact account product monthly data cleared")
        except Exception as e:
            print(f"Error clearing existing data: {e}")
            raise
        finally:
            conn.close()

    def insert_product_monthly_data(self, product_data: List[Dict[str, Any]], batch_size: int = 1000):
        """Insert product monthly data into SQLite database."""
        print(f"Inserting {len(product_data)} product allocation records...")

        insert_sql = """
        INSERT INTO fact_account_product_monthly (
            snapshot_date, account_key, product_id, product_allocation_pct
        ) VALUES (
            :snapshot_date, :account_key, :product_id, :product_allocation_pct
        )
        """

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            for i in range(0, len(product_data), batch_size):
                batch = product_data[i:i + batch_size]
                cursor.executemany(insert_sql, batch)

                if (i // batch_size + 1) % 20 == 0:
                    print(f"Inserted {min(i + batch_size, len(product_data))} records...")

            conn.commit()
            print(f"Successfully inserted all {len(product_data)} product allocation records")
        except Exception as e:
            print(f"Error inserting product monthly data: {e}")
            raise
        finally:
            conn.close()

    def insert_fact_account_product_monthly_data(self, product_data: List[Dict[str, Any]], batch_size: int = 1000):
        """Alias for insert_product_monthly_data() for compatibility with notebook."""
        return self.insert_product_monthly_data(product_data, batch_size)

    def validate_data(self):
        """Validate the inserted data meets schema requirements."""
        validation_queries = {
            'Total product allocation records': "SELECT COUNT(*) FROM fact_account_product_monthly",
            'Allocation sum validation': """
                SELECT
                    COUNT(*) as accounts_with_incorrect_sum
                FROM (
                    SELECT snapshot_date, account_key, SUM(product_allocation_pct) as total_allocation
                    FROM fact_account_product_monthly
                    GROUP BY snapshot_date, account_key
                    HAVING ABS(SUM(product_allocation_pct) - 100.0) > 0.01
                ) incorrect_sums
            """,
            'Products per account distribution': """
                SELECT
                    product_count,
                    COUNT(*) as account_count
                FROM (
                    SELECT snapshot_date, account_key, COUNT(*) as product_count
                    FROM fact_account_product_monthly
                    GROUP BY snapshot_date, account_key
                ) product_counts
                GROUP BY product_count
                ORDER BY product_count
            """,
            'Allocation range validation': """
                SELECT
                    'Below 0%' as range_type,
                    COUNT(*) as count
                FROM fact_account_product_monthly
                WHERE product_allocation_pct < 0
                UNION ALL
                SELECT
                    'Above 100%' as range_type,
                    COUNT(*) as count
                FROM fact_account_product_monthly
                WHERE product_allocation_pct > 100
                UNION ALL
                SELECT
                    'Between 0-20%' as range_type,
                    COUNT(*) as count
                FROM fact_account_product_monthly
                WHERE product_allocation_pct >= 0 AND product_allocation_pct < 20
                UNION ALL
                SELECT
                    'Between 20-50%' as range_type,
                    COUNT(*) as count
                FROM fact_account_product_monthly
                WHERE product_allocation_pct >= 20 AND product_allocation_pct < 50
                UNION ALL
                SELECT
                    'Between 50-100%' as range_type,
                    COUNT(*) as count
                FROM fact_account_product_monthly
                WHERE product_allocation_pct >= 50 AND product_allocation_pct <= 100
            """
        }

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            print("\n" + "="*120)
            print("FACT ACCOUNT PRODUCT MONTHLY DATA VALIDATION RESULTS")
            print("="*120)

            for description, query in validation_queries.items():
                cursor.execute(query)
                results = cursor.fetchall()

                print(f"\n{description}:")

                if len(results) == 1 and len(results[0]) == 1:
                    print(f"  {results[0][0]}")
                elif 'Products per account distribution' in description:
                    total_accounts = sum(row[1] for row in results)
                    for row in results:
                        product_count, account_count = row
                        percentage = (account_count / total_accounts) * 100 if total_accounts > 0 else 0
                        print(f"  {product_count} products: {account_count:,} accounts ({percentage:.1f}%)")
                elif 'Allocation range validation' in description:
                    for row in results:
                        range_type, count = row
                        print(f"  {range_type}: {count:,} allocations")
                else:
                    for row in results:
                        print(f"  {row}")

            print("\n" + "="*120)
        except Exception as e:
            print(f"Error during validation: {e}")
        finally:
            conn.close()

def main():
    """Main function to generate and insert fact account product monthly data."""
    db_config = {
        'db_path': os.getenv('DB_PATH', './demo.db')
    }

    try:
        generator = FactAccountProductMonthlyGenerator(**db_config)

        generator.create_table_if_not_exists()

        response = input("Do you want to clear existing fact account product monthly data? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            generator.clear_existing_data()

        product_data = generator.generate_product_monthly_data()
        generator.insert_product_monthly_data(product_data)
        generator.validate_data()

        print(f"\nFact account product monthly data generation completed successfully!")
        print(f"Generated {len(product_data)} product allocation records")
        print(f"Data shows product allocations for all account-month combinations")

    except Exception as e:
        print(f"Error: {e}")
        print("Please check your SQLite database path configuration.")
        print("Ensure you have run create_fact_account_monthly.py and create_product.py first.")

# Alias for compatibility with notebook
FactAccountProductMonthlyDataGenerator = FactAccountProductMonthlyGenerator

if __name__ == "__main__":
    main()
