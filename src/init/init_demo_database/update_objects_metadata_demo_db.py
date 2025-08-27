import psycopg2
import logging
from typing import Dict, List, Optional
import pandas as pd
from contextlib import contextmanager

class PostgresMetadataManager:
    """Get/Update tables metadata stored in PostgreSQL using psycopg2."""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = psycopg2.connect(self.connection_string)
            yield conn
        finally:
            if conn:
                conn.close()

    def get_table_metadata(self, schema: str, table: str, columns: Optional[List[str]] = None):
        """Get table and column metadata from PostgreSQL information schema."""
        full_table_name = f"{schema}.{table}"
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get table comment
                table_query = """
                SELECT schemaname, tablename, 
                       obj_description(c.oid) as comment
                FROM pg_tables t
                JOIN pg_class c ON c.relname = t.tablename
                JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.schemaname
                WHERE schemaname = %s AND tablename = %s
                """
                cursor.execute(table_query, (schema, table))
                table_result = cursor.fetchone()
                
                if not table_result:
                    self.logger.warning(f"Table {full_table_name} not found")
                    return [{"error": "Table not found"}]
                
                table_comment = table_result[2] if table_result[2] else ""
                
                # Get columns metadata
                columns_data = self.get_columns_metadata(schema, table, columns)
                
                # Build results: one for table, then all columns
                results = []
                
                # Table-level row
                results.append({
                    'table_schema': schema,
                    'table_name': table,
                    'column_name': None,
                    'ordinal_position': None,
                    'comment': table_comment
                })
                
                # Column-level rows
                for col_data in columns_data:
                    results.append(col_data)
                    
                return results
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get metadata for {full_table_name}: {e}")
            return [{"error": str(e)}]

    def get_columns_metadata(self, schema: str, table: str, columns: Optional[List[str]] = None):
        """Get column metadata from PostgreSQL information schema."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                query = """
                SELECT c.table_schema, c.table_name, c.column_name, c.ordinal_position,
                       col_description(pgc.oid, c.ordinal_position) as comment
                FROM information_schema.columns c
                JOIN pg_class pgc ON pgc.relname = c.table_name
                JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace AND pgn.nspname = c.table_schema
                WHERE c.table_schema = %s AND c.table_name = %s
                """
                
                params = [schema, table]
                if columns:
                    column_names = [col['name'] if isinstance(col, dict) else col for col in columns]
                    placeholders = ','.join(['%s'] * len(column_names))
                    query += f" AND c.column_name IN ({placeholders})"
                    params.extend(column_names)
                
                query += " ORDER BY c.ordinal_position"
                
                cursor.execute(query, params)
                results = []
                
                for row in cursor.fetchall():
                    results.append({
                        'table_schema': row[0],
                        'table_name': row[1], 
                        'column_name': row[2],
                        'ordinal_position': row[3],
                        'comment': row[4] if row[4] else ""
                    })
                
                self.logger.info(f"Retrieved metadata for {len(results)} columns in {schema}.{table}")
                return results
                
        except Exception as e:
            self.logger.error(f"Failed to get columns metadata for {schema}.{table}: {e}")
            return [{"error": str(e)}]

    def update_table_metadata(self, schema: str, table: str, comment: str):
        """Update metadata for a table."""
        full_table_name = f"{schema}.{table}"
        if not comment:
            self.logger.warning(f"⚠️  No comment provided for {full_table_name}")
            return {"status": "No comment provided"}
            
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                query = f"COMMENT ON TABLE {schema}.{table} IS %s"
                cursor.execute(query, (comment,))
                conn.commit()
                
                self.logger.info(f"✅ Updated table metadata for {full_table_name}")
                return {"status": "success"}
                
        except Exception as e:
            self.logger.error(f"❌ Failed to update table {full_table_name}: {e}")
            return {"status": f"failure: {e}"}

    def update_column_metadata(self, schema: str, table: str, column_updates: List[Dict[str, str]]):
        """Update column metadata for a table."""
        full_table_name = f"{schema}.{table}"
        if not column_updates:
            self.logger.warning(f"No column updates provided for {full_table_name}")
            return [{"status": "No column updates provided"}]
            
        results = []
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                for update in column_updates:
                    column_name = update.get('name')
                    comment = update.get('comment')
                    
                    if not column_name or not comment:
                        self.logger.warning(f"Invalid column update: {update}")
                        results.append({"column": column_name, "status": "invalid update"})
                        continue
                    
                    query = f"COMMENT ON COLUMN {schema}.{table}.{column_name} IS %s"
                    try:
                        cursor.execute(query, (comment,))
                        conn.commit()
                        self.logger.info(f"✅ Updated column {full_table_name}.{column_name}")
                        results.append({"column": column_name, "status": "success"})
                    except Exception as e:
                        self.logger.error(f"❌ Error updating column {full_table_name}.{column_name}: {e}")
                        results.append({"column": column_name, "status": f"failure: {e}"})
                        
        except Exception as e:
            self.logger.error(f"❌ Database connection error: {e}")
            results.append({"status": f"connection failure: {e}"})
            
        return results

    def bulk_update_metadata(self, metadata_updates: List[Dict]):
        """
        Bulk update metadata for multiple tables and columns.
        Args:
            metadata_updates: List of update specifications.
        Returns:
            List with results for each update
        """
        results = []
        self.logger.info(f"Starting bulk update of {len(metadata_updates)} metadata operations")
        
        for i, update in enumerate(metadata_updates):
            update_type = update.get('type')
            schema = update.get('schema')
            table = update.get('table')
            
            if update_type == 'table':
                res = self.update_table_metadata(schema, table, update['comment'])
                results.append({"update": f"table_{schema}.{table}", "status": res['status']})
                
            elif update_type == 'columns':
                res_list = self.update_column_metadata(schema, table, update['column_updates'])
                for res in res_list:
                    col_name = res.get('column', 'unknown')
                    status = res.get('status', 'unknown')
                    results.append({"update": f"columns_{schema}.{table}.{col_name}", "status": status})
                    
            elif update_type == 'both':
                # Update table comment
                table_res = self.update_table_metadata(schema, table, update['table_comment'])
                results.append({"update": f"table_{schema}.{table}", "status": table_res['status']})
                
                # Update column comments
                col_res_list = self.update_column_metadata(schema, table, update['column_updates'])
                for res in col_res_list:
                    col_name = res.get('column', 'unknown')
                    status = res.get('status', 'unknown')
                    results.append({"update": f"columns_{schema}.{table}.{col_name}", "status": status})
            else:
                self.logger.error(f"Unknown update type: {update_type}")
                results.append({"update": f"unknown_{i+1}", "status": "unknown update type"})
                
        return results

    def create_metadata_reference_table(self, tables: List[Dict], relationships: List[Dict]):
        """Create a metadata reference table with all table and column documentation."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Step 1: Create the metadata reference table
                create_sql = '''
                DROP TABLE IF EXISTS metadata_reference_table;
                CREATE TABLE metadata_reference_table (
                    table_schema VARCHAR(255),
                    table_name VARCHAR(255),
                    column_name VARCHAR(255),
                    table_column_documentation TEXT,
                    column_ordinal_position INTEGER,
                    relationship_key1 VARCHAR(500),
                    relationship_key2 VARCHAR(500),
                    column_values TEXT
                );
                '''
                cursor.execute(create_sql)
                conn.commit()
                self.logger.info("✅ Created metadata_reference_table")
                
                # Step 2: Collect all metadata
                all_rows = []
                table_lookup = {}
                
                for t in tables:
                    schema = t['table_schema']
                    table = t['table_name']
                    columns = t.get('columns', None)
                    column_metadata_map = {}
                    
                    # Build column metadata map for custom queries
                    if columns and isinstance(columns, list) and all(isinstance(col, dict) for col in columns):
                        for col in columns:
                            col_name = col.get('name')
                            if col_name:
                                column_metadata_map[col_name] = col
                    
                    table_key = f"{schema}.{table}"
                    table_lookup[table_key] = set([col['name'] if isinstance(col, dict) else col for col in columns]) if columns else None
                    
                    # Get table metadata
                    table_metadata = self.get_table_metadata(schema, table, [col['name'] if isinstance(col, dict) else col for col in columns] if columns else None)
                    
                    for row in table_metadata:
                        if 'error' in row:
                            continue
                            
                        col_name = row.get('column_name')
                        column_values = None
                        
                        # Execute custom query for column values if provided
                        if col_name and col_name in column_metadata_map:
                            col_meta = column_metadata_map[col_name]
                            custom_query = col_meta.get('column_values')
                            if custom_query:
                                try:
                                    cursor.execute(custom_query)
                                    result_data = cursor.fetchall()
                                    distinct_values = []
                                    for r in result_data:
                                        value = r[0] if r and r[0] is not None else None
                                        if value is not None:
                                            distinct_values.append(str(value))
                                    column_values = ', '.join(sorted(set(distinct_values)))
                                except Exception as e:
                                    self.logger.error(f"Failed to execute custom query for {schema}.{table}.{col_name}: {e}")
                        
                        all_rows.append({
                            'table_schema': row.get('table_schema'),
                            'table_name': row.get('table_name'),
                            'column_name': col_name,
                            'table_column_documentation': row.get('comment', ''),
                            'column_ordinal_position': row.get('ordinal_position'),
                            'relationship_key1': None,
                            'relationship_key2': None,
                            'column_values': column_values
                        })
                
                # Step 3: Map relationships to columns
                col_index = {f"{r['table_schema']}.{r['table_name']}.{r['column_name']}": r 
                           for r in all_rows if r['column_name'] is not None}
                
                for rel in relationships:
                    key1 = rel.get('key1')
                    key2 = rel.get('key2')
                    
                    # Validate keys
                    for key in [key1, key2]:
                        if key not in col_index:
                            self.logger.warning(f"Relationship key {key} not found in provided tables/columns.")
                    
                    # Attach relationship info
                    if key1 in col_index:
                        col_index[key1]['relationship_key1'] = key1
                        col_index[key1]['relationship_key2'] = key2
                    if key2 in col_index:
                        col_index[key2]['relationship_key1'] = key1
                        col_index[key2]['relationship_key2'] = key2
                
                # Step 4: Insert all rows
                if all_rows:
                    insert_sql = """
                    INSERT INTO metadata_reference_table 
                    (table_schema, table_name, column_name, table_column_documentation, 
                     column_ordinal_position, relationship_key1, relationship_key2, column_values)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    for row in all_rows:
                        cursor.execute(insert_sql, (
                            row['table_schema'],
                            row['table_name'], 
                            row['column_name'],
                            row['table_column_documentation'],
                            row['column_ordinal_position'],
                            row['relationship_key1'],
                            row['relationship_key2'],
                            row['column_values']
                        ))
                    
                    conn.commit()
                    self.logger.info(f"✅ Inserted {len(all_rows)} rows into metadata_reference_table")
                
                return {"status": "success", "rows_inserted": len(all_rows)}
                
        except Exception as e:
            self.logger.error(f"❌ Failed to create metadata reference table: {e}")
            return {"status": f"failure: {e}"}


def main(connection_string: str):
    """
    Main function to update metadata for the PostgreSQL demo database.
    
    Args:
        connection_string: PostgreSQL connection string (e.g., "host=localhost dbname=demo user=postgres password=pass")
    """
    manager = PostgresMetadataManager(connection_string)
    
    metadata_updates = [
        # Date dimension table
        {
            'type': 'both',
            'schema': 'public',
            'table': 'date',
            'table_comment': 'Calendar dimension table for time-based analysis',
            'column_updates': [
                {'name': 'calendar_day', 'comment': 'Specific calendar date (PRIMARY KEY)'},
                {'name': 'month_name', 'comment': 'Full month name (January, February, etc.)'},
                {'name': 'month', 'comment': 'Month number (1-12)'},
                {'name': 'day_of_month', 'comment': 'Day within the month (1-31)'},
                {'name': 'month_start_date', 'comment': 'First day of the month'},
                {'name': 'month_end_date', 'comment': 'Last day of the month'},
                {'name': 'quarter', 'comment': 'Quarter number (1-4)'},
                {'name': 'quarter_name', 'comment': 'Quarter label (Q1, Q2, Q3, Q4)'},
                {'name': 'quarter_start_date', 'comment': 'First day of the quarter'},
                {'name': 'quarter_end_date', 'comment': 'Last day of the quarter'},
                {'name': 'year', 'comment': 'Four-digit year'},
                {'name': 'is_weekend', 'comment': 'Boolean flag indicating weekend days'}
            ]
        },
        # Business line lookup table
        {
            'type': 'both',
            'schema': 'public',
            'table': 'business_line',
            'table_comment': 'Investment product categories offered by the firm',
            'column_updates': [
                {'name': 'business_line_key', 'comment': 'Unique identifier for business line (PRIMARY KEY)'},
                {'name': 'business_line_name', 'comment': 'Type of investment service (Managed Portfolio, SMA, Mutual Fund Wrap, Annuity, Cash)', 'column_values': 'SELECT DISTINCT business_line_name FROM business_line ORDER BY business_line_name'}
            ]
        },
        # Advisors table
        {
            'type': 'both',
            'schema': 'public',
            'table': 'advisors',
            'table_comment': 'Financial advisors with historical tracking (SCD Type 2)',
            'column_updates': [
                {'name': 'advisor_key', 'comment': 'Unique surrogate key (PRIMARY KEY)'},
                {'name': 'advisor_id', 'comment': 'Natural business identifier for the advisor'},
                {'name': 'advisor_tenure', 'comment': 'Years of experience as an advisor (1-40 years)'},
                {'name': 'firm_name', 'comment': 'Name of the advisory firm'},
                {'name': 'firm_affiliation_model', 'comment': 'Business model of the firm (RIA, Hybrid RIA, Broker-Dealer, etc.)', 'column_values': 'SELECT DISTINCT firm_affiliation_model FROM advisors ORDER BY firm_affiliation_model'},
                {'name': 'advisor_role', 'comment': 'Position within the firm (Lead Advisor, Associate, etc.)', 'column_values': 'SELECT DISTINCT advisor_role FROM advisors ORDER BY advisor_role'},
                {'name': 'advisor_status', 'comment': 'Current employment status (Active/Terminated)', 'column_values': 'SELECT DISTINCT advisor_status FROM advisors ORDER BY advisor_status'},
                {'name': 'practice_segment', 'comment': 'Size classification of advisory practice', 'column_values': 'SELECT DISTINCT practice_segment FROM advisors ORDER BY practice_segment'},
                {'name': 'from_date', 'comment': 'Effective start date for this record version'},
                {'name': 'to_date', 'comment': 'Effective end date for this record version'}
            ]
        },
        # Household table
        {
            'type': 'both',
            'schema': 'public', 
            'table': 'household',
            'table_comment': 'Client households with historical tracking (SCD Type 2)',
            'column_updates': [
                {'name': 'household_key', 'comment': 'Unique surrogate key (PRIMARY KEY)'},
                {'name': 'household_id', 'comment': 'Natural business identifier for the household'},
                {'name': 'household_tenure', 'comment': 'Years as a client (1-40 years)'},
                {'name': 'household_registration_type', 'comment': 'Legal registration structure (Individual, Joint, Trust, Institutional)', 'column_values': 'SELECT DISTINCT household_registration_type FROM household ORDER BY household_registration_type'},
                {'name': 'household_registration_date', 'comment': 'Date when household became a client'},
                {'name': 'household_segment', 'comment': 'Client service model classification', 'column_values': 'SELECT DISTINCT household_segment FROM household ORDER BY household_segment'},
                {'name': 'household_status', 'comment': 'Current relationship status (Active/Terminated)', 'column_values': 'SELECT DISTINCT household_status FROM household ORDER BY household_status'},
                {'name': 'household_advisor_id', 'comment': 'ID of the primary advisor serving this household'},
                {'name': 'from_date', 'comment': 'Effective start date for this record version'},
                {'name': 'to_date', 'comment': 'Effective end date for this record version'}
            ]
        },
        # Account table
        {
            'type': 'both',
            'schema': 'public',
            'table': 'account',
            'table_comment': 'Individual investment accounts with historical tracking (SCD Type 2)',
            'column_updates': [
                {'name': 'account_key', 'comment': 'Unique surrogate key (PRIMARY KEY)'},
                {'name': 'account_id', 'comment': 'Natural business identifier for the account'},
                {'name': 'advisor_key', 'comment': 'Foreign key to advisors table'},
                {'name': 'household_key', 'comment': 'Foreign key to household table'},
                {'name': 'business_line_key', 'comment': 'Foreign key to business_line table'},
                {'name': 'account_type', 'comment': 'Tax classification (Taxable, IRA, 401k, Trust, Custody)', 'column_values': 'SELECT DISTINCT account_type FROM account ORDER BY account_type'},
                {'name': 'account_custodian', 'comment': 'Firm holding the assets (Schwab, Fidelity, Pershing, etc.)', 'column_values': 'SELECT DISTINCT account_custodian FROM account ORDER BY account_custodian'},
                {'name': 'opened_date', 'comment': 'Date the account was opened'},
                {'name': 'account_status', 'comment': 'Current status (Open/Closed)', 'column_values': 'SELECT DISTINCT account_status FROM account ORDER BY account_status'},
                {'name': 'closed_date', 'comment': 'Date account was closed (if applicable)'},
                {'name': 'account_risk_profile', 'comment': 'Investment risk tolerance (Conservative, Moderate, Aggressive)', 'column_values': 'SELECT DISTINCT account_risk_profile FROM account ORDER BY account_risk_profile'},
                {'name': 'from_date', 'comment': 'Effective start date for this record version'},
                {'name': 'to_date', 'comment': 'Effective end date for this record version'}
            ]
        },
        # Product table
        {
            'type': 'both',
            'schema': 'public',
            'table': 'product',
            'table_comment': 'Investment products and securities',
            'column_updates': [
                {'name': 'product_id', 'comment': 'Unique identifier for investment product (PRIMARY KEY)'},
                {'name': 'asset_category', 'comment': 'High-level asset classification (Equity, Fixed Income, Multi-Asset, Cash)', 'column_values': 'SELECT DISTINCT asset_category FROM product ORDER BY asset_category'},
                {'name': 'asset_subcategory', 'comment': 'Detailed product classification within asset category', 'column_values': 'SELECT DISTINCT asset_subcategory FROM product ORDER BY asset_subcategory'},
                {'name': 'product_line', 'comment': 'Distribution channel or wrapper type', 'column_values': 'SELECT DISTINCT product_line FROM product ORDER BY product_line'},
                {'name': 'product_name', 'comment': 'Full name of the investment product'}
            ]
        },
        # Tier fee table
        {
            'type': 'both',
            'schema': 'public',
            'table': 'tier_fee',
            'table_comment': 'Fee schedule based on asset levels by business line',
            'column_updates': [
                {'name': 'tier_fee_id', 'comment': 'Unique identifier (PRIMARY KEY)'},
                {'name': 'business_line_key', 'comment': 'Foreign key to business_line table'},
                {'name': 'tier_min_aum', 'comment': 'Minimum assets under management for this fee tier'},
                {'name': 'tier_max_aum', 'comment': 'Maximum assets under management for this fee tier'},
                {'name': 'tier_fee_bps', 'comment': 'Fee rate in basis points (e.g., 90 = 0.90%)'}
            ]
        },
        # Advisor payout rate table
        {
            'type': 'both',
            'schema': 'public',
            'table': 'advisor_payout_rate',
            'table_comment': 'Commission/payout rates by firm affiliation model',
            'column_updates': [
                {'name': 'firm_affiliation_model', 'comment': 'Type of firm affiliation (PRIMARY KEY)', 'column_values': 'SELECT DISTINCT firm_affiliation_model FROM advisor_payout_rate ORDER BY firm_affiliation_model'},
                {'name': 'advisor_payout_rate', 'comment': 'Percentage of revenue paid to advisor (e.g., 0.7800 = 78%)'}
            ]
        },
        # Fact account initial assets
        {
            'type': 'both',
            'schema': 'public',
            'table': 'fact_account_initial_assets',
            'table_comment': 'Starting asset values when accounts were opened',
            'column_updates': [
                {'name': 'account_key', 'comment': 'Foreign key to account table (PRIMARY KEY)'},
                {'name': 'account_initial_assets', 'comment': 'Dollar value of assets when account opened'}
            ]
        },
        # Fact account monthly
        {
            'type': 'both',
            'schema': 'public',
            'table': 'fact_account_monthly',
            'table_comment': 'Monthly account performance and asset data',
            'column_updates': [
                {'name': 'snapshot_date', 'comment': 'End-of-month date for the data snapshot'},
                {'name': 'account_key', 'comment': 'Foreign key to account table'},
                {'name': 'account_monthly_return', 'comment': 'Monthly investment return percentage'},
                {'name': 'account_net_flow', 'comment': 'Net deposits/withdrawals during the month'},
                {'name': 'account_assets_previous_month', 'comment': 'Asset value at start of month'},
                {'name': 'account_assets', 'comment': 'Asset value at end of month'},
                {'name': 'advisor_key', 'comment': 'Foreign key to advisors table (current advisor)'},
                {'name': 'household_key', 'comment': 'Foreign key to household table'},
                {'name': 'business_line_key', 'comment': 'Foreign key to business_line table'}
            ]
        },
        # Fact account product monthly
        {
            'type': 'both',
            'schema': 'public',
            'table': 'fact_account_product_monthly',
            'table_comment': 'Monthly asset allocation by product for each account',
            'column_updates': [
                {'name': 'snapshot_date', 'comment': 'End-of-month date for the allocation snapshot'},
                {'name': 'account_key', 'comment': 'Foreign key to account table'},
                {'name': 'product_id', 'comment': 'Foreign key to product table'},
                {'name': 'product_allocation_pct', 'comment': 'Percentage of account allocated to this product'}
            ]
        },
        # Fact household monthly
        {
            'type': 'both',
            'schema': 'public',
            'table': 'fact_household_monthly',
            'table_comment': 'Monthly aggregated household data',
            'column_updates': [
                {'name': 'snapshot_date', 'comment': 'End-of-month date for the data snapshot'},
                {'name': 'household_key', 'comment': 'Foreign key to household table'},
                {'name': 'household_assets', 'comment': 'Total assets across all household accounts'},
                {'name': 'asset_range_bucket', 'comment': 'Categorized asset range for segmentation', 'column_values': 'SELECT DISTINCT asset_range_bucket FROM fact_household_monthly ORDER BY asset_range_bucket'},
                {'name': 'high_net_worth_flag', 'comment': 'Boolean indicator for high-net-worth status'},
                {'name': 'household_net_flow', 'comment': 'Net deposits/withdrawals across all accounts'}
            ]
        },
        # Fact revenue monthly
        {
            'type': 'both',
            'schema': 'public',
            'table': 'fact_revenue_monthly',
            'table_comment': 'Monthly fee and revenue calculations',
            'column_updates': [
                {'name': 'snapshot_date', 'comment': 'End-of-month date for revenue calculation'},
                {'name': 'account_key', 'comment': 'Foreign key to account table'},
                {'name': 'advisor_key', 'comment': 'Foreign key to advisors table'},
                {'name': 'household_key', 'comment': 'Foreign key to household table'},
                {'name': 'business_line_key', 'comment': 'Foreign key to business_line table'},
                {'name': 'account_assets', 'comment': 'Asset value used for fee calculation'},
                {'name': 'fee_percentage', 'comment': 'Annual fee rate applied to assets'},
                {'name': 'gross_fee_amount', 'comment': 'Total fee charged before deductions'},
                {'name': 'third_party_fee', 'comment': 'Fees paid to external parties'},
                {'name': 'advisor_payout_rate', 'comment': 'Percentage of net revenue paid to advisor'},
                {'name': 'advisor_payout_amount', 'comment': 'Dollar amount paid to advisor'},
                {'name': 'net_revenue', 'comment': 'Revenue retained by the firm after payouts'}
            ]
        },
        # Transactions table
        {
            'type': 'both',
            'schema': 'public',
            'table': 'transactions',
            'table_comment': 'Individual transaction-level data (high volume)',
            'column_updates': [
                {'name': 'transaction_id', 'comment': 'Unique transaction identifier (PRIMARY KEY)'},
                {'name': 'advisor_key', 'comment': 'Foreign key to advisors table'},
                {'name': 'account_key', 'comment': 'Foreign key to account table'},
                {'name': 'household_key', 'comment': 'Foreign key to household table'},
                {'name': 'business_line_key', 'comment': 'Foreign key to business_line table'},
                {'name': 'product_id', 'comment': 'Foreign key to product table'},
                {'name': 'transaction_date', 'comment': 'Date the transaction occurred'},
                {'name': 'gross_revenue', 'comment': 'Revenue generated from the transaction'},
                {'name': 'revenue_fee', 'comment': 'Fee component of the transaction'},
                {'name': 'third_party_fee', 'comment': 'External fees associated with transaction'},
                {'name': 'transaction_type', 'comment': 'Type of transaction (deposit, withdrawal, fee)', 'column_values': 'SELECT DISTINCT transaction_type FROM transactions ORDER BY transaction_type'}
            ]
        },
        # Fact customer feedback
        {
            'type': 'both',
            'schema': 'public',
            'table': 'fact_customer_feedback',
            'table_comment': 'Client satisfaction and feedback data',
            'column_updates': [
                {'name': 'feedback_id', 'comment': 'Unique feedback record identifier (PRIMARY KEY)'},
                {'name': 'feedback_date', 'comment': 'Date feedback was collected'},
                {'name': 'household_key', 'comment': 'Foreign key to household table'},
                {'name': 'advisor_key', 'comment': 'Foreign key to advisors table'},
                {'name': 'feedback_text', 'comment': 'Customer comments (max 200 characters)'},
                {'name': 'satisfaction_score', 'comment': 'Numeric satisfaction rating (0-100)'}
            ]
        }
    ]
    
    # Step 1: Update metadata with documentation
    update_result = manager.bulk_update_metadata(metadata_updates)
    
    # Print update results
    print("Metadata Update Results:")
    for result in update_result:
        print(f"  {result['update']}: {result['status']}")
    
    # Step 2: Create metadata reference table
    tables = []
    for update in metadata_updates:
        table_info = {
            'table_schema': update['schema'],
            'table_name': update['table'],
            'columns': update.get('column_updates', None)
        }
        tables.append(table_info)
    
    # Define some key relationships in the demo database
    relationships = [
        {
            'key1': 'public.account.advisor_key',
            'key2': 'public.advisors.advisor_key'
        },
        {
            'key1': 'public.account.household_key', 
            'key2': 'public.household.household_key'
        },
        {
            'key1': 'public.account.business_line_key',
            'key2': 'public.business_line.business_line_key'
        },
        {
            'key1': 'public.fact_account_monthly.account_key',
            'key2': 'public.account.account_key'
        },
        {
            'key1': 'public.fact_revenue_monthly.account_key',
            'key2': 'public.account.account_key'
        },
        {
            'key1': 'public.transactions.product_id',
            'key2': 'public.product.product_id'
        },
        # Date relationships for fact tables
        {
            'key1': 'public.fact_account_monthly.snapshot_date',
            'key2': 'public.date.calendar_day'
        },
        {
            'key1': 'public.fact_account_product_monthly.snapshot_date',
            'key2': 'public.date.calendar_day'
        },
        {
            'key1': 'public.fact_household_monthly.snapshot_date',
            'key2': 'public.date.calendar_day'
        },
        {
            'key1': 'public.fact_revenue_monthly.snapshot_date',
            'key2': 'public.date.calendar_day'
        },
        {
            'key1': 'public.transactions.transaction_date',
            'key2': 'public.date.calendar_day'
        },
        {
            'key1': 'public.fact_customer_feedback.feedback_date',
            'key2': 'public.date.calendar_day'
        }
    ]
    
    reference_result = manager.create_metadata_reference_table(tables, relationships)
    
    print("\nMetadata Reference Table Creation:")
    print(f"  Status: {reference_result['status']}")
    if 'rows_inserted' in reference_result:
        print(f"  Rows inserted: {reference_result['rows_inserted']}")
    
    return reference_result


if __name__ == "__main__":
    # Example usage - replace with your actual connection string
    connection_string = "host=localhost dbname=demo_db user=postgres password=your_password"
    main(connection_string)