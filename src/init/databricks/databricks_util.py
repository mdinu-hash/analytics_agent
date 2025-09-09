import logging
from typing import Dict, List, Optional
import pandas as pd
import databricks.sdk
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import mlflow
import mlflow.langchain
import datetime
import uuid

class DatabricksMetadataManager:
  """ Get/Update tables metadata stored in Databricks. 
  Uses databricks native authentication and Databricks SDK for data operations. 
  Functions:
  - get_table_metadata: Retrieve metadata for 1 table including columns.
  - get_columns_metadata: Get columns metadata for 1 table using SQL script.
  - update_table_metadata: Update table-level metadata using SQL script.
  - update_column_metadata: Update column-level metadata for 1 table using SQL scripts.
  - bulk_update_metadata: Bulk update metadata for multiple tables and columns.
  """

  def __init__(self, warehouse_id: str):      
        # Native Databricks Authentication (automatic)
        self.w = WorkspaceClient()
        self.warehouse_id = warehouse_id
        
        # logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
  
  def _execute_sql(self, query: str, params: Dict = None) -> pd.DataFrame:
        """Execute SQL query and return DataFrame using Databricks SDK"""
        try:
            # Replace parameters in query if provided
            if params:
                for key, value in params.items():
                    # Properly escape string values
                    if isinstance(value, str):
                        escaped_value = value.replace("'", "''")
                        query = query.replace(f":{key}", f"'{escaped_value}'")
                    else:
                        query = query.replace(f":{key}", str(value))
            
            self.logger.debug(f"Executing query: {query}")
            
            # Execute query using the correct SDK API
            response = self.w.statement_execution.execute_statement(
                statement=query,
                warehouse_id=self.warehouse_id,
                wait_timeout="50s"
            )
            
            # Check if we have result data
            if response.result and response.result.data_array:
                # Get column names from manifest
                columns = [col.name for col in response.manifest.schema.columns]
                # Get data
                data = response.result.data_array
                return pd.DataFrame(data, columns=columns)
            else:
                return pd.DataFrame()
                    
        except Exception as e:
            self.logger.error(f"Failed to execute SQL: {e}")
            self.logger.error(f"Query: {query}")
            raise 

  def get_table_metadata(self, catalog: str, schema: str, table: str, columns: Optional[List[str]] = None) -> Dict:
        """
        Retrieve metadata for 1 table including columns
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            columns: Optional list of columns to retrieve
        Returns:
            Dict: Current metadata information    
        """
        full_table_name = f"{catalog}.{schema}.{table}"
        try:
            # Get table metadata
            table_query = """
            SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COMMENT as table_comment
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = :catalog
                  AND TABLE_SCHEMA = :schema
                  AND TABLE_NAME = :table
            """
            table_params = {'catalog': catalog, 'schema': schema, 'table': table}
            table_df = self._execute_sql(table_query, table_params)
            if table_df.empty:
                self.logger.warning(f"Table {full_table_name} not found")
                return {}
            # Get columns metadata
            columns_df = self.get_columns_metadata(catalog, schema, table, columns)
            # Build metadata dictionary
            table_row = table_df.iloc[0]
            metadata = {
                'table_name': table_row['TABLE_NAME'],
                'table_comment': table_row['table_comment'],
                'columns': []
            }
            # Add columns information
            for _, col_row in columns_df.iterrows():
                col_data = {
                    'name': col_row['COLUMN_NAME'],
                    'comment': col_row['COMMENT'],
                    'position': col_row['ORDINAL_POSITION']
                }
                metadata['columns'].append(col_data)
            return metadata
        except Exception as e:
            self.logger.error(f"❌ Failed to get metadata for {full_table_name}: {e}")
            return {}
        
  def _execute_ddl(self, query: str) -> bool:
        """Execute DDL query (ALTER TABLE, etc.) using Databricks SDK"""
        try:
            self.logger.debug(f"Executing DDL: {query}")
            
            response = self.w.statement_execution.execute_statement(
                statement=query,
                warehouse_id=self.warehouse_id,
                wait_timeout="50s"
            )
            
            # Check status
            success = response.status.state == StatementState.SUCCEEDED
            if not success:
                self.logger.error(f"DDL failed with status: {response.status.state}")
            return success
                
        except Exception as e:
            self.logger.error(f"Failed to execute DDL: {e}")
            self.logger.error(f"Query: {query}")
            return False

  def get_columns_metadata(self, catalog: str, schema: str, table: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Get columns metadata for 1 table using SQL script
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            columns: Optional list of columns to retrieve
        Returns:
            DataFrame with column metadata
        """
        query = """
        SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = :catalog
              AND TABLE_SCHEMA = :schema
              AND TABLE_NAME = :table
        """
        if columns:
            # Add filter for columns
            columns_list = ', '.join(["'{}'".format(col.replace("'", "''")) for col in columns])
            query += f" AND COLUMN_NAME IN ({columns_list})"
        query += " ORDER BY TABLE_NAME, ORDINAL_POSITION"
        params = {'catalog': catalog, 'schema': schema, 'table': table}
        try:
            df = self._execute_sql(query, params)
            self.logger.info(f"Retrieved metadata for {len(df)} columns in {catalog}.{schema}.{table}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to get columns metadata for {catalog}.{schema}.{table}: {e}")
            return pd.DataFrame()
        
  def update_table_metadata(self, catalog: str, schema: str, table: str, comment: str) -> bool:
        """
        Update table-level metadata using SQL script
        
        Args:
            catalog: Catalog name
            schema: Schema name  
            table: Table name
            comment: Table description/comment. Recommended to specify the column names that define the table granularity. Ex: Granularity is product_id.
            
        Returns:
            bool: True if successful, False otherwise
        """
        full_table_name = f"{catalog}.{schema}.{table}"
        
        if not comment:
            self.logger.warning(f"⚠️  No comment provided for {full_table_name}")
            return False
        
        # Escape single quotes in comment
        escaped_comment = comment.replace("'", "''")
        
        # Use Databricks SQL script
        query = f"ALTER TABLE {catalog}.{schema}.{table} SET TBLPROPERTIES('comment'='{escaped_comment}')"
        
        try:
            success = self._execute_ddl(query)
            if success:
                self.logger.info(f"✅ Updated table metadata for {full_table_name}")
            return success
            
        except Exception as e:
            self.logger.error(f"❌ Failed to update table {full_table_name}: {e}")
            return False
    
  def update_column_metadata(self, catalog: str, schema: str, table: str, 
                                 column_updates: List[Dict[str, str]]) -> bool:
        """
        Update column-level metadata for 1 table using SQL scripts
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            column_updates: List of dicts with 'name' and 'comment' keys
                          Example: [{'name': 'col1', 'comment': 'Description'}]
                          
        Returns:
            bool: True if all updates successful
        """
        full_table_name = f"{catalog}.{schema}.{table}"
        
        if not column_updates:
            self.logger.warning(f"No column updates provided for {full_table_name}")
            return False
        
        success_count = 0
        total_updates = len(column_updates)
        
        for update in column_updates:
            column_name = update.get('name')
            comment = update.get('comment')
            
            if not column_name or not comment:
                self.logger.warning(f"Invalid column update: {update}")
                continue
            
            # Escape single quotes in comment
            escaped_comment = comment.replace("'", "''")
            
            # Use Databricks SQL script
            query = f"ALTER TABLE {catalog}.{schema}.{table} ALTER COLUMN {column_name} COMMENT '{escaped_comment}'"
            
            try:
                if self._execute_ddl(query):
                    success_count += 1
                    self.logger.info(f"✅ Updated column {full_table_name}.{column_name}")
                else:
                    self.logger.error(f"❌ Failed to update column {full_table_name}.{column_name}")
                    
            except Exception as e:
                self.logger.error(f"❌ Error updating column {full_table_name}.{column_name}: {e}")
        
        all_successful = success_count == total_updates
        self.logger.info(f"Column updates for {full_table_name}: {success_count}/{total_updates} successful")
        return all_successful
    
  def bulk_update_metadata(self, metadata_updates: List[Dict]) -> Dict[str, bool]:
        """
        Bulk update metadata for multiple tables and columns
        
        Args:
            metadata_updates: List of update specifications
                Example:
                [
                    {
                        'type': 'table',
                        'catalog': 'develop',
                        'schema': 'develop', 
                        'table': 'table1',
                        'comment': 'New table comment'
                    },
                    {
                        'type': 'columns',
                        'catalog': 'develop',
                        'schema': 'develop',
                        'table': 'table1', 
                        'column_updates': [
                            {'name': 'col1', 'comment': 'New column comment'},
                            {'name': 'col2', 'comment': 'Another comment'}
                        ]
                    },
                    {
                        'type': 'both',
                        'catalog': 'develop',
                        'schema': 'develop',
                        'table': 'table2',
                        'table_comment': 'Table comment',
                        'column_updates': [
                            {'name': 'col1', 'comment': 'Column comment'}
                        ]
                    }
                ]
                
        Returns:
            Dictionary with results for each update
        """
        results = {}
        
        self.logger.info(f"Starting bulk update of {len(metadata_updates)} metadata operations")
        
        for i, update in enumerate(metadata_updates):
            update_type = update.get('type')
            catalog = update.get('catalog')
            schema = update.get('schema') 
            table = update.get('table')
            
            if update_type == 'table':
                success = self.update_table_metadata(catalog, schema, table, update['comment'])
                update_key = f"table_{catalog}.{schema}.{table}"
                
            elif update_type == 'columns':
                success = self.update_column_metadata(catalog, schema, table, update['column_updates'])
                update_key = f"columns_{catalog}.{schema}.{table}"
                
            elif update_type == 'both':
                # Update both table and columns
                table_success = self.update_table_metadata(catalog, schema, table, update['table_comment'])
                columns_success = self.update_column_metadata(catalog, schema, table, update['column_updates'])
                success = table_success and columns_success
                update_key = f"both_{catalog}.{schema}.{table}"
                
            else:
                self.logger.error(f"Unknown update type: {update_type}")
                success = False
                update_key = f"unknown_{i+1}"
            
            results[update_key] = success
        
        successful_updates = sum(1 for success in results.values() if success)
        self.logger.info(f"Bulk update completed: {successful_updates}/{len(results)} successful")
        
        return results
    
  def create_objects_documentation(self, catalog_metadata_object_source: str, schema_metadata_object_source: str, table_metadata_object_source: str, tables: list = None) -> str:
        """
        Generate documentation for tables and columns based on the specified metadata reference table.
        Args:
            catalog_metadata_object_source: Catalog name for the metadata reference table
            schema_metadata_object_source: Schema name for the metadata reference table
            table_metadata_object_source: Table name for the metadata reference table
            tables: Optional list of dicts, each with keys:
                - 'table_catalog', 'table_schema', 'table_name', and optionally 'columns' (list of column names)
                If None, all tables and columns in the metadata_reference_table are used.
        Returns:
            str: Formatted documentation string
        """
        import pandas as pd
        query = f"SELECT * FROM `{catalog_metadata_object_source}`.{schema_metadata_object_source}.{table_metadata_object_source}"
        df = self._execute_sql(query)
        if df.empty:
            return "No metadata found."
        doc_lines = []
        # Filter tables if provided
        if tables:
            # Build a set of (catalog, schema, table) for fast lookup
            table_set = set((t['table_catalog'], t['table_schema'], t['table_name']) for t in tables)
            df = df[df.apply(lambda row: (row['TABLE_CATALOG'], row['TABLE_SCHEMA'], row['TABLE_NAME']) in table_set, axis=1)]
            # Optionally filter columns per table
            table_columns = { (t['table_catalog'], t['table_schema'], t['table_name']): set(t.get('columns', [])) for t in tables if 'columns' in t }
        else:
            table_columns = {}
        table_groups = df.groupby(['TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME'])
        for (catalog, schema, table), group in table_groups:
            doc_lines.append(f"Table {catalog}.{schema}.{table}: {group['TABLE_COLUMN_DOCUMENTATION'].iloc[0] if 'TABLE_COLUMN_DOCUMENTATION' in group and pd.notnull(group['TABLE_COLUMN_DOCUMENTATION'].iloc[0]) else ''} Columns:")
            for _, row in group.iterrows():
                col_name = row['COLUMN_NAME']
                # Skip rows where COLUMN_NAME is null (table-level metadata)
                if pd.isnull(col_name):
                    continue
                if (catalog, schema, table) in table_columns and col_name not in table_columns[(catalog, schema, table)]:
                    continue
                comment = row['TABLE_COLUMN_DOCUMENTATION'] if pd.notnull(row['TABLE_COLUMN_DOCUMENTATION']) else ''
                doc_lines.append(f"    {col_name}: {comment}")
            doc_lines.append("")
        # Add relationships if present
        rels = df.dropna(subset=['RELATIONSHIP_KEY1', 'RELATIONSHIP_KEY2'])
        if not rels.empty:
            doc_lines.append("Relationships:")
            rel_pairs = set()
            for _, row in rels.iterrows():
                k1 = row['RELATIONSHIP_KEY1']
                k2 = row['RELATIONSHIP_KEY2']
                if (k1, k2) not in rel_pairs and (k2, k1) not in rel_pairs:
                    doc_lines.append(f"{k1} relates to {k2}")
                    rel_pairs.add((k1, k2))
        return '\n'.join(doc_lines)

  def get_database_content(self, catalog_metadata_object_source: str, schema_metadata_object_source: str, table_metadata_object_source: str) -> str:
        """
        Retrieve column values from the specified metadata reference table and format them as a string.
        Args:
            catalog_metadata_object_source: Catalog name for the metadata reference table
            schema_metadata_object_source: Schema name for the metadata reference table
            table_metadata_object_source: Table name for the metadata reference table
        Returns:
            str: Formatted string with table.column and their values
        """
        try:
            # Query to get column values from the specified metadata_reference_table using Spark SQL dialect
            query = (
                f"SELECT table_name, column_name, column_values "
                f"FROM `{catalog_metadata_object_source}`.{schema_metadata_object_source}.{table_metadata_object_source} "
                f"WHERE column_values IS NOT NULL "
                f"ORDER BY table_name, column_name"
            )
            
            df = self._execute_sql(query)
            
            if df.empty:
                self.logger.info("No column values found in metadata_reference_table")
                return ""
            
            # Format the results as required
            content_lines = []
            for _, row in df.iterrows():
                table_name = row['table_name']
                column_name = row['column_name']
                column_values = row['column_values']
                
                if table_name and column_name and column_values:
                    content_lines.append(f"{table_name}.{column_name} column values: {column_values}.")
            
            return '\n'.join(content_lines)
            
        except Exception as e:
            self.logger.error(f"Failed to get database content: {e}")
            return ""

def start_agent_run_mlflow(experiment_folder:str, agent_name:str, scope:str,
                        question: str = None, is_new_thread_id: bool = False, 
                        thread_id: str = None ) -> Dict:
  """
  Create or reuse a thread, create the MLflow experiment/run, and return the
  LangGraph config plus run_id for metrics logging.

  Returns a dict with kets: config, run_id.
  """

  # Enable autologging and set tracking to Databricks
  mlflow.langchain.autolog()
  mlflow.set_tracking_uri("databricks")

  current_month_short = datetime.now().strftime("%b%Y")
  experiment_name = f"{agent_name}_{scope}_{current_month_short}"
  experiment_path = f"{experiment_folder}/{experiment_name}"

  experiment = mlflow.get_experiment_by_name(experiment_path)
  if experiment is None:
      mlflow.create_experiment(experiment_path)
  else:
      mlflow.set_experiment(experiment_path)
  
  # Resolve current user 
  try:
      w=WorkspaceClient()
      current_user = w.current_user.me()
      user_name = current_user.user_name if hasattr(current_user, 'user_name') else 'unknown_user'
  except Exception as e:
      user_name = 'unknown_user'
      logging.warning(f"Could not get current user: {e}")
  
  if is_new_thread_id or not thread_id:
      thread_id = str(uuid.uuid4())
  
  date_time_now = datetime.now().strftime("%Y-%m-%d %H:%M")
  # Naming convention is Run_AgentName_UserName_DateTime_ThreadID
  run_name = f"Run_{agent_name}_{user_name}_{date_time_now}_{thread_id}"

  # Ensure a fresh run context
  if mlflow.active_run() is not None:
      mlflow.end_run()
  run = mlflow.start_run(run_name=run_name)

  # basic params
  mlflow.log_param("agent_name",agent_name)
  mlflow.log_param("scope",scope)
  mlflow.log_param("thread_id",thread_id)
  mlflow.log_param("user_name",user_name)
  mlflow.log_param("run_date",datetime.now().strftime("%Y-%m-%d"))
  mlflow.log_param("run_time",datetime.now().strftime("%H:%M"))
  mlflow.log_param("question",question)

  config = {
      'run_name':run_name,
      'configurable': {'thread_id':thread_id}
  }

  return {
      'config':config,
      'run_id': run.info.run_id
  }

def log_agent_metrics_mlflow(result:Dict) -> None:
    """
    Log post-execution metrics to the active MLflow run.
    The run remains open for additional questions in the same thread.
    """

    # attach to the existing run
    mlflow.set_tracking_uri("databricks")

    # Scenario
    scenario = result.get('generate_answer_details',{}).get('scenario','Unknown')
    mlflow.log_param("scenario",scenario)

    # Analytical intents
    analytical_intents = result.get('analytical_intent',[])
    for i,intent in enumerate(analytical_intents):
        mlflow.log_param(f"analytical_intent_{i+1}",intent)

    # Sql queries and details
    sql_queries = result.get('current_sql_queries',[])
    for i,query_info in enumerate(sql_queries):
        original_query = query_info.get('query','')
        mlflow.log_param(f"sql_query_{i+1}_original",original_query)

        query_result = query_info.get('result','')
        mlflow.log_param(f"sql_query_{i+1}_result",query_result)

        explanation = query_info.get('explanation','')
        insight = query_info.get('insight','')
        metadata = query_info.get('metadata','')
        mlflow.log_param(f"sql_query_{i+1}_explanation",explanation)
        mlflow.log_param(f"sql_query_{i+1}_insight",insight)
        mlflow.log_param(f"sql_query_{i+1}_metadata",metadata)

        was_refined = "Query result too large after 3 refinements" in str(query_result) or "Refinement failed" in str(explanation)
        mlflow.log_param(f"sql_query_{i+1}_was_refined",was_refined)

    # agent response
    agent_response = result['llm_answer'].content
    mlflow.log_param("agent_response",agent_response)



