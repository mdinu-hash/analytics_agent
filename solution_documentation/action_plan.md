
#1 Create glossaries and db schema file.

- Create Glossary for Database Schema
Create a database_schema.py file in the src\init folder with 2 data structures:

**database_schema** - A list of table dictionaries with this format:
```python
[
    {
        'table_name': 'schema.table_name',
        'table_description': 'Description of the table',
        'columns': {
            'column_name': {
                'description': 'Column description',
                'query_to_get_column_values': 'SQL query to retrieve distinct values',
                'query_to_get_date_range': 'SQL query to retrieve date range info'
            }
        }
    }
]
```

**table_relationships** - A list of relationship dictionaries with this format:
```python
[
    {
        'key1': 'schema.table.column',
        'key2': 'schema.table.column'
    }
]
```

- Update the documentation, after discussing with Kelsey and Sam, some terms are outdated.
Also fix assets = sum of split_assets not total_assets my bad.

- Create Glossary for Business Terms
Create a business_glossary.py file in the src\init folder with 3 data structures:

**key_terms** - A list of term dictionaries with this format:
```python
[
    {
        'name': 'Term Name',
        'definition': 'Term definition',
        'query_instructions': 'Instructions on how to query this term',
        'exists_in_database': True/False
    }
]
```

**synonyms** - A dictionary mapping user terms to key_terms:
```python
{
    'user_synonym': 'key_term_name',
    'aum': 'assets under management'
}
```

**related_terms** - A list of related term groups:
```python
[
    ['term1', 'term2', 'term3']
]
```

Populate the business_glossary.py and database_schema.py files with the values from file update_objects_metadata_snowflake.py -> list metadata_updates.

Add a **check_glossary_consistency()** function that validates all terms in synonyms and related_terms exist in key_terms.

def check_glossary_consistency():
    """
    Checks if all terms referenced in synonyms and related_terms exist in key_terms.
    Prints messages for missing terms.
    """
    # Get all term names from key_terms (normalized to lowercase for comparison)
    key_term_names = {term['name'].lower() for term in key_terms}

    missing_terms = set()

    # Check synonyms - the values (right side) should exist in key_terms
    for synonym, key_term in synonyms.items():
        key_term_normalized = key_term.replace('_', ' ').lower()
        if key_term_normalized not in key_term_names:
            missing_terms.add(key_term)

    # Check related_terms - all terms in the lists should exist in key_terms
    for term_group in related_terms:
        for term in term_group:
            term_normalized = term.replace('_', ' ').lower()
            if term_normalized not in key_term_names:
                missing_terms.add(term)

    # Print messages for missing terms
    if missing_terms:
        print("⚠️  Missing terms found in business_glossary:")
        print("="*80)
        for term in sorted(missing_terms):
            print(f"\nPlease add the following term to key_terms:")
            print(f"{{")
            print(f"    'name': '{term}',")
            print(f"    'definition': '<ADD DEFINITION HERE>',")
            print(f"    'query_instructions': '<ADD QUERY INSTRUCTIONS OR LEAVE BLANK IF NOT IN DATABASE>',")
            print(f"    'exists_in_database': <True or False>")
            print(f"}},")
        print("\n" + "="*80)
    else:
        print("✅ All terms in synonyms and related_terms exist in key_terms")

- The values in database_schema and business glossary should be copied from the update_objects_metadata_snowflake.py script, where at the end is the metadata_updates list.

- Database Utility Functions

**For databricks_util.py (Databricks environment):**

**Add execute_sql_query utility function** at the top of databricks_util.py:

```python
def execute_sql_query(query: str, warehouse_id: str, params: Dict = None) -> List[tuple]:
    """
    Execute a SQL query using Databricks SDK with native authentication.

    Args:
        query: SQL query string to execute
        warehouse_id: Databricks SQL warehouse ID
        params: Optional dict of parameters to replace in query (e.g., {'catalog': 'main'})

    Returns:
        List of tuples (each row as a tuple), or None if query fails

    Note: Uses native Databricks authentication (WorkspaceClient).
    When deployed as Model Serving Endpoint, runs with service principal permissions.
    """
    try:
        # Native Databricks Authentication (uses current user/service principal credentials)
        w = WorkspaceClient()

        # Replace parameters in query if provided
        if params:
            for key, value in params.items():
                if isinstance(value, str):
                    escaped_value = value.replace("'", "''")
                    query = query.replace(f":{key}", f"'{escaped_value}'")
                else:
                    query = query.replace(f":{key}", str(value))

        # Execute query using Databricks SDK
        response = w.statement_execution.execute_statement(
            statement=query,
            warehouse_id=warehouse_id,
            wait_timeout="50s"
        )

        # Convert to list of tuples for consistency with PostgreSQL version
        if response.result and response.result.data_array:
            return [tuple(row) for row in response.result.data_array]
        else:
            return []

    except Exception as e:
        print(f"Error executing query: {e}")
        return None
```

- Create objects_documentation builder function

**Create create_objects_documentation() function** in databricks_util.py:

The function signature:
```python
def create_objects_documentation(database_schema, table_relationships, key_terms, warehouse_id):
    """
    Build comprehensive database schema context string.

    Args:
        database_schema: List of table dictionaries with columns
        table_relationships: List of relationship dictionaries
        key_terms: List of business term dictionaries
        warehouse_id: Databricks SQL warehouse ID

    Returns:
        str: Formatted documentation string
    """
    objects_documentation = []
    date_range_entries = []

    # Add all tables with all their columns AND values
    for table in database_schema:
        table_name = table['table_name']
        table_desc = table['table_description']

        # Start with table info
        table_text = f"Table {table_name}: {table_desc}\n"
        table_text += "Columns:\n"

        # Add all columns for this table
        for column_name, column_info in table['columns'].items():
            table_text += f"  - Column {column_name}: {column_info['description']}\n"

            # Check if column has a query to get values
            query = column_info.get('query_to_get_column_values', '')
            if query and query.strip():
                # Execute query to get column values
                results = execute_sql_query(query, warehouse_id)
                if results:
                    # Extract values from results
                    column_values = []
                    for row in results:
                        if row and row[0] is not None:
                            column_values.append(str(row[0]))

                    # Add values as pipe-separated string
                    values_str = ' | '.join(column_values)
                    table_text += f"    Values in column {column_name}: {values_str}\n"

            # Check if column has a query to get date range
            date_query = column_info.get('query_to_get_date_range', '')
            if date_query and date_query.strip():
                # Execute query to get date range
                results = execute_sql_query(date_query, warehouse_id)
                if results and results[0] and results[0][0] is not None:
                    date_info = str(results[0][0])
                    date_range_entries.append(f"  - Table {table_name}, column {column_name}: {date_info}\n")

        objects_documentation.append(table_text)

    # Add ALL table relationships
    relationships_text = "\nRelationships between Tables:\n"
    for rel in table_relationships:
        relationships_text += f"  {rel['key1']} -> {rel['key2']}\n"
    objects_documentation.append(relationships_text)

    # Add date range information
    if date_range_entries:
        date_range = "\nImportant considerations about dates available:\n"
        date_range += "".join(date_range_entries)
        objects_documentation.append(date_range)

    # Add key terms with query instructions
    key_terms_text = "\nQuery instructions for key terms:\n"
    for term in key_terms:
        term_name = term['name']
        term_definition = term['definition']
        query_instructions = term['query_instructions']

        if term_definition:
            key_terms_text += f"  - {term_name}: {term_definition}\n"
        else:
            key_terms_text += f"  - {term_name}\n"

        if query_instructions:
            key_terms_text += f"    {query_instructions}\n"

    objects_documentation.append(key_terms_text)

    # Join all parts
    return "\n".join(objects_documentation)
```

- Update initialization.py imports and initialization

In initialization.py:
```python
from src.init.database_schema import database_schema, table_relationships
from src.init.business_glossary import key_terms, synonyms, related_terms, check_glossary_consistency

# Run consistency check after import
check_glossary_consistency()

# Update objects documentation from databricks_util.py
objects_documentation = create_objects_documentation(database_schema, table_relationships, key_terms, connection_string)
```
**Delete database_content** variable from:
- function get_database_content from databricks_util.py
- initialization.py
- agent.py (remove from imports and State class)
- app.py (remove from any references)

**Update prompts** that use database_content:
- sys_prompt_clear: remove "Summary of database content: {database_content}."
- create_sql_query_or_queries: remove database_content
- correct_syntax_sql_query: remove database_content parameter and function signature
- refine_sql_query: remove database_content parameter and function signature
- All scenario Invoke_Params (A, B, C, D): remove database_content
- response_guidelines: remove "Summary of database content: {database_content}." section and update references to use database schema instead

**Important**: In agent.py, rename the existing `execute_sql_query(state:State)` function to `execute_sql_query_tool(state:State)` to avoid naming conflict with the new utility function from demo_database_util.py. Update the function to use the new `execute_sql_query(query, connection_string)` utility function and convert results to DataFrame for display.

**Update get_date_ranges_for_tables() function** in agent.py:
This function previously queried `metadata_reference_table` using `get_database_manager()`. Update it to:
1. Import `database_schema` from src.init.database_schema
2. Remove `get_database_manager()` call and old SQL query to metadata_reference_table
3. Iterate through database_schema list to find tables matching those in the SQL query
4. For each matching table, check all columns for `query_to_get_date_range`
5. Execute those queries using `execute_sql_query(query, connection_string)` utility function
6. Format results as: `"{table_name}, column {column_name}: {date_info}"`

- Delete update_objects_metadata_snowflake.py and the init_snowflake_database folder.
**Get rid of class DatabricksMetadataManager** from databricks_util.py.

#2 Prep & cleanup
These are tasks making the agent simpler & more organized for this future task: Implement consistency of terms usage

in function create_query_analysis, the prompt asks for explanation and insight, also the class. remove the explanation, leave insight. rename it to create_query_insight.

move scenario from generate_answer_details into the state. 

Move Key Assumption into generate_answer details:
- remove explanation from sql query, add it to Key Assumptions which will be part of generate_answer_details.
- rename function format_sql_query_explanations_for_prompt to add_key_assumptions.

Reduce the number of LLMCalls at this step: 
make prompt_ambiguous and prompt_notes a single LLMCall that outputs the notes: prompt_ambiguous_question_followups.

Need still to decide on this:
"Notes" are followup questions. we can rename notes to "agent_questions". Its being used in generating the final answer.

#3 Implement consistency of terms usage.

With the solution implemented in task #1, implement these flows:

A. Agent pulls the term from Key Terms and queries the database.
B. Agent pulls the synonym of the term and uses the synonym to query the database.
C. Agent pulls out related terms that are available in db to suggest them. If they are just 1, query for them + add in Key Assumptions.
D. Agent pulls out related terms that are available in db to suggest them. If they are > 1: follow-up asking which of them?
E. User asks for terms that have all related terms not available in db, or for metrics not available -> Scenario C.

- Create helper functions for term searching and analysis

**Add search_terms() function** (from dev notebook cell-4):
```python
def search_terms(user_question, key_terms, synonyms, related_terms):
    """
    Searches for key terms, synonyms, and related terms in a user question.
    Uses keyword lookup and fuzzy matching.

    Returns:
        dict with 'key_terms_found', 'synonyms_found', 'related_terms_found',
        'synonym_searched_for', 'term_searched_for_related'
    """
```

This function:
- Uses keyword lookup (exact substring match) and fuzzy matching (difflib.get_close_matches with 0.85 cutoff)
- For multi-word terms, uses n-gram approach to match complete phrases
- Tracks which synonym was found in the query (synonym_searched_for)
- Tracks which term triggered related terms (term_searched_for_related)
- Excludes terms already found in key_terms_found or synonyms_found from related_terms_found

**Add terms_helper_function()** (from dev notebook cell-5):
```python
def terms_helper_function(search_terms_output):
    """
    Analyzes search_terms output to determine if database alternatives exist for terms not in DB.

    Returns:
        dict with boolean flags and formatted string messages:
        - synonym_exists_in_db (bool)
        - related_term_exists_in_db (bool)
        - related_key_terms_exist_in_db (bool)
        - synonym_text (str or None)
        - related_single_term_text (str or None)
        - related_multiple_terms_text (str or None)
    """
```

This function returns:
1. **synonym_exists_in_db**: True if synonym found exists in database (evaluated regardless of key_term_not_in_db)
2. **related_term_exists_in_db**: True if key term doesn't exist but exactly 1 related term exists in DB
3. **related_key_terms_exist_in_db**: True if key term doesn't exist but 2+ related terms exist in DB
4. **synonym_text**: Format: `"<synonym_name> is <synonym_description>"` or None
5. **related_single_term_text**: Format: `"<term_in_query> does not exist in the tables I have access to. I returned the data for <related_term_name> which is <related_term_description>"` or None
6. **related_multiple_terms_text**: Format (multi-line): `"- <term_1_name>: <term_1_definition>\n- <term_2_name>: <term_2_definition>"` or None

Here is how (use helper functions above)

- enhance scenario A: 
Current scenario A becomes A1.
New scenario: A2: queries for synonym / single related item. 
      new function: check if the term asked by the user is not available in db but there is a synonym or a single related item. this new function needs to be taken into consideration by the orchestrator the first time when deciding between scenario C and A. if the sql_query that was being ran contains the synonym, where you update the state['current_sql_queries'], you also update the key_assumptions from the generate_answer_details, adding the synonym definition.

- enhance scenario D: query ambiguous because term does not exist or is vague/undefined and there are multiple related terms available in db.
Current scenario D becomes D1.
New scenario: scenario D2: in extract_analytical_intent, before running the LLMCall that decides whether the question is clear or ambiguous (prompt_clear_or_ambiguous), check if the term asked by the user is not available in db and if related terms are more than 1 (make a function for it). if yes, don't run prompt_clear_or_ambiguous , set scenario D2 and set Notes = "The term is not available in db, here are the options: <parsed from terms>".

#4 Lack of temporary context.
Problem: Although there is a date_range column, it's not used in creating the analytical intent, rather in the function get_date_ranges_for_tables which shows the date ranges from the tables in the query (Key Assumptions).

Solution: in sys_prompt_clear, at "Important considerations about time based analysis:", use the variable date_range.
change this prompt "Derive actual date ranges from the feedback date ranges described in database_content."
Or simply because you added the date_range in db schema, just leave it there and just adjust this prompt to make it aware of "Important considerations about dates available"

#5 Enhance follow-up questions
Problem: Recommended trend over time analysis when tables did not allow this.

Solution: inject date_range info into the prompt input and promp engineering. Still need to decide if I take that part out of response_guidelines and I create a separate LLMCall or not.
What I like is that I already added in the prompt types of analysis the agent can do, and that are useful for any dataset. I can think of maybe enhancing that prompt with flags saying what's possible and what not for each analysis type, but need to think more about it. First thoughts is that in the long run I need a separate LLMCall for followup questions, because I can inject insights from previous queries (long memory) and the agent can make correlations with other info to suggest smart next moves the user did not think of. So I would do a separate LLMCall to fill out agent_questions. and the generate_answer prompt will have the response_guidelines that now are more generic, and a simpler prompt now that it doesnt have to suggest anything.

#6 Adapt UI
Implement the green ugly colours for the background to match the PowerBI apps. The agent will be embedded in the left navigation bar of PBi apps (Nikki suggestion). 

#7 The UI shows some numbers in a weird way.
Need to investigate why. Maybe we move out from streamlit idk.


