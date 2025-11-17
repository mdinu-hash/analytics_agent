
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


- Cleanup & Adjustment - Database Utility Functions

**Get rid of class DemoDatabaseMetadataManager** from demo_database_util.py.

**Add these utility functions to demo_database_util.py:**

```python
@contextmanager
def get_db_connection(connection_string):
    """Context manager for database connections."""
    conn = None
    try:
        conn = psycopg2.connect(connection_string)
        yield conn
    finally:
        if conn:
            conn.close()

def execute_sql_query(query, connection_string):
    """
    Execute a SQL query and return results.

    Args:
        query: SQL query string to execute
        connection_string: PostgreSQL connection string

    Returns:
        List of results (each row as a tuple), or None if query fails
    """
    try:
        with get_db_connection(connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            return results
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
```

- Create objects_documentation builder function

**Create create_objects_documentation() function** in initialization.py (based on dev notebook cell-2):
This function builds the comprehensive database schema context string by:
1. Looping through database_schema and adding all tables with columns
2. Executing query_to_get_column_values for each column to retrieve actual values
3. Collecting date range information from query_to_get_date_range queries
4. Adding all table relationships from table_relationships list
5. Adding date range information as "Important considerations about dates available"
6. Adding key terms with query instructions

The output variable should be named **objects_documentation** (replacing the old one).

- Update initialization.py imports and initialization

In initialization.py:
```python
from src.init.database_schema import database_schema, table_relationships
from src.init.business_glossary import key_terms, synonyms, related_terms, check_glossary_consistency

# Run consistency check after import
check_glossary_consistency()

# Create objects documentation
objects_documentation = create_objects_documentation(database_schema, table_relationships, key_terms, connection_string)
```

- Remove database_content variable completely

**Delete database_content** variable from:
- initialization.py
- agent.py (remove from imports and State class)
- app.py (remove from any references)

**Update prompts** that use database_content:
- sys_prompt_clear: remove "Summary of database content: {database_content}."
- create_sql_query_or_queries: remove database_content
- correct_syntax_sql_query: remove database_content
- Any other chains that reference it

- Delete obsolete files

Delete these files:
- run_update_objects_metadata_demo.ipynb
- update_objects_metadata_demo_db.py 

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


