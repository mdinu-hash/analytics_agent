
#1 Create glossaries and db schema file.

def fill_database_schema(database_schema, warehouse_id):
    """
    Fill column_values and date_range fields in database_schema using optimized batch queries.

    This function executes all queries from query_to_get_column_values and query_to_get_date_range
    using UNION ALL to minimize database round trips.

    Args:
        database_schema: List of table dictionaries with columns (will be modified in-place)
        warehouse_id: Databricks SQL warehouse ID

    Returns:
        database_schema: The modified database_schema with filled column_values and date_range
    """
    # Collect all queries with metadata about their location
    column_value_queries = []
    date_range_queries = []

    for table_idx, table in enumerate(database_schema):
        table_name = table['table_name']
        for column_name, column_info in table['columns'].items():
            # Collect column value queries
            query = column_info.get('query_to_get_column_values', '').strip()
            if query:
                column_value_queries.append({
                    'table_idx': table_idx,
                    'column_name': column_name,
                    'query': query,
                    'identifier': f"{table_name}.{column_name}"
                })

            # Collect date range queries
            date_query = column_info.get('query_to_get_date_range', '').strip()
            if date_query:
                date_range_queries.append({
                    'table_idx': table_idx,
                    'column_name': column_name,
                    'query': date_query,
                    'identifier': f"{table_name}.{column_name}"
                })

    # Execute column value queries with UNION ALL
    if column_value_queries:
        logging.info(f"Executing {len(column_value_queries)} column value queries in a single batch...")
        union_parts = []
        for item in column_value_queries:
            # Wrap each query to add an identifier column
            union_parts.append(f"SELECT '{item['identifier']}' AS _identifier, CAST(value AS STRING) AS value FROM ({item['query']}) AS subq(value)")

        batch_query = " UNION ALL ".join(union_parts)
        result = execute_query(batch_query, warehouse_id)

        if result['success']:
            df = result['data']

            # Use pandas groupby for performance (faster than loops)
            if df is not None and not df.empty:
                grouped = df.groupby('_identifier')['value'].apply(
                    lambda x: ' | '.join(x.dropna().astype(str))
                ).to_dict()

                # Fill the database_schema with results
                for item in column_value_queries:
                    identifier = item['identifier']
                    if identifier in grouped:
                        database_schema[item['table_idx']]['columns'][item['column_name']]['column_values'] = grouped[identifier]
        else:
            logging.error(f"Column value queries failed: {result['error']}")

    # Execute date range queries with UNION ALL
    if date_range_queries:
        logging.info(f"Executing {len(date_range_queries)} date range queries in a single batch...")
        union_parts = []
        for item in date_range_queries:
            # Wrap each query to add an identifier column
            union_parts.append(f"SELECT '{item['identifier']}' AS _identifier, CAST(value AS STRING) AS value FROM ({item['query']}) AS subq(value)")

        batch_query = " UNION ALL ".join(union_parts)
        result = execute_query(batch_query, warehouse_id)

        if result['success']:
            df = result['data']

            # Use pandas set_index for performance (O(1) lookup vs O(n) loops)
            if df is not None and not df.empty:
                date_mapping = df.set_index('_identifier')['value'].to_dict()

                # Fill the database_schema with results
                for item in date_range_queries:
                    identifier = item['identifier']
                    if identifier in date_mapping and date_mapping[identifier] is not None:
                        database_schema[item['table_idx']]['columns'][item['column_name']]['date_range'] = str(date_mapping[identifier])
        else:
            logging.error(f"Date range queries failed: {result['error']}")

    logging.info("Database schema filling completed!")
    return database_schema

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


