# Implementation Requirements: Add Highlights Section to Agent Responses

## **Overview**
Add "Key Assumptions" section at end of responses showing date ranges, filters, limits, and aggregation levels.

**Storage Decision:** New `data_range` column in `metadata_reference_table`

**State Decision:** Use existing `explanation` field (not new `highlights` field)

---

## **Step 1: Update Metadata Schema**

### **File: `src/init/init_demo_database/update_objects_metadata_demo_db.py`**

### **1.1 Update `create_metadata_reference_table` Function**

Add `data_range TEXT` column to table creation:

```python
def create_metadata_reference_table(connection_string: str):
    # ... existing code ...

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS metadata_reference_table (
            table_name TEXT,
            column_name TEXT,
            data_type TEXT,
            is_nullable TEXT,
            column_default TEXT,
            is_primary_key TEXT,
            foreign_key_table TEXT,
            foreign_key_column TEXT,
            comment TEXT,
            column_values TEXT,
            data_range TEXT  -- NEW
        )
    """)

    # ... rest of existing code ...
```
---

## **Step 2: Update `agent.py`**

### **2.1 Add Import**
```python
import sqlglot
from sqlglot import parse_one
```

### **2.2 Add Helper Functions** (after line 415)

```python
def extract_tables_from_sql(sql_query: str) -> list[str]:
    """Parse SQL to extract table names"""
    try:
        ast = parse_one(sql_query, dialect=sql_dialect)
        tables = []
        for items in ast.find_all(sqlglot.expressions.Table):
            tables.append(items.sql())
        return list(dict.fromkeys(tables))
    except:
        import re
        tables = re.findall(r'FROM\s+(\w+)|JOIN\s+(\w+)', sql_query, re.IGNORECASE)
        return list(set([t for group in tables for t in group if t]))

def get_date_ranges_for_tables(sql_query: str) -> list[str]:
    """Fetch date ranges for tables used in SQL query. Returns list of date range strings."""
    tables = extract_tables_from_sql(sql_query)

    if not tables:
        return []

    db_manager = get_database_manager()
    try:
        # Build WHERE IN clause with quoted table names
        table_list = ', '.join([f"'{t}'" for t in tables])
        query = f"SELECT DISTINCT data_range FROM metadata_reference_table WHERE table_name IN ({table_list}) AND data_range IS NOT NULL"

        result_df = db_manager._execute_sql(query)

        if result_df.empty:
            return []

        return result_df['data_range'].tolist()
    except Exception as e:
        return []

class QueryExplanation(TypedDict):
    explanation: Annotated[list[str], "2-5 concise assumptions/highlights"]

def create_query_explanation(sql_query: str, sql_query_result: str) -> dict:
    """Generate explanation highlights for query assumptions"""

    system_prompt = """Analyze SQL query and generate 2-5 concise bullet points.

SQL Query: {sql_query}

Categories: filters, aggregation level, limits, record status

Format: Short phrases, no articles. Examples:
- "Active advisors only"
- "Top 10 by revenue"
- "Aggregated at household level"

Only include TRUE statements. Max 5, min 2."""

    prompt = create_prompt_template('system', system_prompt)
    chain = prompt | llm_fast.with_structured_output(QueryExplanation)

    llm_explanation = chain.invoke({
        'sql_query': sql_query
    })

    # Append date ranges
    date_ranges = get_date_ranges_for_tables(sql_query)
    combined_explanation = llm_explanation['explanation'] + date_ranges

    return {'explanation': combined_explanation}
```

### **2.3 Generate Explanation** (line 491 in `execute_sql_query`)

**After** `analysis = create_query_analysis(...)`, add:
```python
explanation = create_query_explanation(sql_query, sql_query_result)
```

**Update state assignment:**
```python
state['current_sql_queries'][query_index]['explanation'] = explanation['explanation']
```

### **2.4 Format Explanations** (after line 738)

```python
def format_sql_query_explanations_for_prompt(sql_queries: list[dict]) -> str:
    """Format explanations into single section"""
    all_explanations = []
    for q in sql_queries:
        if q.get('explanation') and isinstance(q['explanation'], list):
            all_explanations.extend(q['explanation'])

    if not all_explanations:
        return ""

    unique_explanations = list(dict.fromkeys(all_explanations))
    return "\n\n**Key Assumptions:**\n" + "\n".join([f"• {e}" for e in unique_explanations])
```

### **2.5 Append to Response** (line 752 in `generate_answer`)

**Replace** the `final_answer_chain` with:

```python
def create_final_message(x):
    base_content = x['llm_answer'].content
    explanation_section = ""
    if x['input_state'].get('generate_answer_details', {}).get('scenario') == 'A':
        explanation_section = format_sql_query_explanations_for_prompt(x['input_state']['current_sql_queries'])
    return {'ai_message': AIMessage(content=base_content + explanation_section,
                                   response_metadata=x['llm_answer'].response_metadata)}

final_answer_chain = {
    'llm_answer': llm_answer_chain,
    'input_state': RunnablePassthrough()
} | RunnableLambda(create_final_message)
```

---

## **Checklist**

- [ ] Update `create_metadata_reference_table()` to add `data_range` column
- [ ] Manually populate `data_range` values for temporal tables
- [ ] Add `sqlglot` import to agent.py
- [ ] Add `extract_tables_from_sql()` function
- [ ] Add `get_date_ranges_for_tables()` function
- [ ] Add `create_query_explanation()` function
- [ ] Generate explanation in `execute_sql_query()`
- [ ] Add `format_sql_query_explanations_for_prompt()` function
- [ ] Update `generate_answer()` to append explanations

---

## **Example Output**

```
The average satisfaction score is 8.7 out of 10...

**Key Assumptions:**
• Active households only
• 2024-01-01 to 2025-09-30
```
