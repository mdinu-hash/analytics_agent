# Implementation Requirements: Clean Unused Parts

## Objective
Remove unused metadata parsing functionality and the `limitation` field from the codebase. Archive the removed functions for future reference.

---

## Step 1: Create Archive Directory and File

### 1.1 Create Archive Directory
- Create a new directory: `archive/` at the project root if it doesn't exist

### 1.2 Create Archive File
- Create file: `archive/metadata_parsing.py`
- Add header comment explaining these are archived functions
- Copy the following 4 functions from `agent.py` to this archive file:

1. **extract_metadata_from_sql_query(sql_query)**
   - Location: ~lines 444-490 in agent.py
   - Includes the sqlglot import dependencies

2. **format_sql_metadata_explanation(tables, filters, aggregations, groupings, header)**
   - Location: ~lines 492-505 in agent.py

3. **create_query_metadata(sql_query)**
   - Location: ~lines 507-511 in agent.py

4. **create_queries_metadata(sql_queries)**
   - Location: ~lines 514-533 in agent.py

**Archive file structure:**
```python
# Archived metadata parsing functions
# These functions were removed from the main codebase as they are no longer used

import sqlglot
from sqlglot import parse_one, exp

def extract_metadata_from_sql_query(sql_query):
    # [full function body]

def format_sql_metadata_explanation(tables:list=None, filters:list=None, aggregations:list=None, groupings:list=None, header:str='') -> str:
    # [full function body]

def create_query_metadata(sql_query: str):
    # [full function body]

def create_queries_metadata(sql_queries: list[dict]):
    # [full function body]
```

---

## Step 2: Remove from agent.py

### 2.1 Remove sqlglot Import
**Location:** ~lines 441-442

**Remove:**
```python
import sqlglot
from sqlglot import parse_one, exp
```

**Why:** Only used by the metadata parsing functions being removed.

---

### 2.2 Remove All 4 Metadata Functions
**Location:** ~lines 444-533

**Remove entirely:**
1. `extract_metadata_from_sql_query()` function
2. `format_sql_metadata_explanation()` function
3. `create_query_metadata()` function
4. `create_queries_metadata()` function

---

### 2.3 Update QueryAnalysis TypedDict
**Location:** ~lines 384-388

**Before:**
```python
class QueryAnalysis(TypedDict):
    ''' complete analysis of a sql query, including its explanation, limitation and insight '''
    explanation: str
    limitation: str
    insight: str
```

**After:**
```python
class QueryAnalysis(TypedDict):
    ''' complete analysis of a sql query, including its explanation and insight '''
    explanation: str
    insight: str
```

**Changes:**
- Remove `limitation: str` field
- Update docstring to remove mention of "limitation"

---

### 2.4 Update create_query_analysis() Function Docstring
**Location:** ~lines 390-392

**Before:**
```python
def create_query_analysis(sql_query:str, sql_query_result:str):
   ''' creates: explanation - a concise explanation of what the sql query does.
                limitation - a concise explanation of the sql query by pointing out its limitations.
                insight - insight from the result of the sql query.
   '''
```

**After:**
```python
def create_query_analysis(sql_query:str, sql_query_result:str):
   ''' creates: explanation - a concise explanation of what the sql query does.
                insight - insight from the result of the sql query.
   '''
```

**Changes:**
- Remove the line mentioning "limitation"

---

### 2.5 Simplify create_query_analysis() System Prompt
**Location:** ~lines 393-434

**Before:** (system prompt with 3 steps)
```python
   system_prompt = """
   You are an expert data analyst.

   You are provided with the following SQL query:
   {sql_query}.

   Which yielded the following result:
   {sql_query_result}.

   Provide a structured analysis with three components:

   Step 1: Explanation: A concise description of what the query outputs, in one short phrase.
                   Do not include introductory words like "The query" or "It outputs."

   Step 2: Limitation: Inherent limitations or assumptions of the query based strictly on its structure and logic.
                  Focus on:
                  - How LIMIT, ORDER BY, GROUP BY, or JOINs may introduce assumptions
                  - How filtering or aggregation logic may bias the output
                  - Situations where the query might **return incomplete or misleading results due to logic only**
                  - Cases where ORDER BY combined with LIMIT might exclude other rows with equal values (ties)

                  Only describe things that follow **logically from the query**, not from the dataset itself.

                  🚫 Do NOT mention:
                  - speculate on what the user is trying to analyze
                  - suggest what insights are missing
                  - mention field names being correct or incorrect
                  - mention data types, nulls, formatting, spelling, or schema correctness
                  - mention what other attributes, columns, filters, or relationships "could have" been used
                  - assume anything about the intent behind the query

                  If the query has no structural limitations or assumptions, respond with exactly "No comments for the query".

                  Respond in 1 to 3 concise sentences, or with the exact phrase above.

   Step 3: Insight: Key findings from the results, stating facts directly without technical terms.
               - Include the limitations discovered in step 2, as long as it's different than "No comments for the query".
               - Do not mention your subjective assessment over the results.
               - Avoid technical terms like "data","dataset","table","list","provided information","query" etc.
   """
```

**After:** (system prompt with 2 steps)
```python
   system_prompt = """
   You are an expert data analyst.

   You are provided with the following SQL query:
   {sql_query}.

   Which yielded the following result:
   {sql_query_result}.

   Provide a structured analysis with two components:

   Step 1: Explanation: A concise description of what the query outputs, in one short phrase.
                   Do not include introductory words like "The query" or "It outputs."

   Step 2: Insight: Key findings from the results, stating facts directly without technical terms.
               - Do not mention your subjective assessment over the results.
               - Avoid technical terms like "data","dataset","table","list","provided information","query" etc.
   """
```

**Changes:**
- Remove entire "Step 2: Limitation" section
- Rename old "Step 3: Insight" to "Step 2: Insight"
- Remove bullet point about including limitations from insight step

---

### 2.6 Remove 'metadata' Field from current_sql_queries Dictionary
**Location:** ~line 351 in `create_sql_query_or_queries()` function

**Before:**
```python
  for q in result['query']:
   state['current_sql_queries'].append( {'query': q,
                                     'explanation': '', ## add it later
                                     'result':'', ## add it later
                                     'insight': '', ## add it later
                                     'metadata':'' ## add it later
                                      } )
```

**After:**
```python
  for q in result['query']:
   state['current_sql_queries'].append( {'query': q,
                                     'explanation': '', ## add it later
                                     'result':'', ## add it later
                                     'insight': '' ## add it later
                                      } )
```

**Changes:**
- Remove `'metadata':''` line

---

### 2.7 Remove Metadata Creation and Assignment in execute_sql_query()
**Location:** ~lines 490-500

**Before:**
```python
       # if the sql query does not exceed output context window return its result
       if not check_if_exceed_maximum_context_limit(sql_query_result):
         analysis = create_query_analysis(sql_query, sql_query_result)
         sql_query_metadata = create_query_metadata(sql_query)

         # Update state
         state['current_sql_queries'][query_index]['result'] = sql_query_result
         state['current_sql_queries'][query_index]['insight'] = analysis['insight']
         state['current_sql_queries'][query_index]['query'] = sql_query
         state['current_sql_queries'][query_index]['metadata'] = sql_query_metadata
         state['current_sql_queries'][query_index]['explanation'] = analysis['explanation']
         break
```

**After:**
```python
       # if the sql query does not exceed output context window return its result
       if not check_if_exceed_maximum_context_limit(sql_query_result):
         analysis = create_query_analysis(sql_query, sql_query_result)

         # Update state
         state['current_sql_queries'][query_index]['result'] = sql_query_result
         state['current_sql_queries'][query_index]['insight'] = analysis['insight']
         state['current_sql_queries'][query_index]['query'] = sql_query
         state['current_sql_queries'][query_index]['explanation'] = analysis['explanation']
         break
```

**Changes:**
- Remove `sql_query_metadata = create_query_metadata(sql_query)` line
- Remove `state['current_sql_queries'][query_index]['metadata'] = sql_query_metadata` line

---

### 2.8 Simplify generate_answer() Function
**Location:** ~lines 751-774

**Before:**
```python
  if scenario == 'A': # show filters
    final_answer_chain = { 'llm_answer': llm_answer_chain
                         ,'input_state': RunnablePassthrough()
                           } | RunnableLambda (lambda x: { 'ai_message': AIMessage( content = f"{x['llm_answer'].content.strip()}\n\n{create_queries_metadata(x['input_state']['current_sql_queries'])}"
                                                                         ,response_metadata = x['llm_answer'].response_metadata  ) } )
  else: # filters not necessary
    final_answer_chain = { 'llm_answer': llm_answer_chain
                          , 'input_state': RunnablePassthrough()
                          } | RunnableLambda (lambda x: { 'ai_message': AIMessage( content = f"{x['llm_answer'].content}"
                                                                        ,response_metadata = x['llm_answer'].response_metadata  ) } )

  # invoke parameters based on scenario
  invoke_params = next(s['Invoke_Params'](state) for s in scenario_prompts if s['Type'] == scenario)

  result = final_answer_chain.invoke(invoke_params)
  ai_msg = result['ai_message']

  # Add token count for SQL metadata if applicable
  if scenario == 'A':
    explanation_token_count = llm.get_num_tokens(create_queries_metadata(state['current_sql_queries']))
    if llm_provider == 'anthropic':
        ai_msg.response_metadata['usage']['output_tokens'] += explanation_token_count
    else:
        ai_msg.response_metadata['token_usage']['total_tokens'] += explanation_token_count

  # Update state (common for all scenarios)
```

**After:**
```python
  final_answer_chain = { 'llm_answer': llm_answer_chain
                        , 'input_state': RunnablePassthrough()
                        } | RunnableLambda (lambda x: { 'ai_message': AIMessage( content = f"{x['llm_answer'].content}"
                                                                      ,response_metadata = x['llm_answer'].response_metadata  ) } )

  # invoke parameters based on scenario
  invoke_params = next(s['Invoke_Params'](state) for s in scenario_prompts if s['Type'] == scenario)

  result = final_answer_chain.invoke(invoke_params)
  ai_msg = result['ai_message']

  # Update state (common for all scenarios)
```

**Changes:**
- Remove the `if scenario == 'A'` conditional for showing filters
- Keep only the simple answer chain that doesn't append metadata
- Remove the entire token counting block for SQL metadata (lines 768-774)

---

## Step 3: Verification

### 3.1 Check for Remaining References
Search the entire `agent.py` file for any remaining references to:
- `create_query_metadata`
- `create_queries_metadata`
- `extract_metadata_from_sql_query`
- `format_sql_metadata_explanation`
- `sqlglot`

**Expected result:** No matches found

### 3.2 Validate Python Syntax
Run: `python -m py_compile agent.py`

**Expected result:** No syntax errors

### 3.3 Verify Archive Created
Check that `archive/metadata_parsing.py` exists and contains all 4 functions with their sqlglot dependencies.

---

## Summary of Changes

### Files Created:
- `archive/metadata_parsing.py` - Contains archived metadata parsing functions

### Files Modified:
- `agent.py` - Removed metadata parsing code and limitation field

### Total Removals:
- 1 import statement (sqlglot)
- 4 functions (all metadata parsing)
- 1 TypedDict field (limitation)
- 1 field from current_sql_queries dictionary (metadata)
- 5 function call references
- 1 conditional block (scenario A metadata appending)
- 1 token counting block

### Lines Reduced:
- Approximately 100+ lines removed from agent.py
- Code is now cleaner and more maintainable

---

## Rationale

### Why Remove Metadata Parsing?
- Functions `extract_metadata_from_sql_query()`, `format_sql_metadata_explanation()`, `create_query_metadata()`, and `create_queries_metadata()` were generated but never meaningfully used
- The metadata was appended to messages but not actually utilized in decision-making
- Removing reduces complexity and token usage

### Why Remove Limitation Field?
- The `limitation` field in `QueryAnalysis` was generated but never stored or used anywhere in the codebase
- Generated detailed limitation analysis that was immediately discarded
- Removing simplifies the query analysis prompt and reduces LLM processing time

### Why Archive Instead of Delete?
- Preserves the code in case future requirements change
- Provides reference for what was removed
- Maintains git history context

---

## Notes for Implementation

1. **Order matters**: Create the archive file BEFORE removing from agent.py to avoid losing the code
2. **Test after changes**: Ensure the application still runs correctly after removals
3. **Git commit**: Consider making this a single atomic commit with a clear message like "refactor: archive unused metadata parsing and limitation field"
4. **Dependencies**: Check if `sqlglot` can be removed from `requirements.txt` if it's not used elsewhere in the project
