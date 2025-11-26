
Task #1 Prep & cleanup
These are tasks making the agent simpler & more organized for this future task: Implement consistency of terms usage

1) in function create_query_analysis, the prompt asks for explanation and insight, also the class. remove the explanation, leave insight. rename it to create_query_insight.

2) move scenario from generate_answer_details into the state.

Implementation details:
- Add 'scenario: str' field to State TypedDict in agent.py
- **CRITICAL**: Initialize scenario to empty string (NOT None) in multiple locations:

  1. **In reset_state function**:
     * Add: state['scenario'] = ''

  2. **In orchestrator function (line ~964)**:
     * When result['next_step'] == 'Continue', set: scenario = '' (NOT None)
     * **This is critical!** Setting to None will cause validation errors
     * Reason: The orchestrator runs and can overwrite the initialized value

  3. **In app.py state_dict initialization** (~line 682):
     * Add: 'scenario': ''

  4. **In test notebooks test_state initialization**:
     * Add: 'scenario': ''

- Update all write locations where scenario is assigned:
  * extract_analytical_intent function (output['scenario'])
  * orchestrator function (scenario variable assignment - lines 964, 971, 977, 988)
  * Final write: state['scenario'] = scenario (line 992)

- Update all read locations where scenario is accessed:
  * generate_answer function (state['generate_answer_details']['scenario'] → state['scenario'])
  * create_final_message nested function in generate_answer
  * databricks_util.py in log_to_mlflow function (for MLflow logging)

- Change from: state['generate_answer_details']['scenario']
  To: state['scenario']

**Common Pitfall**: If you see "Input should be a valid string [type=string_type, input_value=None]" error, it means somewhere in the code scenario is being set to None instead of '' (empty string). Check the orchestrator function! 

3) Move Key Assumptions into generate_answer_details:

**IMPORTANT - Initialize all keys in ALL state creation locations:**
State initialization must be complete in MULTIPLE locations to prevent validation errors:

1. **In reset_state function (agent.py ~line 1035)**:
```python
state['generate_answer_details'] = {
    'key_assumptions': [],
    'agent_questions': [],
    'ambiguity_explanation': ''
}
state['scenario'] = ''
```

2. **In app.py when invoking the graph (app.py ~line 676)**:
```python
state_dict = {
    'objects_documentation': objects_documentation,
    'sql_dialect': sql_dialect,
    'messages_log': messages_log,
    'intermediate_steps': [],
    'analytical_intent': [],
    'current_question': prompt,
    'current_sql_queries': [],
    'generate_answer_details': {
        'key_assumptions': [],
        'agent_questions': [],
        'ambiguity_explanation': ''
    },
    'llm_answer': AIMessage(content=''),
    'scenario': ''
}
```

3. **In test notebooks when creating test_state**:
Same structure as app.py above - must include all keys.

This ensures all keys exist from the start, preventing both:
- LangGraph validation errors (missing required State fields)
- KeyError at runtime (missing dict keys)

Current Implementation:
- 'explanation' key exists in each item of state['current_sql_queries']
- Function format_sql_query_explanations_for_prompt() extracts all 'explanation'
  values from state['current_sql_queries'] and formats them as "Key Assumptions"
- Key Assumptions are only added to the final answer in Scenario A

New Implementation:
- Remove 'explanation' key from state['current_sql_queries'] entirely
- Add new key 'key_assumptions' (list of strings) to state['generate_answer_details']
- Initialize: state['generate_answer_details']['key_assumptions'] = []
- The function create_query_explanation runs once per SQL query (after execution)
  and appends each query's explanation to generate_answer_details['key_assumptions']
- Rename format_sql_query_explanations_for_prompt to format_key_assumptions_for_prompt
- New format_key_assumptions_for_prompt() reads from
  generate_answer_details['key_assumptions'] (list) and joins/formats them
  for the final answer (Scenario A only)

Example:
# After Query 1 executes
state['generate_answer_details']['key_assumptions'] = ["Query 1: Filtered to Q1 2024"]

# After Query 2 executes
state['generate_answer_details']['key_assumptions'].append("Query 2: Excluded inactive accounts")

# In generate_answer (Scenario A only)
# After LLM generates the answer:
if state['scenario'] == 'A':
    formatted_key_assumptions = format_key_assumptions_for_prompt(
        state['generate_answer_details']['key_assumptions']
    )
    # Append Key Assumptions to the LLM's response
    final_answer = f"{llm_response}\n\n{formatted_key_assumptions}"

4) Reduce the number of LLMCalls at this step:
make prompt_ambiguous and prompt_notes a single LLMCall that outputs a structured output with 2 fields:

class AmbiguityAnalysis(TypedDict):
  ambiguity_explanation: str # brief explanation of what make it ambiguous
  agent_questions: list[str] # 2-3 alternative analytical intents as questions

Implementation details:
- Create new AmbiguityAnalysis TypedDict class after AnalyticalIntents class definition
- In extract_analytical_intent function:
  * Replace chain_3 to use: llm.with_structured_output(AmbiguityAnalysis)
  * Remove chain_4 (prompt_notes) entirely
  * Update the "Analytical Intent Ambiguous" branch to:
    - Invoke chain_3 once (not chain_3 then chain_4)
    - Store result_3['ambiguity_explanation'] in state['generate_answer_details']['ambiguity_explanation']
    - Store result_3['agent_questions'] in state['generate_answer_details']['agent_questions']
    - Update output dict to use result_3['agent_questions'] for analytical_intent
    - Update output dict to use result_3['ambiguity_explanation'] for notes (will be renamed in Point 5)

The sys prompt should be like that:
"""
The latest user question is ambiguous based on the following database schema: 
{objects_documentation}.

Here is the conversation history with the user:
"{messages_log}".

Latest user message:
"{question}".      

Step 1: Identify what makes the question ambiguous. The question is ambiguous if: 

- Different source columns would give substantially different insights:
  Example: pre-aggregated vs computed metrics with different business logic.

- Multiple fundamentally different metrics could answer the same question:
  Example: "What is the top client?" is ambiguous in a database schema that contains multiple metrics that can answer the question (highest value of sales / highest number of sales). 

- Different columns with the same underlying source data (check database schema) do NOT create ambiguity.

Step 2: Create maximum 3 alternatives of analytical intents to choose from.
    - Do not include redundant intents, be focused.  
    - Each analytical intent is for creating one single sql query.
    - Write each analytical intent using 1 sentence.
    - Mention specific column names, tables names, aggregation functions and filters from the database schema.  
    - Mention only the useful info for creating sql queries.    
	  
Step 3: Create a brief explanation in this format:
  1. One sentence explaining the ambiguity
  2. Present the 2-3 alternatives as clear options for the user to choose from
  
Use simple, non-technical language. Be concise.
  """

5) Rename generate_answer_details['notes'] into generate_answer_details['agent_questions'].
Adjust the codebase.

Implementation details:
- Find all occurrences of 'notes' variable and rename to 'agent_questions':
  * In extract_analytical_intent: output dict 'notes' key → 'agent_questions' key
  * In orchestrator: 'notes' variable → 'agent_questions' variable
  * state['generate_answer_details']['notes'] → state['generate_answer_details']['agent_questions']
- Update scenario prompt Invoke_Params:
  * Scenario C: Remove 'notes' parameter entirely (was showing unavailable data message - no longer needed)
  * Scenario D: Change 'notes' parameter to 'ambiguity_explanation' parameter
- Update scenario prompt templates:
  * Scenario C: Remove {notes} from prompt text
  * Scenario D: Change {notes} to {ambiguity_explanation} in prompt text
- Note: 'agent_questions' in scenarios A/B/C will be populated by Point 6's new function
- Note: 'agent_questions' in scenario D is already populated by Point 4's AmbiguityAnalysis

6) Create a "generate_agent_questions" function that will generate 2 next steps that will guide the user.

Implementation details:
- Create new TypedDict class before the function:
  ```python
  class AgentQuestions(TypedDict):
    agent_questions: Annotated[list[str], "max 2 smart next steps for the user to explore further"]
  ```
- Create new function generate_agent_questions(state: State) -> list[str]
- Place function definition before generate_answer function (around line 790)
- The function should:
  * Use llm.with_structured_output(AgentQuestions)
  * Return result['agent_questions'] (a list of strings)
- Call this function in generate_answer tool:
  * After: scenario = state['scenario']
  * Before: sys_prompt = next(s['Prompt'] for s in scenario_prompts...)
  * Add: if scenario in ['A', 'B', 'C']:
           state['generate_answer_details']['agent_questions'] = generate_agent_questions(state)
- This function will populate generate_answer_details['agent_questions'].
- This function will run only if the scenario is A,B or C and the prompt will include these instructions:

"You are a decision support consultant helping users become more data-driven.
    
Here is the conversation history with the user:
{messages_log}.

Latest user message:
{question}.  

Your task is to guide the users to answer their analytical goal that you derive from the conversation history and from the last user message.

Suggest max 2 smart next steps for the user to explore further, chosen from the examples below and tailored to what's available in the database schema:
  {objects_documentation}

  Example of next steps:
  - Trends over time:
    Example: "Want to see how this changed over time?".
    Suggest trends over time only for tables containing multiple dates available. 

  - Drill-down suggestions:
    Example: "Would you like to explore this by brand or price tier?"

  - Top contributors to a trend:
    Example: "Want to see the top 5 products that drove this increase in satisfaction?"

  - Explore a possible cause:
    Example: "Curious if pricing could explain the drop? I can help with that."

  - Explore the data at higher granularity levels if the user analyzes on low granularity columns. Use database schema to identify such columns.
    Example: Instead of analyzing at product level, suggest at company level.

  - Explore the data on filtered time ranges. Check the database schema for date range information under "Important considerations about dates available".
    Example: Instead of analyzing for all feedback dates, suggest filtering for a year or for a few months.

  - Filter the data on the value of a specific attribute. Use values from the database schema.
    Example: Instead of analyzing for all companies, suggest filtering for a single company and give a few suggestions.
    "

6.2 response_guidelines will be deleted.
scenario_A, scenario_B, scenario_C and scenario_D will not embedd response_guidelines anymore

Implementation details:
- Find the response_guidelines variable definition (around line 640)
- Delete the entire response_guidelines variable and its multi-line string content
- Remove all instances of: + '\n\n' + response_guidelines.strip()
  * From scenario_A Prompt
  * From scenario_B Prompt
  * From scenario_C Prompt
  * From scenario_D Prompt

6.3 Change scenario_A/B/C/D:

Implementation details for ALL scenarios:
- Update each scenario dict's 'Invoke_Params' lambda to include:
  * 'agent_questions': state['generate_answer_details'].get('agent_questions', [])
- Scenario A also needs to keep: 'insights', 'objects_documentation', 'messages_log', 'question'
- Scenario B/C need: 'objects_documentation', 'messages_log', 'question', 'agent_questions'
- Scenario D needs: 'objects_documentation', 'messages_log', 'question', 'ambiguity_explanation', 'agent_questions'
- All prompts should reference {agent_questions} in the template

-->> scenario_A:
"You are a decision support consultant helping users become more data-driven.
Your task is to continue the conversation from the last user message by guiding the users to answer their analytical goal.
    
Here is the conversation history with the user:
{messages_log}.
Latest user message:
{question}.  
- Use both the raw SQL results and the extracted insights below to form your answer: {insights}. 
- Don't assume facts that are not backed up by the data in the insights.    
- Include all details from these insights.
- Suggest these next steps for the user: {agent_questions}.
	
Response guidelines:
  - Respond in clear, non-technical language. 
  - Be concise.
  - Keep it simple and conversational.
  - If the question is smart, reinforce the user's question to build confidence. 
    Example: "Great instinct to ask that - it's how data-savvy pros think!"
  - Ask the user which option they prefer from your suggested next steps.
  - Use warm, supportive closing that makes the user feel good. 
    Example: "Keep up the great work!", "Have a great day ahead!"

-->> scenario_B:

"You are a decision support consultant helping users become more data-driven.
Your task is to continue the conversation from the last user message by guiding the users to answer their analytical goal.
    
Here is the conversation history with the user:
{messages_log}.

Latest user message:
{question}.

- Suggest these next steps for the user: {agent_questions}

Response guidelines:
  - Respond in clear, non-technical language. 
  - Be concise.
  - Keep it simple and conversational.
  - Ask the user which option they prefer from your suggested next steps."

-->> scenario_C:

"You are a decision support consultant helping users become more data-driven.
Your task is to continue the conversation from the last user message by guiding the users to answer their analytical goal.

Here is the conversation history with the user:
{messages_log}.

Latest user message:
{question}.

Unfortunately, the requested information from last prompt is not available in our database. 

- Suggest these next steps for the user: {agent_questions}.

Response guidelines:
  - Respond in clear, non-technical language. 
  - Be concise.
  - Keep it simple and conversational.
  - Ask the user which option they prefer from your suggested next steps."

-->> scenario_D:

"You are a decision support consultant helping users become more data-driven.
Your task is to continue the conversation from the last user message by guiding the users to answer their analytical goal.

Here is the conversation history with the user:
{messages_log}.

Latest user message:
{question}.

The last user prompt could be interpreted in multiple ways. 
Here's what makes it ambiguous: {ambiguity_explanation}.
Suggest these next steps for the user: {agent_questions}.

Response guidelines:
- User to specify which analysis it wants
- Respond in clear, non-technical language. 
- Be concise.
- Keep it simple and conversational."

-->> the orchestrator function runs sys_prompt_notes under the "# if scenario C" branch. we will remove this LLMCall, this entire else: block.

Implementation details:
- In orchestrator function, find the "if scenario C" branch (around line 935)
- The current structure is:
  ```python
  if result['next_step'] == 'Continue':
      scenario = None
      notes = None
  elif result['next_step'] == 'B':
      scenario = result['next_step']
      notes = None
  else:  # This is scenario C
      # Contains sys_prompt_notes, chain creation, and LLM invocation
      scenario = result['next_step']
      notes = notes_text.content
  ```
- Replace the entire else: block with:
  ```python
  else:  # scenario C
      scenario = result['next_step']
      agent_questions = None  # Changed from 'notes' per Point 5
      next_tool_name = 'generate_answer'
  ```
- This removes approximately 20 lines of code that created and ran sys_prompt_notes

7) Adapt UI
In the app.py, the streamlit app has a purple gradient background. remove that and simply use a white background with black text for user questions and agent answers.

Implementation details:
Find and update these CSS selectors in the st.markdown("""<style>...</style>""") block:

1. .stApp background (around line 68):
   Change: background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%) !important;
   To: background: #ffffff !important;

2. .main .block-container background (around line 96):
   Change: background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%) !important;
   To: background: #ffffff !important;

3. .main-header background (around line 101):
   Change: background: inherit !important;
   To: background: #ffffff !important;
   Also change border: border-bottom: 1px solid rgba(255, 255, 255, 0.2);
   To: border-bottom: 1px solid #e5e7eb;

4. .user-message and .ai-message background (around lines 154-160):
   Change both: background: inherit !important;
   To both: background: #ffffff !important;

5. .message-text color (around line 192):
   Change: color: #ffffff;
   To: color: #000000;
   Also change: background: inherit !important;
   To: background: #ffffff !important; 

Task #2 Implement consistency of terms usage.

Implement these flows:

A. Agent pulls the term from Key Terms and queries the database.
B. Agent pulls the synonym of the term and uses the synonym to query the database.
C. Agent pulls out related terms that are available in db to suggest them. If they are just 1, query for them + add in Key Assumptions.
D. Agent pulls out related terms that are available in db to suggest them. If they are > 1: follow-up asking which of them?
E. User asks for terms that have all related terms not available in db, or for metrics not available -> Scenario C.

1) Create helper function for term searching and analysis

**Implementation**: Function already created in `notebooks/dev.ipynb` cell-4. Copy to appropriate module.

```python
def search_terms(user_question, key_terms, synonyms, related_terms):
    """
    Searches for key terms, synonyms, and related terms in a user question.
    Uses keyword lookup and fuzzy matching.

    Returns dict with:
        - key_terms: list of dicts - key_terms that exist in database (exists_in_database=True)
        - synonym_searched_for: str or None - the word/phrase from user question that matched a synonym
        - synonym: dict or None - the key term that the synonym maps to
        - synonym_exists_in_db: bool - True if synonym exists in database
        - synonym_docu: str or None - format: "{synonym_word} is synonym with {key_term_name}"
        - related_term_searched_for: str or None - the word/phrase from user question that has related terms
        - related_term_exists_in_db: bool - True if exactly 1 related term exists in DB
        - related_terms: dict or list or None - single dict if 1 related term, list of dicts if >1
        - related_terms_exists_in_db: bool - True if >1 related terms exist in DB
        - related_terms_docu: str or None - format: "{term_from_q} is related (similar but different) with: {rel1}, {rel2}"
          (multi-line if multiple terms from question have related terms)
    """
```

This function:
- Uses keyword lookup (exact substring match) and fuzzy matching (difflib.get_close_matches with 0.85 cutoff)
- For multi-word terms, uses n-gram approach to match complete phrases

2) Enhance objects_documentation

**State changes required:**
- Add `search_terms_output: dict` field to `State` TypedDict in `agent.py`
- Initialize in `reset_state`: `state['search_terms_output'] = {}`

**Implementation steps:**

- In `create_objects_documentation` function, replace "Query instructions for key terms:" with "Key Terms:".

- Move `create_objects_documentation` call from `initialization.py` to `reset_state` function in `agent.py`.

- In `reset_state`:
  1. Call `search_terms(state['current_question'], key_terms, synonyms, related_terms)`
  2. Store result: `state['search_terms_output'] = search_terms_output`
  3. Build `objects_documentation` using existing `create_objects_documentation` function format, but only include:
     - Relevant key terms from `search_terms_output['key_terms']` (these already have `exists_in_database=True`)
     - Format each: `"{term_name}: {term_definition}."` under "Key Terms:" section
     - Append `synonym_docu` at the end (if not None)
     - Append `related_terms_docu` at the end (if not None)
  4. Store: `state['objects_documentation'] = objects_documentation`

3) Enhance scenario A: queries for synonym / single related item

**3.1 Update orchestrator prompt**

Change:
- "If the user asks for data/metrics not available in the database schema → 'C'"

To:
- "If the user asks for data/metrics not available AND no synonyms or related terms exist in the database schema → 'C'"

**3.2 Add Key Assumptions for synonym/related term usage**

In `execute_sql_query` function, after updating `state['current_sql_queries']` with query results, add to `state['generate_answer_details']['key_assumptions']`:

Get `search_terms_output` from state: `state['search_terms_output']`

- If `search_terms_output['synonym_exists_in_db'] == True`:
  - Add: `"{synonym_name} is {synonym_definition}"`
  - Use: `search_terms_output['synonym']` to get name and definition

- If `search_terms_output['related_term_exists_in_db'] == True`:
  - If `related_term_searched_for` exists in `search_terms_output['key_terms']` AND its definition is not '':
    - Add: `"{searched_term_name} ({searched_term_def}) does not exist in the tables I have access to. I returned the data for {related_term_name} ({related_term_def})"`
  - Otherwise:
    - Add: `"I returned the data for {related_term_name} ({related_term_def})"`
  - Use: `search_terms_output['related_terms']` to get related term name/definition

4) Enhance scenario D: handle multiple related terms

In `extract_analytical_intent` function, **before** running `chain_1` (prompt_clear_or_ambiguous):

Get `search_terms_output` from state: `state['search_terms_output']`

- Check if `search_terms_output['related_terms_exists_in_db'] == True` (meaning > 1 related term found):
  - Set `scenario = 'D'`
  - Set `tool_name = 'generate_answer'`
  - Create `ambiguity_explanation`:
    - If `related_term_searched_for` exists in `search_terms_output['key_terms']` AND definition != '':
      - `"The term {related_term_searched_for} is not available in the tables I have access to, but related terms are available."`
    - Otherwise:
      - `"The term {related_term_searched_for} can mean multiple things."`
  - Create `agent_questions`: Format as list of options:
    - `"Which option are you interested in? - {related_term_1_name}: {related_term_1_definition}. - {related_term_2_name}: {related_term_2_definition}. ..."`
    - Use `search_terms_output['related_terms']` (list of dicts)
  - Skip to "# update the state" section (bypass chain_1/chain_2/chain_3)

