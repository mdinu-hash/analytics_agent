
#1 Prep & cleanup
These are tasks making the agent simpler & more organized for this future task: Implement consistency of terms usage

1) in function create_query_analysis, the prompt asks for explanation and insight, also the class. remove the explanation, leave insight. rename it to create_query_insight.

2) move scenario from generate_answer_details into the state.

Implementation details:
- Add 'scenario: str' field to State TypedDict in agent.py
- Update all write locations where scenario is assigned:
  * extract_analytical_intent function (output['scenario'])
  * orchestrator function (scenario variable assignment)
- Update all read locations where scenario is accessed:
  * generate_answer function (state['generate_answer_details']['scenario'] → state['scenario'])
  * create_final_message nested function in generate_answer
  * databricks_util.py in log_to_mlflow function (for MLflow logging)
- Change from: state['generate_answer_details']['scenario']
  To: state['scenario'] 

3) Move Key Assumptions into generate_answer_details:

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

#2 Implement consistency of terms usage.

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

#3 The UI shows some numbers in a weird way.
Need to investigate why. Maybe we move out from streamlit idk.


