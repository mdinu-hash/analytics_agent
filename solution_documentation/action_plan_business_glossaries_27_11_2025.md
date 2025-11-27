
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

# Task #2: Full Function Implementations

## 1) search_terms function (business_glossary.py)

```python
def search_terms(user_question, key_terms, synonyms, related_terms):
    """
    Searches for key terms, synonyms, and related terms in a user question.
    Uses keyword lookup and fuzzy matching.

    Returns dict with:
        - key_terms: list of dicts - key_terms that exist in database (exists_in_database=True)
        - synonym_searched_for: dict or None - {'name': str, 'exists_in_db': bool, 'definition': str} - the word/phrase from user question that matched a synonym (definition from key_terms if available, empty string otherwise)
        - synonym: dict or None - the key term that the synonym maps to
        - synonym_exists_in_db: bool - True if synonym exists in database
        - related_term_searched_for: dict or None - {'name': str, 'exists_in_db': bool, 'definition': str} - the word/phrase from user question that has related terms (definition from key_terms if available, empty string otherwise)
        - related_term_exists_in_db: bool - True if exactly 1 related term exists in DB
        - related_terms: dict or list or None - single dict if 1 related term, list of dicts if >1
        - related_terms_exists_in_db: bool - True if >1 related terms exist in DB
        - synonyms_related_terms_docu: str - combined documentation of synonyms and related terms
          Format: "{synonym_word} is synonym with {key_term_name}" and/or
                  "{term_from_q} is related (similar but different) with: {rel1}, {rel2}"
          (multi-line if multiple terms from question have related terms, empty string if none)
    """
    user_question_lower = user_question.lower()

    # Create lookup dictionaries for fast access
    key_terms_lookup = {term['name'].lower(): term for term in key_terms}

    # Initialize all return values
    key_terms_found = []
    synonym_searched_for = None  # Will be dict: {'name': str, 'exists_in_db': bool, 'definition': str}
    synonym = None
    synonym_exists_in_db = False
    synonym_docu = None
    related_term_searched_for = None  # Will be dict: {'name': str, 'exists_in_db': bool, 'definition': str}
    related_term_exists_in_db = False
    related_terms_found = None
    related_terms_exists_in_db = False
    related_terms_docu = None

    # Track all related terms matches (can be multiple)
    all_related_matches = []

    # 1. Check for direct key terms (keyword and fuzzy match)
    for term in key_terms:
        term_name = term['name'].lower()

        # Keyword lookup (exact substring match)
        if term_name in user_question_lower:
            if term.get('exists_in_database', False):
                key_terms_found.append(term)
            continue

        # Fuzzy match on the complete phrase
        term_words = term_name.split()
        if len(term_words) > 1:
            # For multi-word terms, try to find the complete phrase with fuzzy matching
            words = user_question_lower.split()
            for i in range(len(words) - len(term_words) + 1):
                phrase = ' '.join(words[i:i+len(term_words)])
                if get_close_matches(term_name, [phrase], n=1, cutoff=0.85):
                    if term.get('exists_in_database', False):
                        key_terms_found.append(term)
                    break
        else:
            # Single word term - fuzzy match directly
            words = user_question_lower.split()
            if get_close_matches(term_name, words, n=1, cutoff=0.85):
                if term.get('exists_in_database', False):
                    key_terms_found.append(term)

    # 2. Check for synonyms
    for syn_word, key_term_ref in synonyms.items():
        syn_word_lower = syn_word.lower()
        found_synonym = False

        # Keyword lookup
        if syn_word_lower in user_question_lower:
            found_synonym = True
        else:
            # Fuzzy match for synonym (complete phrase)
            syn_words = syn_word_lower.split()
            if len(syn_words) > 1:
                # Multi-word synonym
                words = user_question_lower.split()
                for i in range(len(words) - len(syn_words) + 1):
                    phrase = ' '.join(words[i:i+len(syn_words)])
                    if get_close_matches(syn_word_lower, [phrase], n=1, cutoff=0.85):
                        found_synonym = True
                        break
            else:
                # Single word synonym
                words = user_question_lower.split()
                if get_close_matches(syn_word_lower, words, n=1, cutoff=0.85):
                    found_synonym = True

        if found_synonym:
            # Look up the actual key term
            key_term_normalized = key_term_ref.replace('_', ' ').lower()
            if key_term_normalized in key_terms_lookup:
                actual_term = key_terms_lookup[key_term_normalized]

                # Check if synonym exists in database
                if actual_term.get('exists_in_database', False):
                    synonym_exists_in_db = True
                    synonym = actual_term

                    # Check if the synonym word itself exists in DB as a key term and get definition
                    syn_word_exists_in_db = False
                    syn_word_definition = ''
                    if syn_word.lower() in key_terms_lookup:
                        syn_word_exists_in_db = key_terms_lookup[syn_word.lower()].get('exists_in_database', False)
                        syn_word_definition = key_terms_lookup[syn_word.lower()].get('definition', '')

                    synonym_searched_for = {'name': syn_word, 'exists_in_db': syn_word_exists_in_db, 'definition': syn_word_definition}

                    # Add to key_terms_found
                    if actual_term not in key_terms_found:
                        key_terms_found.append(actual_term)

                    # Create synonym_docu: "<term_1> is synonym with <term_2>"
                    # term_1 = word from user question, term_2 = synonym name that exists in DB
                    syn_name = actual_term.get('name', '')
                    synonym_docu = f"{syn_word} is synonym with {syn_name}"

                    break  # Only process first synonym match

    # 3. Check for related terms (process ALL matches, not just first)
    for term_group in related_terms:
        found_term_in_group = None
        found_term_obj = None
        found_term_from_question = None  # Track the actual word/phrase from user question

        for related_term in term_group:
            related_term_lower = related_term.replace('_', ' ').lower()

            # Keyword lookup
            if related_term_lower in user_question_lower:
                found_term_in_group = related_term_lower
                found_term_from_question = related_term  # Use original casing from glossary
                if related_term_lower in key_terms_lookup:
                    found_term_obj = key_terms_lookup[related_term_lower]
                break

            # Fuzzy match (complete phrase)
            related_words = related_term_lower.split()
            if len(related_words) > 1:
                # Multi-word related term
                words = user_question_lower.split()
                for i in range(len(words) - len(related_words) + 1):
                    phrase = ' '.join(words[i:i+len(related_words)])
                    if get_close_matches(related_term_lower, [phrase], n=1, cutoff=0.85):
                        found_term_in_group = related_term_lower
                        found_term_from_question = related_term  # Use original casing from glossary
                        if related_term_lower in key_terms_lookup:
                            found_term_obj = key_terms_lookup[related_term_lower]
                        break
                if found_term_in_group:
                    break
            else:
                # Single word related term
                words = user_question_lower.split()
                if get_close_matches(related_term_lower, words, n=1, cutoff=0.85):
                    found_term_in_group = related_term_lower
                    found_term_from_question = related_term  # Use original casing from glossary
                    if related_term_lower in key_terms_lookup:
                        found_term_obj = key_terms_lookup[related_term_lower]
                    break

        # If we found a term in this group, collect all OTHER related terms that exist in DB
        if found_term_in_group:
            related_terms_in_db = []
            for group_term in term_group:
                group_term_normalized = group_term.replace('_', ' ').lower()
                # Only add if it's NOT the term we found
                if group_term_normalized != found_term_in_group:
                    if group_term_normalized in key_terms_lookup:
                        term_data = key_terms_lookup[group_term_normalized]
                        # Only include if exists in database
                        if term_data.get('exists_in_database', False):
                            related_terms_in_db.append(term_data)
                            # Add to key_terms_found
                            if term_data not in key_terms_found:
                                key_terms_found.append(term_data)

            # Store this match
            if related_terms_in_db:
                all_related_matches.append({
                    'term_from_question': found_term_from_question,
                    'related_terms_in_db': related_terms_in_db
                })

            # Continue to check other term groups (don't break)

    # Now process all related term matches
    if all_related_matches:
        # Combine counts from all matches
        total_related_count = sum(len(match['related_terms_in_db']) for match in all_related_matches)

        if total_related_count == 1:
            # Exactly 1 related term found across all matches
            related_term_exists_in_db = True
            term_from_q = all_related_matches[0]['term_from_question']
            related_terms_found = all_related_matches[0]['related_terms_in_db'][0]  # Single dict

            # Check if the related term word itself exists in DB as a key term and get definition
            term_exists_in_db = False
            term_definition = ''
            if term_from_q.lower() in key_terms_lookup:
                term_exists_in_db = key_terms_lookup[term_from_q.lower()].get('exists_in_database', False)
                term_definition = key_terms_lookup[term_from_q.lower()].get('definition', '')

            related_term_searched_for = {'name': term_from_q, 'exists_in_db': term_exists_in_db, 'definition': term_definition}

            # Create related_terms_docu
            rel_name = all_related_matches[0]['related_terms_in_db'][0].get('name', '')
            related_terms_docu = f"{term_from_q} is related (similar but different) with: {rel_name}"

        elif total_related_count > 1:
            # Multiple related terms found
            related_terms_exists_in_db = True

            # Collect all related terms
            all_related_terms = []
            for match in all_related_matches:
                all_related_terms.extend(match['related_terms_in_db'])
            related_terms_found = all_related_terms  # List of dicts

            # For related_term_searched_for, use first match
            term_from_q = all_related_matches[0]['term_from_question']

            # Check if the related term word itself exists in DB as a key term and get definition
            term_exists_in_db = False
            term_definition = ''
            if term_from_q.lower() in key_terms_lookup:
                term_exists_in_db = key_terms_lookup[term_from_q.lower()].get('exists_in_database', False)
                term_definition = key_terms_lookup[term_from_q.lower()].get('definition', '')

            related_term_searched_for = {'name': term_from_q, 'exists_in_db': term_exists_in_db, 'definition': term_definition}

            # Create related_terms_docu with multiple lines if multiple terms from question
            docu_lines = []
            for match in all_related_matches:
                term_from_q = match['term_from_question']
                rel_names = [t.get('name', '') for t in match['related_terms_in_db']]
                docu_lines.append(f"{term_from_q} is related (similar but different) with: {', '.join(rel_names)}")
            related_terms_docu = '\n'.join(docu_lines)

    # Build synonyms_related_terms_docu by combining synonym_docu and related_terms_docu
    docu_parts = []
    if synonym_docu:
        docu_parts.append(synonym_docu)
    if related_terms_docu:
        docu_parts.append(related_terms_docu)

    synonyms_related_terms_docu = '\n'.join(docu_parts) if docu_parts else ''

    return {
        'key_terms': key_terms_found,
        'synonym_searched_for': synonym_searched_for,
        'synonym': synonym,
        'synonym_exists_in_db': synonym_exists_in_db,
        'related_term_searched_for': related_term_searched_for,
        'related_term_exists_in_db': related_term_exists_in_db,
        'related_terms': related_terms_found,
        'related_terms_exists_in_db': related_terms_exists_in_db,
        'synonyms_related_terms_docu': synonyms_related_terms_docu
    }
```

## 2) add_key_terms_to_objects_documentation function (functions.py)

```python
def add_key_terms_to_objects_documentation(base_documentation: str, search_terms_output: dict) -> str:
    """
    Adds relevant key terms section to the base objects documentation.

    Args:
        base_documentation: Base documentation string (tables, relationships, date ranges)
        search_terms_output: Output from search_terms() function containing relevant terms

    Returns:
        Complete documentation string with key terms section added
    """
    # Build custom Key Terms section with only relevant terms
    key_terms_text = "\nKey Terms:\n"
    for term in search_terms_output['key_terms']:
        term_name = term.get('name', '')
        term_definition = term.get('definition', '')
        query_instructions = term.get('query_instructions', '')

        if term_definition:
            key_terms_text += f"  - {term_name}: {term_definition}\n"
        else:
            key_terms_text += f"  - {term_name}\n"

        if query_instructions:
            key_terms_text += f"    {query_instructions}\n"

    # Append synonyms_related_terms_docu if exists
    if search_terms_output.get('synonyms_related_terms_docu'):
        key_terms_text += f"\n{search_terms_output['synonyms_related_terms_docu']}\n"

    # Combine everything
    return base_documentation + key_terms_text
```

## 3) extract_analytical_intent function (agent.py)
Copy the entire function from agent.py (lines 102-320). The prompts have been iteratively refined during testing.

## 4) execute_sql_query function (agent.py)

**Change**: Add the following code after appending explanation to key_assumptions (after line 690):

```python
# Add Key Assumptions for term substitutions
assumptions_output = add_key_assumptions_from_term_substitutions(state['search_terms_output'])
if assumptions_output.get('key_assumptions'):
    state['generate_answer_details']['key_assumptions'].extend(assumptions_output['key_assumptions'])
```

**Complete code block** (lines 688-695):
```python
# Append explanation to key_assumptions
if explanation.get('explanation') and isinstance(explanation['explanation'], list):
    state['generate_answer_details']['key_assumptions'].extend(explanation['explanation'])

# Add Key Assumptions for term substitutions
assumptions_output = add_key_assumptions_from_term_substitutions(state['search_terms_output'])
if assumptions_output.get('key_assumptions'):
    state['generate_answer_details']['key_assumptions'].extend(assumptions_output['key_assumptions'])
```

This is the ONLY change to execute_sql_query for Task #2.

## 5) add_key_assumptions_from_term_substitutions function (agent.py)

```python
def add_key_assumptions_from_term_substitutions(search_terms_output: dict) -> dict:
    """
    Creates Key Assumptions based on term substitutions reported by LLM.

    Args:
        search_terms_output: Output from search_terms function (contains term_substitutions)

    Returns:
        dict with 'key_assumptions': list of assumption strings
    """
    key_assumptions = []
    term_substitutions = search_terms_output.get('term_substitutions', [])

    for substitution in term_substitutions:
        relationship = substitution.get('relationship')
        searched_for = substitution.get('searched_for', '')
        replacement_term = substitution.get('replacement_term', '')

        # Look up definitions for the replacement term from search_terms_output
        replacement_def = ''
        for term in search_terms_output.get('key_terms', []):
            if term.get('name', '').lower() == replacement_term.lower():
                replacement_def = term.get('definition', '')
                break

        if relationship == 'synonym':
            # "{replacement_term} is {replacement_def}"
            if replacement_def:
                key_assumptions.append(f"{replacement_term} is {replacement_def}")

        elif relationship == 'related_term':
            # Get searched_for definition from related_term_searched_for
            searched_for_def = ''
            related_term_searched_for = search_terms_output.get('related_term_searched_for')
            if related_term_searched_for and related_term_searched_for.get('name', '').lower() == searched_for.lower():
                searched_for_def = related_term_searched_for.get('definition', '')

            if searched_for_def:
                # "{searched_for} ({searched_for_def}) does not exist in the tables I have access to. I returned the data for {replacement_term} ({replacement_def})"
                if replacement_def:
                    key_assumptions.append(
                        f"{searched_for} ({searched_for_def}) does not exist in the tables I have access to. I returned the data for {replacement_term} ({replacement_def})"
                    )
                else:
                    key_assumptions.append(
                        f"{searched_for} ({searched_for_def}) does not exist in the tables I have access to. I returned the data for {replacement_term}"
                    )
            else:
                # "I returned the data for {replacement_term} ({replacement_def})"
                if replacement_def:
                    key_assumptions.append(f"I returned the data for {replacement_term} ({replacement_def})")
                else:
                    key_assumptions.append(f"I returned the data for {replacement_term}")

    return {'key_assumptions': key_assumptions}
```

## Implementation Changes

### State Changes (agent.py)
```python
class State(TypedDict):
 objects_documentation: str
 sql_dialect: str
 messages_log: Annotated[Sequence[BaseMessage], add_messages]
 intermediate_steps: list[AgentAction]
 analytical_intent: list[str]
 current_question: str
 current_sql_queries: list[dict]
 generate_answer_details: dict
 llm_answer: BaseMessage
 scenario: str
 search_terms_output: dict  # NEW - contains term_substitutions as a key
```

### TypedDict Additions (agent.py)
```python
class TermSubstitution(TypedDict):
  ''' term substitution made when creating analytical intent '''
  relationship: Annotated[Literal["synonym", "related_term"], "type of relationship between terms"]
  searched_for: Annotated[str, "term from user question"]
  replacement_term: Annotated[str, "term used in analytical intent"]

class AnalyticalIntents(TypedDict):
  ''' list of analytical intents with term substitutions '''
  analytical_intent: Annotated[Union[list[str], None] ,"natural language descriptions to capture the analytical intents"]
  term_substitutions: Annotated[list[TermSubstitution], "list of term substitutions made (empty list if none)"]
```

### Import Changes (agent.py)
Add to imports:
```python
from src.init.init_demo_database.demo_database_util import execute_query, create_objects_documentation
from src.init.business_glossary import key_terms, synonyms, related_terms, search_terms
from src.init.database_schema import database_schema, table_relationships
```

### reset_state Changes (agent.py)
```python
def reset_state(state:State):
    # ... existing resets ...

    # Call search_terms to get relevant terms for this question
    search_terms_output = search_terms(state['current_question'], key_terms, synonyms, related_terms)
    search_terms_output['term_substitutions'] = []  # Will be populated in extract_analytical_intent
    state['search_terms_output'] = search_terms_output

    # Add relevant key terms to the base objects_documentation (imported from initialization)
    state['objects_documentation'] = add_key_terms_to_objects_documentation(objects_documentation, search_terms_output)

    state['sql_dialect'] = sql_dialect
    return state
```

### create_objects_documentation Changes (demo_database_util.py)
Remove the entire "Key Terms:" section (lines 186-187). Add comment:
```python
# Key terms section removed - will be added dynamically in reset_state based on search_terms
```

### Orchestrator Prompt Change (agent.py)
Change:
```
If the user asks for data/metrics not available in the database schema → 'C'
```

To:
```
If the user asks for data/metrics not available AND no synonyms or related terms exist in the database schema → 'C'
```

