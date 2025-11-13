# Technical Documentation Detailed

## Nodes

### Orchestrator

- If orchestrator runs first time, checks if scenario B or C, otherwise Continue.
  - if Continue: 
     - set next_tool_name = get_next_tool(state).
  - if scenario B , set next_tool_name = 'generate_answer'.
  - if scenario C:
           -  LLMCall: "Write a sentence suggesting an analysis with the existing schema.". Place it in notes.
		   -  set next_tool_name: generate_answer.
		   -  set Scenario = C.
- If orchestrator run once: 
   - set next_tool = get_next_tool(state).
   - if next_tool = 'generate_answer', set scenario = A and set notes = None.

### extract_analytical_intent

- LLMCall prompt_clear_or_ambiguous:
  - If Output 1: Analytical Intent Extracted
      - LLMCall prompt_clear ("Refine technically the user ask")
	  - set analytical_intent = result of LLMCall.
	  - set Scenario = A
	  - set Notes = None.
	  - set tool_name = create_sql_query_or_queries.
  - If Output 2: Analytical Intent Ambiguous
    - LLMCall prompt_ambiguous ("identifying what makes the question ambiguous & provide max 3 alternatives to choose from.)
	- set analytical_intent = result of LLMCall.
	- LLMCall prompt_notes ("here are different intents: xxx. Explain what makes the question ambiguous and mention the alternatives.")
	- set Notes = result of LLMCall.
	- set tool_name = generate_answer
	- set Scenario = D.


### create_sql_query_or_queries

- LLMCall: "create sql query basedon on intents, db schema, db content".
- set current_sql_queries[query] =  result of LLMCall.
- set current_sql_queries[explanation,result,insight]= ''.
- execute_sql_query: for every sql query:
  - executes it:
        - if execution is without errors, set sql_query_result = result of execution.
		- if execution has errors, 3 attempts to:
		     - correct_syntax_sql_query: LLMCall ("Correct the following sql query which returns an error caused by wrong syntax.")			   
			 - execute new sql.
			 - set sql_query_result 
		- checks if sql_query_result exceeds 500 tokens.
		  - if does not exceed:
		       - set analysis = create_query_analysis(sql_query, sql_query_result)
			   - set ['current_sql_queries']['insight'] = analysis
			   - set ['current_sql_queries']['explanation'] = create_query_explanation(sql_query) which runs LLMCall ("highlight parts of this query")
			   - set ['current_sql_queries']['query']
			   - set ['current_sql_queries']['result']
		  - if exceeds,3 iterations to:
		          - calls to refine_sql_query() which runs an LLMCall ("optimize a sql query that returns > 20 rows or exceeds the token limit.")

### generate_answer

- retrieves scenario.
- based on scenario, retrieves prompt.
- runs the LLMCall (if scenario is A, attaches to the prompt the Key Assumptions)
- set llm_answer to the result of LLMCall.
- manage_memory_chat_history.

## Scenarios

Scenario A: intent clear -> sql generation and execution.
Orchestrator -> extract_analytical_intent -> Checks if intent is clear or ambiguous, decides it's clear -> Orchestrator -> create_sql_query_or_queries -> generate_answer

Scenario B: pleasentries or the agent does not need to generate sql to answer the question (question is answered in chat history).
Orchestrator -> decides its scenario B -> generate_answer

Scenario C: user asks for data/metrics not available in the database schema.
Orchestrator -> decides its scenario C -> generate_answer

Scenario D: analytical intent ambiguous -> needs followup.
Orchestrator 
-> extract_analytical_intent 
-> Checks if intent is clear or ambiguous, decides it's ambigous 
-> set analytical_intent = LLMCall "provide max 3 alternatives" 
-> set Notes = LLMCall "Explain what makes the question ambiguous and mention the alternatives." 
-> generate_answer

## Key Variables

### Notes:
Scenario A: blank.
Scenario B: blank.
Scenario C: Write a sentence suggesting an analysis with the existing schema.
Scenario D: brief explanation of what makes the question ambiguous and mention alternatives.

## functions

get_next_tool(state): - if extract_analytical_intent and create sql query ran once: generate_answer, 
                      - otherwise: extract_analytical_intent.	