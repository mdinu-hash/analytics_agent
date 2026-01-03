1) new node: clarification_check.
which checks whether the question is clear or ambiguous (scenario D or A).
which contains sys_prompt_clear_or_ambiguous, as well as:
checks whether multiple related terms exist and related_term_searched_for does not exist in DB (scenario D triggered) and updates the state accordingly.

if the question is ambiguous, the next node will be node clarification (new one). 
if the question is clear, then the agent will follow scenario A.

2) new node: clarification
which contains sys_prompt_ambiguous.
the next node will be generate_answer.

3) the scenario A
will contain extract_analytical_intent node (which will contain just the sys_prompt_clear), 
create_sql_query_or_queries node, 
execute_sql_query (which will be a new node)

4) the part where key assumptions is added at the end of the answer will be a new node, "add_assumptions", which will be triggered after generate_answer.
