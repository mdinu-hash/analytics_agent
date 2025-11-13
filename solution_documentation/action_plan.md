
#1 Create glossaries and db schema file.

Current solution: meta data reference table 
Proposed solution: 2 glossaries in dictionaries (waay simpler and more practical) that you import at run time.

Glossary 1: Database Schema
- What is now metadata reference table.
- Update the documentation, after discussing with Kelsey and Sam, some terms are outdated. 
- Also fix assets = sum of split_assets not total_assets my bad.

Glossary 2: Business Terms
This is the secret sauce:
3 sections:
Key Terms (name,definition,query_instructions).
Synonyms
Related Terms

Include date_range somewhere.

Create a function to validate terms against database schema: the columns you add in query_instructions need to exist in glossary #1.

Create retrieval function(s):
- Based on the user query, retrieve just the relevant terms or tables/columns from the database schema and business terms. 
This is useful because we avoid to bloat the agent context with the entire database schema, and instead we give the agent just the relevant columns and terms to create the query.

- We also need some additional helper functions/variables to be used later in task #3:
Is the term in the database? true/false.
Is the term missing from db but a synonym of the term exists in db? true/false.
Is the term missing from db but a single related term exists in db? true/false.
Is the term missing from db but multiple related term exist in db? true/false.

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

Here is how (use helper functions from task #1)

- enhance scenario A: 
Current scenario A becomes A1.
New scenario: A2: queries for synonym / single related item. 
      new function: check if the term asked by the user is not available in db but there is a synonym or a single related item. this new function needs to be taken into consideration by the orchestrator the first time when deciding between scenario C and A. if the sql_query that was being ran contains the synonym, where you update the state['current_sql_queries'], you also update the key_assumptions from the generate_answer_details, adding the synonym definition.

- enhance scenario D: query ambiguous because term does not exist or is vague/undefined and there are multiple related terms available in db.
Current scenario D becomes D1.
New scenario: scenario D2: in extract_analytical_intent, before running the LLMCall that decides whether the question is clear or ambiguous (prompt_clear_or_ambiguous), check if the term asked by the user is not available in db and if related terms are more than 1 (make a function for it). if yes, don't run prompt_clear_or_ambiguous , set scenario D2 and set Notes = "The term is not available in db, here are the options: <parsed from terms>".

#4 Lack of temporary context.
Problem: Although there is a date_range column, it's not used in creating the analytical intent, rather in the function get_date_ranges_for_tables which shows the date ranges from the tables in the query (Key Assumptions).

Solution: inject date_range info in the prompt input of sys_prompt_clear?

#5 Enhance follow-up questions
Problem: Recommended trend over time analysis when tables did not allow this.

Solution: inject date_range info into the prompt input and promp engineering. Still need to decide if I take that part out of response_guidelines and I create a separate LLMCall or not.
What I like is that I already added in the prompt types of analysis the agent can do, and that are useful for any dataset. I can think of maybe enhancing that prompt with flags saying what's possible and what not for each analysis type, but need to think more about it. First thoughts is that in the long run I need a separate LLMCall for followup questions, because I can inject insights from previous queries (long memory) and the agent can make correlations with other info to suggest smart next moves the user did not think of. But maybe I keep it simple for now idk. 

#6 Adapt UI
Implement the green ugly colours for the background to match the PowerBI apps. The agent will be embedded in the left navigation bar of PBi apps (Nikki suggestion). 

#7 The UI shows some numbers in a weird way.
Need to investigate why. Maybe we move out from streamlit idk.


