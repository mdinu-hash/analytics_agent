# test access to db file: import db tables into data frames and select by the column names

import pandas as pd
import langchain, langgraph, langchain_openai, langsmith
import os
from pathlib import Path
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool
import datetime
from typing_extensions import TypedDict, Annotated, Literal, Union
from langgraph.graph.message import add_messages
from typing import Sequence
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage, RemoveMessage
from langchain_core.agents import AgentAction
import operator
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from typing import Iterator
from langchain_core.vectorstores import InMemoryVectorStore
from langchain_openai import OpenAIEmbeddings
from langchain_core.documents import Document
from langchain_core.runnables import RunnableLambda, RunnablePassthrough
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver
import queue
import sqlglot
from sqlglot import parse_one

# Import initialization components
from src.init.initialization import (
    llm, llm_fast, create_config, tracer,
    objects_documentation, sql_dialect, connection_string
)
from src.init.llm_util import create_prompt_template, get_token_usage, calculate_chat_history_tokens, llm_provider

# Import PostgreSQL database utility
from src.init.init_demo_database.demo_database_util import execute_query, create_objects_documentation

# Import business glossary
from src.init.business_glossary import key_terms, synonyms, related_terms, search_terms

# Import database schema
from src.init.database_schema import database_schema, table_relationships

_progress_queue = queue.Queue()  # Global shared progress queue

def set_progress_queue(q):
    global _progress_queue
    _progress_queue = q

def get_progress_queue():
    return _progress_queue

def show_progress(message: str):
    """Push a message to the Streamlit progress queue"""
    _progress_queue.put(message)


vector_store = None

# define the state of the graph, which includes user's question, AI's answer, query that has been created and its result;

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
 search_terms_output: dict  # contains term_substitutions as a key

def extract_msg_content_from_history(messages_log:list):
 ''' from a list of base messages, extract just the content '''
 content = []
 for msg in messages_log:
     content.append(msg.content)
 return "\n".join(content)

class ClearOrAmbiguous(TypedDict):
  ''' conclusion about the analytical intent extraction process '''
  analytical_intent_clearness: Annotated[Literal["Analytical Intent Extracted", "Analytical Intent Ambiguous"],"conclusion about the analytical intent extraction process"] 

class TermSubstitution(TypedDict):
  ''' term substitution made when creating analytical intent '''
  relationship: Annotated[Literal["synonym", "related_term"], "type of relationship between terms"]
  searched_for: Annotated[str, "term from user question"]
  replacement_term: Annotated[str, "term used in analytical intent"]

class AnalyticalIntents(TypedDict):
  ''' list of analytical intents with term substitutions '''
  analytical_intent: Annotated[Union[list[str], None] ,"natural language descriptions to capture the analytical intents"]
  term_substitutions: Annotated[list[TermSubstitution], "list of term substitutions made (empty list if none)"]

class AmbiguityAnalysis(TypedDict):
  ''' analysis of ambiguous question with explanation and alternatives '''
  ambiguity_explanation: Annotated[str, "brief explanation of what makes the question ambiguous"]
  agent_questions: Annotated[list[str], "2-3 alternative analytical intents as questions"]

@tool
def extract_analytical_intent(state:State):
  ''' generates a natural language description to capture the analytical intent and refine the user ask ''' 
  
  sys_prompt_clear_or_ambiguous = """Decide whether the user question is clear or ambigous based on this specific database schema:
  {objects_documentation}.

  Conversation history:
  "{messages_log}".

  User question:
  "{question}".

  *** The question is CLEAR if ***
  - It has a single, obvious analytical approach in terms of underlying source columns, relationships or past conversations.    
    Example: "what is the revenue?" is clear in a database schema that contains just 1 single metric that can answer the question (ex: net_revenue).

  - The column and metric naming in the schema clearly points to one dominant method of interpretation. 
    Example: "what is the top client?" is clear in a database schema that contains just 1 single metric that can answer the question (ex: sales_amount). 

  - You can apply reasonable assumptions. Examples:
    No specific time periods indicated -> assume a recent period -> CLEAR.
    No level of details specified -> use highest aggregation level -> CLEAR.

  - You can deduct the analytical intent from the conversation history.

  - User questions with terms referring to a single related term are CLEAR. See here: {available_term_mappings}.
    Example: "A is related (similar but different) with: B".
  
  *** The question is AMBIGUOUS if ***
  - Different source columns would give different insights.     

  - Different metrics could answer the same question:
    Example: "What is the top client?" is ambigous in a database schema that contains multiple metrics that can answer the question (highest value of sales / highest number of sales). 

  Response format:
  If CLEAR -> "Analytical Intent Extracted".
  If AMBIGUOUS -> "Analytical Intent Ambiguous". 
  """

  sys_prompt_clear = """Refine technically the user ask for a sql developer with access to the following database schema:
  {objects_documentation}.

  Conversation history:
  "{messages_log}".

  Last user prompt:
  "{question}".

Important considerations about creating analytical intents:
    - The analytical intent will be used to create a single sql query.
    - Write it in 1 sentence.
    - Mention just the column names, tables names, grouping levels, aggregation functions (preffered if it doesn't restrict insights) and filters from the database schema.
    - If the user ask is exploratory (ex: "What can you tell me about the dataset?"), create 3-5 analytical intents.
    - If the user ask is non-exploratory, create only one analytical intent.
    - If the user asks for statistical analysis between variables (ex correlation) do not compute the statistical metrics, instead just show a simple side by side or group summary.

  Important considerations about time based analysis:
    - If the source columns are from tables showing evolutions of metrics over time, clearly specify the time range to apply the analysis on:
      Example: If the question does not specify a time range, specify a recent period like last 12 months, last 3 months.
    - Use explicit date filters instead of relative expressions like "last 12 months".
    - Derive actual date ranges from the database schema under "Important considerations about dates available".
    - Group the specified period in time-based groups (monthly, quarterly) and compare first with last group.

  Important considerations about complex, multi-steps analytical intents:
  - An analytical query is multi-step if it requires sequential data gathering and statistical analysis,
    where each search builds upon previous results to examine relationships, correlations, or comparative patterns between variables.
  - Break it down into sequential steps where each represents a distinct analytical intent:
    Template: "Step 1: <analytical intent 1>. Step 2: <analytical intent 2>. Step 3: <analytical intent 3>"

  Important: Track term substitutions
  - Available term mappings (synonyms and related terms):
    {available_term_mappings}

  - ONLY use the term mappings provided above when deciding on substitutions.
  - If you used a synonym or related term from the available term mappings instead of the user's exact terminology, report it in term_substitutions.
  - For each substitution, specify:
    * relationship: "synonym" if the terms mean the same thing, "related_term" if they're similar but different concepts.
    * searched_for: the exact term from the user's question.
    * replacement_term: the term from the database schema you used in the analytical intent.
  - If no substitutions were made, return an empty list for term_substitutions.
    """

  sys_prompt_ambiguous = """
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
  
  # decide if question is clear or ambiguous
  prompt_clear_or_ambiguous = create_prompt_template('system', sys_prompt_clear_or_ambiguous)
  chain_1= prompt_clear_or_ambiguous | llm.with_structured_output(ClearOrAmbiguous)  

  # if question is clear, create analytical intent
  prompt_clear = create_prompt_template('system', sys_prompt_clear)
  chain_2= prompt_clear | llm.with_structured_output(AnalyticalIntents)

  # if question is ambiguous, explain why and create follow-up questions.
  prompt_ambiguous = create_prompt_template('system', sys_prompt_ambiguous)
  chain_3= prompt_ambiguous | llm.with_structured_output(AmbiguityAnalysis)

  # Prepare common input data
  search_terms_output = state['search_terms_output']

  input_data = {
        'objects_documentation': state['objects_documentation'],
        'question': state['current_question'],
        'messages_log': extract_msg_content_from_history(state['messages_log']),
        'available_term_mappings': search_terms_output.get('documentation', 'None')
   }

  # Check for scenario D: multiple related terms exist and related_term_searched_for does not exist in DB
  related_terms_data = search_terms_output.get('related_terms')

  # Check if: related_terms exists, has multiple matches, and the searched_for term doesn't exist in DB
  if related_terms_data and len(related_terms_data.get('matches', [])) > 1:
      searched_for_exists_in_db = False
      # Check if searched_for term exists in key_terms
      searched_for_lower = related_terms_data.get('searched_for', '').lower()
      for term in search_terms_output.get('key_terms', []):
          if term.get('name', '').lower() == searched_for_lower:
              searched_for_exists_in_db = True
              break

      if not searched_for_exists_in_db:
          # Scenario D triggered
          related_term_name = related_terms_data.get('searched_for', '')
          related_term_definition = related_terms_data.get('definition', '')
          related_terms_list = related_terms_data.get('matches', [])

          # Create ambiguity_explanation based on whether the searched term has a definition
          if related_term_definition != '':
              ambiguity_explanation = f"The term {related_term_name} is not available in the tables I have access to, but related terms are available."
          else:
              ambiguity_explanation = f"The term {related_term_name} can mean multiple things."

          # Create agent_questions from related_terms
          agent_questions_list = []
          for rel_term in related_terms_list:
              rel_name = rel_term.get('name', '')
              rel_def = rel_term.get('definition', '')
              if rel_def:
                  agent_questions_list.append(f"{rel_name}: {rel_def}")
              else:
                  agent_questions_list.append(f"{rel_name}")

          # Format as "Which option are you interested in? - option1. - option2..."
          agent_questions_formatted = "Which option are you interested in? " + " ".join([f"- {q}." for q in agent_questions_list])

          # Update state
          tool_name = 'generate_answer'
          state['scenario'] = 'D'
          state['analytical_intent'] = []
          state['generate_answer_details']['ambiguity_explanation'] = ambiguity_explanation
          state['generate_answer_details']['agent_questions'] = [agent_questions_formatted]

          # control flow
          action = AgentAction(tool='extract_analytical_intent', tool_input='', log='tool ran successfully')
          state['intermediate_steps'].append(action)
          state['intermediate_steps'].append(AgentAction(tool=tool_name, tool_input='', log=''))

          return state

  # determine if clear or ambiguous
  result_1 = chain_1.invoke(input_data)

  # Based on result, invoke appropriate chain
  if result_1['analytical_intent_clearness'] == "Analytical Intent Extracted":
        # create analytical intents
        result_2 = chain_2.invoke(input_data)
        # Store term_substitutions in search_terms_output
        state['search_terms_output']['term_substitutions'] = result_2.get('term_substitutions', [])
        # next tool to call
        tool_name = 'create_sql_query_or_queries'
        output = {
            'scenario': 'A',
            'analytical_intent': result_2['analytical_intent'],
            'ambiguity_explanation': '',
            'agent_questions': []
        }
  elif result_1['analytical_intent_clearness'] == "Analytical Intent Ambiguous":
         # create ambiguity analysis (combines both analytical intents and explanation)
         result_3 = chain_3.invoke(input_data)
         # next tool to call
         tool_name = 'generate_answer'
         output = {
            'scenario': 'D',
            'analytical_intent': result_3['agent_questions'],
            'ambiguity_explanation': result_3['ambiguity_explanation'],
            'agent_questions': result_3['agent_questions'] }

  # update the state
  state['scenario'] = output['scenario']
  state['analytical_intent'] = output['analytical_intent']
  state['generate_answer_details']['ambiguity_explanation'] = output['ambiguity_explanation']
  state['generate_answer_details']['agent_questions'] = output['agent_questions']
  
  # control flow
  action = AgentAction(tool='extract_analytical_intent', tool_input='',log='tool ran successfully')
  state['intermediate_steps'].append(action)
  state['intermediate_steps'].append(AgentAction(tool=tool_name, tool_input='',log=''))    
  
  return state

class OutputAsAQuery(TypedDict):
  """ generated sql query or sql queries if there are multiple """
  query: Annotated[list[str],"clean sql query"]

@tool
def create_sql_query_or_queries(state:State):
  """ creates sql query/queries to anwser a question based on documentation of tables and columns available """

  system_prompt = """You are a sql expert and an expert data modeler.

  Your task is to create sql scripts in {sql_dialect} dialect to answer the analytical intent(s). In each sql script, use only these tables and columns you have access to:
  {objects_documentation}.

  Analytical intent(s):
  {analytical_intent}

  Answer just with the resulting sql code(s).

  Important quality requirements for every sql string:
    - Return one sql string for every analytical intent.
    - Return only raw SQL strings in the list.
    - DO NOT include comments (like "-- Query 1"), labels, or explanations.
    - If only one SQL query is needed, just return a list with that one query.
    - GROUP BY expressions must match the non-aggregated SELECT expressions.
    - Ensure that any expression used in ORDER BY also appears in the SELECT clause.
    - If you filter by specific text values, use trim, lowercase and pattern matching with LIKE and wildcard (ex: "where trim(lower(column_name)) LIKE trim(lower('%ValueTofilterBy%'))"). For multiple search terms, use multiple wildcards (ex: "where trim(lower(firm_name)) like '%oak%wealth%'"). 
    - Keep query performance in mind. 
      Example: Avoid CROSS JOIN by using a (scalar) subquery directly in CASE statements.

  Important considerations about multi-steps analytical intents (the ones that contain "Step 1:", "Step 2:" etc):
  Create a sophisticated SQL query using CTEs that mirror the steps:
  - Each "Step X" becomes a corresponding CTE.
  - Name CTEs descriptively based on what each step accomplishes.
  - Build each CTE using results from previous CTEs.
  - Final SELECT provides the complete analysis.   

  Example output (simple, non multi-steps):
    [
      "SELECT COUNT(*) FROM feedback;",
      "SELECT AVG(product_price) FROM products;"
    ]

   Example output (multi-steps):
    [
      "    WITH step1_descriptive_name AS (
        -- Implementation of Step 1 from analytical intent
        SELECT ...
    ),
    step2_descriptive_name AS (
        -- Implementation of Step 2, using step1 results
        SELECT ... FROM step1_descriptive_name ...
    ),
    step3_final_analysis AS (
        -- Implementation of Step 3, final analysis
        SELECT ... FROM step2_descriptive_name ...
    )
    SELECT
        clear_result_columns,
        meaningful_calculations,
        percentage_or_comparison_metrics
    FROM step3_final_analysis
    ORDER BY logical_sort_order;"
    ]  
  """

  prompt = create_prompt_template('system', system_prompt)

  chain = prompt | llm.with_structured_output(OutputAsAQuery)

  result = chain.invoke({'objects_documentation':state['objects_documentation'], 'analytical_intent': state['analytical_intent'],'sql_dialect':state['sql_dialect']})
  show_progress(f"✅ SQL queries created:{len(result['query'])}")
  for q in result['query']:
   state['current_sql_queries'].append( {'query': q,
                                     'explanation': '', ## add it later
                                     'result':'', ## add it later
                                     'insight': '' ## add it later
                                      } )
  
  
  
  # control flow
  action = AgentAction(tool='create_sql_query_or_queries', tool_input='',log='tool ran successfully')
  state['intermediate_steps'].append(action)  
  return state

# since gpt-4o allows a maximum completion limit (output context limit) of 4k tokens, I half it to get maximum context size, so 2k. Assuming the entire context is not just the data,
# I divide this number by 5 and arrive at a limit of 400 tokens for the result of the sql query.

import tiktoken

maximum_nr_tokens_sql_query = 500

# create a function that counts the tokens from a string
def count_tokens(string:str):
 """ returns the number of tokens in a text string """
 encoding = tiktoken.encoding_for_model("gpt-4o")
 num_tokens = len(encoding.encode(string))
 return num_tokens

# create a function that compares the tokens from the sql query result with the maximum token limit, and returns true if the context limit has been exceeded, false otherwise.
def check_if_exceed_maximum_context_limit(sql_query_result):
 """ compares the tokens from the sql query result with the maximum token limit, and returns true if the context limit has been exceeded, false otherwise """
 tokens_sql_query_result = count_tokens(sql_query_result)
 if tokens_sql_query_result > maximum_nr_tokens_sql_query:
  return True
 else:
  return False  

class QueryInsight(TypedDict):
    ''' insight extracted from a sql query result '''
    insight: str

def create_query_insight(sql_query:str, sql_query_result:str):
   ''' creates insight from the result of the sql query '''
   system_prompt = """
   You are an expert data analyst.

   You are provided with the following SQL query:
   {sql_query}.

   Which yielded the following result:
   {sql_query_result}.

   Provide an insight based on the query results:

   Insight: Key findings from the results, stating facts directly without technical terms.
       - Do not mention your subjective assessment over the results.
       - Avoid technical terms like "data","dataset","table","list","provided information","query" etc.
   """

   prompt = create_prompt_template('system', system_prompt)
   chain = prompt | llm_fast.with_structured_output(QueryInsight)
   return chain.invoke({'sql_query':sql_query,
                        'sql_query_result':sql_query_result})


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
    """
    Fetch date ranges for tables used in SQL query from pre-filled database_schema.

    Note: This function now reads from pre-filled date_range fields in database_schema.

    Returns list of date range strings.
    """
    from src.init.database_schema import database_schema

    tables = extract_tables_from_sql(sql_query)

    if not tables:
        return []

    date_ranges = []

    try:
        # Iterate through database_schema to find matching tables
        for table in database_schema:
            table_name = table['table_name']

            # Check if this table is in the SQL query
            if any(table_name.lower() in t.lower() or t.lower() in table_name.lower() for t in tables):
                # Check all columns for pre-filled date_range values
                for column_name, column_info in table['columns'].items():
                    date_range = column_info.get('date_range', '').strip()
                    if date_range:
                        date_ranges.append(f"{table_name}, column {column_name}: {date_range}")

        return date_ranges
    except Exception as e:
        return []


class QueryExplanation(TypedDict):
    explanation: Annotated[list[str], "2-5 concise assumptions/highlights"]


def create_query_explanation(sql_query: str) -> dict:
    """Generate explanation highlights for query assumptions"""

    system_prompt = """You are provided with the following SQL query:
{sql_query}.
Your task is to highlight parts of this query to a non-technical user, including only the highlight types below if they exist.

Guidelines:
- Use only bullet points, max 0-3 bullet points, keep just the most important info.
- Keep every bullet very concise, very few words.
- Don't include filters applied to current records.
- Don't include highlights that are not part of the list below. 

List of highlight types:
  - filters applied. 
    Ex: “excluded inactive affiliates”.
  - Show time range of the source table (min/max dates) if the source table for the query has data over time. 
    Ex: “account snapshot dates between 2021 and 2022”
  - TOP X rows limits the result.
    Ex: "Results limited to top 10 affiliates by assets”  
    """

    prompt = create_prompt_template('system', system_prompt)
    chain = prompt | llm_fast.with_structured_output(QueryExplanation)

    llm_explanation = chain.invoke({
        'sql_query': sql_query
    })

    # Append date ranges
    date_ranges = get_date_ranges_for_tables(sql_query)
    combined_explanation = llm_explanation['explanation'] + date_ranges

    return {'explanation': combined_explanation}


# the function checks if the query output exceeds context window limit and if yes, send the query for refinement

class OutputAsASingleQuery(TypedDict):
  """ generated sql query """
  query: Annotated[str,...,"clean sql query"]


def correct_syntax_sql_query(sql_query: str, error:str, objects_documentation: str, sql_dialect: str):
 """ corrects the syntax of sql query """

 system_prompt = """
  Correct the following sql query which returns an error caused by wrong syntax.

  Sql query to correct: {sql_query}.
  Error details: {error}.

  *** Important considerations for correcting the sql query ***
      - Make sure the query is valid in {sql_dialect} dialect.
      - Use only these tables and columns you have access to: {objects_documentation}.
      - If possible, simplify complex operations (e.g., percentile estimation) using built-in functions compatible with SQLite.
      - Keep query performance in mind.
        Example: Avoid CROSS JOIN by using a (scalar) subquery directly in CASE statements.

  Output the corrected version of the query.
  """

 prompt = create_prompt_template('system', system_prompt)
 chain = prompt | llm.with_structured_output(OutputAsASingleQuery)
 result = chain.invoke({'sql_query':sql_query,'error':error,'objects_documentation':objects_documentation, 'sql_dialect':sql_dialect})
 sql_query = result['query']
 return sql_query

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
            # Get searched_for definition from related_terms
            searched_for_def = ''
            related_terms_data = search_terms_output.get('related_terms')
            if related_terms_data and related_terms_data.get('searched_for', '').lower() == searched_for.lower():
                searched_for_def = related_terms_data.get('definition', '')

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

def execute_sql_query(state:State):
  """ executes the sql query and retrieve the result """

  for query_index, q in enumerate(state['current_sql_queries']):

    if state['current_sql_queries'][query_index]['result'] == '':
     sql_query = q['query']

     # refine the query 3 times if necessary.
     for i in range(3):

       # executes the query and if it throws an error, correct it (max 3x times) then execute it again.
       try:
           results = execute_query(sql_query, connection_string)
           if results is None or len(results) == 0:
               sql_query_result = "No results found."
           else:
               # Convert results to DataFrame for string representation
               sql_query_result_df = pd.DataFrame(results)
               sql_query_result = sql_query_result_df.to_string(index=False, header=False)
       except Exception as e:
           sql_query_result = f"Error: {str(e)}"

       attempt = 0
       while 'Error' in sql_query_result and attempt < 3:
            error = sql_query_result
            sql_query = correct_syntax_sql_query(sql_query,error,objects_documentation,state['sql_dialect'])

            try:
                results = execute_query(sql_query, connection_string)
                if results is None or len(results) == 0:
                    sql_query_result = "No results found."
                else:
                    sql_query_result_df = pd.DataFrame(results)
                    sql_query_result = sql_query_result_df.to_string(index=False, header=False)
            except Exception as e:
                sql_query_result = f"Error: {str(e)}"

            attempt += 1

       # if the sql query does not exceed output context window return its result
       if not check_if_exceed_maximum_context_limit(sql_query_result):
         analysis = create_query_insight(sql_query, sql_query_result)
         explanation = create_query_explanation(sql_query)

         # Update state
         state['current_sql_queries'][query_index]['result'] = sql_query_result
         state['current_sql_queries'][query_index]['insight'] = analysis['insight']
         state['current_sql_queries'][query_index]['query'] = sql_query

         # Append explanation to key_assumptions
         if explanation.get('explanation') and isinstance(explanation['explanation'], list):
             state['generate_answer_details']['key_assumptions'].extend(explanation['explanation'])

         # Add Key Assumptions for term substitutions
         assumptions_output = add_key_assumptions_from_term_substitutions(state['search_terms_output'])
         if assumptions_output.get('key_assumptions'):
             state['generate_answer_details']['key_assumptions'].extend(assumptions_output['key_assumptions'])

         break

       # if the sql query exceeds output context window and there is more room for iterations, refine the query
       else:
        sql_query = refine_sql_query(state['analytical_intent'],sql_query,state['objects_documentation'],state['sql_dialect'])['query']

       # if there is no more room for sql query iterations and the result still exceeds context window, throw a message
    else:
        state['current_sql_queries'][query_index]['result'] = 'Query result too large after 3 refinements.'
        state['generate_answer_details']['key_assumptions'].append("Refinement failed.")
      
  return state

def refine_sql_query(analytical_intent: str, sql_query: str, objects_documentation: str, sql_dialect:str):
 """ refines the sql query so that its output tokens do not exceed the maximum context limit """

 system_prompt = """
  As a sql expert, your task is to optimize a sql query that returns more than 20 rows or exceeds the token limit.

  You are trying to answer the following analytical intent: {analytical_intent}.
  Sql query to optimize: {sql_query}.

  *** Important considerations for creating the sql query ***
  - Make sure the query is valid in {sql_dialect} dialect.
  - Use only these tables and columns you have access to: {objects_documentation}.
  
  *** Optimization Examples ***  
  
  A. Apply aggregation functions instead of returning a list of records.
      Example: - Analytical intent: "number of distinct ids in table where column equals 5"
               - Original sql query: "SELECT DISTINCT id FROM table WHERE column = 5;"
			         - Refined sql query: "SELECT COUNT(DISTINCT id) FROM table WHERE column = 5;"
			
  B. Group the data at a higher granularity.
     Example: If sql query shows data by days, group by months and return last N months.
  
  C. Group the data in buckets.
      Example: - Analytical intent: "Analyze the relationship between products.product_price and products.product_average-rating in the products table to determine if product price influences average rating."
               - Original sql query: "SELECT product_price, product_average_rating FROM products GROUP BY product_price, product_average_rating"
			         - Refined sql query: "SELECT 
                                         CASE 
                                             WHEN product_price < 10 THEN '<$10'
                                             WHEN product_price >= 10 AND product_price < 50 THEN '$10–$50'
                                             ELSE '$50+'
                                             END AS price_bucket,
                                         CASE 
                                             WHEN product_price < 10 THEN 1
                                             WHEN product_price >= 10 AND product_price < 50 THEN 2
                                             ELSE 3
                                             END AS price_bucket_sort,                                             
                                         ROUND(AVG(product_average_rating), 2) AS avg_rating,
                                         COUNT(*) AS product_count
                                   FROM products
                                   GROUP BY price_bucket
                                   ORDER BY price_bucket_sort;"
   
  
  D. Apply filters.
     Examples: - Time-based filters: Show records for the last 3 months. Check the database schema for date range information under "Important considerations about dates available".
              - Filter for a single company. Use values from the database schema. 
  
  E. Show top records.
     Provide a snapshot of data by retrieving maximum 20 rows and 5 columns.
     Example: The Analytical intent is: "average sale per customer", but there are too many customers, so show top N.
                       - Original sql query: "SELECT customer_name, avg(sale) as avg_sale from sales group by customer_name"
			                 - Refined sql query: "SELECT customer_name, avg(sale) as avg_sale from sales group by customer_name ORDER BY avg_sale desc limit 10;"

  *** Optimization Guidelines ***  
      1. Do not eliminate key dimensions that are explicitly part of the analytical intent.  
         For example, if the user asks for time based analysis per customer, do not drop time or customer attributes. 
         Instead, you can use optimization example D (filter date range) or example B (aggregate time at higher level).                   
  """
 
 prompt = create_prompt_template('system', system_prompt)
 chain = prompt | llm.with_structured_output(OutputAsASingleQuery)

 sql_query = chain.invoke({'analytical_intent': analytical_intent,
               'sql_query':sql_query,
               'objects_documentation':objects_documentation,
               'sql_dialect':sql_dialect}
               )
 return sql_query

# Each scenario

scenario_A = {
    'Type': 'A',
    'Prompt': """You are a decision support consultant helping users become more data-driven.
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
""",
    'Invoke_Params': lambda state: {
        'messages_log': state['messages_log'],
        'question': state['current_question'],
        'objects_documentation': state['objects_documentation'],
        'insights': format_sql_query_results_for_prompt(state['current_sql_queries']),
        'agent_questions': state['generate_answer_details'].get('agent_questions', [])
    }
}

scenario_B = {
    'Type': 'B',
    'Prompt': """You are a decision support consultant helping users become more data-driven.
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
  - Ask the user which option they prefer from your suggested next steps.""",
    'Invoke_Params': lambda state: {
        'messages_log': state['messages_log'],
        'question': state['current_question'],
        'objects_documentation': state['objects_documentation'],
        'agent_questions': state['generate_answer_details'].get('agent_questions', [])
    }
}

scenario_C = {
    'Type': 'C',
    'Prompt': """You are a decision support consultant helping users become more data-driven.
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
  - Ask the user which option they prefer from your suggested next steps.""",
    'Invoke_Params': lambda state: {
        'messages_log': state['messages_log'],
        'question': state['current_question'],
        'objects_documentation': state['objects_documentation'],
        'agent_questions': state['generate_answer_details'].get('agent_questions', [])
    }
}

scenario_D = {
    'Type': 'D',
    'Prompt': """You are a decision support consultant helping users become more data-driven.
Your task is to continue the conversation from the last user message by guiding the users to answer their analytical goal.

Here is the conversation history with the user:
{messages_log}.

Latest user message:
{question}.

The last user prompt could be interpreted in multiple ways.
Explain the user this ambiguity reason: {ambiguity_explanation}.
And ask user to specify which of these analysis it wants: {agent_questions}.
Respond in clear, non-technical language.
Be concise.
Keep it simple and conversational.""",
    'Invoke_Params': lambda state: {
        'messages_log': state['messages_log'],
        'question': state['current_question'],
        'objects_documentation': state['objects_documentation'],
        'ambiguity_explanation': state['generate_answer_details'].get('ambiguity_explanation', ''),
        'agent_questions': state['generate_answer_details'].get('agent_questions', [])
    }
}

scenario_prompts = [scenario_A,scenario_B,scenario_C,scenario_D]

def format_sql_query_results_for_prompt (sql_queries : list[dict]) -> str:
    """ based on the current_sql_queries, creates a string like so: Insight 1: ... Raw Result of insight 1: ... Insight 2 ... etc """
    formatted_queries = []
    for query_index,q in enumerate(sql_queries):
        block = f"Insight {query_index+1}:\n{q['insight']}\n\nRaw Result of insight {query_index+1}:\n{q['result']}"
        formatted_queries.append(block)
    return "\n\n".join(formatted_queries)


def format_key_assumptions_for_prompt(key_assumptions: list[str]) -> str:
    """Format key assumptions into single section"""
    if not key_assumptions:
        return ""

    unique_assumptions = list(dict.fromkeys(key_assumptions))
    return "\n\n**Key Assumptions:**\n" + "\n".join([f"• {a}" for a in unique_assumptions])

class AgentQuestions(TypedDict):
  ''' next step suggestions for the user '''
  agent_questions: Annotated[list[str], "max 2 smart next steps for the user to explore further"]

def generate_agent_questions(state: State) -> list[str]:
    """Generate 2 next steps to guide the user. Only runs for scenarios A, B, or C."""
    sys_prompt = """You are a decision support consultant helping users become more data-driven.

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
    """

    prompt = create_prompt_template('system', sys_prompt)
    chain = prompt | llm.with_structured_output(AgentQuestions)
    result = chain.invoke({
        'messages_log': extract_msg_content_from_history(state['messages_log']),
        'question': state['current_question'],
        'objects_documentation': state['objects_documentation']
    })
    return result['agent_questions']


## create a function that generates the agent answer based on sql query result

@tool
def generate_answer(state:State):
  """ generates the AI answer taking into consideration the explanation and the result of the sql query that was executed """

  scenario = state['scenario']

  # Generate agent_questions for scenarios A, B, C
  if scenario in ['A', 'B', 'C']:
      state['generate_answer_details']['agent_questions'] = generate_agent_questions(state)

  # create prompt template based on scenario
  sys_prompt = next(s['Prompt'] for s in scenario_prompts if s['Type'] == scenario)
  prompt = create_prompt_template('system', sys_prompt, messages_log=True)
  llm_answer_chain = prompt | llm

  def create_final_message(llm_response):
      base_content = llm_response.content
      key_assumptions_section = ""
      if state.get('scenario') == 'A':
          key_assumptions_section = format_key_assumptions_for_prompt(
              state['generate_answer_details'].get('key_assumptions', [])
          )
      return {'ai_message': AIMessage(content=base_content + key_assumptions_section,
                                     response_metadata=llm_response.response_metadata)}

  final_answer_chain = llm_answer_chain | RunnableLambda(create_final_message)      

  # invoke parameters based on scenario
  invoke_params = next(s['Invoke_Params'](state) for s in scenario_prompts if s['Type'] == scenario)

  result = final_answer_chain.invoke(invoke_params)
  ai_msg = result['ai_message']

  # Update state (common for all scenarios)
  state['llm_answer'] = ai_msg
  state['messages_log'].append(HumanMessage(state['current_question']))
  state['messages_log'].append(ai_msg) 

  return state

def manage_memory_chat_history(state:State):
    """ Manages the chat history so that it does not become too large in terms of output tokens.
    Specifically, it checks if the chat history is larger than 1000 tokens. If yes, keep just the last 4 pairs of human prompts and AI responses, and summarize the older messages.
    Additionally, check if the logs of sql queries is larger than 20 entries. If yes, delete the older records. """           

    tokens_chat_history = calculate_chat_history_tokens(state['messages_log'])    

    if tokens_chat_history >= 1000 and len(state['messages_log']) > 4:
        message_history_to_summarize = [msg.content for msg in state['messages_log'][:-4]]
        prompt = create_prompt_template('user', 'Distill the below chat messages into a single summary paragraph.The summary paragraph should have maximum 400 tokens.Include as many specific details as you can.Chat messages:{message_history_to_summarize}')
        runnable = prompt | llm_fast # use the cheap model
        chat_history_summary = runnable.invoke({'message_history_to_summarize':message_history_to_summarize})
        last_4_messages = state['messages_log'][-4:]
        state['messages_log'] = [chat_history_summary] +[*last_4_messages]
    else:
        state['messages_log'] = state['messages_log']  
        
    return state

def retrieve_scratchpad(state:State):
 ''' retrieves the number of executions for important tools or functions (from intermediate steps) executed by the agent ''' 
 nr_executions_orchestrator= 0
 nr_executions_extract_analytical_intent = 0
 nr_executions_create_sql_query_or_queries = 0
 
 for index,action in enumerate(state['intermediate_steps']):
      
  if action.tool == 'extract_analytical_intent' and action.log == 'tool ran successfully':
      nr_executions_extract_analytical_intent +=1

  if action.tool == 'create_sql_query_or_queries' and action.log == 'tool ran successfully':
      nr_executions_create_sql_query_or_queries +=1

  if action.tool == 'orchestrator' and action.log == 'tool ran successfully':
     nr_executions_orchestrator +=1    

 output = {'nr_executions_orchestrator':nr_executions_orchestrator,
           'nr_executions_extract_analytical_intent':nr_executions_extract_analytical_intent,
           'nr_executions_create_sql_query_or_queries':nr_executions_create_sql_query_or_queries}   
 return output 
  
def get_next_tool(state:State):
  ''' creates a list of actions taken by the agent from the scratchpad '''  
  scratchpad = retrieve_scratchpad(state)
  nr_executions_extract_analytical_intent = scratchpad['nr_executions_extract_analytical_intent']
  nr_executions_create_sql_query_or_queries = scratchpad['nr_executions_create_sql_query_or_queries']

  if nr_executions_extract_analytical_intent == 0:
    next_tool = 'extract_analytical_intent' 
  elif nr_executions_create_sql_query_or_queries == nr_executions_extract_analytical_intent == 1:
    next_tool = 'generate_answer'

  return next_tool

class ScenarioBC(TypedDict):
  ''' indication of the next step to be performed by the agent '''
  next_step: Annotated[Literal["B", "C","Continue"],"indication of the next step to be performed by the agent"]   

def orchestrator(state:State):
  ''' Function that decides which tools to use '''

  scratchpad = retrieve_scratchpad(state)
  nr_executions_orchestrator = scratchpad['nr_executions_orchestrator']

  # if this is the 1st time when orchestrator is called, check scenarios B or C to decide whether you call directly generate_answer.  
  if nr_executions_orchestrator == 0:
    system_prompt = f"""You are a decision support consultant helping users make data-driven decisions.

    Your task is to decide the next action for this question: {{question}}.

    Conversation history: {{messages_log}}. 
    Current insights: "{{insights}}".
    Database schema: {{objects_documentation}}

    Decision process:  

    Step 1. Check if question is non-analytical or already answered:
       - If question is just pleasantries ("thank you", "hello", "how are you") → "B"
       - If the same question was already answered in conversation history → "B"

    Step 2. Check if requested data exists in schema:
      - If the user asks for data/metrics not available AND no synonyms or related terms exist in the database schema → "C"
    
    Step 3. Otherwise → "Continue".
    """
    prompt = create_prompt_template('system', system_prompt)
    chain = prompt | llm_fast.with_structured_output(ScenarioBC)
    result = chain.invoke({'messages_log':extract_msg_content_from_history(state['messages_log']),
                         'question': state['current_question'], 
                         'insights': format_sql_query_results_for_prompt(state['current_sql_queries']),
                         'objects_documentation':state['objects_documentation']
                         })   
    if result['next_step'] == 'Continue':
      scenario = ''  # empty string
      agent_questions = None
      next_tool_name = get_next_tool(state)
      pass

    # if scenario B, set the scenario in the state and log the generate_answer as next step
    elif result['next_step'] == 'B':
      scenario = result['next_step']
      agent_questions = None
      next_tool_name = 'generate_answer'

    # if scenario C, set the scenario in the state and log the generate_answer as next step
    else:
      scenario = result['next_step']
      agent_questions = None
      next_tool_name = 'generate_answer'

  # if this is not the 1st time when orchestrator runs
  else:

    # go directly to answer because analytical intent has been extracted, queries created and executed
    next_tool = get_next_tool(state)
    if next_tool == 'generate_answer':
       next_tool_name = 'generate_answer'
       scenario = 'A' # can be changed later for the situation when insights are not enough and a subsequent analysis is needed
       agent_questions = None

  # update state
  state['scenario'] = scenario
  state['generate_answer_details']['agent_questions'] = agent_questions    

  # log orchestrator run
  action = AgentAction(tool='orchestrator', tool_input='', log = 'tool ran successfully')
  state['intermediate_steps'].append(action)     

  # log next tool to call
  action = AgentAction( tool=next_tool_name, tool_input='', log = '' )
  state['intermediate_steps'].append(action)  
  return state     

# run the nodes

def run_control_flow(state:State):
    ''' Based on the last tool name stored in intermediate_steps (generated by the orchestrator), it executes the next node that will trigger the control flow '''
    
    # get the next tool to execute by looking in the last tool_name in the intermediate steps
    tool_name = state['intermediate_steps'][-1].tool
    
    # extract_analytical_intent
    if tool_name == 'extract_analytical_intent':
      state = extract_analytical_intent.invoke({'state':state})  

    # creating & executing new queries
    elif tool_name == 'create_sql_query_or_queries':
      state = create_sql_query_or_queries.invoke({'state':state})
      execute_sql_query(state)

    # generate answer & manage chat history.
    elif tool_name == 'generate_answer':  
      state = generate_answer.invoke({'state':state}) 
      manage_memory_chat_history(state)

    return state
  
# assemble graph

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

    # Append documentation if exists
    if search_terms_output.get('documentation'):
        key_terms_text += f"\n{search_terms_output['documentation']}\n"

    # Combine everything
    return base_documentation + key_terms_text

# function to reset the state current queries (to add in the start of graph execution)
def reset_state(state:State):
    state['current_sql_queries'] = []
    state['intermediate_steps'] = []
    state['llm_answer'] = AIMessage(content='')
    state['generate_answer_details'] = {
        'key_assumptions': [],
        'agent_questions': [],
        'ambiguity_explanation': ''
    }
    state['analytical_intent'] = []
    state['scenario'] = ''

    # Call search_terms to get relevant terms for this question
    search_terms_output = search_terms(state['current_question'], key_terms, synonyms, related_terms)
    search_terms_output['term_substitutions'] = []  # Will be populated in extract_analytical_intent
    state['search_terms_output'] = search_terms_output

    # Add relevant key terms to the base objects_documentation
    state['objects_documentation'] = add_key_terms_to_objects_documentation(objects_documentation, search_terms_output)

    state['sql_dialect'] = sql_dialect
    return state

def router(state:State):
    # returns the tool name to use
    return state['intermediate_steps'][-1].tool

graph= StateGraph(State)
graph.add_node("reset_state",reset_state)
graph.add_node("orchestrator",orchestrator)

# here you add the node corresponding to the first tool of each control flow, as the subsequent tools are run by the run_control_flow node
graph.add_node("extract_analytical_intent",run_control_flow)
graph.add_node("create_sql_query_or_queries",run_control_flow)
graph.add_node("generate_answer",run_control_flow)

# starting the agent
graph.add_edge(START,"reset_state")
graph.add_edge("reset_state","orchestrator")
graph.add_conditional_edges(source='orchestrator',path=router)
graph.add_conditional_edges(source='extract_analytical_intent',path=router)

# here you add a link from each the control flow node back to the orchestator - except for the generate_answer node.
graph.add_edge("create_sql_query_or_queries","orchestrator")

# last control flow is generate_answer
graph.add_edge("generate_answer",END)

memory = MemorySaver()
graph = graph.compile(checkpointer=memory)