# test access to db file: import db tables into data frames and select by the column names

import pandas as pd
import sqlite3
from sqlalchemy import create_engine, inspect
import uuid
import langchain, langgraph, langchain_openai, langsmith
import os
from pathlib import Path
from dotenv import load_dotenv
from langchain_core.runnables import RunnableConfig
from langchain.callbacks.tracers.langchain import LangChainTracer
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI 
import datetime
from typing_extensions import TypedDict, Annotated, Literal, Union
from langgraph.graph.message import add_messages
from typing import Sequence
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage, RemoveMessage
from langchain_core.agents import AgentAction
import operator
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_community.tools import QuerySQLDataBaseTool
from langchain_community.utilities import SQLDatabase
from typing import Iterator
from langchain_core.vectorstores import InMemoryVectorStore
from langchain_openai import OpenAIEmbeddings
from langchain_core.documents import Document
from langchain_core.runnables import RunnableLambda, RunnablePassthrough
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver
import queue

# LAZY LOADING: Database connection
_engine = None
_db = None

_progress_queue = queue.Queue()  # Global shared progress queue

def set_progress_queue(q):
    global _progress_queue
    _progress_queue = q

def get_progress_queue():
    return _progress_queue

def show_progress(message: str):
    """Push a message to the Streamlit progress queue"""
    _progress_queue.put(message)

def get_database_connection():
    """Get database connection, creating it if it doesn't exist"""
    global _engine, _db
    if _engine is None:
        # Check multiple possible database locations
        current_dir = Path(__file__).parent
        db_paths = [
            'feedbacks_db.db',  # Current working directory
            str(current_dir / 'feedbacks_db.db'),  # Same directory as this script
            str(current_dir.parent / 'feedbacks_db.db'),  # Parent directory
            str(current_dir.parent.parent / 'feedbacks_db.db'),  # Root directory
        ]
        
        db_found = False
        for db_path in db_paths:
            if os.path.exists(db_path):
                _engine = create_engine(f'sqlite:///{db_path}')
                _db = SQLDatabase(_engine)
                db_found = True
                break
        
        if not db_found:
            raise FileNotFoundError("Database file 'feedbacks_db.db' not found in any expected location. Make sure it's downloaded first.")
    
    return _engine, _db

# Load environment variables from root directory
current_dir = Path(__file__).parent
root_env_path = current_dir.parent.parent / '.env'
load_dotenv(root_env_path, override=True)
openai_api_key = os.getenv('OPENAI_API_KEY')
LANGSMITH_API_KEY = os.getenv('LANGSMITH_API_KEY')
os.environ['OPENAI_API_KEY'] = openai_api_key
os.environ['LANGSMITH_API_KEY'] = LANGSMITH_API_KEY
os.environ['LANGSMITH_TRACING'] = "true"
os.environ['LANGSMITH_ENDPOINT'] = "https://api.smith.langchain.com"
langsmith_project_name = "db_agent_v1"
os.environ['LANGSMITH_PROJECT'] = langsmith_project_name

# Set up LangSmith tracer manually
tracer = LangChainTracer(project_name=langsmith_project_name)

llm = ChatOpenAI(model='gpt-4.1',temperature=0) # Smart & expensive
llm_fast = ChatOpenAI(model='gpt-4o',temperature=0) # Faster

def create_config(run_name: str, is_new_thread_id: bool = False, thread_id: str = None):
    """
    Create a config dictionary for LCEL runnables.
    Includes LangSmith run tracing and optional thread_id management.

    Args:
        run_name (str): Descriptive run name shown in LangSmith.
        is_new_thread_id (bool): Whether to generate a new thread_id.
        thread_id (str): Optionally provide an existing thread_id to reuse.

    Returns:
        dict: Config dictionary with callbacks, run_name, and thread_id.

    Use it like so (example): 
        config, thread_id = create_config('create_sql_query_or_queries', True) (start a new thread)
        config, _ = create_config('generate_answer', False, thread_id) (re-use same thread)
    """

    time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    full_run_name = f"{run_name} {time_now}"
    if is_new_thread_id or not thread_id:
        thread_id = str(uuid.uuid4())

    config={'callbacks': [tracer],
            'run_name': full_run_name,
            'configurable' : { 'thread_id':thread_id }
            }

    return config,thread_id

vector_store = None

objects_documentation = '''Table company: List of public companies. Granularity is company-name. Column (prefixed with table name):
     company.company-name: the name of the public company.
     company.annual_revenue_usd: revenue in last 12 months ($).

     Table feedback: Feedbacks given by clients to products. Granularity is feedback. Key is feedback_id. Columns (prefixed with table name):
     feedback.feedback_id: id of the feedback.
     feedback.feedback_date: date of feedback.
     feedback.product_id: id of the product the feedback was given for.
     feedback.product_company_name: company owning the product.
     feedback.feedback_text: the text of the feedback.
     feedback.feedback_rating: rating of the feedback from 1 to 5, 5 being the highest score.

     Table products: Shows product metadata. Granularity is product. Key is product_id. Columns (prefixed with table name):
     products.product_id: id of the product.
     products.product_name: name of the product.
     products.product_brand: the brand under which the product was presented.
     products.product_manufacturer: product manufacturer.
     products.product_company_name: company owning the product.
     products.product_price: price of the product at crawling time.
     products.product_average_rating: average ratings across all feedbacks for the product, at crawling time.

     Table company -> column company_name relates to table feedback -> column product_company_name
     Table products -> column product_company_name relates to table feedback -> column product_company-name
     Table feedback -> column product_id relates to table products -> column product_id'''

database_content = '''Feedback dates between 18 November 2002 and 12 september 2023. 
These feedbacks are given to amazon website by purchasers of various products. 
There are 12 companies selling these products, some of them being Apple, Samsung, Sony, Nike, Adidas, Microsoft or Verizon. 
The feedback ratings range from 1 the lowest to 5 the highest.
Other main data points are product price, product name, or company last annual revenue. 
Less important attributes of the dataset are product brands or manufacturers, I say less important because there are many facturers or brands to give as examples. 

Feedback table has 413k rows. Products table has 8145 rows.
'''

sql_dialect = 'SQLite'

# define the state of the graph, which includes user's question, AI's answer, query that has been created and its result;

class State(TypedDict):
 objects_documentation: str
 database_content: str
 sql_dialect: str
 messages_log: Sequence[BaseMessage]
 intermediate_steps: list[AgentAction]
 analytical_intent: list[str]
 current_question: str
 current_sql_queries: list[dict]
 generate_answer_details: dict
 llm_answer: BaseMessage

def extract_msg_content_from_history(messages_log:list):
 ''' from a list of base messages, extract just the content '''
 content = []
 for msg in messages_log:
     content.append(msg.content)
 return "\n".join(content)

class ClearOrAmbiguous(TypedDict):
  ''' conclusion about the analytical intent extraction process '''
  analytical_intent_clearness: Annotated[Literal["Analytical Intent Extracted", "Analytical Intent Ambiguous"],"conclusion about the analytical intent extraction process"] 

class AnalyticalIntents(TypedDict):
  ''' list of analytical intents '''
  analytical_intent: Annotated[Union[list[str], None] ,"natural language descriptions to capture the analytical intents"]                                      

@tool
def extract_analytical_intent(state:State):
  ''' generates a natural language description to capture the analytical intent and refine the user ask ''' 
  
  sys_prompt_clear_or_ambiguous = """Decide whether the user question is clear or ambigous based on this specific database schema:
  {objects_documentation}.

  Conversation history:
  "{messages_log}".

  User question:
  "{question}".

  *** The question is clear if ***
  - It has a single, obvious analytical approach in terms of grouping, filtering, aggregations using available columns and relationships or past conversations.

  - The column and metric naming in the schema clearly points to one dominant method of interpretation. 
    Example: "what is the top client?" is clear in a database schema that contains just 1 single metric that can answer the question (ex: sales_amount). 
  
  - The question is exploratory or open-ended.
    Example: "What can you tell me about the dataset?".

  - It refers to the evolution of metrics over time (ex last 12 months sales).  

  - You can deduct the analytical intent from the conversation history.
  
  *** The question is ambigous if ***
  - The question could be answered from different analytical intents that use different metrics, grouping, filtering or aggregations.
    Example: Use pre-aggregated metrics vs metrics computed from aggregations across detailed tables.

  - It can be answered by different metrics or metric definitions.
    Example: "What is the top client?" is ambigous in a database schema that contains multiple metrics that can answer the question (highest value of sales / highest number of sales). 

  Response format:
  If clear -> "Analytical Intent Extracted".
  If ambigous -> "Analytical Intent Ambiguous". 
  """

  sys_prompt_clear = """Refine technically the user ask for a sql developer with access to the following database schema:
  {objects_documentation}.

  Summary of database content:
  {database_content}.

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
    - Use explicit date filters instead of relative expressions like “last 12 months”. Derive actual date ranges from the feedback date ranges described in database_content. 
    - Group the specified period in time-based groups (monthly, quarterly) and compare first with last group.

  Important considerations about complex, multi-steps analytical intents:  
  - An analytical query is multi-step if it requires sequential data gathering and statistical analysis, 
    where each search builds upon previous results to examine relationships, correlations, or comparative patterns between variables.  
  - Break it down into sequential steps where each represents a distinct analytical intent:
    Template: "Step 1: <analytical intent 1>. Step 2: <analytical intent 2>. Step 3: <analytical intent 3>"  
    """

  sys_prompt_ambiguous = """
  Conversation history:
  "{messages_log}".

  Last user prompt:
  "{question}". 

  The last user question is ambiguous from the analytical point of view, because it can be answered using different analytical intents that can be interpreted in multiple ways leading to different results.
  
  That is, there are different sql queries with different metadata (object names/filters/aggregations) that can answer the question.

  Your task is to create all analytical intents that can possibly answer the user question using the following database schema:  
  {objects_documentation}.             

  Important considerations about creating analytical intents:
      - Each analytical intent is for creating one single sql query.
      - Write each analytical intent using 1 sentence.
      - Mention specific column names, tables names as well as aggregation functions (preffered if it doesn't restrict insights) and filters from the database schema.  
      - Mention only the useful info for creating sql queries.   
      - Do not include redundant intents. 

  Create one analytical intent for every possible pattern from the checklist that can answer the user quesion:  

  ** Pattern Checklist **
      1. filter on same table.
        Example: select product_id from product table where avg_sales = 5.

      2. Retrieve records from table A based on filter criteria from table B (assuming tables A and B are related).
        Example: count of product_id from product table where unit_sale from sales table = 12.

      3. filter records from table A based on calculated aggregations (AVG, SUM, COUNT) from table B (assuming tables A and B are related).
        Example: count products from products table where AVG(amount) from sales table grouped by product > 100.     
  """
  sys_prompt_notes = """
  Conversation history:
  "{messages_log}".

  Last user prompt:
  "{question}". 
  
  The last user question is ambiguous from the analytical point of view, because it can be answered using different analytical intents that can be interpreted in multiple ways leading to different results.
  That is, there are different sql queries with different metadata (object names/filters/aggregations) that can answer the question.
  The sql queries can only pull data from this database schema:
  {objects_documentation}.

  The different analytical intents that make the question ambiguous are the following:
  {analytical_intents}.         
  
  Your task is to create an explanation of what makes the question unclear and show the alternatives.
  Just acknowledge why is ambiguous and mention the alternatives, nothing more.
  Be short, concise, explain in simple, non-technical language.
  """  

  prompt_clear_or_ambiguous = ChatPromptTemplate.from_messages([('system', sys_prompt_clear_or_ambiguous)])
  chain_1= prompt_clear_or_ambiguous | llm.with_structured_output(ClearOrAmbiguous)  

  prompt_clear = ChatPromptTemplate.from_messages([('system', sys_prompt_clear)])
  chain_2= prompt_clear | llm.with_structured_output(AnalyticalIntents)

  prompt_ambiguous = ChatPromptTemplate.from_messages([('system', sys_prompt_ambiguous)])
  chain_3= prompt_ambiguous | llm.with_structured_output(AnalyticalIntents)

  prompt_notes = ChatPromptTemplate.from_messages([('system', sys_prompt_notes)])
  chain_4= prompt_notes | llm_fast

  # Prepare common input data
  input_data = {
        'objects_documentation': state['objects_documentation'], 
        'question': state['current_question'], 
        'messages_log': extract_msg_content_from_history(state['messages_log'])
   }

  # determine if clear or ambiguous
  result_1 = chain_1.invoke(input_data)

  # Based on result, invoke appropriate chain
  if result_1['analytical_intent_clearness'] == "Analytical Intent Extracted":
        # create analytical intents
        input_data.update({'database_content':state['database_content']})        
        result_2 = chain_2.invoke(input_data)
        # next tool to call 
        tool_name = 'create_sql_query_or_queries' 
        output = {
            'scenario': 'A',
            'analytical_intent': result_2['analytical_intent'],
            'notes': None
        }
  elif result_1['analytical_intent_clearness'] == "Analytical Intent Ambiguous":
         # create analytical intents
         result_3 = chain_3.invoke(input_data)
         input_data.update({'analytical_intents':result_3['analytical_intent']})
         result_4 = chain_4.invoke(input_data)
         # next tool to call 
         tool_name = 'generate_answer'
         output = {
            'scenario': 'D', 
            'analytical_intent': result_3['analytical_intent'],
            'notes': result_4.content }

  # update the state 
  state['generate_answer_details'].update({'scenario':output['scenario'],
                                           'notes':output['notes']})
  state['analytical_intent'] = output['analytical_intent']
  
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
  {objects_documentation}

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
    - If you filter by specific text values, use trim and lowercase (ex: "where trim(lower(column_name)) = trim(lower("ValueTofilterBy")) "). 
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

  prompt = ChatPromptTemplate.from_messages([('system', system_prompt)])

  chain = prompt | llm.with_structured_output(OutputAsAQuery)

  result = chain.invoke({'objects_documentation':state['objects_documentation'], 'analytical_intent': state['analytical_intent'],'sql_dialect':state['sql_dialect']})
  show_progress(f"✅ SQL queries created:{len(result['query'])}")
  for q in result['query']:
   state['current_sql_queries'].append( {'query': q,
                                     'explanation': '', ## add it later
                                     'result':'', ## add it later
                                     'insight': '', ## add it later
                                     'metadata':'' ## add it later
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

class QueryAnalysis(TypedDict):
    ''' complete analysis of a sql query, including its explanation, limitation and insight '''
    explanation: str
    limitation: str
    insight: str

def create_query_analysis(sql_query:str, sql_query_result:str):
   ''' creates: explanation - a concise explanation of what the sql query does.
                limitation - a concise explanation of the sql query by pointing out its limitations.
                insight - insight from the result of the sql query.
   '''
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

   prompt = ChatPromptTemplate.from_messages(('system',system_prompt))
   chain = prompt | llm_fast.with_structured_output(QueryAnalysis)
   return chain.invoke({'sql_query':sql_query,
                        'sql_query_result':sql_query_result})   

import sqlglot
from sqlglot import parse_one, exp

def extract_metadata_from_sql_query(sql_query):
   # returns a dictionary with parsed names of tables and columns used in filters, aggregations and groupings 
   
 ast = parse_one(sql_query)

 sql_query_metadata = {
    "tables": [],
    "filters": [],
    "aggregations": [],
    "groupings": []
 }

 # extract tables
 table_generator = ast.find_all(sqlglot.expressions.Table)
 for items in table_generator:
    sql_query_metadata['tables'].append(items.sql())
 # remove dups
 sql_query_metadata['tables'] = list(dict.fromkeys(sql_query_metadata['tables']))

 # extract filters
 where_conditions = ast.find_all(sqlglot.expressions.Where)
 for item in where_conditions:
  sql_query_metadata['filters'].append(item.this.sql())
  # remove dups
 sql_query_metadata['filters'] = list(dict.fromkeys(sql_query_metadata['filters']))

 # extract aggregate functions
 funcs = ast.find_all(sqlglot.expressions.AggFunc)
 for item in funcs:
  sql_query_metadata['aggregations'].append(item.sql())
 # remove dups
 sql_query_metadata['aggregations'] = list(dict.fromkeys(sql_query_metadata['aggregations']))

 # extract groupings
 groupings = ast.find_all(sqlglot.expressions.Group)
 for item in groupings:
  groupings_flattened = item.flatten()
  for item in groupings_flattened:
    sql_query_metadata['groupings'].append(item.sql())
 # remove dups
 sql_query_metadata['groupings'] = list(dict.fromkeys(sql_query_metadata['groupings']))

 return {'tables':sql_query_metadata.get('tables'),
         'filters':sql_query_metadata.get('filters'),
         'aggregations':sql_query_metadata.get('aggregations'),
         'groupings':sql_query_metadata.get('groupings'),
          }

def format_sql_metadata_explanation(tables:list=None, filters:list=None, aggregations:list=None, groupings:list=None,header :str='') -> str:
    # creates a string explanation of the filters, tables, aggregations and groupings used by the query
    explanation = header

    if tables:
        explanation += "\n\n🧊 Tables: • " + " • ".join(tables)
    if filters:
        explanation += "\n\n🔍 Filters applied: • " + " • ".join(filters)
    if aggregations:
        explanation += "\n\n🧮 Aggregations: • " + " • ".join(aggregations)
    if groupings:
        explanation += "\n\n📦 Groupings: • " + " • ".join(groupings)

    return explanation.strip()

def create_query_metadata(sql_query: str):
 """ creates an explanation for one single query """

 metadata = extract_metadata_from_sql_query(sql_query)
 return format_sql_metadata_explanation(metadata['tables'],metadata['filters'],metadata['aggregations'],metadata['groupings'])


def create_queries_metadata(sql_queries: list[dict]):
 """ creates an explanation for multiple queries: used in the generate_answer tool """

 all_tables = []
 all_filters = []

 for q in sql_queries: 

  metadata = extract_metadata_from_sql_query(q['query'])
  all_tables.extend(metadata["tables"])
  all_filters.extend(metadata["filters"])

  # include the default min/max feedback filters if feedback table has been used and was not filtered at all
  if all_filters:    
     output = format_sql_metadata_explanation(filters = all_filters,header='')
  # if no filters were applied, don't include other metadata for the sake of keeping the message simple
  else:
     output = ''   

 return output

# the function checks if the query output exceeds context window limit and if yes, send the query for refinement

class OutputAsASingleQuery(TypedDict):
  """ generated sql query """
  query: Annotated[str,...,"clean sql query"]


def correct_syntax_sql_query(sql_query: str, error:str, objects_documentation: str, database_content: str, sql_dialect: str):
 """ corrects the syntax of sql query """

 system_prompt = """
  Correct the following sql query which returns an error caused by wrong syntax.  

  Sql query to correct: {sql_query}.
  Error details: {error}.

  *** Important considerations for correcting the sql query ***
      - Make sure the query is valid in {sql_dialect} dialect.
      - Use only these tables and columns you have access to: {objects_documentation}.
      - Summary of database content: {database_content}.
      - If possible, simplify complex operations (e.g., percentile estimation) using built-in functions compatible with SQLite.
      - Keep query performance in mind. 
        Example: Avoid CROSS JOIN by using a (scalar) subquery directly in CASE statements.

  Output the corrected version of the query.
  """
 
 prompt = ChatPromptTemplate.from_messages(('system',system_prompt))
 chain = prompt | llm.with_structured_output(OutputAsASingleQuery)
 result = chain.invoke({'sql_query':sql_query,'error':error,'objects_documentation':objects_documentation,'database_content':database_content, 'sql_dialect':sql_dialect})
 sql_query = result['query']
 return sql_query

def execute_sql_query(state:State):
  """ executes the sql query and retrieve the result """
  
  show_progress("⚙️ Analysing results...")
  _, db = get_database_connection()
  for query_index, q in enumerate(state['current_sql_queries']):
     
    if state['current_sql_queries'][query_index]['result'] == '':    
     sql_query = q['query'] 
    
     # refine the query 3 times if necessary.
     for i in range(3):

       # executes the query and if it throws an error, correct it (max 3x times) then execute it again.
       sql_query_result = QuerySQLDataBaseTool(db=db).invoke(sql_query)
       attempt = 0
       while 'Error' in sql_query_result and attempt < 3:   
            error = sql_query_result
            sql_query = correct_syntax_sql_query(sql_query,error,objects_documentation,database_content,state['sql_dialect'])
            sql_query_result = QuerySQLDataBaseTool(db=db).invoke(sql_query)
            attempt += 1

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

       # if the sql query exceeds output context window and there is more room for iterations, refine the query
       else:
        show_progress(f"🔧 Refining query {query_index+1}/{len(state['current_sql_queries'])} as its output its too large...")
        sql_query = refine_sql_query(state['analytical_intent'],sql_query,state['objects_documentation'],state['database_content'],state['sql_dialect'])['query']

       # if there is no more room for sql query iterations and the result still exceeds context window, throw a message
    else:
        state['current_sql_queries'][query_index]['result'] = 'Query result too large after 3 refinements.'
        state['current_sql_queries'][query_index]['explanation'] = "Refinement failed."
      
  return state

def refine_sql_query(analytical_intent: str, sql_query: str, objects_documentation: str, database_content: str, sql_dialect:str):
 """ refines the sql query so that its output tokens do not exceed the maximum context limit """

 system_prompt = """
  As a sql expert, your task is to optimize a sql query that returns more than 20 rows or exceeds the token limit.
  
  You are trying to answer the following analytical intent: {analytical_intent}.
  Sql query to optimize: {sql_query}.

  *** Important considerations for creating the sql query ***
  - Make sure the query is valid in {sql_dialect} dialect.
  - Use only these tables and columns you have access to: {objects_documentation}.
  - Summary of database content: {database_content}.
  
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
     Examples: - Time-based filters: Show records for the last 3 months. Use database content to identify the temporal context for this conversation.
              -  filter for a single company. Use database content to identify specific values. 
  
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
 
 prompt = ChatPromptTemplate.from_messages(('system',system_prompt))
 chain = prompt | llm.with_structured_output(OutputAsASingleQuery)

 sql_query = chain.invoke({'analytical_intent': analytical_intent,
               'sql_query':sql_query,
               'objects_documentation':objects_documentation,
               'database_content':database_content,
               'sql_dialect':sql_dialect}
               )
 return sql_query

 # response guidelines to be added at the end of every prompt
response_guidelines = '''
  Response guidelines:
  - Respond in clear, non-technical language. 
  - Be concise. 

  Use these methods at the right time, optionally and not too much, keep it simple and conversational:

  If the question is smart, reinforce the user’s question to build confidence. 
    Example: “Great instinct to ask that - it’s how data-savvy pros think!”

  If the context allows, suggest max 2 next steps to explore further. 
  Suggest next steps that can only be achieved with the database schema you have access to:
  {objects_documentation}

  Summary of database content:
  {database_content}.
  
  Example of next steps:
  - Trends over time:
    Example: "Want to see how this changed over time?".

  - Drill-down suggestions:
    Example: “Would you like to explore this by brand or price tier?”

  - Top contributors to a trend:
    Example: “Want to see the top 5 products that drove this increase in satisfaction?”

  - Explore a possible cause:
    Example: “Curious if pricing could explain the drop? I can help with that.”

  - Explore the data at higher granularity levels if the user analyzes on low granularity columns. Use database schema to identify such columns.
    Example: Instead of analyzing at product level, suggest at company level.

  - Explore the data on filtered time ranges. Use database content to identify the temporal context for this conversation .
    Example: Instead of analyzing for all feedback dates, suggest filtering for a year or for a few months.   

  - Filter the data on the value of a specific attribute. Use database content to identify values of important dataset attributes.
    Example: Instead of analyzing for all companies, suggest filtering for a single company and give a few suggestions.       

  Close the prompt in one of these ways:
  A. If you suggest next steps, ask the user which option prefers.
  B. Use warm, supportive closing that makes the user feel good. 
    Example: “Keep up the great work!”, “Have a great day ahead!”.
  '''

# Each scenario

scenario_A = {  
    'Type': 'A',
    'Description': 'insights are retrieved to answer the question, from queries or vector store.',
    'Prompt': """You are a decision support consultant helping users become more data-driven.
    Continue the conversation from the last user prompt. 
    
    Conversation history:
    {messages_log}.

    Last user prompt:
    {question}.  

    Use both the raw SQL results and the extracted insights below to form your answer: {insights}. 
    
    Include all details from these insights.""" + '\n\n' + response_guidelines.strip(),
    'Invoke_Params': lambda state: {
        'messages_log': state['messages_log'],
        'question': state['current_question'],
        'objects_documentation': state['objects_documentation'],
        'database_content': state['database_content'],
        'insights': format_sql_query_results_for_prompt(state['current_sql_queries']),
        'current_sql_queries': state['current_sql_queries']
    }
}

scenario_B = {  
    'Type': 'B',
    'Description': 'answer is in the chat history, or the question is pleasantries. With response guidelines',
    'Prompt': """ You are a decision support consultant helping users become more data-driven.
    Continue the conversation from the last user prompt. 
    
    Conversation history:
    {messages_log}.

    Last user prompt:
    {question}.""" + '\n\n' + response_guidelines.strip(),
    'Invoke_Params': lambda state: {
        'messages_log': state['messages_log'],
        'question': state['current_question'],
        'objects_documentation': state['objects_documentation'],
        'database_content': state['database_content']
    }
}

scenario_C = {  
    'Type': 'C',
    'Description': 'request is not in db schema',
    'Prompt': """You are a decision support consultant helping users become more data-driven.
    Continue the conversation from the last user prompt. 
    
    Conversation history:
    {messages_log}.

    Last user prompt:
    {question}.
    
    Unfortunately, the requested information from last prompt is not available in our database. Here are the details: {notes}.
    
    Use the response guidelines below to explain what information is not available by suggesting alternative analyses that can be performed with the available data.""" + '\n\n' + response_guidelines.strip(),
    'Invoke_Params': lambda state: {
        'messages_log': state['messages_log'],
        'question': state['current_question'],
        'objects_documentation': state['objects_documentation'],
        'database_content': state['database_content'],
        'notes': state['generate_answer_details']['notes']
    }
}

scenario_D = {  
    'Type': 'D',
    'Description': 'Analytical intent ambiguous',
    'Prompt': """You are a decision support consultant helping users become more data-driven.
    
    Continue the conversation from the last user prompt. 
    
    Conversation history:
    {messages_log}.

    Last user prompt:
    {question}.
    
    The last user prompt could be interpreted in multiple ways. Here's what makes it ambiguous: {notes}.
    
    Acknowledge what makes the question ambiguous, present different options as possible interpretations and ask the user to specify which analysis it wants.

    Respond in clear, non-technical language. Be concise.""" + '\n\n' + response_guidelines.strip(),
    'Invoke_Params': lambda state: {
        'messages_log': state['messages_log'],
        'question': state['current_question'],
        'objects_documentation': state['objects_documentation'],
        'database_content': state['database_content'],
        'notes': state['generate_answer_details']['notes']
    }
}

scenario_prompts = [scenario_A,scenario_B,scenario_C,scenario_D]

def format_sql_query_results_for_prompt (sql_queries : list[dict]) -> str:
    
    formatted_queries = []
    for query_index,q in enumerate(sql_queries):
        block = f"Insight {query_index+1}:\n{q['insight']}\n\nRaw Result of insight {query_index+1}:\n{q['result']}"
        formatted_queries.append(block)
    return "\n\n".join(formatted_queries)

## create a function that generates the agent answer based on sql query result

@tool
def generate_answer(state:State):
  """ generates the AI answer taking into consideration the explanation and the result of the sql query that was executed """
  
  scenario = state['generate_answer_details']['scenario']

  # create prompt template based on scenario
  sys_prompt = next(s['Prompt'] for s in scenario_prompts if s['Type'] == scenario)
  prompt = ChatPromptTemplate.from_messages([MessagesPlaceholder("messages_log"),('system',sys_prompt)] )
  llm_answer_chain = prompt | llm

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
    ai_msg.response_metadata['token_usage']['total_tokens'] += explanation_token_count
  
  # Update state (common for all scenarios)
  state['llm_answer'] = ai_msg
  state['messages_log'].append(HumanMessage(state['current_question']))
  state['messages_log'].append(ai_msg) 

  show_progress("📣 Final Answer:")
  return state

def manage_memory_chat_history(state:State):
    """ Manages the chat history so that it does not become too large in terms of output tokens.
    Specifically, it checks if the chat history is larger than 1000 tokens. If yes, keep just the last 4 pairs of human prompts and AI responses, and summarize the older messages.
    Additionally, check if the logs of sql queries is larger than 20 entries. If yes, delete the older records. """           

    tokens_chat_history = state['messages_log'][-1].response_metadata.get('token_usage', {}).get('total_tokens', 0) if state['messages_log'] else 0    

    if tokens_chat_history >= 1000 and len(state['messages_log']) > 4:
        message_history_to_summarize = [msg.content for msg in state['messages_log'][:-4]]
        prompt = ChatPromptTemplate.from_messages( [('user', 'Distill the below chat messages into a single summary paragraph.The summary paragraph should have maximum 400 tokens.Include as many specific details as you can.Chat messages:{message_history_to_summarize}') ])
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
      - If the user asks for data/metrics not available in the database schema → "C"
    
    Step 3. Otherwise → "Continue".
    """
    prompt = ChatPromptTemplate.from_messages([('system', system_prompt)])
    chain = prompt | llm_fast.with_structured_output(ScenarioBC)
    result = chain.invoke({'messages_log':extract_msg_content_from_history(state['messages_log']),
                         'question': state['current_question'], 
                         'insights': format_sql_query_results_for_prompt(state['current_sql_queries']),
                         'objects_documentation':state['objects_documentation']
                         })   
    if result['next_step'] == 'Continue':
      scenario = None
      notes = None 
      next_tool_name = get_next_tool(state)
      pass  
    
    # if scenario B, set the scenario in the state and log the generate_answer as next step
    elif result['next_step'] == 'B':
      scenario = result['next_step']
      notes = None
      next_tool_name = 'generate_answer'
    
    # if scenario C, set the scenario in the state, generate the notes and log the generate_answer as next step
    else:
      sys_prompt_notes = """
      Conversation history:
      {messages_log}.  

      Last user prompt:
      {question}. 
  
      The user asked for data that is not available in the database schema.
      Write a sentence suggesting an analysis with the existing schema.
      Database schema:
      {objects_documentation}.

      Be short, concise, explain in simple, non-technical language.
      """
      prompt_notes = ChatPromptTemplate.from_messages([('system', sys_prompt_notes)]) 
      chain = prompt_notes | llm_fast
      notes_text = chain.invoke({'messages_log':extract_msg_content_from_history(state['messages_log']),
                         'question': state['current_question'], 
                         'objects_documentation':state['objects_documentation']
                                   })
      scenario = result['next_step']
      notes = notes_text.content
      next_tool_name = 'generate_answer'

  # if this is not the 1st time when orchestrator runs
  else:

    # go directly to answer because analytical intent has been extracted, queries created and executed
    next_tool = get_next_tool(state)
    if next_tool == 'generate_answer':
       next_tool_name = 'generate_answer'
       scenario = 'A' # can be changed later for the situation when insights are not enough and a subsequent analysis is needed
       notes = None

  # update generate_answer details
  state['generate_answer_details'].update({'scenario':scenario,'notes':notes})    

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

# function to reset the state current queries (to add in the start of graph execution)
def reset_state(state:State):
    state['current_sql_queries'] = []
    state['intermediate_steps'] = []
    state['llm_answer'] = AIMessage(content='')
    state['generate_answer_details'] = {}
    state['analytical_intent'] = []
    state['objects_documentation'] = objects_documentation
    state['database_content'] = database_content
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
