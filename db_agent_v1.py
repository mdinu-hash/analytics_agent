
# ### Import feedbacks.db file

# %%
# test access to db file: import db tables into data frames and select by the column names

import pandas as pd
import sqlite3
from sqlalchemy import create_engine, inspect
import uuid

engine = create_engine('sqlite:///feedbacks_db.db')
inspector = inspect(engine)

df_company = pd.read_sql_query('SELECT company_name,annual_revenue_usd FROM company', engine)
df_feedback = pd.read_sql_query('SELECT feedback_id,feedback_date,product_id,product_company_name,feedback_text,"feedback_rating" FROM feedback', engine)
df_products = pd.read_sql_query('SELECT product_id,product_name,product_brand,product_manufacturer,product_company_name,product_price,product_average_rating FROM products', engine)

# %% [markdown]
# ### Instantiate chat model (OpenAI)

# %%
import langchain, langgraph, langchain_openai, langsmith

import os
from dotenv import load_dotenv
from langchain_core.runnables import RunnableConfig
from langchain.callbacks.tracers.langchain import LangChainTracer

load_dotenv(override=True)
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

from langchain_openai import ChatOpenAI
llm = ChatOpenAI(model='gpt-4o',temperature=0)

# %% [markdown]
# ### Create config

# %%
import datetime

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
            'run_name': run_name,
            'configurable' : { 'thread_id':thread_id }
            }

    return config,thread_id

# %% [markdown]
# ### Constants

# %%
# constants

objects_documentation = """
  Table company: List of public companies. Granularity is company-name. Column (prefixed with table name):
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
  products.product_average-rating: average ratings across all feedbacks for the product, at crawling time.

  Table company -> column company_name relates to table feedback -> column product_company_name
  Table products -> column product_company_name relates to table feedback -> column product_company-name
  Table feedback -> column product_id relates to table products -> column product_id
  """

# %% [markdown]
# ### Define state

# %%
# define the state of the graph, which includes user's question, AI's answer, query that has been created and its result;
from typing_extensions import TypedDict, Annotated
from langgraph.graph.message import add_messages
from typing import Sequence
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage, RemoveMessage

class State(TypedDict):
 messages_log: Sequence[BaseMessage]
 question: str
 sql_queries: list[dict]
 llm_answer: BaseMessage

# %% [markdown]
# ### Create sql query or queries

# %%
# create a function that generates the sql query to be executed

from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder, SystemMessagePromptTemplate

class OutputAsAQuery(TypedDict):
  """ generated sql query or sql queries if there are multiple """
  query: Annotated[list[str],"clean sql query"]

def create_sql_query_or_queries(state:State):
  """ creates a sql query based on the question """

  system_prompt = """You are a sql expert and an expert data modeler.  

  Your task is to create a sql script to answer the user's question. In the sql script, use only these tables and columns you have access to:
  {objects_documentation}

  User question:
  {question}

  Answer just with the resulting sql code.

  IMPORTANT:
    - Return only raw SQL strings in the list.
    - DO NOT include comments (like "-- Query 1"), labels, or explanations.
    - If only one SQL query is needed, just return a list with that one query.
    - Do not generate more than 5 queries.

  Example output:
    [
      "SELECT COUNT(*) FROM feedback;",
      "SELECT AVG(product_price) FROM products;"
    ]
  """

  prompt = ChatPromptTemplate.from_messages(
    ('system', system_prompt)
  )

  chain = prompt | llm.with_structured_output(OutputAsAQuery)

  result = chain.invoke({'objects_documentation':objects_documentation, 'question': state['question']})
  for q in result['query']:
   state['sql_queries'].append( {'query': q,
                                     'explanation': '', ## add it later
                                     'result':'' ## add it later
                                      } )
  return state

# %%
# since gpt-4o allows a maximum completion limit (output context limit) of 4k tokens, I half it to get maximum context size, so 2k. Assuming the entire context is not just the data,
# I divide this number by 5 and arrive at a limit of 400 tokens for the result of the sql query.

import tiktoken

maximum_nr_tokens_sql_query = 200

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

# %% [markdown]
# ### Create sql query explanation

# %%
# create a function that creates an explanation of a sql query

def create_sql_query_explanation(sql_query:str):
 """ creates a concise explanation of the sql query """

 system_prompt = """
 As a data expert, you are provided with this sql query:
 {sql_query}

 Create a brief explanation of this query in simple terms by taking into account these factors, if exist:
 - Pay attention to the nuances of the query: the filters, aggregations, groupings, etc.
 - Take into account underlying assumptions.
 - Query limitations.
 Keep it short.
 """

 prompt = ChatPromptTemplate.from_messages(
     ('system',system_prompt)
 )

 chain = prompt | llm
 sql_query_explanation = chain.invoke({'sql_query':sql_query}).content
 return sql_query_explanation

# %%
class OutputAsASingleQuery(TypedDict):
  """ generated sql query """
  query: Annotated[str,...,"the generated sql query"]

def refine_sql_query(question: str, sql_query: str, maximum_nr_tokens_sql_query: int):
 """ refines the sql query so that its output tokens do not exceed the maximum context limit """

 system_prompt = """
 You are a sql expert an an expert data modeler.

 You are tying to answer the following question from the user:
 {question}

 The following sql query produces an output that exceeds {maximum_nr_tokens_sql_query} tokens:
 {sql_query}

 Please optimize this query so that its output stays within the token limit while still providing as much insight as possible to answer the question.
 Prefer using WHERE or LIMIT clauses to reduce the size of the result.
 """
 
 prompt = ChatPromptTemplate.from_messages(
   ('system',system_prompt)
 )

 chain = (prompt
         | llm.with_structured_output(OutputAsASingleQuery)
 )

 sql_query = chain.invoke({'question': question,
               'sql_query':sql_query,
               'maximum_nr_tokens_sql_query':maximum_nr_tokens_sql_query}
               )
 return sql_query

# %% [markdown]
# ### Execute sql query

# %%
# the function checks if the query output exceeds context window limit and if yes, send the query for refinement

from langchain_community.tools import QuerySQLDataBaseTool
from langchain_community.utilities import SQLDatabase
from typing import Iterator

db = SQLDatabase(engine)

def execute_sql_query(state:State):
  """ executes the sql query and retrieve the result """

  for query_index, q in enumerate(state['sql_queries']):
     
    sql_query = q['query'] 
    print(f"🚀 Executing query {query_index+1}/{len(state['sql_queries'])}...")
    # refine the query 3 times if necessary.
    for i in range(3):

      sql_query_result = QuerySQLDataBaseTool(db=db).invoke(sql_query)

      # if the sql query does not exceed output context window return its result
      if not check_if_exceed_maximum_context_limit(sql_query_result):

       sql_query_explanation = create_sql_query_explanation(sql_query)
       state['sql_queries'][query_index]['result'] = sql_query_result
       state['sql_queries'][query_index]['explanation'] = sql_query_explanation
       state['sql_queries'][query_index]['query'] = sql_query
       break

      # if the sql query exceeds output context window and there is more room for iterations, refine the query
      else:
        print(f"🔧 Refining query {query_index+1}/{len(state['sql_queries'])} as its output its too large...")
        sql_query = refine_sql_query(state['question'],sql_query,maximum_nr_tokens_sql_query)['query']

      # if there is no more room for sql query iterations and the result still exceeds context window, throw a message

    else:
      print(f"⚠️ Query result too large after 3 refinements.")
      state['sql_queries'][query_index]['result'] = 'Query result too large after 3 refinements.'
      state['sql_queries'][query_index]['explanation'] = "Refinement failed."
      
  return state

# %% [markdown]
# ### Extract metadata from sql query
# 
# 

# %%
import sqlglot
from sqlglot import parse_one, exp

def extract_metadata_from_sql_query(sql_query):
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

 return sql_query_metadata

# %%
def create_explanation(sql_queries: list[dict]):
 """ based on the sql query metadata that was parsed, it creates a natural language message describing filters and transformations used by the query"""

 tables = []
 filters = []
 aggregations = []
 groupings = []

 for query_index,q in enumerate(sql_queries):
 # get sql query metadata
  sql_query = q['query']
  sql_query_metadata = extract_metadata_from_sql_query(sql_query)

  if sql_query_metadata['tables']:
   tables.extend(sql_query_metadata['tables'])
   tables = list(dict.fromkeys(tables))

  if sql_query_metadata['filters']:
   filters.extend(sql_query_metadata['filters'])
   filters = list(dict.fromkeys(filters))

  if sql_query_metadata['aggregations']:
   aggregations.extend(sql_query_metadata['aggregations'])
   aggregations = list(dict.fromkeys(aggregations))

  if sql_query_metadata['groupings']:
   groupings.extend(sql_query_metadata['groupings'])
   groupings = list(dict.fromkeys(groupings))

 # wrapping it all together
 sql_query_explanation = "I analyzed data based on the following filters and transformations:"

 if tables:
  tables = f"🧊 Tables: • {' • '.join(tables)}"
  sql_query_explanation = sql_query_explanation + "\n\n" + tables

 if filters:
  filters = f"🔍 Filters: • {' • '.join(filters)}"
  sql_query_explanation = sql_query_explanation + "\n\n" + filters

 if aggregations:
  aggregations = f"🧮 Aggregations: • {' • '.join(aggregations)}"
  sql_query_explanation = sql_query_explanation + "\n\n" + aggregations

 if groupings:
  groupings = f"📦 Groupings: • {' • '.join(groupings)}"
  sql_query_explanation = sql_query_explanation + "\n\n" + groupings

 return sql_query_explanation

# %% [markdown]
# ### Generate answer

# %%
def format_sql_queries_for_prompt (sql_queries : list[dict]) -> str:
    # expects a dictionary with a structure like query, explanation, result
    
    formatted_queries = []
    for query_index,q in enumerate(sql_queries):
        formatted_queries.append(f"Query {query_index+1} explanation:\n {q['explanation']}\n\n Query {query_index+1} result:\n {q['result']}")
    return "\n\n".join(formatted_queries)

# print(format_sql_queries_for_prompt(test_state['sql_queries']))

# %%
## create a function that generates the agent answer based on sql query result

from langchain_core.runnables import RunnableLambda, RunnablePassthrough
from langchain.schema.output_parser import StrOutputParser

def generate_answer(state:State):
  """ generates the AI answer taking into consideration the explanation and the result of the sql query that was executed """

  system_prompt = """ You are a decision support consultant helping users become more data-driven.
     Your task is to answer the user question using the result of the sql queries:

 - The sql queries were created for the purpose of answering the user question.
 - The query explanations are short descriptions of the query making you aware of its limitations and underlying assumptions.

 User question:
 {question}

 Context from SQL queries:
 {context_sql_queries}

 Take into account the insights from this explanation in your answer.
 Answer in simple terms, conversational, non-technical language. Be concise.
 """

  prompt = ChatPromptTemplate.from_messages([
    ('system',system_prompt),
    MessagesPlaceholder("messages_log")          
  ] )

  llm_answer_chain = prompt | llm 
  final_answer_chain = { 'llm_answer': llm_answer_chain, 'input_state': RunnablePassthrough() } | RunnableLambda (lambda x: { 'ai_message': AIMessage( content = f"{x['llm_answer'].content}\n\n{create_explanation(x['input_state']['sql_queries'])}", 
                                                                                                                                                        response_metadata = x['llm_answer'].response_metadata  ) } ) 

  result = final_answer_chain.invoke({ 'messages_log':state['messages_log'],
               'question':state['question'],
               'context_sql_queries': format_sql_queries_for_prompt(state['sql_queries']),
              'sql_queries': state['sql_queries'] })
  
  ai_msg = result['ai_message']

  explanation_token_count = llm.get_num_tokens(create_explanation(state['sql_queries']))
  ai_msg.response_metadata['token_usage']['total_tokens'] += explanation_token_count

  state['llm_answer'] = ai_msg
  state['messages_log'].append(HumanMessage(state['question']))
  state['messages_log'].append(ai_msg)

  return state

# %% [markdown]
# ### Manage memory and chat history

# %%
def manage_memory_chat_history(state:State):
    """ Manages the chat history so that it does not become too large in terms of output tokens.
    Specifically, it checks if the chat history is larger than 1000 tokens. If yes, keep just the last 4 pairs of human prompts and AI responses, and summarize the older messages.
    Additionally, check if the logs of sql queries is larger than 20 entries. If yes, delete the older records. """           

    tokens_chat_history = state['messages_log'][-1].response_metadata.get('token_usage', {}).get('total_tokens', 0) if state['messages_log'] else 0
    

    if tokens_chat_history >= 1000 and len(state['messages_log']) > 4:
        message_history_to_summarize = [msg.content for msg in state['messages_log'][:-4]]
        prompt = ChatPromptTemplate.from_messages( [('user', 'Distill the below chat messages into a single summary paragraph.The summary paragraph should have maximum 400 tokens.Include as many specific details as you can.Chat messages:{message_history_to_summarize}') ])
        runnable = prompt | llm
        chat_history_summary = runnable.invoke({'message_history_to_summarize':message_history_to_summarize})
        last_4_messages = state['messages_log'][-4:]
        state['messages_log'] = [chat_history_summary] +[*last_4_messages]
    else:
        state['messages_log'] = state['messages_log']

    # Truncate SQL logs to the most recent 20
    if len(state['sql_queries']) > 20:
        state['sql_queries']= state['sql_queries'][-20:]    
        
    return state

# %% [markdown]
# ### Assemble graph

# %%
# assemble graph

from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver

graph= StateGraph(State)
graph.add_node("create_sql_query_or_queries",create_sql_query_or_queries)
graph.add_node("execute_sql_query",execute_sql_query)
graph.add_node("generate_answer",generate_answer)
graph.add_node("manage_memory_chat_history",manage_memory_chat_history)

graph.add_edge(START,"create_sql_query_or_queries")
graph.add_edge("create_sql_query_or_queries","execute_sql_query")
graph.add_edge("execute_sql_query","generate_answer")
graph.add_edge("generate_answer","manage_memory_chat_history")
graph.add_edge("manage_memory_chat_history",END)

memory = MemorySaver()
graph = graph.compile(checkpointer=memory)

# %% [markdown]
# 
# ### test the agent

# %%
# Start a new conversation

question = 'How many companies are there?'
messages_log = []

initial_dict = {'objects_documentation':objects_documentation,
     'messages_log': messages_log,
     'question':question,
     'sql_queries': [],
     'llm_answer': []
     }

config, thread_id = create_config('Run Agent',True)
if __name__ == "__main__":
 for step in graph.stream(initial_dict, config = config, stream_mode="updates"):
   step_name, output = list(step.items())[0]
   if step_name == 'create_sql_query_or_queries':
    print(f"✅ SQL queries created:{len(output['sql_queries'])}")
   elif step_name == 'execute_sql_query':
    print("⚙️ Analysing results...")
   elif step_name == 'generate_answer':
    print("\n📣 Final Answer:\n")
    print(output['llm_answer'].content)

# %%
# Continue the conversation

initial_dict['question'] = 'follow up question' 


config, _ = create_config('Run Agent',False,thread_id)
if __name__ == "__main__":
 for step in graph.stream(initial_dict, config = config, stream_mode="updates"):
   step_name, output = list(step.items())[0]
   if step_name == 'create_sql_query_or_queries':
    print(f"✅ SQL queries created:{len(output['sql_queries'])}")
   elif step_name == 'execute_sql_query':
    print("⚙️ Analysing results...")
   elif step_name == 'generate_answer':
    print("\n📣 Final Answer:\n")
    print(output['llm_answer'].content)

# %% [markdown]
# ### Testing Locally

# %%
# question = 'How many companies are there?'

# test_state = {'messages_log':[],
#  'question':question,
#  'sql_queries': [],
#  'llm_answer': []
#  }
# create_sql_query_or_queries(test_state)
# execute_sql_query(test_state)
# generate_answer(test_state)
# manage_memory_chat_history(test_state)
# #test_state 


