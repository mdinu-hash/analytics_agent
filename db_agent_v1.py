
# ### Import feedbacks.db file

# %%
# test access to db file: import db tables into data frames and select by the column names

import pandas as pd
import sqlite3
from sqlalchemy import create_engine, inspect

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

# %%
# config function for tracing in langsmith
import datetime

def create_config(run_name):
    time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    run_name = f"{run_name} {time_now}"
    config={'callbacks':[tracer], 'run_name': run_name}
    return config

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
  
question = 'What can you tell me about the dataset?'

# %% [markdown]
# ### State of the graph

# %%
# define the state of the graph, which includes user's question, AI's answer, query that has been created and its result;
from typing_extensions import TypedDict, Annotated
from langgraph.graph.message import add_messages
from typing import Sequence
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage, RemoveMessage

class State(TypedDict):
 messages_log: Sequence[BaseMessage]
 question: str
 sql_query: list[str]
 sql_query_explanation : list[str]
 sql_query_result: list[str]
 llm_answer: str


# %%
# create a function that generates the sql query to be executed

from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

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

  chain = (prompt
          | llm.with_structured_output(OutputAsAQuery)
          | (lambda output: {'sql_query':output['query']} 
        )  )

  return chain.invoke({'objects_documentation':objects_documentation, 'question': state['question']},config = create_config('create_sql_query_or_queries'))

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
 sql_query_explanation = chain.invoke({'sql_query':sql_query},config = create_config('create_sql_query_explanation')).content
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
               ,create_config('refine_sql_query'))
 return sql_query

# %%
# the function checks if the query output exceeds context window limit and if yes, send the query for refinement

from langchain_community.tools import QuerySQLDataBaseTool
from langchain_community.utilities import SQLDatabase
from typing import Iterator

db = SQLDatabase(engine)

def execute_sql_query(state:State):
  """ executes the sql query and retrieve the result """

  for query_index, sql_query in enumerate(state['sql_query']):

    print(f"🚀 Executing query {query_index+1}/{len(state['sql_query'])}...")
    # refine the query 3 times if necessary.
    for i in range(3):

      sql_query_result = QuerySQLDataBaseTool(db=db).invoke(sql_query)

      # if the sql query does not exceed output context window return its result
      if not check_if_exceed_maximum_context_limit(sql_query_result):

       sql_query_explanation = create_sql_query_explanation(sql_query)
       state['sql_query_result'].append(sql_query_result)
       state['sql_query_explanation'].append(sql_query_explanation)
       break

      # if the sql query exceeds output context window and there is more room for iterations, refine the query
      else:
        print(f"🔧 Refining query {query_index+1}/{len(state['sql_query'])} as its output its too large...")
        sql_query = refine_sql_query(state['question'],sql_query,maximum_nr_tokens_sql_query)['query']

      # if there is no more room for sql query iterations and the result still exceeds context window, throw a message

    else:
      print(f"⚠️ Query result too large after 3 refinements.")
      state['sql_query_result'].append('Query result too large after 3 refinements.')
      state['sql_query_explanation'].append("Refinement failed.")

# %% [markdown]
# ### Extract metadata from sql query

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
def create_explanation(sql_queries: list[str]):
 """ based on the sql query metadata that was parsed, it creates a natural language message describing filters and transformations used by the query"""

 tables = []
 filters = []
 aggregations = []
 groupings = []

 for item in sql_queries:
 # get sql query metadata
  sql_query = item
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
# ### Manage memory and chat history

def manage_memory_chat_history(state:State):
    """ Manages the chat history so that it does not become too large in terms of output tokens.
    Specifically, it checks if the chat history is larger than 1000 tokens. If yes, keep just the last 2 pairs of human prompts and AI responses, and summarize the older messages.
    Additionally, check if the logs of sql queries is larger than 20 entries. If yes, delete the older records. """

    tokens_chat_history = state['messages_log'][-1].response_metadata['token_usage']['total_tokens']        

    if tokens_chat_history >= 1000 and len(state['messages_log']) > 4:
        message_history_to_summarize = [msg.content for msg in state['messages_log'][:-4]]
        prompt = ChatPromptTemplate.from_messages( [('user', 'Distill the below chat messages into a single summary paragraph.The summary paragraph should have maximum 400 tokens.Include as many specific details as you can.Chat messages:{message_history_to_summarize}') ])
        runnable = prompt | llm
        chat_history_summary = runnable.invoke({'message_history_to_summarize':message_history_to_summarize},config = create_config('manage_memory_chat_history'))
        last_4_messages = state['messages_log'][-4:]
        last_user_question = HumanMessage(state['question'])
        #delete_messages = [RemoveMessage(id = msg.id) for msg in state['messages_log'][:-4]] # delete messages older than the last 4 
        state['messages_log'] = [chat_history_summary] +[*last_4_messages] + [last_user_question]  

    # Optional: Truncate SQL logs to the most recent 20
    if len(state['sql_query']) > 20:
        state['sql_query'] = state['sql_query'][-20:]
        state['sql_query_explanation'] = state['sql_query_explanation'][-20:]
        state['sql_query_result'] = state['sql_query_result'][-20:]

    return state

# %%
messages_log = [
    HumanMessage(content = 'How many companies are there?',id=1),
    AIMessage(content = '''📣 Final Answer:

There are 12 unique companies listed in the data. Keep in mind, though, that if there are any variations in how the company names are written (like different spellings or capitalization), it might affect the count slightly.

I analyzed data based on the following filters and transformations:

🧊 Tables: • company

🧮 Aggregations: • COUNT(DISTINCT company.company_name)''',id=2),

    HumanMessage(content = 'What can you tell me about the dataset?',id=3),
    AIMessage(content = '''📣 Final Answer:

The dataset provides some interesting insights:

1. There are 12 unique companies listed in the dataset.
2. On average, these companies have an annual revenue of about $26.3 trillion USD.
3. There are 413,898 unique feedback entries, which means a lot of feedback has been collected.
4. The average feedback rating is approximately 3.84 out of a possible scale (not specified here).
5. There are 8,145 unique products in the dataset.

These numbers give a broad overview of the dataset, showing a diverse range of companies, a significant amount of feedback, and a large variety of products. However, it doesn't dive into specifics like industry types, regional data, or detailed company attributes.

I analyzed data based on the following filters and transformations:

🧊 Tables: • company • feedback • products

🧮 Aggregations: • COUNT(DISTINCT company.company_name) • AVG(company.annual_revenue_usd) • COUNT(DISTINCT feedback.feedback_id) • AVG(feedback.feedback_rating) • COUNT(DISTINCT products.product_id)',id=5),
    HumanMessage(content = 'tell me a joke about rum refering to my name''',id=4),
   
    HumanMessage(content = 'Can you share the average feedback rating per company?',id=5),
    AIMessage(content = '''📣 Final Answer:

Sure! Here's the average feedback rating for each company based on the data we have:

- Adidas: 4.06
- Apple: 3.85
- AT&T: 3.66
- Cisco: 3.37
- Google: 3.56
- Microsoft: 3.91
- Nike: 3.95
- Samsung: 3.87
- Sony: 3.83
- Target: 3.33
- Verizon: 3.84
- Walmart: 3.31

These numbers represent the average feedback ratings from customers for each company. Keep in mind that this is a straightforward average and doesn't account for things like the number of feedback entries or any unusual ratings that might affect the average.

I analyzed data based on the following filters and transformations:
...

🧮 Aggregations: • AVG(feedback.feedback_rating)

📦 Groupings: • feedback.product_company_name''',id=6),
    
    HumanMessage(content = 'fine, and how many products each company has?',id=7),
    AIMessage(content = '''📣 Final Answer:

Sure! Here's a quick rundown of how many products each company has:

- Adidas has 181 products.
- Apple offers 1,178 products.
- AT&T has 134 products.
- Cisco has 9 products.
- Google has 459 products.
- Microsoft offers 75 products.
- Nike has 115 products.
- Samsung has a whopping 4,801 products.
- Sony offers 780 products.
- Target has 2 products.
- Verizon has 405 products.
- Walmart has 6 products.

This list includes all companies, even those with no products, thanks to the way the data was gathered.

I analyzed data based on the following filters and transformations:
...

🧮 Aggregations: • COUNT(products.product_id)

📦 Groupings: • company.company_name''',
              response_metadata = { 'token_usage' : {'total_tokens' : 1200 } },
              id=8  ) 

]

question = 'what is the first company you listed in your previous response?'

# %%
## create a function that generates the agent answer based on sql query result

def generate_answer(state:State):
  """ generates the AI answer taking into consideration the explanation and the result of the sql query that was executed """

  system_prompt = """ You are a decision support consultant helping users become more data-driven.
     Your task is to answer the user question based on the following information:

 - The sql query result which is the result of a query created for the purpose of answering the question.
 - The query explanation is a short explanation of the query making you aware of its limitations and underlying assumptions.

 User question:
 {question}

 SQL query explanation:
 {sql_query_explanation}

 SQL query result:
 {sql_query_result}

 Take into account the insights from this explanation in your answer.
 Answer in simple terms, conversational, non-technical language. Be concise.
 """

  prompt = ChatPromptTemplate.from_messages([
    SystemMessage(content = system_prompt),
    MessagesPlaceholder("messages_log")
  ]
  )

  chain = (prompt
        | llm
        | (lambda output: {'llm_answer': f"{output.content}\n\n{create_explanation(state['sql_query'])}"})
  )

  return chain.invoke({ 'messages_log': state['messages_log'],
                     'question':state['question'],
                     'sql_query_explanation':state['sql_query_explanation'],
                     'sql_query_result':state['sql_query_result']}
                     ,config = create_config('generate_answer'))

# %% [markdown]
# ### assemble the graph

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
graph.add_edge("execute_sql_query","manage_memory_chat_history")
graph.add_edge("manage_memory_chat_history","generate_answer")
graph.add_edge("generate_answer",END)

memory = MemorySaver()
graph = graph.compile(checkpointer=memory)

# %% [markdown]
# ### test the agent

# %%
messages_log = [
    HumanMessage(content = 'How many companies are there?',id=1),
    AIMessage(content = '''📣 Final Answer:

There are 12 unique companies listed in the data. Keep in mind, though, that if there are any variations in how the company names are written (like different spellings or capitalization), it might affect the count slightly.

I analyzed data based on the following filters and transformations:

🧊 Tables: • company

🧮 Aggregations: • COUNT(DISTINCT company.company_name)''',id=2),

    HumanMessage(content = 'What can you tell me about the dataset?',id=3),
    AIMessage(content = '''📣 Final Answer:

The dataset provides some interesting insights:

1. There are 12 unique companies listed in the dataset.
2. On average, these companies have an annual revenue of about $26.3 trillion USD.
3. There are 413,898 unique feedback entries, which means a lot of feedback has been collected.
4. The average feedback rating is approximately 3.84 out of a possible scale (not specified here).
5. There are 8,145 unique products in the dataset.

These numbers give a broad overview of the dataset, showing a diverse range of companies, a significant amount of feedback, and a large variety of products. However, it doesn't dive into specifics like industry types, regional data, or detailed company attributes.

I analyzed data based on the following filters and transformations:

🧊 Tables: • company • feedback • products

🧮 Aggregations: • COUNT(DISTINCT company.company_name) • AVG(company.annual_revenue_usd) • COUNT(DISTINCT feedback.feedback_id) • AVG(feedback.feedback_rating) • COUNT(DISTINCT products.product_id)',id=5),
    HumanMessage(content = 'tell me a joke about rum refering to my name''',id=4),
   
    HumanMessage(content = 'Can you share the average feedback rating per company?',id=5),
    AIMessage(content = '''📣 Final Answer:

Sure! Here's the average feedback rating for each company based on the data we have:

- Adidas: 4.06
- Apple: 3.85
- AT&T: 3.66
- Cisco: 3.37
- Google: 3.56
- Microsoft: 3.91
- Nike: 3.95
- Samsung: 3.87
- Sony: 3.83
- Target: 3.33
- Verizon: 3.84
- Walmart: 3.31

These numbers represent the average feedback ratings from customers for each company. Keep in mind that this is a straightforward average and doesn't account for things like the number of feedback entries or any unusual ratings that might affect the average.

I analyzed data based on the following filters and transformations:
...

🧮 Aggregations: • AVG(feedback.feedback_rating)

📦 Groupings: • feedback.product_company_name''',id=6),
    
    HumanMessage(content = 'fine, and how many products each company has?',id=7),
    AIMessage(content = '''📣 Final Answer:

Sure! Here's a quick rundown of how many products each company has:

- Adidas has 181 products.
- Apple offers 1,178 products.
- AT&T has 134 products.
- Cisco has 9 products.
- Google has 459 products.
- Microsoft offers 75 products.
- Nike has 115 products.
- Samsung has a whopping 4,801 products.
- Sony offers 780 products.
- Target has 2 products.
- Verizon has 405 products.
- Walmart has 6 products.

This list includes all companies, even those with no products, thanks to the way the data was gathered.

I analyzed data based on the following filters and transformations:
...

🧮 Aggregations: • COUNT(products.product_id)

📦 Groupings: • company.company_name''',
              response_metadata = { 'token_usage' : {'total_tokens' : 1200 } },
              id=8  ) 

]

#messages_log = []

question = 'what is the first company you listed in your previous response?'

initial_dict = {'objects_documentation':objects_documentation,
     'messages_log': messages_log,
     'question':question,
     'sql_query': [],
     'sql_query_result': [],
     'sql_query_explanation': [],
     'llm_answer': ''
     }

thread_id = 'abc139'
config = { 'configurable' : { 'thread_id':thread_id} }

if __name__ == "__main__":
 for step in graph.stream(initial_dict, config = config, stream_mode="updates"):
   step_name, output = list(step.items())[0]
   if step_name == 'create_sql_query_or_queries':
    print(f"✅ SQL queries created:{len(output['sql_query'])}")
   elif step_name == 'execute_sql_query':
    print("⚙️ Analysing results...")
   elif step_name == 'generate_answer':
    print("\n📣 Final Answer:\n")
    print(output['llm_answer'])