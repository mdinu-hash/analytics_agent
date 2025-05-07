import langchain, langchain_openai, langsmith
import os
from dotenv import load_dotenv
from langsmith import Client

load_dotenv(override=True)
openai_api_key = os.getenv('OPENAI_API_KEY')
LANGSMITH_API_KEY = os.getenv('LANGSMITH_API_KEY')
os.environ['OPENAI_API_KEY'] = openai_api_key
os.environ['LANGSMITH_API_KEY'] = LANGSMITH_API_KEY
os.environ['LANGSMITH_TRACING'] = "true"
os.environ['LANGSMITH_ENDPOINT'] = "https://api.smith.langchain.com"
langsmith_project_name = "db_agent_v1"
os.environ['LANGSMITH_PROJECT'] = langsmith_project_name

from langchain_openai import ChatOpenAI
llm = ChatOpenAI(model='gpt-4o',temperature=0)

# %%
from db_agent_v1 import objects_documentation

# %%

question_q1 = 'How many companies are there?'
input_state_q1 = {
               'objects_documentation':objects_documentation,
               'messages_log': [],
               'question':question_q1,
               'sql_query': [],
               'sql_query_result': [],
               'sql_query_explanation': [],
               'llm_answer': ''
               }    


client = Client()
dataset = client.create_dataset('Evaluation Dataset db_agent_v1')

client.create_examples(
    dataset_id = dataset.id,
    examples = [
        {
          'inputs': input_state_q1,
          'outputs': {'answer': 'There are 12 companies'}
        }
    ]
                      )

