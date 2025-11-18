### This file is used to initialize the global variables for the agent:
### objects_documentation, sql_dialect, llm

import os
import uuid
import datetime
from pathlib import Path
from dotenv import load_dotenv
from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI
from langchain.callbacks.tracers.langchain import LangChainTracer
from src.init.init_demo_database.demo_database_util import create_objects_documentation
from src.init.llm_util import llm_provider
from src.init.database_schema import database_schema, table_relationships
from src.init.business_glossary import key_terms, synonyms, related_terms, check_glossary_consistency

# Load environment variables from root directory
current_dir = Path(__file__).parent
root_env_path = current_dir.parent.parent / '.env'
load_dotenv(root_env_path, override=True)

# Get environment variables
openai_api_key = os.getenv('OPENAI_API_KEY')
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
LANGSMITH_API_KEY = os.getenv('LANGSMITH_API_KEY')
connection_string = os.getenv('CONNECTION_STRING_DB')

# Set environment variables for OpenAI and LangSmith
os.environ['OPENAI_API_KEY'] = openai_api_key
os.environ['ANTHROPIC_API_KEY'] = ANTHROPIC_API_KEY
os.environ['LANGSMITH_API_KEY'] = LANGSMITH_API_KEY
os.environ['LANGSMITH_TRACING'] = "true"
os.environ['LANGSMITH_ENDPOINT'] = "https://api.smith.langchain.com"
langsmith_project_name = "db_agent_v1"
os.environ['LANGSMITH_PROJECT'] = langsmith_project_name

# Set up LangSmith tracer manually
tracer = LangChainTracer(project_name=langsmith_project_name)

# Initialize LLM models based on provider
if llm_provider == 'anthropic':
    llm = ChatAnthropic(model='claude-sonnet-4-20250514', temperature=0)  # Smart & expensive
    llm_fast = ChatAnthropic(model='claude-sonnet-4-20250514', temperature=0)  # Faster
elif llm_provider == 'openai':
    llm = ChatOpenAI(model='gpt-4o', temperature=0)  # Smart & expensive
    llm_fast = ChatOpenAI(model='gpt-4o-mini', temperature=0)  # Faster
else:
    raise ValueError(f"Unsupported LLM provider: {llm_provider}")  

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

    config = {
        'callbacks': [tracer],
        'run_name': full_run_name,
        'configurable': {'thread_id': thread_id}
    }

    return config, thread_id

# Run consistency check after import
check_glossary_consistency()

# Create objects documentation
objects_documentation = create_objects_documentation(database_schema, table_relationships, key_terms, connection_string)

# Set SQL dialect for PostgreSQL demo database
sql_dialect = 'PostgreSQL'

