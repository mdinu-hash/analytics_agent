### This file is used to initialize the global variables for the agent:
### objects_documentation, sql_dialect, llm

import os
import uuid
import datetime
from pathlib import Path
from dotenv import load_dotenv
from langchain.callbacks.tracers.langchain import LangChainTracer
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
import yaml
from langchain_anthropic import ChatAnthropic
from initialize_demo_database.demo_database_util import create_objects_documentation

# Load environment variables from root directory
current_dir = Path(__file__).parent
root_env_path = current_dir / '.env'
load_dotenv(root_env_path, override=True)

# Get environment variables
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
LANGSMITH_API_KEY = os.getenv('LANGSMITH_API_KEY')

# Set environment variables for LangSmith
os.environ['ANTHROPIC_API_KEY'] = ANTHROPIC_API_KEY
os.environ['LANGSMITH_API_KEY'] = LANGSMITH_API_KEY
os.environ['LANGSMITH_TRACING'] = "true"
os.environ['LANGSMITH_ENDPOINT'] = "https://api.smith.langchain.com"
langsmith_project_name = "db_agent_v1"
os.environ['LANGSMITH_PROJECT'] = langsmith_project_name

# Set up LangSmith tracer manually
tracer = LangChainTracer(project_name=langsmith_project_name)

llm = ChatAnthropic(model='claude-sonnet-4-5-20250929', temperature=0)
llm_fast = ChatAnthropic(model='claude-haiku-4-5', temperature=0)

def get_token_usage(response):
    return response.response_metadata['usage']['output_tokens']


def calculate_chat_history_tokens(messages_log):
    if not messages_log:
        return 0
    total_tokens = 0
    for msg in messages_log:
        if hasattr(msg, 'response_metadata') and 'usage' in msg.response_metadata:
            total_tokens += msg.response_metadata['usage'].get('output_tokens', 0)
    return total_tokens


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

def _load_semantic_model():
    model_path = Path(__file__).parent / 'semantic_model.yaml'
    with open(model_path) as f:
        model = yaml.safe_load(f)
    return model['tables'], model['relationships'], model['key_terms']

database_schema, table_relationships, key_terms = _load_semantic_model()
objects_documentation = create_objects_documentation(database_schema, table_relationships, key_terms)

# Set SQL dialect for SQLite demo database
sql_dialect = 'SQLite'

db_path = str((current_dir / os.getenv('DB_PATH', './demo.db')).resolve())

