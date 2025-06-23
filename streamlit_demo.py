import streamlit as st
import os
import requests
import uuid
from pathlib import Path
from langchain_core.messages import HumanMessage, AIMessage
import io
import contextlib
import sys

# Context manager to capture print statements
@contextlib.contextmanager
def capture_prints():
    """Capture print statements and return them as a list"""
    old_stdout = sys.stdout
    stdout_capture = io.StringIO()
    sys.stdout = stdout_capture
    try:
        yield stdout_capture
    finally:
        sys.stdout = old_stdout

# Configuration
GITHUB_REPO = "mdinu-hash/db_agent_v1"  
RELEASE_TAG = "v1.0"  
DATABASE_FILE = "feedbacks_db.db"
DOWNLOAD_URL = f"https://github.com/{GITHUB_REPO}/releases/download/{RELEASE_TAG}/{DATABASE_FILE}"

@st.cache_resource
def download_database():
    """Download database from GitHub releases if it doesn't exist"""
    
    if os.path.exists(DATABASE_FILE):
        return True
    
    try:
        st.info("🔄 Setting up database... This will take a moment (133MB download)")
        
        # Create progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Download with progress tracking
        response = requests.get(DOWNLOAD_URL, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(DATABASE_FILE, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB chunks
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        progress = downloaded / total_size
                        progress_bar.progress(progress)
                        status_text.text(f"Downloaded: {downloaded // (1024*1024)}MB / {total_size // (1024*1024)}MB")
        
        progress_bar.empty()
        status_text.empty()
        st.success("✅ Database ready!")
        
        return True
        
    except Exception as e:
        st.error(f"❌ Failed to download database: {e}")
        st.info("Please check if the GitHub release URL is correct.")
        return False

# Download database first
if not download_database():
    st.stop()

# Try to import agent components safely
try:
    # Import the individual components we need
    import db_agent_v1
    
    # Get the components after database is available
    graph = db_agent_v1.graph
    create_config = db_agent_v1.create_config
    objects_documentation = db_agent_v1.objects_documentation
    database_content = db_agent_v1.database_content
    sql_dialect = db_agent_v1.sql_dialect
    
except Exception as e:
    st.error(f"❌ Failed to import agent: {e}")
    st.info("Make sure db_agent_v1.py is in the same directory and the database is available.")
    st.stop()

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "thread_id" not in st.session_state:
    st.session_state.thread_id = str(uuid.uuid4())

#### UI

st.set_page_config(page_title="DB Agent Demo", page_icon="🤖", layout="wide")
st.title("🤖 Database Query Agent Demo")
st.markdown("Ask questions about the feedback database and get insights!")

# Sidebar with database info
with st.sidebar:
    st.title("🤖 Database Query Agent Demo")
    st.header("Database Info")
    st.text("This is a demo database based on public amazon reviews")
    st.markdown("""
    - 413k feedback records (2002-2023)
    - 8,145 products
    - 12 companies (Apple, Samsung, Sony, Nike, etc.)
    - Ratings from 1-5 stars
    """)
    # Show database status
    if os.path.exists(DATABASE_FILE):
      file_size = os.path.getsize(DATABASE_FILE) / (1024*1024)
      st.sidebar.markdown(f"**Status:** Database Connected")
    else:
       st.sidebar.markdown("**Status:** Database Not Available")

    # Clear chat button in sidebar - SIMPLE SOLUTION
    st.markdown("---")
    if st.button("🗑️ Clear & Start New Chat", use_container_width=True, type="secondary"):
        st.session_state.messages = []
        st.session_state.thread_id = str(uuid.uuid4())
        st.rerun()   

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Ask about the database..."):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Display user message
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Generate response
    with st.chat_message("assistant"):
        # Create containers for progress and final response
        progress_container = st.container()
        response_container = st.container()
        
        with progress_container:
            status_placeholder = st.empty()
            progress_placeholder = st.empty()
        
        try:
            # Convert session messages to your agent's format
            messages_log = []
            for msg in st.session_state.messages[:-1]:  # Exclude the current message
                if msg["role"] == "user":
                    messages_log.append(HumanMessage(content=msg["content"]))
                else:
                    messages_log.append(AIMessage(content=msg["content"]))
            
            # Prepare state for your agent
            if len(messages_log) == 0:  # First message
                initial_dict = {
                    'objects_documentation': objects_documentation,
                    'database_content': database_content,
                    'sql_dialect': sql_dialect,
                    'messages_log': messages_log,
                    'intermediate_steps': [],
                    'analytical_intent': [],
                    'current_question': prompt,
                    'current_sql_queries': [],
                    'generate_answer_details': {},
                    'llm_answer': AIMessage(content='')
                }
                config, st.session_state.thread_id = create_config('Run Agent', True)
                
                # Capture prints during agent execution
                with capture_prints() as captured:
                    result = graph.invoke(initial_dict, config=config)
                    
            else:  # Continuing conversation
                config, _ = create_config('Run Agent', False, st.session_state.thread_id)
                
                # Capture prints during agent execution
                with capture_prints() as captured:
                    result = graph.invoke({'current_question': prompt}, config=config)
            
            # Process captured prints and show progress
            captured_output = captured.getvalue()
            if captured_output.strip():
                # Split by lines and show each progress message
                progress_lines = [line.strip() for line in captured_output.split('\n') if line.strip()]
                
                # Show progress messages
                for i, line in enumerate(progress_lines):
                    if line.startswith('✅'):
                        status_placeholder.success(line)
                    elif line.startswith('⚙️'):
                        status_placeholder.info(line)
                    elif line.startswith('🔧'):
                        status_placeholder.warning(line)
                    elif line.startswith('⚠️'):
                        status_placeholder.error(line)
                    elif line.startswith('📣'):
                        status_placeholder.success(line)
                        # Clear progress after final message
                        import time
                        time.sleep(1)
                        progress_container.empty()
                    else:
                        progress_placeholder.text(line)
            
            # Show final response
            with response_container:
                response = result['llm_answer'].content
                st.markdown(response)
                
                # Add assistant response to chat history
                st.session_state.messages.append({"role": "assistant", "content": response})
                
        except Exception as e:
            progress_container.empty()
            error_msg = f"Sorry, I encountered an error: {str(e)}"
            st.error(error_msg)
            st.session_state.messages.append({"role": "assistant", "content": error_msg})