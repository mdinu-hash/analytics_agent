
import streamlit as st
import os
import requests
import uuid
import time  # <-- ADD THIS IMPORT (you were missing it)
from pathlib import Path
from langchain_core.messages import HumanMessage, AIMessage
import threading
import queue

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

progress_queue = queue.Queue()
db_agent_v1.set_progress_queue(progress_queue)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "thread_id" not in st.session_state:
    st.session_state.thread_id = str(uuid.uuid4())

#### UI

st.set_page_config(page_title="GenAI Data Copilot", page_icon="🤖", layout="wide")
st.title("GenAI Data Copilot - Demo")
st.header("Instant Database Insights Through Natural Language")
st.text("""Simply ask questions in plain English and get instant, data-driven answers based on enterprise, tabular data:
- Root cause analysis ("Why did the ratings for adidas decreased in early 2016 from january to may?")
- Key drivers ("which companies contributed to the increase in ratings from September 2022?") 
- Time-based trends ("how these ratings changed over time per company?").
- Comparative analysis ("Are premium-priced products (top 25% by price) getting better ratings than budget products?") 
""")
st.markdown("---")

# Sidebar with database info
with st.sidebar:
    st.title("GenAI Data Copilot")
    st.header("Data Model")
    st.text("This is a demo database based on public amazon reviews")
    st.markdown("""
    - Table feedback: 413k feedback records (2002-2023). Ratings from 1-5 stars.
    - Table products: 8,145 products
    - Table company: 12 companies (Apple, Samsung, Sony, Nike, etc.)
    """)
    
    # Show database status
    if os.path.exists(DATABASE_FILE):
        file_size = os.path.getsize(DATABASE_FILE) / (1024*1024)
        st.sidebar.markdown(f"**Status:** ✅ Database Connected")
    else:
        st.sidebar.markdown("**Status:** ❌ Database Not Available")

    # Clear chat button in sidebar
    st.markdown("---")
    if st.button("🗑️ Clear & Start New Chat", use_container_width=True, type="secondary"):
        st.session_state.messages = []
        st.session_state.thread_id = str(uuid.uuid4())

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
      try:
        # Create containers for progress updates
        status_placeholder = st.empty()
        progress_placeholder = st.empty()
        response_container = st.container()

        # Convert chat history to LangGraph format
        messages_log = []
        for msg in st.session_state.messages[:-1]:  # exclude current user prompt
            if msg["role"] == "user":
                messages_log.append(HumanMessage(content=msg["content"]))
            else:
                messages_log.append(AIMessage(content=msg["content"]))

        # Prepare agent input state
        if len(messages_log) == 0:  # first message
            state_dict = {
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
        else:  # continuation
            state_dict = {
                'current_question': prompt
            }
            config, _ = create_config('Run Agent', False, st.session_state.thread_id)

        # Start streaming
        progress_log = []
        final_state = None

        for step in graph.stream(state_dict, config=config, stream_mode="updates"):
            step_name, output = list(step.items())[0]
            final_state = output  # Keep most recent full state

            # Check for progress message from db_agent_v1.show_progress()
            while not db_agent_v1._progress_queue.empty():
                try:
                    msg = db_agent_v1._progress_queue.get_nowait()
                    progress_log.append(msg)

                    if msg.startswith('✅'):
                        status_placeholder.text(msg)
                    elif msg.startswith('⚙️'):
                        status_placeholder.text(msg)
                    elif msg.startswith('🔧'):
                        status_placeholder.text(msg)
                    elif msg.startswith('⚠️'):
                        status_placeholder.text(msg)
                    elif msg.startswith('📣'):
                        status_placeholder.text(msg)
                    else:
                        status_placeholder.text(msg)

                except queue.Empty:
                    pass

        # Final response
        response_container.empty()
        with response_container:
            final_response = final_state["llm_answer"].content
            st.markdown(final_response)
            st.session_state.messages.append({"role": "assistant", "content": final_response})

      except Exception as e:
        error_msg = f"Sorry, I encountered an error: {str(e)}"
        st.error(error_msg)
        st.session_state.messages.append({"role": "assistant", "content": error_msg})