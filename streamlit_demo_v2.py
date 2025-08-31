import streamlit as st
import os
import uuid
import time
from pathlib import Path
from langchain_core.messages import HumanMessage, AIMessage
import threading
import queue
import sys

# Add the current directory to Python path to ensure imports work
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# Database connection will be handled by db_agent_v2's PostgreSQL database manager
# No need for database download functionality as we're using PostgreSQL

# Try to import agent components safely
try:
    # Import the individual components we need from db_agent_v2
    import db_agent_v2
    
    # Get the components after database manager is available
    graph = db_agent_v2.graph
    create_config = db_agent_v2.create_config
    objects_documentation = db_agent_v2.objects_documentation
    database_content = db_agent_v2.database_content
    sql_dialect = db_agent_v2.sql_dialect
    
except Exception as e:
    st.error(f"❌ Failed to import agent: {e}")
    st.info("Make sure db_agent_v2.py is in the same directory and the PostgreSQL connection is configured properly.")
    st.stop()

progress_queue = queue.Queue()
db_agent_v2.set_progress_queue(progress_queue)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "thread_id" not in st.session_state:
    st.session_state.thread_id = str(uuid.uuid4())

# Configure Streamlit page
st.set_page_config(
    page_title="Growth Analytics Agent", 
    page_icon="🤖", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Sidebar for new chat and navigation
with st.sidebar:
    st.header("Growth Analytics Agent")
    
    # New Chat button
    if st.button("🆕 New Chat", use_container_width=True, type="primary"):
        st.session_state.messages = []
        st.session_state.thread_id = str(uuid.uuid4())
        st.rerun()
    
    st.divider()
    
    # Database info
    st.subheader("📊 Dataset Info")
    st.info("**Amazon Reviews (2002-2023)**")
    
    with st.expander("📋 Data Details"):
        st.write("• **413k** feedback records")
        st.write("• **8,145** products") 
        st.write("• **12** companies")
        st.write("• Ratings: 1-5 stars")
        st.write("• Companies: Apple, Samsung, Sony, Nike, Adidas, etc.")
    
    # Connection status
    try:
        db_manager = db_agent_v2.get_database_manager()
        st.success("✅ Database Connected")
    except Exception as e:
        st.error("❌ Database Disconnected")

# Main content area
st.title("🔍 Ask anything about your data")

# Show welcome screen with example prompts if no messages
if not st.session_state.messages:
    st.markdown("### 💡 Try these example questions:")
    
    # Create columns for example prompts
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔍 Root Cause Analysis", use_container_width=True):
            prompt = "Why did adidas ratings decrease in early 2016 from january to may?"
            st.session_state.messages.append({"role": "user", "content": prompt})
            st.rerun()
        st.caption("Why did adidas ratings decrease in early 2016?")
        
        if st.button("📊 Time Trends", use_container_width=True):
            prompt = "how these ratings changed over time per company?"
            st.session_state.messages.append({"role": "user", "content": prompt})
            st.rerun()
        st.caption("How did ratings change over time per company?")
    
    with col2:
        if st.button("📈 Key Drivers", use_container_width=True):
            prompt = "which companies contributed to the increase in ratings from September 2022?"
            st.session_state.messages.append({"role": "user", "content": prompt})
            st.rerun()
        st.caption("Which companies drove rating improvements since 2022?")
        
        if st.button("⚖️ Comparative Analysis", use_container_width=True):
            prompt = "Are premium-priced products getting better ratings than budget products?"
            st.session_state.messages.append({"role": "user", "content": prompt})
            st.rerun()
        st.caption("Do premium products get better ratings?")
    
    st.divider()
    st.markdown("💬 **Or type your own question in the chat below!**")

# Display chat messages
for i, message in enumerate(st.session_state.messages):
    if message["role"] == "user":
        with st.chat_message("user", avatar="👤"):
            st.write(message["content"])
    else:
        with st.chat_message("assistant", avatar="🤖"):
            st.write(message["content"])

# Chat input
if prompt := st.chat_input("Ask anything about your data..."):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Display user message
    with st.chat_message("user", avatar="👤"):
        st.write(prompt)
    
    # Generate response
    with st.chat_message("assistant", avatar="🤖"):
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

            # Check for progress message from db_agent_v2.show_progress()
            while not progress_queue.empty():
                try:
                    msg = progress_queue.get_nowait()
                    progress_log.append(msg)

                    if msg.startswith('✅'):
                        status_placeholder.success(msg)
                    elif msg.startswith('⚙️'):
                        status_placeholder.info(msg)
                    elif msg.startswith('🔧'):
                        status_placeholder.warning(msg)
                    elif msg.startswith('⚠️'):
                        status_placeholder.error(msg)
                    elif msg.startswith('📣'):
                        status_placeholder.info(msg)
                    else:
                        status_placeholder.text(msg)

                except queue.Empty:
                    pass

        # Final response
        response_container.empty()
        with response_container:
            final_response = final_state["llm_answer"].content
            st.write(final_response)
            st.session_state.messages.append({"role": "assistant", "content": final_response})

      except Exception as e:
        error_msg = f"Sorry, I encountered an error: {str(e)}"
        st.error(error_msg)
        st.session_state.messages.append({"role": "assistant", "content": error_msg})