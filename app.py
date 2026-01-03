import streamlit as st
import os
import uuid
import time
from pathlib import Path
from langchain_core.messages import HumanMessage, AIMessage
import threading
import queue
import sys
import pandas as pd

# Configure page without sidebar
st.set_page_config(
    page_title="Analytics Agent",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add the current directory to Python path to ensure imports work
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# Database connection will be handled by agent's PostgreSQL database manager
# No need for database download functionality as we're using PostgreSQL

# Try to import agent components safely
try:
    # Import the individual components we need from agent
    import agent
    
    # Get the components after database manager is available
    graph = agent.graph
    create_config = agent.create_config
    objects_documentation = agent.objects_documentation
    sql_dialect = agent.sql_dialect
    
except Exception as e:
    st.error(f"❌ Failed to import agent: {e}")
    st.info("Make sure agent.py is in the same directory and the PostgreSQL connection is configured properly.")
    st.stop()

progress_queue = queue.Queue()
agent.set_progress_queue(progress_queue)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "thread_id" not in st.session_state:
    st.session_state.thread_id = str(uuid.uuid4())
if "show_welcome" not in st.session_state:
    st.session_state.show_welcome = True

# Custom CSS matching the UI specs - Databricks compatible
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Maven+Pro:wght@400;500;600;700&display=swap');

/* Global app styling */
.stApp {
    font-family: 'Maven Pro', -apple-system, BlinkMacSystemFont, sans-serif;
    background: #ffffff !important;
}

/* Hide default streamlit elements */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
.css-1rs6os {visibility: hidden;}
.css-17ziqus {visibility: hidden;}

/* Sidebar styling - Light theme */
.stSidebar, .css-1d391kg, .css-1lcbmhc, .css-k1vhr4, [data-testid="stSidebar"] {
    background-color: #f8fafc !important;
    width: 260px !important;
}
.stSidebar > div, .css-1d391kg > div, .css-1lcbmhc > div, .css-k1vhr4 > div {
    background-color: #f8fafc !important;
}
/* Ensure sidebar is visible */
.stSidebar, [data-testid="stSidebar"] {
    display: block !important;
    visibility: visible !important;
}

/* Main content area styling */
.main .block-container {
    padding: 0 !important;
    max-width: 100% !important;
    background: #ffffff !important;
}

/* Header section */
.main-header {
    background: #ffffff !important;
    border-bottom: 1px solid #e5e7eb;
    padding: 24px;
    margin: 0;
}
.header-title {
    font-size: 24px;
    font-weight: 600;
    color: #000000;
    margin: 0;
    text-align: left;
    font-family: 'Maven Pro', -apple-system, BlinkMacSystemFont, sans-serif;
}
.dataset-badge {
    background: none;
    border: none;
    border-radius: 0;
    padding: 0;
    font-size: 16px;
    color: #000000;
    font-weight: 400;
    display: block;
    margin-top: 4px;
    text-align: left;
}

/* Welcome screen styling */
.welcome-container {
    text-align: center;
    max-width: 600px;
    margin: 60px auto;
    padding: 0 40px;
}
.welcome-title {
    font-size: 32px;
    font-weight: 600;
    color: #000000;
    margin-bottom: 40px;
    font-family: 'Maven Pro', -apple-system, BlinkMacSystemFont, sans-serif;
}
.example-prompts {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 12px;
    margin-bottom: 40px;
    max-width: 400px;
    margin-left: auto;
    margin-right: auto;
}

/* Chat message styling */
.user-message {
    background: #ffffff !important;
    padding: 24px 0;
    margin: 0;
}
.ai-message {
    background: #ffffff !important;
    padding: 24px 0;
    margin: 0;
}
.message-content {
    max-width: 800px;
    margin: 0 auto;
    display: flex;
    gap: 16px;
    padding: 0 24px;
}
.message-avatar {
    width: 32px;
    height: 32px;
    border-radius: 4px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 14px;
    font-weight: 500;
    flex-shrink: 0;
    color: white;
}
.user-avatar {
    background: #0d9488;
}
.ai-avatar {
    background: #8b5cf6;
}
.message-text {
    flex: 1;
    line-height: 1.6;
    font-size: 16px;
    color: #000000;
    background: #ffffff !important;
}

/* No chat input CSS - let Streamlit handle everything */

/* Custom input styling without form */
.element-container:has(input[data-testid="stTextInput"]) input {
    border: 1px solid #d1d5db !important;
    border-radius: 12px !important;
    padding: 14px 16px !important;
    font-size: 16px !important;
    background: white !important;
    min-height: 48px !important;
    font-family: 'Maven Pro', sans-serif !important;
}

.element-container:has(input[data-testid="stTextInput"]) input:focus {
    border-color: #8b5cf6 !important;
    box-shadow: 0 0 0 3px rgba(139, 92, 246, 0.1) !important;
    outline: none !important;
}

/* Send button styling */
button[key="send_btn"] {
    border-radius: 12px !important;
    background: #8b5cf6 !important;
    color: white !important;
    border: none !important;
    min-height: 48px !important;
    font-size: 16px !important;
    font-weight: 600 !important;
    font-family: 'Maven Pro', sans-serif !important;
    cursor: pointer !important;
}

button[key="send_btn"]:hover {
    background: #7c3aed !important;
}

/* Input container to match example prompts width */
.input-container {
    max-width: 600px;
    margin: 0 auto;
    padding: 20px 0;
}

/* Sidebar elements */
.stButton > button {
    font-family: 'Maven Pro', sans-serif;
    background: #ffffff !important;
    border: 1px solid #e5e7eb !important;
    color: #374151 !important;
    font-size: 14px !important;
}
.stButton > button:hover {
    background: #f3f4f6 !important;
}

/* Hide sidebar completely on all devices */
.stSidebar, [data-testid="stSidebar"] {
    display: none !important;
}

/* Remove unused mobile button CSS */

/* Loading animation */
.loading-dots {
    display: flex;
    gap: 4px;
    justify-content: center;
    padding: 20px;
}
.loading-dot {
    width: 8px;
    height: 8px;
    background: #6b7280;
    border-radius: 50%;
    animation: bounce 1.4s infinite ease-in-out both;
}
.loading-dot:nth-child(1) { animation-delay: -0.32s; }
.loading-dot:nth-child(2) { animation-delay: -0.16s; }
@keyframes bounce {
    0%, 80%, 100% { transform: scale(0); }
    40% { transform: scale(1); }
}

/* Tab styling */
.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
    background: transparent;
}

.stTabs [data-baseweb="tab"] {
    background: #f3f4f6 !important;
    color: #374151 !important;
    border: 1px solid #e5e7eb !important;
    border-radius: 8px !important;
    padding: 8px 16px !important;
    font-weight: 500 !important;
    font-family: 'Maven Pro', sans-serif !important;
}

.stTabs [data-baseweb="tab"]:hover {
    background: #e5e7eb !important;
    color: #1f2937 !important;
    border-color: #d1d5db !important;
}

.stTabs [aria-selected="true"] {
    background: #8b5cf6 !important;
    color: #ffffff !important;
    border-color: #8b5cf6 !important;
    font-weight: 600 !important;
}

.stTabs [data-baseweb="tab-panel"] {
    background: transparent;
    padding: 0;
}

/* Remove the red underline indicator */
.stTabs [data-baseweb="tab-highlight"] {
    background-color: rgba(255, 255, 255, 0.6) !important;
    height: 2px !important;
    border-radius: 1px !important;
}

/* Alternative: completely hide the underline indicator */
.stTabs [data-baseweb="tab-border"] {
    display: none !important;
}

/* Responsive design */

/* Small laptops and tablets (768px - 1024px) */
@media (max-width: 1024px) {
    .header-title {
        font-size: 22px;
    }
    .welcome-title {
        font-size: 28px;
        margin-bottom: 32px;
    }
    .welcome-container {
        max-width: 500px;
        padding: 0 32px;
        margin: 50px auto;
    }
    .main-header {
        padding: 20px;
    }
    .message-content {
        padding: 0 20px;
    }
    .stChatInput > div {
        padding: 0 20px !important;
    }
}

/* Tablets (768px and below) */
@media (max-width: 768px) {
    .header-title {
        font-size: 20px;
    }
    .dataset-badge {
        font-size: 12px;
    }
    .welcome-title {
        font-size: 24px;
        margin-bottom: 24px;
    }
    .example-prompts {
        gap: 10px;
        max-width: 350px;
    }
    .welcome-container {
        padding: 0 24px;
        margin: 32px auto;
    }
    .main-header {
        padding: 16px;
    }
    .message-content {
        padding: 0 16px;
        gap: 12px;
    }
    .message-avatar {
        width: 28px;
        height: 28px;
        font-size: 12px;
    }
    .message-text {
        font-size: 14px;
    }
    .stChatInput > div {
        padding: 16px !important;
    }
}

/* Mobile phones (480px and below) */
@media (max-width: 480px) {
    .header-title {
        font-size: 18px;
    }
    .dataset-badge {
        font-size: 11px;
    }
    .welcome-title {
        font-size: 20px;
        margin-bottom: 20px;
    }
    .example-prompts {
        max-width: 300px;
        gap: 8px;
    }
    .welcome-container {
        padding: 0 16px;
        margin: 24px auto;
    }
    .main-header {
        padding: 12px;
    }
    .message-content {
        padding: 0 12px;
        gap: 10px;
    }
    .message-avatar {
        width: 24px;
        height: 24px;
        font-size: 10px;
    }
    .message-text {
        font-size: 13px;
        line-height: 1.5;
    }
    .stChatInput > div {
        padding: 12px !important;
    }
    .ai-message, .user-message {
        padding: 16px 0;
    }
    
    /* Mobile input styling */
    .element-container:has(input[data-testid="stTextInput"]) input {
        border: 1px solid #d1d5db !important;
        border-radius: 12px !important;
        padding: 12px 16px !important;
        font-size: 14px !important;
        background: white !important;
        min-height: 44px !important;
    }
    
    button[key="send_btn"] {
        border-radius: 12px !important;
        background: #8b5cf6 !important;
        color: white !important;
        border: none !important;
        min-height: 44px !important;
        font-size: 14px !important;
        font-weight: 600 !important;
    }
    
    button[key="send_btn"]:hover {
        background: #7c3aed !important;
    }
    
    /* Make prompt examples much smaller on mobile - smaller than welcome title (20px) */
    .example-prompt-btn {
        font-size: 9px !important;
        padding: 6px 10px !important;
        line-height: 1.2 !important;
    }
}
</style>
""", unsafe_allow_html=True)

# Main content area
# Header section
st.markdown("""
<div class="main-header">
    <div class="header-title">Analytics Agent</div>
</div>
""", unsafe_allow_html=True)

# Create tabs
tab1, tab2 = st.tabs(["Agent Chat", "Tables"])

# Fetch table data from database
def get_table_data_from_db():
    """Fetch actual data from database tables"""
    import psycopg2

    tables_data = {}
    tables_to_show = ['account', 'advisors', 'household', 'fact_account_monthly']

    try:
        conn = psycopg2.connect(agent.connection_string)
        cursor = conn.cursor()

        for table_name in tables_to_show:
            # Get first 3 rows from each table
            query = f"SELECT * FROM {table_name} LIMIT 3"
            cursor.execute(query)

            # Get column names from cursor
            columns = [desc[0] for desc in cursor.description]

            # Fetch results
            results = cursor.fetchall()

            if results:
                # Convert to DataFrame with proper column names
                df = pd.DataFrame(results, columns=columns)
                tables_data[table_name] = df

        cursor.close()
        conn.close()
    except Exception as e:
        st.error(f"Error loading table data: {e}")

    return tables_data

def get_table_counts():
    """Get record counts for each table"""
    import psycopg2

    counts = {}
    tables_to_show = ['account', 'advisors', 'household', 'fact_account_monthly']

    try:
        conn = psycopg2.connect(agent.connection_string)
        cursor = conn.cursor()

        for table_name in tables_to_show:
            query = f"SELECT COUNT(*) FROM {table_name}"
            cursor.execute(query)
            result = cursor.fetchone()

            if result:
                counts[table_name] = result[0]
            else:
                counts[table_name] = 0

        cursor.close()
        conn.close()
    except Exception as e:
        st.error(f"Error getting table counts: {e}")
        counts = {table: 0 for table in tables_to_show}

    return counts

with tab2:
    st.markdown("### Database Tables Overview")

    # Get table counts and data
    table_counts = get_table_counts()
    tables_data = get_table_data_from_db()

    # Define display order and formatting
    table_display_config = {
        'account': 'Account',
        'advisors': 'Advisors',
        'household': 'Household',
        'fact_account_monthly': 'Fact Account Monthly: Monthly account performance and asset data'
    }

    # Display each table
    for table_key, display_name in table_display_config.items():
        if table_key in tables_data:
            count = table_counts.get(table_key, 0)
            st.markdown(f"#### {display_name}: {count:,} entries")
            st.dataframe(tables_data[table_key], use_container_width=True)
            st.markdown("---")

with tab1:
    # Add some spacing before Clear & New Chat button
    st.markdown("<br>", unsafe_allow_html=True)

    # Clear & New Chat button with more spacing from header
    if st.button("🗑️ Clear & New Chat", key="clear_new_chat_btn", help="Clear current conversation and start fresh"):
        st.session_state.messages = []
        st.session_state.thread_id = str(uuid.uuid4())
        st.session_state.show_welcome = True
        st.rerun()

    # Welcome screen section moved to after prompt processing

    # Display chat messages with ChatGPT-style layout
    for message in st.session_state.messages:
        if message["role"] == "user":
            st.markdown(f"""
            <div class="user-message">
                <div class="message-content">
                    <div class="message-avatar user-avatar">You</div>
                    <div class="message-text">{message["content"]}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="ai-message">
                <div class="message-content">
                    <div class="message-avatar ai-avatar">AI</div>
                    <div class="message-text">{message["content"]}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    # Simple approach: Input always visible, handle messages cleanly
    prompt = None

    # Check if example prompt was selected
    if st.session_state.get("selected_prompt", ""):
        prompt = st.session_state.selected_prompt
        st.session_state.selected_prompt = ""
        st.session_state.messages.append({"role": "user", "content": prompt})

    # Check for pending prompt from chat input at bottom
    if st.session_state.get("pending_prompt"):
        prompt = st.session_state.pending_prompt
        del st.session_state.pending_prompt  # Clear pending prompt
        # Add user message for chat input (not already added like example prompts)
        st.session_state.messages.append({"role": "user", "content": prompt})
    elif prompt:
        pass  # prompt from example selection (already added to messages)
    else:
            prompt = None

    if prompt:
        # Message already added above, proceed with processing
        
        # Display user message
        st.markdown(f"""
        <div class="user-message">
            <div class="message-content">
                <div class="message-avatar user-avatar">You</div>
                <div class="message-text">{prompt}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Generate response with loading indicator
        loading_placeholder = st.empty()
        loading_placeholder.markdown("""
        <div class="ai-message">
            <div class="message-content">
                <div class="message-avatar ai-avatar">AI</div>
                <div class="message-text">
                    <div class="loading-dots">
                        <div class="loading-dot"></div>
                        <div class="loading-dot"></div>
                        <div class="loading-dot"></div>
                    </div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        try:
            # Convert chat history to LangGraph format
            messages_log = []
            for msg in st.session_state.messages:  # include all messages including current prompt
                if msg["role"] == "user":
                    messages_log.append(HumanMessage(content=msg["content"]))
                else:
                    messages_log.append(AIMessage(content=msg["content"]))

            # Prepare agent input state
            if len(st.session_state.messages) == 1:  # first message (only current user message)
                state_dict = {
                    'objects_documentation': objects_documentation,
                    'sql_dialect': sql_dialect,
                    'messages_log': messages_log,
                    'intermediate_steps': [],
                    'analytical_intent': [],
                    'current_question': prompt,
                    'current_sql_queries': [],
                    'generate_answer_details': {
                        'key_assumptions': [],
                        'agent_questions': [],
                        'ambiguity_explanation': ''
                    },
                    'llm_answer': AIMessage(content=''),
                    'scenario': ''
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

                # Check for progress message from agent.show_progress()
                while not progress_queue.empty():
                    try:
                        msg = progress_queue.get_nowait()
                        progress_log.append(msg)
                        # Show progress in loading area
                    except queue.Empty:
                        pass

            # Display final response
            loading_placeholder.empty()
            final_response = final_state["llm_answer"].content
            st.markdown(f"""
            <div class="ai-message">
                <div class="message-content">
                    <div class="message-avatar ai-avatar">AI</div>
                    <div class="message-text">{final_response}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            st.session_state.messages.append({"role": "assistant", "content": final_response})

        except Exception as e:
            loading_placeholder.empty()
            error_msg = f"Sorry, I encountered an error: {str(e)}"
            st.markdown(f"""
            <div class="ai-message">
                <div class="message-content">
                    <div class="message-avatar ai-avatar">AI</div>
                    <div class="message-text" style="color: #dc2626;">{error_msg}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            st.session_state.messages.append({"role": "assistant", "content": error_msg})

    # Show welcome content after prompt processing (so it can hide when messages exist)
    if st.session_state.show_welcome and not st.session_state.messages:
        st.markdown("""
        <div class="welcome-container">
            <div class="welcome-title">Ask anything about your data</div>
            <div class="example-prompts" id="example-prompts-container">
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Initialize example prompt selection state
        if "selected_prompt" not in st.session_state:
            st.session_state.selected_prompt = ""
        
        # Example prompt buttons for functionality - 3 prompts stacked vertically and centered
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("Which practice segment has the highest asset growth rate (%) in last 12 months?", key="btn1", help="Click to use this prompt", use_container_width=True):
                st.session_state.selected_prompt = "Which practice segment has the highest asset growth rate (%) in last 12 months?"
                st.rerun()

            if st.button("Which segment has more room to grow?", key="btn2", help="Click to use this prompt", use_container_width=True):
                st.session_state.selected_prompt = "Which segment has more room to grow?"
                st.rerun()

            if st.button("Which households have the higher pay grade?", key="btn3", help="Click to use this prompt", use_container_width=True):
                st.session_state.selected_prompt = "Which households have the higher pay grade?"
                st.rerun()

    # Define callback function for immediate welcome screen hiding
    def handle_chat_submit():
        st.session_state.show_welcome = False

    # Chat input at bottom - appears in same position during welcome and chat phases
    col1, col2, col3 = st.columns([1, 2, 1])  # Center column for input
    with col2:
        if new_prompt := st.chat_input("Ask anything about your data...", on_submit=handle_chat_submit):
            # Set prompt for processing and rerun to trigger agent response
            st.session_state.pending_prompt = new_prompt
            st.rerun()