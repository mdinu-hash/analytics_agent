import streamlit as st
import os
import uuid
import time
from pathlib import Path
from langchain_core.messages import HumanMessage, AIMessage
import threading
import queue
import sys

# Configure page without sidebar
st.set_page_config(
    page_title="Growth Analytics Agent",
    layout="wide"
)

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
if "show_welcome" not in st.session_state:
    st.session_state.show_welcome = True

# Configure Streamlit page
st.set_page_config(
    page_title="Growth Analytics Agent", 
    page_icon="🤖", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS matching the UI specs - Databricks compatible
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Maven+Pro:wght@400;500;600;700&display=swap');

/* Global app styling */
.stApp {
    font-family: 'Maven Pro', -apple-system, BlinkMacSystemFont, sans-serif;
    background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%) !important;
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
    background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%) !important;
}

/* Header section */
.main-header {
    background: inherit !important;
    border-bottom: 1px solid rgba(255, 255, 255, 0.2);
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
    background: inherit !important;
    padding: 24px 0;
    margin: 0;
}
.ai-message {
    background: inherit !important;
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
    color: #ffffff;
    background: inherit !important;
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
    <div class="header-title">Growth Analytics Agent</div>
    <div class="dataset-badge">Amazon Reviews (2002-2023)</div>
</div>
""", unsafe_allow_html=True)

# Clear & New Chat button under subtitle
if st.button("🗑️ Clear & New Chat", key="clear_new_chat_btn", help="Clear current conversation and start fresh"):
    st.session_state.messages = []
    st.session_state.thread_id = str(uuid.uuid4())
    st.session_state.show_welcome = True
    st.rerun()

# Show welcome content only if in welcome mode AND no messages exist
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
        if st.button("Why did adidas ratings decrease in early 2016?", key="btn1", help="Click to use this prompt", use_container_width=True):
            st.session_state.selected_prompt = "Why did adidas ratings decrease in early 2016 from january to may?"
            st.rerun()
        if st.button("Which companies drove rating improvements since 2022?", key="btn2", help="Click to use this prompt", use_container_width=True):
            st.session_state.selected_prompt = "which companies contributed to the increase in ratings from September 2022?"
            st.rerun()
        if st.button("How did ratings change over time per company?", key="btn3", help="Click to use this prompt", use_container_width=True):
            st.session_state.selected_prompt = "how these ratings changed over time per company?"
            st.rerun()

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
    st.session_state.show_welcome = False
    st.session_state.messages.append({"role": "user", "content": prompt})

# Input area - ALWAYS visible regardless of state
st.markdown('<div class="input-container">', unsafe_allow_html=True)
col1, col2 = st.columns([8, 1])
with col1:
    user_input = st.text_input("", placeholder="Ask anything about your data...", 
                              label_visibility="collapsed", key="chat_input")
with col2:
    send_clicked = st.button("Send", use_container_width=True, key="send_button")
st.markdown('</div>', unsafe_allow_html=True)

# Handle send button click
if send_clicked and user_input:
    st.session_state.show_welcome = False
    st.session_state.messages.append({"role": "user", "content": user_input})
    prompt = user_input
    st.session_state.chat_input = ""  # Clear input
    st.rerun()

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