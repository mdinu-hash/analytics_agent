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
    layout="wide"
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

# Configure Streamlit page
st.set_page_config(
    page_title="Analytics Agent", 
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

/* Tab styling to match gradient background */
.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
    background: transparent;
}

.stTabs [data-baseweb="tab"] {
    background: rgba(255, 255, 255, 0.1) !important;
    color: #ffffff !important;
    border: 1px solid rgba(255, 255, 255, 0.2) !important;
    border-radius: 8px !important;
    padding: 8px 16px !important;
    font-weight: 500 !important;
    font-family: 'Maven Pro', sans-serif !important;
    backdrop-filter: blur(10px);
}

.stTabs [data-baseweb="tab"]:hover {
    background: rgba(255, 255, 255, 0.2) !important;
    color: #ffffff !important;
    border-color: rgba(255, 255, 255, 0.4) !important;
}

.stTabs [aria-selected="true"] {
    background: rgba(255, 255, 255, 0.25) !important;
    color: #ffffff !important;
    border-color: rgba(255, 255, 255, 0.5) !important;
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

# Sample data for tables
def create_sample_data():
    """Create sample data for each table"""
    # Household table
    household_data = pd.DataFrame({
        'household_id': ['HH001', 'HH002', 'HH003'],
        'household_tenure': [5, 12, 3],
        'household_registration_type': ['Online', 'Branch', 'Online'],
        'household_segment': ['Premium', 'Standard', 'Basic']
    })
    
    # Advisor table
    advisor_data = pd.DataFrame({
        'advisor_id': ['ADV001', 'ADV002', 'ADV003'],
        'business_line_name': ['Wealth Management', 'Investment Advisory', 'Private Banking'],
        'account_type': ['Managed', 'Advisory', 'Discretionary'],
        'account_custodian': ['Internal', 'External', 'Internal'],
        'account_risk_profile': ['Conservative', 'Moderate', 'Aggressive']
    })
    
    # Product table
    product_data = pd.DataFrame({
        'product_id': ['PROD001', 'PROD002', 'PROD003'],
        'asset_category': ['Equity', 'Fixed Income', 'Alternative'],
        'asset_subcategory': ['Large Cap', 'Government Bonds', 'Real Estate'],
        'product_line': ['Mutual Funds', 'ETFs', 'REITs'],
        'product_name': ['Growth Fund A', 'Treasury Bond ETF', 'Commercial REIT']
    })
    
    # Fact account monthly table
    fact_account_monthly_data = pd.DataFrame({
        'snapshot_date': ['2024-01-01', '2024-01-01', '2024-01-01'],
        'account_id': ['ACC001', 'ACC002', 'ACC003'],
        'account_monthly_return': [0.05, -0.02, 0.03],
        'account_net_flow': [10000, -5000, 7500],
        'account_assets': [250000, 150000, 300000],
        'advisor_id': ['ADV001', 'ADV002', 'ADV001'],
        'household_id': ['HH001', 'HH002', 'HH003'],
        'business_line_name': ['Wealth Management', 'Investment Advisory', 'Wealth Management']
    })
    
    # Fact account product monthly table
    fact_account_product_monthly_data = pd.DataFrame({
        'snapshot_date': ['2024-01-01', '2024-01-01', '2024-01-01'],
        'account_id': ['ACC001', 'ACC002', 'ACC003'],
        'product_name': ['Growth Fund A', 'Treasury Bond ETF', 'Growth Fund A'],
        'product_allocation_pct': [0.65, 0.40, 0.45]
    })
    
    # Fact revenue monthly table
    fact_revenue_monthly_data = pd.DataFrame({
        'snapshot_date': ['2024-01-01', '2024-01-01', '2024-01-01'],
        'account_id': ['ACC001', 'ACC002', 'ACC003'],
        'advisor_id': ['ADV001', 'ADV002', 'ADV001'],
        'household_id': ['HH001', 'HH002', 'HH003'],
        'business_line_name': ['Wealth Management', 'Investment Advisory', 'Wealth Management'],
        'gross_fee_amount': [2500, 1800, 3200],
        'third_party_fee': [150, 120, 200],
        'net_revenue': [2350, 1680, 3000]
    })
    
    # Fact customer feedback table
    fact_customer_feedback_data = pd.DataFrame({
        'feedback_date': ['2024-01-15', '2024-01-20', '2024-01-25'],
        'household_id': ['HH001', 'HH002', 'HH003'],
        'advisor_id': ['ADV001', 'ADV002', 'ADV001'],
        'feedback_text': ['Very satisfied with service', 'Could improve response time', 'Excellent portfolio performance'],
        'satisfaction_score': [9, 6, 10]
    })
    
    return {
        'household': household_data,
        'advisor': advisor_data,
        'product': product_data,
        'fact_account_monthly': fact_account_monthly_data,
        'fact_account_product_monthly': fact_account_product_monthly_data,
        'fact_revenue_monthly': fact_revenue_monthly_data,
        'fact_customer_feedback': fact_customer_feedback_data
    }

with tab2:
    st.markdown("### Database Tables Overview")
    st.markdown("Here are the tables available to the Analytics Agent with sample data:")
    
    sample_data = create_sample_data()
    
    # Display each table
    for table_name, data in sample_data.items():
        st.markdown(f"#### {table_name.replace('_', ' ').title()}")
        st.dataframe(data, use_container_width=True)
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
            if st.button("Which household segment is responsible for most revenue?", key="btn1", help="Click to use this prompt", use_container_width=True):
                st.session_state.selected_prompt = "Which household segment is responsible for most revenue?"
                st.rerun()
            if st.button("For advisor ID 8, show their product usage status, total assets and payout.", key="btn2", help="Click to use this prompt", use_container_width=True):
                st.session_state.selected_prompt = "For advisor ID 8, show their product usage status, total assets and payout."
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