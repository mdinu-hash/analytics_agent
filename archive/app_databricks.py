import streamlit as st
import os
import requests
from typing import Dict, Any

# Configure page
st.set_page_config(
    page_title="Analytics Agent",
    layout="wide"
)

# Simple white background, black text CSS
st.markdown("""
<style>
/* Global app styling - Simple white background */
.stApp {
    background-color: #ffffff !important;
}

/* Hide default streamlit elements */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}

/* Main content area */
.main .block-container {
    padding: 2rem;
    max-width: 1200px;
    margin: 0 auto;
}

/* Header */
.main-header {
    padding: 1rem 0;
    margin-bottom: 2rem;
    border-bottom: 1px solid #e5e7eb;
}

.header-title {
    font-size: 28px;
    font-weight: 600;
    color: #000000;
}

/* Chat messages */
.chat-message {
    padding: 1rem;
    margin: 1rem 0;
    border-radius: 8px;
}

.user-message {
    background-color: #f3f4f6;
}

.assistant-message {
    background-color: #f9fafb;
    border: 1px solid #e5e7eb;
}

.message-role {
    font-weight: 600;
    margin-bottom: 0.5rem;
    color: #374151;
}

.message-content {
    color: #000000;
    line-height: 1.6;
}

/* Buttons */
.stButton > button {
    background-color: #ffffff;
    color: #000000;
    border: 1px solid #d1d5db;
    border-radius: 6px;
}

.stButton > button:hover {
    background-color: #f3f4f6;
    border-color: #9ca3af;
}

/* Input */
.stChatInput {
    border-color: #d1d5db !important;
}

/* Loading spinner */
.stSpinner > div {
    border-color: #d1d5db !important;
    border-top-color: #000000 !important;
}
</style>
""", unsafe_allow_html=True)

# Databricks serving endpoint configuration
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")  # e.g., "https://your-workspace.cloud.databricks.com"
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")  # Personal access token
ENDPOINT_NAME = os.environ.get("ENDPOINT_NAME", "analytics-agent-endpoint")

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "thread_id" not in st.session_state:
    st.session_state.thread_id = None

def query_serving_endpoint(user_message: str, thread_id: str = None) -> Dict[str, Any]:
    """
    Call the Databricks serving endpoint with thread management for conversation memory.

    Args:
        user_message: The user's question
        thread_id: Optional thread ID for conversation continuity. If None, starts new conversation.

    Returns:
        Dict with 'success' (bool), 'response' (str), 'thread_id' (str), and optional 'error' (str)
    """
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        return {
            "success": False,
            "error": "DATABRICKS_HOST and DATABRICKS_TOKEN environment variables must be set"
        }

    url = f"{DATABRICKS_HOST}/serving-endpoints/{ENDPOINT_NAME}/invocations"

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    # Format request to match ResponsesAgent format
    # Include thread_id if provided (for follow-up questions)
    if thread_id:
        payload = {
            "input": [
                {
                    "role": "user",
                    "content": user_message
                },
                {
                    "role": "system",
                    "content": f"thread_id:{thread_id}"
                }
            ]
        }
    else:
        payload = {
            "input": [{
                "role": "user",
                "content": user_message
            }]
        }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=120)
        response.raise_for_status()

        # Parse response
        result = response.json()
        response_text = result["output"][0]["content"][0]["text"]

        # Extract thread_id from response metadata
        returned_thread_id = None
        if len(result["output"][0]["content"]) > 1:
            metadata = result["output"][0]["content"][1]["text"]
            if metadata.startswith("thread_id:"):
                returned_thread_id = metadata.split("thread_id:", 1)[1]

        return {
            "success": True,
            "response": response_text,
            "thread_id": returned_thread_id
        }

    except requests.exceptions.Timeout:
        return {
            "success": False,
            "error": "Request timed out after 120 seconds"
        }
    except requests.exceptions.RequestException as e:
        return {
            "success": False,
            "error": f"Request failed: {str(e)}"
        }
    except (KeyError, IndexError) as e:
        return {
            "success": False,
            "error": f"Failed to parse response: {str(e)}"
        }

# Header
st.markdown("""
<div class="main-header">
    <div class="header-title">Analytics Agent</div>
</div>
""", unsafe_allow_html=True)

# Clear chat button
if st.button("🗑️ Clear Chat", help="Clear conversation history"):
    st.session_state.messages = []
    st.session_state.thread_id = None
    st.rerun()

st.markdown("<br>", unsafe_allow_html=True)

# Display chat messages
for message in st.session_state.messages:
    role_class = "user-message" if message["role"] == "user" else "assistant-message"
    role_label = "You" if message["role"] == "user" else "Assistant"

    st.markdown(f"""
    <div class="chat-message {role_class}">
        <div class="message-role">{role_label}</div>
        <div class="message-content">{message["content"]}</div>
    </div>
    """, unsafe_allow_html=True)

# Chat input
if prompt := st.chat_input("Ask anything about your data..."):
    # Add user message
    st.session_state.messages.append({"role": "user", "content": prompt})

    # Display user message
    st.markdown(f"""
    <div class="chat-message user-message">
        <div class="message-role">You</div>
        <div class="message-content">{prompt}</div>
    </div>
    """, unsafe_allow_html=True)

    # Generate response with thread management
    with st.spinner("Thinking..."):
        result = query_serving_endpoint(prompt, st.session_state.thread_id)

    if result["success"]:
        response = result["response"]

        # Store thread_id for conversation continuity
        if result.get("thread_id"):
            st.session_state.thread_id = result["thread_id"]

        st.session_state.messages.append({"role": "assistant", "content": response})

        # Display assistant message
        st.markdown(f"""
        <div class="chat-message assistant-message">
            <div class="message-role">Assistant</div>
            <div class="message-content">{response}</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        error_msg = f"Error: {result['error']}"
        st.session_state.messages.append({"role": "assistant", "content": error_msg})

        # Display error message
        st.markdown(f"""
        <div class="chat-message assistant-message">
            <div class="message-role">Assistant</div>
            <div class="message-content" style="color: #dc2626;">{error_msg}</div>
        </div>
        """, unsafe_allow_html=True)

    st.rerun()

# Welcome message if no conversation
if not st.session_state.messages:
    st.markdown("""
    <div style="text-align: center; padding: 3rem 0;">
        <h3 style="color: #374151; margin-bottom: 1rem;">Welcome to Analytics Agent</h3>
        <p style="color: #6b7280;">Ask questions about your data to get started</p>
    </div>
    """, unsafe_allow_html=True)
