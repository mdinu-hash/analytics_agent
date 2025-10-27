"""
LLM Utility Functions
Handles provider-specific differences between OpenAI and Anthropic
"""

from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

# LLM Provider Configuration
llm_provider = 'anthropic'  # Change to 'openai' or 'anthropic' as needed


def create_prompt_template(role, prompt, messages_log=False):
    """
    Create ChatPromptTemplate with provider-specific message formatting
    
    Args:
        role: Message role ('system', 'user', etc.)
        prompt: Message prompt text (can contain variables like {variable_name})
        messages_log: If True, includes MessagesPlaceholder("messages_log") before the role/prompt
        
    Returns:
        ChatPromptTemplate formatted for the current LLM provider
    """
    # Note: prompt can contain variables that will be filled in later, e.g. {objects_documentation}, {question}
    
    if messages_log:
        # Include MessagesPlaceholder for chat history
        if llm_provider == 'anthropic' and role == 'system':
            # Convert system to user for Anthropic
            return ChatPromptTemplate.from_messages([
                MessagesPlaceholder("messages_log"),
                ('user', prompt)
            ])
        else:
            # Use original role for OpenAI or non-system messages
            return ChatPromptTemplate.from_messages([
                MessagesPlaceholder("messages_log"),
                (role, prompt)
            ])
    else:
        # Simple format without MessagesPlaceholder
        if llm_provider == 'anthropic' and role == 'system':
            return ChatPromptTemplate.from_messages([('user', prompt)])
        else:
            # Use original role for OpenAI or non-system messages
            return ChatPromptTemplate.from_messages([(role, prompt)])


def get_token_usage(response):
    """
    Extract token usage from response metadata with provider-specific key
    
    Args:
        response: The LLM response message with response_metadata
        
    Returns:
        dict: Token usage dictionary
    """
    if llm_provider == 'anthropic':
        return response.response_metadata['usage']['output_tokens']
    else:
        return response.response_metadata['token_usage']['total_tokens']


def calculate_chat_history_tokens(messages_log):
    """
    Calculate total tokens from chat history for memory management
    
    Args:
        messages_log: List of chat messages
        
    Returns:
        int: Total token count from chat history
    """
    if not messages_log:
        return 0
        
    if llm_provider == 'anthropic':
        # For Anthropic: Sum up all output_tokens from AI messages
        total_tokens = 0
        for msg in messages_log:
            if hasattr(msg, 'response_metadata') and 'usage' in msg.response_metadata:
                total_tokens += msg.response_metadata['usage'].get('output_tokens', 0)
        return total_tokens
    else:
        # For OpenAI: Use total_tokens from the last message
        last_msg = messages_log[-1]
        if hasattr(last_msg, 'response_metadata') and 'token_usage' in last_msg.response_metadata:
            return last_msg.response_metadata['token_usage'].get('total_tokens', 0)
        return 0