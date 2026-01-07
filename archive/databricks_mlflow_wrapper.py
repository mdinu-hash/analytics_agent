import mlflow
from uuid import uuid4
from mlflow.pyfunc import ResponsesAgent
from mlflow.types import ResponsesAgentRequest, ResponsesAgentResponse
from langchain_core.messages import AIMessage

class Database_Copilot_Agent(ResponsesAgent):
    def __init__(self):
        self.graph = None

    def predict(self, request) -> ResponsesAgentResponse:
        """Convert ResponsesAgent format to LangGraph format and back with thread management"""
        # Lazy load on first predict call (at serving time, not registration time)
        if self.graph is None:
            from src.agent.agent import graph
            from src.init.initialization import objects_documentation, sql_dialect, database_content, llm

            self.graph = graph
            self.objects_documentation = objects_documentation
            self.sql_dialect = sql_dialect
            self.database_content = database_content
            self.llm = llm

        # Extract user message
        if isinstance(request.input[0].content, str):
            user_message = request.input[0].content
        else:
            user_message = request.input[0].content.text

        # Extract thread_id if provided (for conversation continuity)
        # Client can pass thread_id in the second input item
        thread_id = None
        is_first_message = True

        if len(request.input) > 1 and hasattr(request.input[1], 'content'):
            # Thread ID passed as second input item with content like "thread_id:xxxxx"
            thread_info = request.input[1].content
            if isinstance(thread_info, str) and thread_info.startswith("thread_id:"):
                thread_id = thread_info.split("thread_id:", 1)[1]
                is_first_message = False

        # Generate new thread_id if not provided
        if not thread_id:
            thread_id = str(uuid4())
            is_first_message = True

        # Build state based on whether it's first message or continuation
        if is_first_message:
            # First message: full initial state
            state = {
                'objects_documentation': self.objects_documentation,
                'database_content': self.database_content,
                'sql_dialect': self.sql_dialect,
                'messages_log': [],
                'intermediate_steps': [],
                'analytical_intent': [],
                'current_question': user_message,
                'current_sql_queries': [],
                'generate_answer_details': {},
                'llm_answer': AIMessage(content='')
            }
        else:
            # Continuation: minimal state (LangGraph loads previous state from checkpoint)
            state = {
                'current_question': user_message
            }

        config = {'configurable': {'thread_id': thread_id}}

        # Invoke agent
        result = self.graph.invoke(state, config=config)
        response_text = result['llm_answer'].content

        # Return response with thread_id included for client to reuse
        return ResponsesAgentResponse(
            output=[{
                "type": "message",
                "id": str(uuid4()),
                "content": [
                    {"type": "output_text", "text": response_text},
                    {"type": "metadata", "text": f"thread_id:{thread_id}"}
                ],
                "role": "assistant",
            }]
        )

mlflow.models.set_model(Database_Copilot_Agent())

   
