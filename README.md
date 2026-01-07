# Databricks Genie Agent

The agent graph has memory for multi-turn conversations.

Queries an already created genie space.

Uses databricks-claude Databricks foundational model

Checks for edge cases and decide if the question needs to query the database or not.
Questions asks for data not available in database
Clarification

## Setup

Update `GENIE_SPACE_ID` in agent.py and `objects_documentation` in utilities.py with your schema.

**Note:** Genie API's `include_serialized_space` parameter (for fetching schema) is in beta
(https://docs.databricks.com/api/workspace/genie/getspace)

Python SDK may not support it yet. Manually populate `objects_documentation` until SDK is updated.
