# Agent Refactoring Specification

## Overview

This refactor improves separation of concerns in the agent workflow by:
1. Extracting decision logic from execution logic
2. Making SQL execution a separate observable node
3. Creating dedicated transparency node for key assumptions
4. Maintaining current orchestrator behavior for scenarios B and C

**Goal**: Better modularity, observability, and maintainability without changing agent behavior.

---

## Current vs New Architecture

### Current Flow
```
START → reset_state → orchestrator (1st)
                         ↓ [conditional]
                         ├─ [B/C] → generate_answer → END
                         └─ [Continue] → extract_analytical_intent
                                           ↓ [conditional]
                                           ├─ [Scenario D] → generate_answer → END
                                           └─ [Scenario A] → create_sql_query_or_queries
                                                              (execute_sql_query embedded)
                                                              ↓
                                                          orchestrator (2nd)
                                                              ↓
                                                          generate_answer
                                                          (key assumptions embedded)
                                                              ↓
                                                             END
```

### New Flow
```
START → reset_state → orchestrator (1st)
                         ↓ [conditional]
                         ├─ [B/C] → generate_answer → add_assumptions → END
                         └─ [Continue] → clarification_check
                                           ↓ [conditional]
                                           ├─ [clear/A] → extract_analytical_intent
                                           │                ↓
                                           │             create_sql_query_or_queries
                                           │                ↓
                                           │             execute_sql_query (NEW)
                                           │                ↓
                                           │             orchestrator (2nd)
                                           │                ↓
                                           │             generate_answer
                                           │                ↓
                                           │             add_assumptions (NEW)
                                           │                ↓
                                           │               END
                                           │
                                           └─ [ambiguous/D] → clarification (NEW)
                                                                ↓
                                                             generate_answer
                                                                ↓
                                                             add_assumptions (NEW)
                                                                ↓
                                                               END
```

---

## Changes Summary

### New Nodes (4 total)
1. **clarification_check** - Decides if question is clear or ambiguous
2. **clarification** - Generates ambiguity analysis for scenario D
3. **execute_sql_query** - Executes SQL queries (extracted from create_sql_query_or_queries)
4. **add_assumptions** - Formats and appends key assumptions to answer

### Modified Nodes (2 total)
1. **extract_analytical_intent** - Simplified to only generate analytical intents
2. **generate_answer** - No longer appends key assumptions

### Unchanged
- **orchestrator** - Still handles scenarios B and C detection
- **reset_state** - No changes
- **create_sql_query_or_queries** - Only change: remove embedded execute_sql_query
- **run_control_flow** - Still wraps node execution, updated to handle new nodes
- **manage_memory_chat_history** - Still called from run_control_flow (now after add_assumptions)

---

## Detailed Node Specifications

### 1. NEW NODE: `clarification_check`

**Purpose**: Determine if question is clear (scenario A) or ambiguous (scenario D)

**Responsibilities**:
1. Check for scenario D trigger condition:
   - Multiple related terms exist
   - Searched term does NOT exist in database
   - If triggered: set scenario='D', format special ambiguity message, route to clarification

2. If no scenario D trigger, use LLM to decide:
   - Invoke `sys_prompt_clear_or_ambiguous` (currently lines 104-138 in extract_analytical_intent)
   - If "Analytical Intent Extracted" → route to extract_analytical_intent (scenario A)
   - If "Analytical Intent Ambiguous" → route to clarification (scenario D)

**Input State**:
- `search_terms_output` (contains related_terms data)
- `objects_documentation`
- `current_question`
- `messages_log`

**Output State**:
- `scenario` (set to 'D' if ambiguous, empty string '' if clear)
- `generate_answer_details['ambiguity_explanation']` (set if scenario D triggered)
- `generate_answer_details['agent_questions']` (set if scenario D triggered)
- `intermediate_steps` (append routing decision)

**Routing**:
- If clear → route to `extract_analytical_intent`
- If ambiguous → route to `clarification`

**Implementation Notes**:
- Extract related terms check logic from current `extract_analytical_intent` (lines 239-289)
- Extract LLM clear/ambiguous decision from current `extract_analytical_intent` (lines 217-292)
- Structured output: `ClearOrAmbiguous` TypedDict
- Wrapped by `run_control_flow`

---

### 2. NEW NODE: `clarification`

**Purpose**: Generate ambiguity analysis when question is ambiguous (scenario D)

**Responsibilities**:
1. Invoke `sys_prompt_ambiguous` (currently lines 183-215 in extract_analytical_intent)
2. Generate:
   - `ambiguity_explanation`: Brief explanation of what makes question ambiguous
   - `agent_questions`: 2-3 alternative analytical intents as questions

**Input State**:
- `objects_documentation`
- `current_question`
- `messages_log`

**Output State**:
- `analytical_intent` (set to agent_questions list)
- `generate_answer_details['ambiguity_explanation']`
- `generate_answer_details['agent_questions']`
- `intermediate_steps` (append clarification action, then generate_answer routing)

**Routing**:
- Always routes to `generate_answer`

**Implementation Notes**:
- Extract sys_prompt_ambiguous logic from current `extract_analytical_intent` (lines 183-215, 226-227, 309-317)
- Structured output: `AmbiguityAnalysis` TypedDict
- Wrapped by `run_control_flow`

---

### 3. MODIFIED NODE: `extract_analytical_intent`

**Purpose**: Generate analytical intents when question is clear (scenario A only)

**Current Responsibilities** (REMOVE these):
- ❌ Check if clear or ambiguous (moves to clarification_check)
- ❌ Related terms scenario D check (moves to clarification_check)
- ❌ Generate ambiguity analysis (moves to clarification)

**New Responsibilities** (KEEP only these):
- ✅ Invoke `sys_prompt_clear` (currently lines 140-181)
- ✅ Generate analytical_intent list
- ✅ Track term_substitutions (synonyms/related terms used)

**Input State**:
- `search_terms_output`
- `objects_documentation`
- `current_question`
- `messages_log`

**Output State**:
- `scenario` (set to 'A')
- `analytical_intent` (list of intents)
- `search_terms_output['term_substitutions']`
- `intermediate_steps` (append action, then route to create_sql_query_or_queries)

**Routing**:
- Always routes to `create_sql_query_or_queries`

**Implementation Notes**:
- Keep only lines 222-223, 295-307 logic from current implementation
- Remove all clear/ambiguous decision logic
- Remove all scenario D handling
- Still wrapped by `run_control_flow`

---

### 4. NEW NODE: `execute_sql_query`

**Purpose**: Execute SQL queries and generate insights

**Responsibilities**:
1. Execute SQL queries from `current_sql_queries`
2. Handle execution errors (up to 3 correction attempts)
3. Refine queries if results exceed 500 tokens
4. Generate insights and explanations for each query result
5. Track key assumptions

**Input State**:
- `current_sql_queries` (contains list of queries to execute)
- `objects_documentation`
- `sql_dialect`

**Output State**:
- `current_sql_queries` (updated with results, insights, explanations)
- `generate_answer_details['key_assumptions']`
- `intermediate_steps` (append action, then route to orchestrator)

**Routing**:
- Always routes to `orchestrator` (2nd call)

**Implementation Notes**:
- Extract `execute_sql_query()` function from current implementation (called at line 1142)
- Find the function definition (search for `def execute_sql_query`)
- Make it a graph node instead of embedded function call
- Wrapped by `run_control_flow`

---

### 5. MODIFIED NODE: `create_sql_query_or_queries`

**Current Responsibilities**:
- ✅ Generate SQL queries from analytical intents
- ❌ Execute queries (REMOVE - moves to execute_sql_query node)

**New Responsibilities**:
- ✅ Generate SQL queries from analytical intents (KEEP)

**Changes**:
- Remove the embedded `execute_sql_query()` call
- Route directly to `execute_sql_query` node instead

**Routing**:
- Changed from: route to `orchestrator`
- Changed to: route to `execute_sql_query`

---

### 6. NEW NODE: `add_assumptions`

**Purpose**: Append key assumptions to the generated answer for transparency

**Responsibilities**:
1. Format key assumptions from `generate_answer_details['key_assumptions']`
2. Append formatted assumptions to `llm_answer.content`
3. Trigger `manage_memory_chat_history` (via run_control_flow)

**Input State**:
- `llm_answer` (AIMessage with base response)
- `generate_answer_details['key_assumptions']`
- `scenario`

**Output State**:
- `llm_answer` (updated with key assumptions appended)

**Routing**:
- Always routes to END

**Implementation Notes**:
- Extract key assumptions logic from current `generate_answer` (lines 969-977)
- Only append assumptions if `scenario == 'A'`
- Use existing `format_key_assumptions_for_prompt()` helper function
- Wrapped by `run_control_flow`
- After this node executes, `run_control_flow` calls `manage_memory_chat_history()`

---

### 7. MODIFIED NODE: `generate_answer`

**Current Responsibilities**:
- ✅ Generate LLM answer based on scenario
- ✅ Generate agent_questions (next steps)
- ❌ Append key assumptions (REMOVE - moves to add_assumptions)
- ❌ Call manage_memory_chat_history (REMOVE - moves to run_control_flow/add_assumptions)

**New Responsibilities**:
- ✅ Generate LLM answer based on scenario
- ✅ Generate agent_questions (next steps)
- ✅ Update messages_log

**Changes**:
- Remove `create_final_message()` function (lines 969-977)
- Change chain from `final_answer_chain` to just `llm_answer_chain`
- Remove key assumptions concatenation
- Store raw LLM response in `llm_answer`

**Routing**:
- Changed from: route to END
- Changed to: route to `add_assumptions`

---

## Graph Assembly Changes

### Current Graph Definition (lines 1214-1233)
```python
graph = StateGraph(State)
graph.add_node("reset_state", reset_state)
graph.add_node("orchestrator", orchestrator)
graph.add_node("extract_analytical_intent", run_control_flow)
graph.add_node("create_sql_query_or_queries", run_control_flow)
graph.add_node("generate_answer", run_control_flow)

graph.add_edge(START, "reset_state")
graph.add_edge("reset_state", "orchestrator")
graph.add_conditional_edges(source='orchestrator', path=router)
graph.add_conditional_edges(source='extract_analytical_intent', path=router)
graph.add_edge("create_sql_query_or_queries", "orchestrator")
graph.add_edge("generate_answer", END)
```

### New Graph Definition
```python
graph = StateGraph(State)
graph.add_node("reset_state", reset_state)
graph.add_node("orchestrator", orchestrator)
graph.add_node("clarification_check", run_control_flow)  # NEW
graph.add_node("extract_analytical_intent", run_control_flow)
graph.add_node("clarification", run_control_flow)  # NEW
graph.add_node("create_sql_query_or_queries", run_control_flow)
graph.add_node("execute_sql_query", run_control_flow)  # NEW
graph.add_node("generate_answer", run_control_flow)
graph.add_node("add_assumptions", run_control_flow)  # NEW

# Starting the agent
graph.add_edge(START, "reset_state")
graph.add_edge("reset_state", "orchestrator")

# Orchestrator routes to clarification_check or generate_answer (B/C scenarios)
graph.add_conditional_edges(source='orchestrator', path=router)

# Clarification_check routes to extract_analytical_intent or clarification
graph.add_conditional_edges(source='clarification_check', path=router)

# Extract_analytical_intent routes to create_sql_query_or_queries
graph.add_conditional_edges(source='extract_analytical_intent', path=router)

# Create_sql_query_or_queries routes to execute_sql_query
graph.add_edge("create_sql_query_or_queries", "execute_sql_query")

# Execute_sql_query routes to orchestrator (2nd call)
graph.add_edge("execute_sql_query", "orchestrator")

# Clarification routes to generate_answer
graph.add_edge("clarification", "generate_answer")

# Generate_answer routes to add_assumptions
graph.add_edge("generate_answer", "add_assumptions")

# Add_assumptions is the final node
graph.add_edge("add_assumptions", END)
```

---

## `run_control_flow` Updates

### Current Implementation (lines 1129-1149)
```python
def run_control_flow(state:State):
    tool_name = state['intermediate_steps'][-1].tool

    if tool_name == 'extract_analytical_intent':
        state = extract_analytical_intent.invoke({'state':state})

    elif tool_name == 'create_sql_query_or_queries':
        state = create_sql_query_or_queries.invoke({'state':state})
        state = execute_sql_query(state)  # Embedded call

    elif tool_name == 'generate_answer':
        state = generate_answer.invoke({'state':state})
        state = manage_memory_chat_history(state)  # Embedded call

    return state
```

### New Implementation
```python
def run_control_flow(state:State):
    tool_name = state['intermediate_steps'][-1].tool

    if tool_name == 'clarification_check':  # NEW
        state = clarification_check(state)

    elif tool_name == 'extract_analytical_intent':
        state = extract_analytical_intent.invoke({'state':state})

    elif tool_name == 'clarification':  # NEW
        state = clarification(state)

    elif tool_name == 'create_sql_query_or_queries':
        state = create_sql_query_or_queries.invoke({'state':state})
        # Remove: state = execute_sql_query(state)

    elif tool_name == 'execute_sql_query':  # NEW
        state = execute_sql_query(state)

    elif tool_name == 'generate_answer':
        state = generate_answer.invoke({'state':state})
        # Remove: state = manage_memory_chat_history(state)

    elif tool_name == 'add_assumptions':  # NEW
        state = add_assumptions(state)
        state = manage_memory_chat_history(state)  # Moved here

    return state
```

---

## Router Updates

The `router()` function remains the same - it reads the last tool name from `intermediate_steps`.

However, nodes must correctly append routing decisions to `intermediate_steps`:

### Routing Logic Summary
- **orchestrator** → routes to `clarification_check` or `generate_answer`
- **clarification_check** → routes to `extract_analytical_intent` or `clarification`
- **extract_analytical_intent** → routes to `create_sql_query_or_queries`
- **clarification** → routes to `generate_answer`
- **create_sql_query_or_queries** → fixed edge to `execute_sql_query`
- **execute_sql_query** → fixed edge to `orchestrator`
- **generate_answer** → fixed edge to `add_assumptions`
- **add_assumptions** → fixed edge to END

---

## Implementation Checklist

### Phase 1: Create New Node Functions
- [ ] Create `clarification_check(state)` function
  - [ ] Extract related terms logic from extract_analytical_intent (lines 239-289)
  - [ ] Extract clear/ambiguous decision logic (lines 217-219, 291-293)
  - [ ] Update intermediate_steps with routing decision

- [ ] Create `clarification(state)` function
  - [ ] Extract sys_prompt_ambiguous logic (lines 183-215, 226-227)
  - [ ] Generate ambiguity_explanation and agent_questions
  - [ ] Update state and intermediate_steps

- [ ] Create `add_assumptions(state)` function
  - [ ] Extract key assumptions formatting from generate_answer (lines 969-977)
  - [ ] Append to llm_answer.content
  - [ ] Only if scenario == 'A'

- [ ] Extract `execute_sql_query` as standalone node
  - [ ] Find existing execute_sql_query function definition
  - [ ] Ensure it appends routing to intermediate_steps

### Phase 2: Modify Existing Nodes
- [ ] Modify `extract_analytical_intent`
  - [ ] Remove lines 217-219 (clear/ambiguous chain setup)
  - [ ] Remove lines 226-227 (ambiguous chain setup)
  - [ ] Remove lines 239-289 (related terms scenario D check)
  - [ ] Remove lines 291-293 (clear/ambiguous decision)
  - [ ] Remove lines 308-317 (ambiguous result handling)
  - [ ] Keep only lines 222-223, 295-307 (clear intent generation)

- [ ] Modify `generate_answer`
  - [ ] Remove create_final_message function (lines 969-977)
  - [ ] Remove final_answer_chain
  - [ ] Use llm_answer_chain directly
  - [ ] Store raw response in llm_answer

- [ ] Modify `create_sql_query_or_queries`
  - [ ] Verify it routes to execute_sql_query (not orchestrator)

### Phase 3: Update run_control_flow
- [ ] Add clarification_check handler
- [ ] Add clarification handler
- [ ] Add execute_sql_query handler (remove from create_sql_query_or_queries)
- [ ] Add add_assumptions handler
- [ ] Move manage_memory_chat_history to add_assumptions handler
- [ ] Remove manage_memory_chat_history from generate_answer handler

### Phase 4: Update Graph Assembly
- [ ] Add new nodes to graph
- [ ] Update edges as specified above
- [ ] Test graph compiles without errors

### Phase 5: Update Orchestrator
- [ ] Modify orchestrator to route to `clarification_check` instead of `extract_analytical_intent`
- [ ] Update get_next_tool() if needed

### Phase 6: Testing
- [ ] Test scenario A (clear analytical question)
- [ ] Test scenario B (pleasantries)
- [ ] Test scenario C (missing data)
- [ ] Test scenario D (ambiguous - LLM detected)
- [ ] Test scenario D (ambiguous - related terms)
- [ ] Verify key assumptions appear correctly
- [ ] Verify memory management works
- [ ] Check LangSmith traces for proper node visualization

---

## Expected Outcome

After refactoring:
1. ✅ Better separation of concerns (decision vs execution)
2. ✅ Improved observability (SQL execution and assumptions as separate trace steps)
3. ✅ Easier to test individual components
4. ✅ Same user-facing behavior
5. ✅ Clearer code organization
6. ✅ Transparency step visible in traces

---

## Rollback Plan

If issues arise:
1. Keep old `extract_analytical_intent` as `extract_analytical_intent_old`
2. Create new version as separate function
3. Test thoroughly before removing old version
4. Git commit after each phase for easy rollback
