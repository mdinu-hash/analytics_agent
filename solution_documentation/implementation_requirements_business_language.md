# Implementation Requirements: Business Language Glossary

## **Solution Architecture**

### **Decision Logic**
```
User asks about term X
  ↓
Glossary lookup
  ↓
├─ Synonym? → Replace with canonical term, continue
├─ Available? → Apply query logic, continue
└─ Unavailable?
    ├─ 0 alternatives → Explain unavailable (Scenario C)
    ├─ 1 alternative → Auto-use, note in Key Assumptions
    └─ 2+ alternatives → Ask for clarification (Scenario D)
```

---

## **Agent Behavior Summary**

This table shows exactly how the agent responds to different types of glossary terms:

| User Input | Glossary State | Agent Behavior | Example |
|------------|---------------|----------------|---------|
| "Show me production" | **Synonym** → "affiliation_credit" | Silently replace term, continue to SQL generation. User doesn't see any notification. | Query uses `affiliation_credit` column |
| "Show me net revenue" | **Available** = True | Apply query_logic filters/joins, continue to SQL generation | SQL includes filters from glossary |
| "Show me AUM trends" | **Unavailable** with **3 alternatives** | Ask for clarification (Scenario D): "Which AUM? Total/Regulatory/Admin Fee?" | Agent waits for user to choose |
| "Show me advisor payout rate" | **Unavailable** with **1 alternative** | Auto-use "advisor_net_revenue", explain in Key Assumptions | Key Assumptions: "• Payout rate not available. Showing net revenue instead." |
| "Show me base salary" | **Unavailable** with **0 alternatives** | Explain not available (Scenario C), suggest other analyses | "Base salary not tracked. Try net revenue or affiliation credit?" |

## **STEP 1: Create Business Glossary**

### **File: `src/business_glossary.py` (NEW FILE)**

Create this new file with the unified glossary structure:

```python
"""
Business Glossary for Analytics Agent
Defines business terms, synonyms, and query logic
"""

BUSINESS_GLOSSARY = {
    # ============================================================================
    # AVAILABLE TERMS - Direct database access
    # ============================================================================

    "client": {
        "type": "column",
        "definition": "Individual account holder, not stakeholder",
        "available": True,
        "synonyms": ["account_party", "customer"],
        "query_logic": {
            "tables": ["account_party"],
            "filters": ["party_type = 'CLIENT'"],
            "columns": ["account_party_key", "party_name"],
            "note": "Filters out stakeholders - only includes direct clients"
        }
    },

    "firm": {
        "type": "column",
        "definition": "Main advisor firm or DBA (Doing Business As)",
        "available": True,
        "synonyms": ["advisor_firm", "company"],
        "query_logic": {
            "tables": ["advisors"],
            "filters": ["entity_flag = TRUE OR is_main_firm = TRUE"],
            "columns": ["firm_name", "advisor_key"],
            "note": "Includes both main firms and DBAs"
        }
    },

    "entity": {
        "type": "column",
        "definition": "Legal business entity, excluding DBAs",
        "available": True,
        "synonyms": [],
        "query_logic": {
            "tables": ["advisors"],
            "filters": ["entity_flag = FALSE", "is_active = TRUE"],
            "columns": ["firm_name", "advisor_key"]
        }
    },

    "affiliate": {
        "type": "column",
        "definition": "Active affiliated advisor or entity",
        "available": True,
        "synonyms": ["affiliated_advisor"],
        "query_logic": {
            "tables": ["advisors"],
            "filters": ["entity_flag = FALSE", "is_active = TRUE"],
            "columns": ["advisor_id", "firm_name"]
        }
    },

    "producing_advisor": {
        "type": "computed_term",
        "definition": "Advisor with affiliation credit >= $50,000",
        "available": True,
        "synonyms": ["high_producer"],
        "query_logic": {
            "tables": ["advisors", "affiliation_credit"],
            "filters": ["affiliation_credit >= 50000"],
            "joins": ["advisors.advisor_key = affiliation_credit.advisor_key"],
            "columns": ["advisor_id", "firm_name", "affiliation_credit"]
        }
    },

    "firm_revenue": {
        "type": "column",
        "definition": "Amount of money ($) received by Commonwealth from client fees",
        "available": True,
        "synonyms": ["gross_revenue", "total_fees"],
        "query_logic": {
            "tables": ["revenue"],
            "columns": ["gross_fee_amount"],
            "note": "Total fees before any deductions"
        }
    },

    "advisor_revenue": {
        "type": "computed_term",
        "definition": "Firm revenue after payout calculations (Firm Revenue × Payout Rate)",
        "available": True,
        "synonyms": [],
        "query_logic": {
            "tables": ["revenue"],
            "columns": ["gross_fee_amount", "advisor_payout_rate"],
            "computation": "gross_fee_amount * advisor_payout_rate",
            "note": "Revenue allocated to advisor based on payout rate"
        }
    },

    "advisor_net_revenue": {
        "type": "column",
        "definition": "Advisor revenue after ticket charges and admin fees are deducted",
        "available": True,
        "synonyms": ["net_revenue"],
        "query_logic": {
            "tables": ["revenue"],
            "columns": ["net_revenue"],
            "note": "Final revenue after all deductions"
        }
    },

    "total_aum": {
        "type": "column",
        "definition": "Value of all holdings and positions with no limiting criteria",
        "available": True,
        "synonyms": ["total_assets_under_management", "total_market_value"],
        "query_logic": {
            "tables": ["assets"],
            "columns": ["total_market_value"],
            "note": "Includes all asset types"
        }
    },

    "regulatory_aum": {
        "type": "computed_term",
        "definition": "Assets related to specific business lines used for regulatory reporting",
        "available": True,
        "synonyms": ["cfn_ria_aum"],
        "query_logic": {
            "tables": ["assets"],
            "filters": ["cfn_ria_regulatory_aum_flag = TRUE"],
            "columns": ["account_key", "market_value"],
            "note": "Regulatory reporting subset"
        }
    },

    "admin_fee_aum": {
        "type": "computed_term",
        "definition": "Total assets used as the basis for calculating the PPS Admin Fee",
        "available": True,
        "synonyms": ["pps_aum"],
        "query_logic": {
            "tables": ["assets"],
            "filters": ["admin_fee_eligible = TRUE"],
            "columns": ["account_key", "market_value"],
            "note": "Subset used for admin fee calculations"
        }
    },

    "affiliation_credit": {
        "type": "column",
        "definition": "Commission based on client relationships and production",
        "available": True,
        "synonyms": ["production"],
        "query_logic": {
            "tables": ["revenue"],
            "columns": ["affiliation_credit_amount"]
        }
    },

    # ============================================================================
    # UNAVAILABLE TERMS - Multiple alternatives (disambiguation needed)
    # ============================================================================

    "AUM": {
        "type": "computed_term",
        "definition": "Assets Under Management - ambiguous without context",
        "available": False,
        "synonyms": ["assets_under_management"],
        "alternatives": [
            {
                "term": "total_aum",
                "description": "All holdings and positions with no limiting criteria",
                "query_logic": {
                    "tables": ["assets"],
                    "columns": ["total_market_value"]
                }
            },
            {
                "term": "regulatory_aum",
                "description": "Assets related to specific business lines (regulatory reporting)",
                "query_logic": {
                    "tables": ["assets"],
                    "filters": ["cfn_ria_regulatory_aum_flag = TRUE"],
                    "columns": ["market_value"]
                }
            },
            {
                "term": "admin_fee_aum",
                "description": "Assets used for calculating PPS Admin Fee",
                "query_logic": {
                    "tables": ["assets"],
                    "filters": ["admin_fee_eligible = TRUE"],
                    "columns": ["market_value"]
                }
            }
        ]
    },

    "advisor_compensation": {
        "type": "computed_term",
        "definition": "Advisor payment - could refer to multiple revenue metrics",
        "available": False,
        "synonyms": ["compensation", "advisor_payment"],
        "alternatives": [
            {
                "term": "affiliation_credit",
                "description": "Commission based on client relationships and production",
                "query_logic": {
                    "tables": ["revenue"],
                    "columns": ["affiliation_credit_amount"]
                }
            },
            {
                "term": "advisor_revenue",
                "description": "Revenue allocated to advisor after payout calculations",
                "query_logic": {
                    "tables": ["revenue"],
                    "columns": ["gross_fee_amount", "advisor_payout_rate"],
                    "computation": "gross_fee_amount * advisor_payout_rate"
                }
            },
            {
                "term": "advisor_net_revenue",
                "description": "Net revenue after ticket charges and admin fees",
                "query_logic": {
                    "tables": ["revenue"],
                    "columns": ["net_revenue"]
                }
            }
        ]
    },

    # ============================================================================
    # UNAVAILABLE TERMS - Single alternative (auto-use with explanation)
    # ============================================================================

    "advisor_payout_rate": {
        "type": "computed_term",
        "definition": "Percentage of net revenue paid to advisor based on production tier",
        "available": False,
        "synonyms": ["payout_percentage", "compensation_rate"],
        "alternatives": [
            {
                "term": "advisor_net_revenue",
                "description": "Net revenue received by advisor (dollar amount, not rate)",
                "query_logic": {
                    "tables": ["revenue"],
                    "columns": ["net_revenue"]
                },
                "explanation_note": "Payout rate percentage is not tracked. Showing net revenue dollar amounts instead."
            }
        ]
    },

    # ============================================================================
    # UNAVAILABLE TERMS - No alternatives (truly unavailable)
    # ============================================================================

    "advisor_base_salary": {
        "type": "unavailable",
        "definition": "Fixed salary paid to advisor (not tracked in our system)",
        "available": False,
        "synonyms": ["base_pay", "salary"],
        "alternatives": [],
        "suggestion": "You might be interested in performance-based metrics like advisor_net_revenue or affiliation_credit instead."
    },

    "client_satisfaction_score": {
        "type": "unavailable",
        "definition": "Client satisfaction rating (not collected in our system)",
        "available": False,
        "synonyms": ["satisfaction_rating", "nps"],
        "alternatives": [],
        "suggestion": "We don't track satisfaction scores, but you can analyze client retention, asset growth, or account activity patterns."
    },

    # ============================================================================
    # SYNONYMS - Redirect to canonical terms
    # ============================================================================

    "production": {
        "type": "synonym",
        "canonical_term": "affiliation_credit",
        "redirect": True
    },

    "account_party": {
        "type": "synonym",
        "canonical_term": "client",
        "redirect": True
    },

    "sponsor": {
        "type": "synonym",
        "canonical_term": "custodian",
        "redirect": True
    },

    "total_assets": {
        "type": "synonym",
        "canonical_term": "total_aum",
        "redirect": True
    },
}


def validate_glossary():
    """
    Validate that all tables/columns referenced in query_logic exist in metadata.
    Should be run during initialization to catch configuration errors.
    """
    # TODO: Implement validation against metadata_reference_table
    # For each term with query_logic, check:
    #   - tables exist in metadata_reference_table
    #   - columns exist for those tables
    #   - filters reference valid columns
    pass


def get_glossary_term(term: str) -> dict:
    """
    Get glossary entry for a term (case-insensitive).

    Args:
        term: Business term to look up

    Returns:
        Glossary entry dict or None if not found
    """
    term_lower = term.lower().strip()

    # Check exact match
    for key, value in BUSINESS_GLOSSARY.items():
        if key.lower() == term_lower:
            return {"term": key, **value}

    return None


def get_term_by_synonym(synonym: str) -> dict:
    """
    Find glossary term by synonym.

    Args:
        synonym: Synonym to search for

    Returns:
        Glossary entry dict or None if not found
    """
    synonym_lower = synonym.lower().strip()

    for key, value in BUSINESS_GLOSSARY.items():
        # Check synonyms list
        if "synonyms" in value and isinstance(value["synonyms"], list):
            if any(syn.lower() == synonym_lower for syn in value["synonyms"]):
                return {"term": key, **value}

    return None
```

---

## **STEP 2: Add Helper Functions**

### **File: `agent.py` - Add after imports (around line 50)**

```python
# Import business glossary
from src.business_glossary import BUSINESS_GLOSSARY, get_glossary_term, get_term_by_synonym


def check_business_glossary(question: str) -> dict:
    """
    Check if question contains known business terms from glossary.

    Args:
        question: User's question text

    Returns:
        Dict with glossary match info or {"found": False}
    """
    question_lower = question.lower()

    # Check each glossary term (exact match and synonyms)
    for term_key, metadata in BUSINESS_GLOSSARY.items():
        # Check canonical term
        if term_key.lower() in question_lower:
            return {"found": True, "term": term_key, "matched_via": term_key, **metadata}

        # Check synonyms
        if metadata.get("synonyms"):
            for synonym in metadata["synonyms"]:
                if synonym.lower() in question_lower:
                    return {
                        "found": True,
                        "term": term_key,
                        "matched_via": synonym,
                        **metadata
                    }

    return {"found": False}


def replace_with_canonical(question: str, glossary_result: dict) -> str:
    """
    Replace synonym in question with canonical term.

    Args:
        question: Original question
        glossary_result: Result from check_business_glossary

    Returns:
        Question with synonym replaced by canonical term
    """
    if glossary_result.get("type") == "synonym":
        canonical = glossary_result.get("canonical_term")
        matched_via = glossary_result.get("matched_via")
        if canonical and matched_via:
            return question.replace(matched_via, canonical)

    return question


def replace_term_in_question(question: str, old_term: str, new_term: str) -> str:
    """
    Replace one term with another in question (case-insensitive).

    Args:
        question: Original question
        old_term: Term to replace
        new_term: Replacement term

    Returns:
        Question with term replaced
    """
    import re
    pattern = re.compile(re.escape(old_term), re.IGNORECASE)
    return pattern.sub(new_term, question)


def handle_glossary_result(glossary_result: dict, state: State) -> str:
    """
    Unified handler for all glossary lookups.

    Args:
        glossary_result: Result from check_business_glossary
        state: Current agent state

    Returns:
        Next action: 'continue' or 'generate_answer'
    """

    # Case 1: Synonym - silently replace and continue
    if glossary_result.get('type') == 'synonym':
        state['current_question'] = replace_with_canonical(
            state['current_question'],
            glossary_result
        )
        return 'continue'

    # Case 2: Available term - apply query logic and continue
    if glossary_result.get('available') == True:
        # Store query_logic for use in SQL generation
        if 'glossary_query_logic' not in state:
            state['glossary_query_logic'] = []
        state['glossary_query_logic'].append({
            'term': glossary_result['term'],
            'query_logic': glossary_result.get('query_logic', {})
        })
        return 'continue'

    # Case 3: Unavailable with alternatives
    if glossary_result.get('available') == False:
        alternatives = glossary_result.get('alternatives', [])

        # Sub-case 3a: No alternatives - truly unavailable (Scenario C)
        if len(alternatives) == 0:
            suggestion = glossary_result.get('suggestion', 'Unfortunately, this data is not available.')
            state['generate_answer_details'].update({
                'scenario': 'C',
                'notes': f"'{glossary_result['term']}' is not available in our database.\n\n{glossary_result.get('definition', '')}\n\n{suggestion}"
            })
            return 'generate_answer'

        # Sub-case 3b: Single alternative - use it, explain in Key Assumptions
        elif len(alternatives) == 1:
            alt = alternatives[0]
            state['current_question'] = replace_term_in_question(
                state['current_question'],
                glossary_result['term'],
                alt['term']
            )

            # Store query logic
            if 'glossary_query_logic' not in state:
                state['glossary_query_logic'] = []
            state['glossary_query_logic'].append({
                'term': alt['term'],
                'query_logic': alt.get('query_logic', {})
            })

            # Add note for Key Assumptions
            explanation_note = alt.get('explanation_note',
                f"'{glossary_result['term']}' is not directly available. Analyzed {alt['term']} instead: {alt['description']}")

            if 'key_assumptions_notes' not in state:
                state['key_assumptions_notes'] = []
            state['key_assumptions_notes'].append(explanation_note)

            return 'continue'

        # Sub-case 3c: Multiple alternatives - ask for clarification (Scenario D)
        else:
            alternatives_text = "\n".join([
                f"{i+1}. **{alt['term']}**: {alt['description']}"
                for i, alt in enumerate(alternatives)
            ])

            state['generate_answer_details'].update({
                'scenario': 'D',
                'notes': f"'{glossary_result['term']}' could refer to multiple metrics:\n\n{alternatives_text}\n\nWhich one would you like to analyze?"
            })

            # Store analytical intents for potential follow-up
            state['analytical_intent'] = [
                f"{alt['term']}: {alt['description']}"
                for alt in alternatives
            ]

            return 'generate_answer'

    # Default: continue normal flow
    return 'continue'
```

---

## **STEP 3: Update State Schema**

### **File: `agent.py` - Update State class (line 68)**

```python
class State(TypedDict):
    objects_documentation: str
    database_content: str
    sql_dialect: str
    messages_log: Sequence[BaseMessage]
    intermediate_steps: list[AgentAction]
    analytical_intent: list[str]
    current_question: str
    current_sql_queries: list[dict]
    generate_answer_details: dict
    llm_answer: BaseMessage
    glossary_query_logic: list[dict]  # NEW: Query logic from glossary
    key_assumptions_notes: list[str]  # NEW: Notes for Key Assumptions section
```

---

## **STEP 4: Integrate Glossary Check in Orchestrator**

### **File: `agent.py` - Update orchestrator function (line 930)**

```python
def orchestrator(state: State):
    ''' Function that decides which tools to use '''

    scratchpad = retrieve_scratchpad(state)
    nr_executions_orchestrator = scratchpad['nr_executions_orchestrator']

    # if this is the 1st time when orchestrator is called
    if nr_executions_orchestrator == 0:

        # NEW: Check glossary FIRST before other scenario checks
        glossary_result = check_business_glossary(state['current_question'])

        if glossary_result.get('found'):
            show_progress("🔍 Checking business terminology...")
            next_action = handle_glossary_result(glossary_result, state)

            if next_action == 'generate_answer':
                # Glossary handler already set scenario + notes
                scenario = state['generate_answer_details']['scenario']
                notes = state['generate_answer_details']['notes']
                next_tool_name = 'generate_answer'

                # Log orchestrator run
                action = AgentAction(tool='orchestrator', tool_input='', log='tool ran successfully')
                state['intermediate_steps'].append(action)

                # Log next tool to call
                action = AgentAction(tool=next_tool_name, tool_input='', log='')
                state['intermediate_steps'].append(action)

                return state

            # If next_action == 'continue', fall through to existing scenario checks

        # EXISTING: Check scenarios B or C for non-glossary reasons
        system_prompt = f"""You are a decision support consultant helping users make data-driven decisions.

        Your task is to decide the next action for this question: {{question}}.

        Conversation history: {{messages_log}}.
        Current insights: "{{insights}}".
        Database schema: {{objects_documentation}}

        Decision process:

        Step 1. Check if question is non-analytical or already answered:
           - If question is just pleasantries ("thank you", "hello", "how are you") → "B"
           - If the same question was already answered in conversation history → "B"

        Step 2. Check if requested data exists in schema:
          - If the user asks for data/metrics not available in the database schema → "C"

        Step 3. Otherwise → "Continue".
        """

        # ... rest of existing orchestrator logic remains the same
```

---

## **STEP 5: Enhance SQL Generation with Glossary Query Logic**

### **File: `agent.py` - Update create_sql_query_or_queries (line 279)**

Add this enhancement to include glossary query logic in the SQL generation prompt:

```python
@tool
def create_sql_query_or_queries(state: State):
    """ creates sql query/queries to answer a question based on documentation of tables and columns available """

    # NEW: Format glossary query logic if available
    glossary_context = ""
    if state.get('glossary_query_logic'):
        glossary_items = []
        for item in state['glossary_query_logic']:
            term = item['term']
            logic = item['query_logic']

            logic_parts = []
            if logic.get('tables'):
                logic_parts.append(f"Tables: {', '.join(logic['tables'])}")
            if logic.get('filters'):
                logic_parts.append(f"Filters: {' AND '.join(logic['filters'])}")
            if logic.get('joins'):
                logic_parts.append(f"Joins: {' AND '.join(logic['joins'])}")
            if logic.get('columns'):
                logic_parts.append(f"Columns: {', '.join(logic['columns'])}")
            if logic.get('note'):
                logic_parts.append(f"Note: {logic['note']}")

            glossary_items.append(f"• {term}: {' | '.join(logic_parts)}")

        glossary_context = "\n\nBusiness Term Query Requirements:\n" + "\n".join(glossary_items) + "\n\nIMPORTANT: Apply the filters and joins specified above for the business terms mentioned."

    system_prompt = """You are a sql expert and an expert data modeler.

  Your task is to create sql scripts in {sql_dialect} dialect to answer the analytical intent(s). In each sql script, use only these tables and columns you have access to:
  {objects_documentation}.

  Summary of database content:
  {database_content}.

  Analytical intent(s):
  {analytical_intent}
  """ + glossary_context + """

  Answer just with the resulting sql code(s).

  Important quality requirements for every sql string:
    - Return one sql string for every analytical intent.
    - Return only raw SQL strings in the list.
    - DO NOT include comments (like "-- Query 1"), labels, or explanations.
    - If only one SQL query is needed, just return a list with that one query.
    - GROUP BY expressions must match the non-aggregated SELECT expressions.
    - Ensure that any expression used in ORDER BY also appears in the SELECT clause.
    - If you filter by specific text values, use trim, lowercase and pattern matching with LIKE and wildcard (ex: "where trim(lower(column_name)) LIKE trim(lower('%ValueTofilterBy%'))"). For multiple search terms, use multiple wildcards (ex: "where trim(lower(firm_name)) like '%oak%wealth%'").
    - Keep query performance in mind.
      Example: Avoid CROSS JOIN by using a (scalar) subquery directly in CASE statements.

  Important considerations about multi-steps analytical intents (the ones that contain "Step 1:", "Step 2:" etc):
  Create a sophisticated SQL query using CTEs that mirror the steps:
  - Each "Step X" becomes a corresponding CTE.
  - Name CTEs descriptively based on what each step accomplishes.
  - Build each CTE using results from previous CTEs.
  - Final SELECT provides the complete analysis.

  """ + """

  Example output (simple, non multi-steps):
    [
      "SELECT COUNT(*) FROM feedback;",
      "SELECT AVG(product_price) FROM products;"
    ]

   Example output (multi-steps):
    [
      "    WITH step1_descriptive_name AS (
        -- Implementation of Step 1 from analytical intent
        SELECT ...
    ),
    step2_descriptive_name AS (
        -- Implementation of Step 2, using step1 results
        SELECT ... FROM step1_descriptive_name ...
    ),
    step3_final_analysis AS (
        -- Implementation of Step 3, final analysis
        SELECT ... FROM step2_descriptive_name ...
    )
    SELECT
        clear_result_columns,
        meaningful_calculations,
        percentage_or_comparison_metrics
    FROM step3_final_analysis
    ORDER BY logical_sort_order;"
    ]
  """

    prompt = create_prompt_template('system', system_prompt)

    chain = prompt | llm.with_structured_output(OutputAsAQuery)

    result = chain.invoke({'objects_documentation': state['objects_documentation'],
                          'database_content': state['database_content'],
                          'analytical_intent': state['analytical_intent'],
                          'sql_dialect': state['sql_dialect']})

    # ... rest of existing function remains the same
```

---

## **STEP 6: Update Key Assumptions Formatting**

### **File: `agent.py` - Update format_sql_query_explanations_for_prompt (line 821)**

```python
def format_sql_query_explanations_for_prompt(sql_queries: list[dict]) -> str:
    """Format explanations into single section"""
    all_explanations = []

    # Collect SQL-specific explanations
    for q in sql_queries:
        if q.get('explanation') and isinstance(q['explanation'], list):
            all_explanations.extend(q['explanation'])

    if not all_explanations:
        return ""

    unique_explanations = list(dict.fromkeys(all_explanations))
    return "\n\n**Key Assumptions:**\n" + "\n".join([f"• {e}" for e in unique_explanations])


def format_glossary_notes_for_response(state: State) -> str:
    """
    Format glossary-related notes for Key Assumptions section.
    NEW FUNCTION to add glossary explanations.
    """
    if not state.get('key_assumptions_notes'):
        return ""

    unique_notes = list(dict.fromkeys(state['key_assumptions_notes']))
    return "\n".join([f"• {note}" for note in unique_notes])
```

### **File: `agent.py` - Update generate_answer (line 838)**

```python
@tool
def generate_answer(state: State):
    """ generates the AI answer taking into consideration the explanation and the result of the sql query that was executed """

    scenario = state['generate_answer_details']['scenario']

    # create prompt template based on scenario
    sys_prompt = next(s['Prompt'] for s in scenario_prompts if s['Type'] == scenario)
    prompt = create_prompt_template('system', sys_prompt, messages_log=True)
    llm_answer_chain = prompt | llm

    def create_final_message(llm_response):
        base_content = llm_response.content
        explanation_section = ""

        if state.get('generate_answer_details', {}).get('scenario') == 'A':
            # SQL query explanations
            sql_explanations = format_sql_query_explanations_for_prompt(state['current_sql_queries'])

            # Glossary notes
            glossary_notes = format_glossary_notes_for_response(state)

            # Combine both
            if sql_explanations and glossary_notes:
                explanation_section = sql_explanations + "\n" + glossary_notes
            elif sql_explanations:
                explanation_section = sql_explanations
            elif glossary_notes:
                explanation_section = "\n\n**Key Assumptions:**\n" + glossary_notes

        return {'ai_message': AIMessage(content=base_content + explanation_section,
                                       response_metadata=llm_response.response_metadata)}

    final_answer_chain = llm_answer_chain | RunnableLambda(create_final_message)

    # invoke parameters based on scenario
    invoke_params = next(s['Invoke_Params'](state) for s in scenario_prompts if s['Type'] == scenario)

    result = final_answer_chain.invoke(invoke_params)
    ai_msg = result['ai_message']

    # Update state (common for all scenarios)
    state['llm_answer'] = ai_msg
    state['messages_log'].append(HumanMessage(state['current_question']))
    state['messages_log'].append(ai_msg)

    show_progress("📣 Final Answer:")
    return state
```

---

## **STEP 7: Update reset_state Function**

### **File: `agent.py` - Update reset_state (line 1051)**

```python
def reset_state(state: State):
    state['current_sql_queries'] = []
    state['intermediate_steps'] = []
    state['llm_answer'] = AIMessage(content='')
    state['generate_answer_details'] = {}
    state['analytical_intent'] = []
    state['objects_documentation'] = objects_documentation
    state['database_content'] = database_content
    state['sql_dialect'] = sql_dialect
    state['glossary_query_logic'] = []  # NEW
    state['key_assumptions_notes'] = []  # NEW
    return state
```

---

## **STEP 8: Update Response Guidelines**

### **File: `agent.py` - Update response_guidelines (line 667)**

Add business term clarification guidance:

```python
response_guidelines = '''
  Response guidelines:
  - Respond in clear, non-technical language.
  - Be concise.

  **IMPORTANT: Business Term Clarification**
  - When analyzing data that was requested using a different term (e.g., user asked for X but we analyzed Y),
    this will be automatically noted in Key Assumptions - don't repeat it in your main response.
  - If the analysis involves financial or business metrics with potential confusion, briefly acknowledge
    the specific metric being analyzed.
    Example: "Here's the advisor net revenue distribution..." (not just "here's the data")

  Use these methods at the right time, optionally and not too much, keep it simple and conversational:

  If the question is smart, reinforce the user's question to build confidence.
    Example: "Great instinct to ask that - it's how data-savvy pros think!"

  If the context allows, suggest max 2 next steps to explore further.
  Suggest next steps that can only be achieved with the database schema you have access to:
  {objects_documentation}

  Summary of database content:
  {database_content}.

  Example of next steps:
  - Trends over time:
    Example: "Want to see how this changed over time?".

  - Drill-down suggestions:
    Example: "Would you like to explore this by brand or price tier?"

  - Top contributors to a trend:
    Example: "Want to see the top 5 products that drove this increase in satisfaction?"

  - Explore a possible cause:
    Example: "Curious if pricing could explain the drop? I can help with that."

  - Explore the data at higher granularity levels if the user analyzes on low granularity columns. Use database schema to identify such columns.
    Example: Instead of analyzing at product level, suggest at company level.

  - Explore the data on filtered time ranges. Use database content to identify the temporal context for this conversation .
    Example: Instead of analyzing for all feedback dates, suggest filtering for a year or for a few months.

  - Filter the data on the value of a specific attribute. Use database content to identify values of important dataset attributes.
    Example: Instead of analyzing for all companies, suggest filtering for a single company and give a few suggestions.

  Close the prompt in one of these ways:
  A. If you suggest next steps, ask the user which option prefers.
  B. Use warm, supportive closing that makes the user feel good.
    Example: "Keep up the great work!", "Have a great day ahead!".
  '''
```

---

## **Implementation Checklist**

- [ ] Create `src/business_glossary.py` with unified glossary structure
- [ ] Add import statement in `agent.py`
- [ ] Add helper functions: `check_business_glossary()`, `replace_with_canonical()`, `replace_term_in_question()`, `handle_glossary_result()`
- [ ] Update `State` TypedDict with new fields: `glossary_query_logic`, `key_assumptions_notes`
- [ ] Update `orchestrator()` to check glossary before scenario checks
- [ ] Update `create_sql_query_or_queries()` to include glossary query logic in prompt
- [ ] Add `format_glossary_notes_for_response()` function
- [ ] Update `generate_answer()` to include glossary notes in Key Assumptions
- [ ] Update `reset_state()` to initialize new state fields
- [ ] Update `response_guidelines` with business term clarification guidance
- [ ] Populate glossary with client-specific terms (update examples in `business_glossary.py`)
- [ ] Test all four scenarios: synonym redirect, single alternative, multiple alternatives, no alternatives

---

## **Testing Scenarios**

### **Test 1: Synonym Redirect (Silent)**
```
User: "Show me production by advisor"
Expected: Agent silently replaces "production" with "affiliation_credit" and proceeds
Result: SQL query uses affiliation_credit, no user notification
```

### **Test 2: Single Alternative (Auto-use with Note)**
```
User: "What's the advisor payout rate distribution?"
Expected: Agent uses "advisor_net_revenue" and explains in Key Assumptions
Result:
  - SQL query uses net_revenue
  - Response includes: "**Key Assumptions:**
    • Payout rate percentage is not tracked. Showing net revenue dollar amounts instead."
```

### **Test 3: Multiple Alternatives (Disambiguation)**
```
User: "Show me AUM trends"
Expected: Agent asks which AUM type
Result: "AUM could refer to multiple metrics:
  1. Total AUM: All holdings and positions
  2. Regulatory AUM: Assets for regulatory reporting
  3. Admin Fee AUM: Assets used for PPS Admin Fee calculation

  Which one would you like to analyze?"
```

### **Test 4: No Alternatives (Unavailable)**
```
User: "Show me advisor base salaries"
Expected: Agent explains unavailable and suggests alternatives
Result: "Advisor base salary data is not available in our database.

  You might be interested in performance-based metrics like advisor_net_revenue
  or affiliation_credit instead."
```

### **Test 5: Available Term with Query Logic**
```
User: "Show me client breakdown by firm"
Expected: Agent applies query_logic filters from glossary
Result: SQL includes: WHERE party_type = 'CLIENT' (filters out stakeholders)
  Key Assumptions includes: "• Filters out stakeholders - only includes direct clients"
```

---
