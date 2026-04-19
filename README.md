# Governed NL2SQL Agent: Decision-Safe Analytics for Enterprises

*Add AI on top of your data warehouse — without breaking user trust.*

---

*"Let's put AI on top of our data warehouse so users can query analytics just by asking questions."*

Most enterprises start here. Native AI services (Databricks Genie, Snowflake Cortex Analyst, Microsoft Data Fabric Agents) make it fast to spin up a PoC.

But in regulated environments where detail and words matter, these PoCs often break — and decision-makers lose trust.

---

## Common failures of analytics agents

**Hidden assumptions.** User: *"What is the revenue?"* — Answer 1: *"$6M"* — Answer 2: *"$5M"*. Both technically correct — $5M is YTD, $6M is rolling 12 months. The agent did not disclose these assumptions.

**Fabricated conclusions.** User asks: *"Which client segments have the most room to grow?"* Agent retrieves correct data, but concludes without justification that *"all segments have the most room to grow."* SQL was correct. Conclusion was not.

**Lack of clarification.** Users often don't know what's analytically possible and ask vague questions like *"Who are the top clients?"* The agent shouldn't query anything. It should follow up: *"By top, do you mean by headcount or by revenue?"*

**No data acknowledgment.** When users ask about data that isn't available, the agent should explicitly state so — not guess a near-match.

**Undefined business terminology.** Terms like *"assets under management"* may have no official definition, refer to multiple asset types, or be interpreted differently by different teams. The agent should call out the ambiguity, not pick the most convenient interpretation.

**Incorrect use of key terms.** User: *"What is the compensation for firm X?"* Agent: *"The compensation for firm X is $Y. This amount of revenue means..."* — using *"revenue"* loosely, not according to the internal revenue statement. The agent should enforce formal definitions.

**Missing default filters.** User: *"How many accounts do we have?"* In natural language, this means only open accounts. The agent should apply business-default filters automatically.

**Acknowledgment of time frame.** Date columns refresh frequently. If the agent is unaware of the data refresh state, *"EOM asset value"* becomes ambiguous — it doesn't know *"EOM"* means, for example, *December 2024*.

**Incorrect metric aggregation.** Some metrics can't be aggregated across time but can be aggregated across other dimensions (e.g., assets). The agent should be prevented from aggregating assets over time.

---

## What makes analytics agents safe for decision-making

This repo is a **decision-safety layer for NL2SQL agents**. It sits between the user and the NL2SQL engine (Databricks Genie, Snowflake Cortex Analyst, or custom NL2SQL agents) and enforces the policies that keep user trust intact:

**Clarification before execution.** The agent must not execute queries unless there is a clear analytical intent. When a question is ambiguous, the agent blocks execution, explains why it can't be answered as stated, and proposes 2–3 alternative interpretations.

**Explicit acknowledgment of missing data.** If the requested data is not available, the agent must explicitly state that it cannot answer and suggest viable alternatives.

**Assumptions disclosure.** Every analytical answer must disclose its assumptions — timeframe, filters, definitions — in non-technical language. This ensures answers are understood by non-technical decision-makers.

**Full observability.** Every user–agent interaction — question, answer, SQL, results, assumptions — is logged for governance review.

### Architecture

```
[START] ──► ┌─────────────────┐
            │ orchestrator    │
            │ (Decision Logic)│
            └────────┬────────┘
                     │
       ┌─────────────┼─────────────┐
       │             │             │
       ▼             ▼             ▼
┌──────────────┐ ┌──────────────┐ ┌────────────────────┐
│ Scenario B   │ │ Scenario C   │ │ clarification_check│
│ Pleasantries │ │ No Data      │ │ (Intent Analysis)  │
└──────┬───────┘ └──────┬───────┘ └─────────┬──────────┘
       │                │                    │
       │                │         ┌──────────┼──────────┐
       │                │         │ CLEAR    │ AMBIGUOUS│
       │                │         ▼          ▼
       │                │   ┌─────────────┐ ┌──────────────┐
       │                │   │ nl2sql_     │ │ clarification│
       │                │   │ backend     │ │ (Scenario D) │
       │                │   │ (Scenario A)│ │              │
       │                │   │             │ │ - Generate   │
       │                │   │ - Gen SQL   │ │   Altern.    │
       │                │   │ - Execute   │ │              │
       │                │   │ - Get Data  │ │              │
       │                │   └──────┬──────┘ └──────┬───────┘
       │                │          │                │
       └────────────────┼──────────┘                │
                        │                           │
                        ▼                           ▼
              ┌──────────────────────────────────────────┐
              │ generate_answer                          │
              │ (Create Conversational Response)         │
              └─────────────┬────────────────────────────┘
                            │
                     ┌──────┴──────┐
                     │ Scenario A? │
                     └──────┬──────┘
                            │
                  ┌─────────┼─────────┐
                  │ Yes              │ No
                  ▼                  ▼
          ┌────────────────┐      [END]
          │ add_assumptions│
          │ (Query         │
          │  Explanations) │
          └────────┬───────┘
                   │
                   ▼
                [END]
```

### Pluggable NL2SQL backends

This solution works for native NL2SQL agents from Databricks (Genie), Snowflake (Cortex Analyst), Microsoft (Data Fabric Agents), but also for custom NL2SQL agents.

---

## Results

- Consistent answers decision-makers can rely on across contexts
- Insights in minutes instead of days
- Full audit trail for governance and compliance
- Portable across Databricks, Snowflake, and Azure — no vendor lock-in on the decision layer

---

## Let's connect

If you're deploying agentic AI in regulated environments, I'd like to hear about it. Reach me on LinkedIn (link in my GitHub profile)
