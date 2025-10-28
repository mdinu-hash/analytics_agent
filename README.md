# AI Analytics Agent

Instant Insights Through Natural Language

[Try The Live Demo: https://analytics-agent-7mzj7.ondigitalocean.app/]

Tired of waiting weeks for data insights? This AI Analytics Agent turns natural language into powerful database queries — delivering business answers in seconds.

## Why Use This Agent?

Traditional BI tools and analyst ticket queues come with these problems:
- Slow response times (days or weeks)
- Poor UX: complex dashboards, overwhelming filters, pages that go unused

This AI Analytics Agent changes that:
- From delayed reports to instant insights and fast decisions
- From SQL expertise required to natural conversation
- (For data teams): From guessing what users want to a backlog driven by what users actually need 

## What It Does

Simply ask questions in plain English and get instant answers from your database — no SQL knowledge required, no dashboard navigation.

Examples:
- "Which practice segment has the highest amount of HNW clients?"
- "List advisors under tenure 20 and their assets."
- "For Oak Wealth firm, what was their EOM asset value and payout?"

The agent handles:
- Root Cause Analysis
- Trend Monitoring
- Comparative Analytics
- Multi-step Reasoning
- Conversational Follow-ups
---

## Demo Dataset

The demo runs on a **wealth management platform dataset** spanning September 2024 - September 2025, featuring a dimensional data model optimized for financial analytics.

### Data Model Overview

**Dimension Tables (with SCD Type 2 history tracking):**
- **household** (45,000 records) - Client households with tenure, registration type, segment, advisor assignment
- **advisors** (500 records) - Financial advisors with firm name, affiliation model, role, practice segment
- **account** (72,000 records) - Investment accounts with type (Taxable, IRA, 401k, Trust), custodian, risk profile
- **business_line** (5 records) - Product lines: Managed Portfolio, SMA, Mutual Fund Wrap, Annuity, Cash
- **product** (350 records) - Investment products across equity, fixed income, multi-asset, and cash
- **tier_fee** - Tiered fee structure by business line and AUM ranges
- **advisor_payout_rate** - Payout rates by firm affiliation model
- **date** - Calendar dimension table

**Fact Tables (Monthly grain):**
- **fact_account_monthly** - Monthly account metrics: returns, net flows, assets
- **fact_revenue_monthly** - Revenue analytics: gross fees, third-party fees, advisor payouts, net revenue
- **fact_household_monthly** - Household aggregations: total assets, HNW flags, asset range buckets
- **fact_account_product_monthly** - Product allocation percentages per account
- **fact_customer_feedback** - Customer satisfaction surveys with sentiment scores (0-100)
- **transactions** - Detailed transaction history (deposits, withdrawals, fees)

### Sample Analytical Questions

**Revenue & Growth Analysis:**
- "What was our net inflow of client assets this month compared to same month last year?"
- "What's our YTD fee revenue by product line, and which products are growing fastest?"
- "How much month-to-date fee revenue have we generated vs same period last month?"

**Advisor Performance:**
- "Which advisors brought in the most net new AUM in the past 30 days?"
- "Which advisors show warning signals: <7 satisfaction scores in last 90 days AND net outflows?"
- "What helps an advisor generate more revenue? Products, business lines, or account types?"

**Client Segmentation:**
- "How many high-net-worth clients do we have and what's their total AUM?"
- "Which practice segment has the highest amount of HNW clients?"

**Risk & Operations:**
- "Which clients have >70% of AUM in a single product or in cash?"
- "Which top 20 clients had the largest withdrawals in the past 7 days?"

---

## How It Works

Built on LangGraph, the agent executes a series of intelligent steps for every user prompt. Each step builds on the previous one to deliver accurate, data-driven answers.

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                          Analytics Agent - LangGraph Architecture                                │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

[START] ──► ┌─────────────┐ ──► ┌─────────────────────┐
            │orchestrator │     │extract_analytical   │
            │(Decision    │     │_intent              │
            │ Logic)      │     │(Ambiguity Detection)│
            └─────┬───────┘     └─────────┬───────────┘
                  │                       │
       ┌──────────┼──────────┐            │
       │          │          │            │
       ▼          ▼          ▼            ▼
┌────────────┐ ┌────────────┐ ┌─────────────────────┐ ┌─────────────────────┐
│Scenario B  │ │Scenario C  │ │Clear Intent         │ │Ambiguous Intent     │
│Pleasantries│ │No Data     │ │        │            │ │                     │
│     │      │ │     │      │ │        ▼            │ │            │        │
│     ▼      │ │     ▼      │ │┌─────────────────────┐│ │          ▼        │
│┌──────────┐│ │┌──────────┐│ ││create_sql_query_or  ││ │ ┌─────────────────┐│
││generate  ││ ││generate  ││ ││_queries             ││ │ │generate_answer  ││
││ answer   ││ ││ answer   ││ ││         │           ││ │ │(Clarification)  ││
││          ││ ││          ││ ││         ▼           ││ │ │                 ││
│└─────┬────┘│ │└─────┬────┘│ ││┌─────────────────────┐││ │ └────────┬──────┘│
└──────┼─────┘ └──────┼─────┘ │││execute_sql_query    │││ └──────────┼────────┘
       │              │       │││+ Error Handling     │││            │
       │              │       │││+ Token Management   │││            │
       │              │       ││└─────────┬───────────┘││            │
       │              │       ││          │            ││            │
       │              │       ││          ▼            ││            │
       │              │       ││┌─────────────────────┐││            │
       │              │       │││generate_answer      │││            │
       │              │       │││(Data Insights)      │││            │
       │              │       ││└─────────┬───────────┘││            │
       │              │       │└──────────┼────────────┘│            │
       │              │       └───────────┼─────────────┘            │
       │              │                   │                          │
       └──────────────┼───────────────────┼──────────────────────────┘
                      │                   │
                      ▼                   ▼
               ┌─────────────────────┐ ┌─────────────────────┐
               │manage_memory_chat   │ │manage_memory_chat   │
               │_history             │ │_history             │
               └─────────┬───────────┘ └─────────┬───────────┘
                         │                       │
                         ▼                       ▼
               [END]
```

### 4-Step Process

**Step 1: Orchestrator (Decision Logic)**
- Analyzes the user question against conversation history and database schema
- Routes to appropriate scenario:
  - **Scenario B**: Pleasantries or questions already answered in chat history
  - **Scenario C**: Requested data not available in database (suggests alternatives)
  - **Continue**: Proceeds to analytical intent extraction

**Step 2: Extract Analytical Intent**
- Translates natural language questions into technical analytical requirements
- Detects ambiguity and asks for clarification when needed
- Example: "Top client" could mean by revenue OR by tenure - which one?
- Generates 1-5 analytical intents depending on question complexity

**Step 3: Create & Execute SQL Queries**
- Converts analytical intents into clean, executable SQL queries
- Automatic error correction (up to 3 attempts)
- Query optimization if results exceed token limits
- Executes queries and stores results with insights

**Step 4: Generate Answer & Manage Memory**
- Provides user-friendly responses with positive, encouraging tone
- Suggests relevant next steps for deeper analysis
- Manages conversation history through intelligent summarization to maintain performance

## Advanced Capabilities

**Conversational Memory**
- Maintains chat history for contextual follow-ups
- Example: *"Which advisors brought in the most net new AUM?"* → *"What about their satisfaction scores?"* (remembers the advisor context)

**Ambiguity Detection & Clarification**
- Detects when questions have multiple valid interpretations
- Example: *"Show me top advisors"* → *"Top advisors can mean by net new AUM, by total assets, or by client satisfaction scores - which would you like?"*

**Missing Data Handling**
- Detects when requested data isn't available in the database
- Suggests alternative analyses with available data
- Example: *"Unfortunately the database doesn't contain geographic region data. Would you like to explore by firm affiliation model or practice segment instead?"*

**Multi-step Reasoning**
- Handles complex analytical questions requiring multiple sequential steps
- Uses CTEs (Common Table Expressions) to build sophisticated queries
- Example: *"Which advisors show warning signals: ≥2 low satisfaction scores in last 90 days AND net outflows?"* (requires joining feedback data with account flow data)

**Automatic Error Recovery**
- Self-corrects SQL syntax errors (up to 3 attempts)
- Optimizes queries that return too much data
- Ensures reliable execution

## Tech Stack

**Framework**: LangGraph for agentic workflow orchestration
**UI**: Streamlit with purple gradient interface
**Database**: PostgreSQL.
**LLM Providers**: OpenAI and Anthropic (via abstraction layer)
**Deployment**: Docker container, deployed on Digital Ocean

## Deployment

This application is containerized with Docker and can be deployed anywhere:
- **Current Demo**: Digital Ocean App Platform.
- **Cloud Portable**: Azure, AWS, Snowflake, Databricks, GCP, or on-premise.

[Try The Live Demo: https://analytics-agent-7mzj7.ondigitalocean.app/]

