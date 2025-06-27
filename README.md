# GenAI Data Copilot

Instant Insights Through Natural Language

[Try The Live Demo: https://genai-data-copilot-1.streamlit.app/]
[View LangSmith Traces - What Happens Under The Hood: https://smith.langchain.com/o/a016dac4-a501-45c3-b12f-e746a3b726f9/projects/p/703518c1-2cd1-44fa-8d4e-1e24f2370e49?timeModel=%7B%22duration%22%3A%221h%22%7D ]

Tired of waiting weeks for data insights? GenAI Data Copilot turns natural language into powerful database queries — delivering business answers in seconds.

## Why Use This Copilot?

Traditional BI tools and analyst ticket queues come with these problems:
- Slow response times (days or weeks)
- Poor UX: filters missing, pages unused

GenAI Copilot changes that:
- From delayed reports to instant insights and fast decisions.
- From SQL to natural conversation.
- (For data teams): From guessing what users want to a backlog driven by what users actually need. 

## What It Does

Simply ask questions in plain English and get instant answers from enterprise tabular data — no SQL, no dashboards.

Examples:
- "Why did the ratings for Adidas decrease in early 2016 from January to May?"
- "Which companies contributed to the increase in ratings from September 2022?"
- "Are premium-priced products getting better ratings than budget products?"

The copilot handles:
- Root Cause Analysis  
- Trend Monitoring  
- Comparative Analytics  
- Key Driver Insights  
- Business Q&A
---

## Demo Dataset

The demo runs on a simplified Amazon reviews dataset with 3 tables:

### Table feedback
| Feedback_ID | Feedback_Date | Product_ID   | Product_Company | Feedback_Rating |
|-------------|----------------|--------------|------------------|------------------|
| 1           | 04/22/2022     | B00UCZGS6S   | Samsung          | 1                |
| 2           | 09/27/2019     | B01LLLISAK   | Google           | 5                |
| ...         | ...            | ...          | ...              | ...              |

### Table products
| Product_ID  | Product_Name                               | Product_Brand | Product_Company_Name | Product_Manufacturer | Product_Avg_Rating |
|-------------|---------------------------------------------|----------------|------------------------|------------------------|----------------------|
| B00UCZGS6S  | Samsung Galaxy S3                          | Samsung        | Samsung                | Samsung                | 3.6                  |
| B01LLLISAK  | Encased Pixel 4 Belt Clip Case             | Encased        | Google                 | Encased for Pixel 4    | 4.3                  |
| ...         | ...                                         | ...            | ...                    | ...                    | ...                  |

### Table company
| Company_Name | Annual_Revenue_USD |
|--------------|---------------------|
| Apple        | $391M               |
| Adidas       | $23.6M              |
| Samsung      | $300.8M             |
| ...          | ...                 |

---

## How It Works

For every user prompt, the agent executes a series of intelligent steps, where each step builds on the previous one. By the end of the flow, the agent has enough knowledge to provide data-driven answers.

[See a real trace of the prompt-to-answer via LangSmith](https://smith.langchain.com/o/a016dac4-a501-45c3-b12f-e746a3b726f9/projects/p/703518c1-2cd1-44fa-8d4e-1e24f2370e49?timeModel=%7B%22duration%22%3A%221h%22%7D)

### The 4-Step Process

Step 1: Extract Analytical Intent
Behaves like a business analyst, translating user questions from natural language into technical requirements.

Step 2: Create & Execute SQL Queries
Converts the analytical intent into clean, executable SQL queries.
Runs the query and logs results in memory.

Step 3: Generate Answers
Uses query results to provide user-friendly responses with:
- Positive tone: "Great instinct to ask about that!"
- Suggested next steps: "Would you like to analyze this over time?"

Step 4: Manage Chat History
Maintains conversation context while managing memory efficiently through intelligent summarization.

## Advanced Capabilities

- Conversational memory (chat history)
"Can you share the top rated products of Samsung?"
"What about google?"

- Ambiguity detection and clarification
"Top client can mean a couple of things: Top as measured by sales volume, or by tenure?"

- Missing data detection with alternative analysis suggestions
"Unfortunately the database does not contain region. Would you like to explore by brand instead?"

- Multi-step reasoning for complex queries:
“Are premium-priced products (top 25% by price) getting better ratings than budget products?”

## Deployment

This demo runs in the browser (Streamlit) with a cloud-based backend, but it is fully portable to your own data infrastructure (e.g., Databricks or Azure).

[Try The Live Demo: https://genai-data-copilot-1.streamlit.app/]

