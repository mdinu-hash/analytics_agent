What production-ready AI (for analytics) means:

Observability (user question, agent response, agent's intermediate steps).
Custom orchestration ability for explainability, debugging, testing, determinism.
For multi-turn reasoning, clarification loops, result interpretation, and more transparency over the answers.
Annotations / Labeling.
Collaborate with experts to manually review agent answers.
Add labeled questions and guidelines to improve the agent.
Adding examples to a dataset to fine-tune or improve the agent (building a training/eval dataset of question-reasoning pairs).
Define custom criteria (scores) for evaluations.
Run custom evals at scale.
Once you're making a change to the agent, we need to run the agent against 100+ benchmark questions, without manually asking every question → waiting for the agent to respond → logging the answer → evaluating each answer.
CI/CD.
Run LLM judges in production.
Certain LLM judges will score agent responses and log the results into dashboards.
Used to monitor whether the quality of responses decreases due to specific rollouts in the data analytics.
Monitor: latency, token usage, costs.
Domain-specific situations for analytics:

Business terminology:
The agent should use the terms defined by our organization properly.
Situations:

User: "What is the admin fee AUM for PPS Select?"
The agent is expected to know that "admin fee AUM" is calculated by aggregating a certain column and filtering specific business lines.
Certain terms are used but they are not properly defined in the organization. In this case, the agent should highlight the proper definition of the terms used in the query.
Example:

User asks for AUM.
Agent response 1: "AUM can refer to total assets (value of all holdings), regulatory assets (assets specific to custody arrangements), or admin fee AUM (assets used as a basis to calculate PPS admin fee). Which one do you prefer?".
Agent response 2: "Here is the data for total assets, which means value of all holdings and positions with no limiting criteria".


Agent uses terms too loosely.
Ex: User asks for affiliation credit, agent replies "Here is the affiliation credit... this amount of revenue means...".
The problem is that the agent uses "revenue" not according to the revenue statement. We should enforce the usage of these terms.

Aggregate metrics correctly:

Don't sum up semi-additive metrics over time (ex: assets).
When grouping over metrics that contain null values, apply coalesce to not filter out groupings containing nulls.

Apply default filters:
Ex: When users ask about accounts, they expect open accounts.
Other default filters: active clients, active households, point-in-time case reporting.
Acknowledgment of static values found in key columns.
Ex: User asks about "PPS Select assets".
Agent should know that in the business line column, one of the values is PPS Select. It should reason that it should filter for that value in the business line column.
Acknowledgment of time-frame:
As opposed to static columns, there are certain columns whose values refresh daily. A specific example is time-based columns.
For these key time columns, the agent should pull min/max ranges when it starts to become aware of the period available.
Situations:

User: "EOM assets and affiliation credit for advisor X"
Agent:

Is expected to know that EOM means "December 2024".
Is expected to apply aggregations over different columns, depending on the metric. Ex: snapshot date for assets, date key for affiliation credit.


User: "What is the affiliation credit for firm Y?"
Agent: Is expected to define a time range over which it is aggregating affiliation credit, a time range enforced by the data available.
In addition, it is expected to reveal this assumption to the user, although the user did not explicitly ask for it.
("The affiliation credit is X, which refers to the entire 2025 year").


Possible architectural ideas (for analytics copilots at scale) and their "fit for production":
Out-of-the-box solutions (Microsoft Power BI Copilot): Not fit for production.
No observability, evaluations, or custom orchestration ability.
Native services with minimal customization (Ex: Copilot Studio from Fabric, Cortex Analyst from Snowflake): Not fit for production.
They enable documentation about database schema and agent instructions. This customization alone is not enough.
The problem is that the agent instructions are like a "soup": the entire instructions block is evaluated at every question. This increases token costs. We also don't have any visibility or control (determinism) into which instructions are evaluated.
In addition, these services lack other critical functionalities: custom orchestration, observability, evaluations.
Deployment on Databricks: Fit for production.
Requires Delta tables from Snowflake to Databricks.
Most mature technology.
Enables both no-code (Genie/Agent Bricks) as well as code-heavy solutions.
Many cost-saving functionalities.
Deployment on Foundry (Azure) - leverage Genie (Databricks) as query engine - distribution via Copilot Studio (Fabric): Fit for production.
Requires Delta tables from Snowflake to Databricks.
CI/CD requires 2 deployments (agent on Foundry, distribution on Copilot Studio).
Downside: Although we leverage Genie, we still rely on code-heavy agents and custom platform solutions.
Deployment on Foundry - leverage Fabric Data agents as query engine - distribution via Copilot Studio: Not fit for production.
Why: Fabric Data agents don't expose the DAX it generated: Can't explain why the agent gave a certain answer.
Downside: Although we leverage Fabric, we still rely on code-heavy agents and custom platform solutions.