# Analytics Agent - LangGraph Architecture (Hybrid)

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   Analytics Agent - Architecture (Hybrid with Genie)                            │
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
│Scenario B  │ │Scenario C  │ │Genie                │ │Scenario D           │
│Pleasantries│ │No Data     │ │(Scenario A)         │ │Ambiguous Intent     │
│            │ │            │ │                     │ │                     │
│            │ │            │ │  - Clear Intent     │ │  - Clarification    │
│            │ │            │ │  - Create Query     │ │                     │
│            │ │            │ │  - Execute SQL      │ │                     │
│            │ │            │ │  - Manage Memory    │ │                     │
│            │ │            │ │                     │ │                     │
└─────┬──────┘ └─────┬──────┘ └──────────┬──────────┘ └──────────┬──────────┘
      │              │                   │                        │
      │              │                   │                        │
      └──────────────┼───────────────────┼────────────────────────┘
                     │                   │
                     ▼                   ▼
              ┌──────────────────────────────────┐
              │Guardrails:                       │
              │  - Transparency Validation       │
              │  - Terminology Enforcement       │
              │  - Other guardrails              │
              └─────────────┬────────────────────┘
                            │
                            ▼
              ┌──────────────────────────────────┐
              │generate_answer                   │
              │(Final Response)                  │
              └─────────────┬────────────────────┘
                            │
                            ▼
                         [END]
```

