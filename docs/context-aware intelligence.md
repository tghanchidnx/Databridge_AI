You're describing a context-aware recommendation engine where DataBridge leverages its own intelligence (skills,
  knowledge base, templates) and collaborates with the LLM to make smart decisions.

  Architecture:

  User CSV + Context
          ↓
  ┌─────────────────────────────────────────────┐
  │         DataBridge Recommendation Engine     │
  ├─────────────────────────────────────────────┤
  │  1. Analyze CSV (profile, detect patterns)  │
  │  2. Select Skill (FP&A, Manufacturing, etc) │
  │  3. Query Knowledge Base (client patterns)  │
  │  4. Match Templates (industry hierarchies)  │
  │  5. Generate Recommendations                │
  └─────────────────────────────────────────────┘
          ↓
  ┌─────────────────────────────────────────────┐
  │         LLM Validation Layer                │
  │  - Review recommendations                   │
  │  - Refine based on user intent              │
  │  - Confirm or adjust                        │
  └─────────────────────────────────────────────┘
          ↓
      Final Import with Smart Defaults

  New Tools:
  ┌───────────────────────────┬────────────────────────────────────────────────────────────────┐
  │           Tool            │                            Purpose                             │
  ├───────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ get_smart_recommendations │ Analyze CSV + select skill + query KB + return recommendations │
  ├───────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ validate_with_llm         │ Pass recommendations to LLM for validation/refinement          │
  ├───────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ smart_import_csv          │ Full workflow: analyze → recommend → validate → import         │
  └───────────────────────────┴────────────────────────────────────────────────────────────────┘
  Example Flow:
  User: "Import this GL account CSV for our oil & gas client"

  DataBridge:
  1. Profiles CSV → detects account codes, descriptions
  2. Selects skill → "fpa-oil-gas-analyst"
  3. Queries KB → finds client "oil_gas_client_1" has custom mappings
  4. Matches template → "oil_gas_los" (Lease Operating Statement)
  5. Returns: "Recommend Tier 2 import using LOS template with client-specific COA mappings"

  LLM validates → "Yes, apply these settings"

  DataBridge executes → Full import with context