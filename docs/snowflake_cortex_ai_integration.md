# Snowflake Cortex AI Integration Research for DataBridge

> **Research Date:** February 2026
> **Purpose:** Evaluate Cortex AI integration options for DataBridge to enhance data reconciliation, hierarchy building, and semantic view generation.

---

## Executive Summary

Snowflake Cortex AI offers a compelling opportunity to enhance DataBridge's capabilities by leveraging AI-powered data analysis, natural language to SQL conversion, and intelligent agents. This document explores the integration options, pros/cons, and provides recommendations for incorporating Cortex AI into the DataBridge ecosystem.

**Key Recommendations:**
1. **Integrate Cortex Analyst** for natural language querying of hierarchies and mappings
2. **Use Cortex LLM Functions** to enhance recommendation quality for CSV imports
3. **Build a Cortex Agent** that can orchestrate DataBridge tools within Snowflake
4. **Leverage Semantic Views** as the bridge between DataBridge hierarchy definitions and Cortex AI

---

## Table of Contents

1. [What is Snowflake Cortex AI?](#what-is-snowflake-cortex-ai)
2. [Cortex AI Components](#cortex-ai-components)
3. [Integration Options](#integration-options)
4. [Pros and Cons Analysis](#pros-and-cons-analysis)
5. [DataBridge-Cortex Agent Architecture](#databridge-cortex-agent-architecture)
6. [Implementation Roadmap](#implementation-roadmap)
7. [Cost Considerations](#cost-considerations)
8. [Security & Governance](#security--governance)
9. [Conclusion & Recommendations](#conclusion--recommendations)

---

## What is Snowflake Cortex AI?

Snowflake Cortex AI is a fully managed suite of AI services that runs natively within Snowflake's data cloud. It provides:

- **LLM Functions**: SQL-callable AI functions (COMPLETE, SENTIMENT, TRANSLATE, SUMMARIZE)
- **Cortex Analyst**: Natural language to SQL conversion using semantic models
- **Cortex Search**: RAG-as-a-Service for hybrid keyword + vector search
- **Cortex Agents**: Orchestration layer for complex multi-step AI tasks
- **Cortex Code** (Feb 2026): AI coding agent for data engineering automation

### Why Cortex AI for DataBridge?

DataBridge already generates semantic views, hierarchies, and mappings. Cortex AI can:
1. **Query hierarchies using natural language** ("Show me all Revenue accounts under Operating Income")
2. **Validate mappings** by analyzing actual data patterns
3. **Suggest improvements** to hierarchy structures based on data profiling
4. **Auto-generate SQL** for complex reconciliation queries

---

## Cortex AI Components

### 1. Cortex Analyst

**What it does:** Converts natural language questions into SQL queries using semantic models.

**Accuracy:** 95% on verified query repositories; 90%+ on real-world BI use cases.

**Key Features:**
- Uses semantic views/models to understand business context
- Respects RBAC - users only see data they're authorized to access
- REST API available for external application integration
- Data never leaves Snowflake's governance boundary

**Limitations:**
- Cannot reference results from previous queries
- Limited to SQL-answerable questions (no trend insights)
- Complex multi-turn conversations may confuse it
- Performance can degrade with overly complex semantic models (60+ second response times reported)

### 2. Cortex LLM Functions

| Function | Description | Use Case for DataBridge |
|----------|-------------|------------------------|
| `COMPLETE` | General-purpose text generation | Generate hierarchy descriptions, suggest column mappings |
| `SENTIMENT` | Sentiment scoring (-1 to 1) | N/A for data reconciliation |
| `TRANSLATE` | Multi-language translation | Translate column names/descriptions |
| `SUMMARIZE` | Text summarization | Summarize reconciliation results |
| `CLASSIFY_TEXT` | Text classification | Classify account types automatically |
| `EXTRACT_ANSWER` | Q&A extraction | Extract business rules from documentation |

**Pricing:** Charged per million tokens processed. Both input and output tokens counted for generative functions.

### 3. Cortex Search (RAG)

**What it does:** Hybrid search combining keyword and vector similarity over unstructured data.

**Use Case for DataBridge:**
- Search through uploaded PDFs/documents for account definitions
- Find similar hierarchies across projects
- Search mapping documentation

### 4. Cortex Agents

**What it does:** Orchestrates complex tasks across structured and unstructured data sources.

**Components:**
- Uses Cortex Analyst for SQL queries
- Uses Cortex Search for document retrieval
- Can invoke external tools via MCP (Model Context Protocol)

**Key Capability:** An agent can plan multi-step workflows, execute them, and synthesize results.

---

## Integration Options

### Option A: Direct Snowflake Connection (Recommended for Production)

```
┌─────────────────────────────────────────────────────────────────┐
│                        DataBridge                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ MCP Server  │  │ Hierarchy   │  │ SQL Translator          │  │
│  │ (Python)    │  │ Builder     │  │ (semantic views)        │  │
│  └──────┬──────┘  └──────┬──────┘  └───────────┬─────────────┘  │
│         │                │                      │                │
│         └────────────────┼──────────────────────┘                │
│                          │                                       │
│                          ▼                                       │
│              ┌───────────────────────┐                          │
│              │ Snowflake Connector   │                          │
│              │ (snowflake-connector- │                          │
│              │  python)              │                          │
│              └───────────┬───────────┘                          │
└──────────────────────────┼──────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                     Snowflake Data Cloud                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐   │
│  │ Cortex LLM      │  │ Cortex Analyst  │  │ Cortex Agent    │   │
│  │ Functions       │  │ + Semantic Views│  │ (orchestration) │   │
│  │ (SQL callable)  │  │                 │  │                 │   │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘   │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              DataBridge Hierarchies & Mappings               │ │
│  │              (deployed as Semantic Views)                    │ │
│  └─────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

**How it works:**
1. DataBridge generates semantic view DDL using `sql_translator.py`
2. Deploy semantic views to Snowflake
3. Cortex Analyst can now answer natural language questions about hierarchies
4. LLM functions enhance DataBridge suggestions via SQL calls

**Pros:**
- Data never leaves Snowflake (governance)
- Lowest latency - no external API calls
- Full RBAC integration
- Can use all Cortex features including Agents
- Token costs are transparent and within Snowflake billing

**Cons:**
- Requires Snowflake account with Cortex enabled
- Warehouse costs for LLM functions
- Must deploy hierarchies as semantic views first

### Option B: Cortex REST API (Recommended for Flexibility)

```
┌─────────────────────────────────────────────────────────────────┐
│                        DataBridge                                │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    MCP Server (Python)                       ││
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  ││
│  │  │ cortex_      │  │ cortex_      │  │ cortex_          │  ││
│  │  │ analyst.py   │  │ llm.py       │  │ agent.py         │  ││
│  │  └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘  ││
│  │         │                 │                    │            ││
│  │         └─────────────────┼────────────────────┘            ││
│  │                           │                                  ││
│  │                           ▼                                  ││
│  │               ┌─────────────────────┐                       ││
│  │               │ Cortex REST Client  │                       ││
│  │               │ (OpenAI SDK compat) │                       ││
│  │               └──────────┬──────────┘                       ││
│  └──────────────────────────┼──────────────────────────────────┘│
└──────────────────────────────┼──────────────────────────────────┘
                               │ HTTPS (PAT/JWT auth)
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│              Snowflake Cortex REST API                           │
│  https://<account>.snowflakecomputing.com/api/v2/cortex/         │
│                                                                   │
│  Endpoints:                                                       │
│  - /v1/complete (LLM completions)                                │
│  - /v1/analyst/messages (Cortex Analyst)                         │
│  - /v1/agents/run (Cortex Agents)                                │
└──────────────────────────────────────────────────────────────────┘
```

**How it works:**
1. DataBridge connects to Cortex via REST API
2. Uses PAT (Programmatic Access Token) or JWT for authentication
3. Can use OpenAI-compatible SDK for familiar interface
4. No warehouse required for REST API calls

**Pros:**
- Works without direct Snowflake connector
- OpenAI SDK compatible (easy migration)
- No warehouse costs for API calls
- Can integrate with existing DataBridge MCP tools

**Cons:**
- Network latency for API calls
- PAT/JWT token management required
- Some features may be limited compared to direct SQL

### Option C: Hybrid Approach (Recommended)

Use **both** approaches:
- **REST API** for real-time suggestions during hierarchy building
- **Direct SQL** for batch processing and deployed semantic views

---

## Pros and Cons Analysis

### Direct Snowflake Connection

| Pros | Cons |
|------|------|
| Data stays in Snowflake governance | Requires Snowflake account |
| Full Cortex feature access | Warehouse costs for LLM calls |
| Lowest latency | More complex setup |
| RBAC integration | Must deploy semantic views |
| Cortex Agents available | |

### REST API Connection

| Pros | Cons |
|------|------|
| No warehouse costs | Network latency |
| OpenAI SDK compatible | Token management complexity |
| Flexible integration | Some features unavailable |
| Works without full Snowflake setup | Still needs Snowflake account |

### Not Using Cortex (External LLM)

| Pros | Cons |
|------|------|
| Provider flexibility | Data leaves Snowflake |
| Potentially lower costs | No semantic view integration |
| No Snowflake dependency | Manual context building |
| | No Cortex Analyst accuracy |

---

## DataBridge-Cortex Agent Architecture

### Proposed Agent: `DataBridgeCortexAgent`

An AI agent that lives in Snowflake and uses DataBridge tools via MCP to:

1. **Analyze data quality** using Cortex LLM functions
2. **Suggest hierarchy structures** based on actual data patterns
3. **Validate mappings** against database schemas
4. **Answer natural language questions** about deployed hierarchies

```python
# Proposed Architecture

class DataBridgeCortexAgent:
    """
    A Cortex Agent that enhances DataBridge functionality
    by leveraging AI for data analysis and recommendations.
    """

    capabilities = [
        "analyze_hierarchy_data",      # Validate hierarchies against actual data
        "suggest_mappings",            # AI-powered mapping suggestions
        "natural_language_query",      # Query hierarchies via NL
        "validate_reconciliation",     # Check reconciliation results
        "explain_conflicts",           # Explain why orphans/conflicts exist
    ]

    tools = [
        "cortex_analyst",              # NL to SQL
        "cortex_search",               # Search documentation
        "databridge_hierarchy_api",    # Create/modify hierarchies
        "databridge_mapping_api",      # Manage mappings
        "snowflake_query",             # Execute validation queries
    ]
```

### Agent Workflow Example

```
User: "Show me all accounts that are mapped to Revenue but have
       negative balances in 2024"

DataBridgeCortexAgent:
  1. Use Cortex Analyst to understand the question
  2. Query the semantic view for Revenue mappings
  3. Join with GL data to find negative balances
  4. Format and return results
  5. Optionally suggest corrections
```

---

## Implementation Roadmap

### Phase 1: Foundation (2 weeks)

1. **Create `src/cortex/` module**
   - `client.py` - Cortex REST API client (OpenAI SDK compatible)
   - `analyst.py` - Cortex Analyst integration
   - `llm_functions.py` - LLM function wrappers

2. **Add MCP tools**
   - `ask_cortex_analyst` - Natural language hierarchy queries
   - `cortex_complete` - General LLM completions
   - `cortex_suggest_mapping` - AI-powered mapping suggestions

3. **Update SQL Translator**
   - Ensure generated semantic views are Cortex Analyst compatible
   - Add AI_SQL_GENERATION hints automatically

### Phase 2: Enhanced Recommendations (2 weeks)

1. **Integrate with Smart Recommendation Engine**
   - Use Cortex to analyze CSV patterns
   - Generate better template/skill matches
   - Explain recommendations in natural language

2. **Add Hierarchy Validation**
   - Validate mappings against actual database values
   - Identify orphaned accounts
   - Suggest missing mappings

### Phase 3: Cortex Agent (3 weeks)

1. **Build DataBridgeCortexAgent**
   - Register as Cortex Agent in Snowflake
   - Expose DataBridge tools via MCP
   - Enable multi-step reasoning

2. **Create Natural Language Interface**
   - "Create a P&L hierarchy from my GL data"
   - "Why do I have orphaned accounts?"
   - "Suggest mappings for ACCOUNT_CODE 4100-4199"

### Phase 4: Production Hardening (2 weeks)

1. **Cost optimization**
   - Token usage monitoring
   - Caching frequent queries
   - Batch processing for bulk operations

2. **Error handling & fallbacks**
   - Graceful degradation if Cortex unavailable
   - Retry logic with exponential backoff
   - Clear error messages

---

## Cost Considerations

### Cortex Pricing Model

| Component | Pricing Model | Estimated Cost |
|-----------|---------------|----------------|
| Cortex LLM Functions | Per million tokens | $0.10 - $15/M tokens (model dependent) |
| Cortex Analyst | Per query | ~$0.01 - $0.05 per query |
| Cortex Search | Per query | ~$0.001 per query |
| Cortex Agents | Per execution | Varies by complexity |
| REST API | Per million tokens | Same as LLM functions |

### Cost Optimization Strategies

1. **Cache responses** for repeated queries
2. **Use smaller models** for simple tasks (Mistral vs GPT-5)
3. **Batch similar requests** to reduce overhead
4. **Right-size semantic models** - smaller = faster = cheaper
5. **Monitor usage** via `METERING_DAILY_HISTORY` view

### Estimated Monthly Costs

| Usage Level | Queries/Month | Est. Monthly Cost |
|-------------|---------------|-------------------|
| Light | 1,000 | $10 - $50 |
| Moderate | 10,000 | $100 - $500 |
| Heavy | 100,000 | $1,000 - $5,000 |

---

## Security & Governance

### Data Security

- **Data stays in Snowflake** - By default, Cortex uses Snowflake-hosted LLMs (Mistral, Meta)
- **No data exfiltration** - Queries processed within Snowflake's boundary
- **Encryption** - All data encrypted at rest and in transit

### Access Control

- **RBAC integration** - Cortex respects all role-based access controls
- **Query filtering** - Users only see data they're authorized to access
- **Audit logging** - All Cortex queries logged for compliance

### Governance Considerations

1. **Semantic model governance** - Control who can create/modify semantic views
2. **LLM access control** - Grant `SNOWFLAKE.CORTEX_USER` role selectively
3. **Cost controls** - Set resource monitors for Cortex usage
4. **Response validation** - Implement guardrails for sensitive data

---

## Conclusion & Recommendations

### Should DataBridge Integrate with Cortex AI?

**Yes, with a phased approach.**

### Recommended Strategy

1. **Start with REST API integration** for flexibility
2. **Use Cortex Analyst** to query deployed semantic views
3. **Enhance recommendations** with Cortex LLM functions
4. **Build a Cortex Agent** for advanced orchestration (Phase 3)

### Key Benefits for DataBridge

| Feature | Benefit |
|---------|---------|
| Natural language queries | Users can ask questions about hierarchies without SQL |
| AI-powered suggestions | Better mapping and template recommendations |
| Data validation | Verify hierarchies against actual data |
| Reduced manual work | Auto-generate semantic views, mappings |
| Enterprise-grade security | Data never leaves Snowflake |

### When NOT to Use Cortex

- Client doesn't have Snowflake Enterprise edition
- Data cannot be stored in Snowflake (regulatory)
- Cost constraints are severe
- Real-time sub-second latency required

### Next Steps

1. Create proof-of-concept with REST API
2. Test Cortex Analyst with existing semantic view DDL
3. Measure accuracy and performance
4. Develop cost model based on actual usage
5. Build full integration if POC successful

---

---

## Agent Orchestration & Communication Console

For detailed architecture on multi-agent communication with full observability, see:

- **[Agent Orchestration Architecture](cortex_agent_orchestration_architecture.md)** - Full design for agent-to-agent communication
- **[Console Demo](cortex_agent_console_demo.html)** - Interactive demo of the communication console

### Key Features:
1. **Chain-of-thought reasoning** - Agents think step-by-step like Claude/Gemini
2. **Execution planning** - Create and execute multi-step plans
3. **Clarification requests** - Ask follow-up questions when needed
4. **Full observability** - See all agent communication in real-time
5. **Multiple outputs** - CLI, WebSocket, Database, File logging

---

## References

- [Snowflake Cortex AI Features](https://www.snowflake.com/en/product/features/cortex/)
- [Cortex Analyst Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)
- [Semantic Views Overview](https://docs.snowflake.com/en/user-guide/views-semantic/overview)
- [Cortex REST API](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-rest-api)
- [Cortex LLM Functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql)
- [Cortex Agents](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents)
- [Getting Started with Cortex Analyst](https://www.snowflake.com/en/developers/guides/getting-started-with-cortex-analyst/)
- [OpenAI SDK Compatibility](https://docs.snowflake.com/en/user-guide/snowflake-cortex/open_ai_sdk)
