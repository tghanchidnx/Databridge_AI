# DataBridge AI Skills Library

## Overview

Skills are AI personas that adapt communication style, terminology, and approach based on the user's professional role. When activated, the AI responds as if it were an expert in that domain.

## Available Skills (7 Total)

| Skill ID | Name | Domain | Best For |
|----------|------|--------|----------|
| `fpa-analyst` | FP&A Analyst | Finance | Budgeting, forecasting, variance analysis, KPI dashboards |
| `accountant` | Accountant/Controller | Accounting | GL reconciliation, month-end close, GAAP/IFRS, audit support |
| `operations-manager` | Operations Manager | Operations | OEE tracking, throughput, downtime analysis, ops-to-finance translation |
| `c-suite-executive` | C-Suite Executive | Executive | Board prep, strategic insights, M&A, rapid decision support |
| `business-analyst` | Business/Data Analyst | Analytics | Data profiling, hierarchy building, reconciliation workflows |
| `database-developer` | Database Developer | Technical | Schema design, SQL optimization, ETL pipelines, Snowflake deployment |
| `it-manager-cio` | IT Manager/CIO | Technology | System strategy, security, TCO analysis, vendor management |

## How to Use Skills

### CLI Usage

```bash
# List available skills
databridge skill list

# View skill details
databridge skill show fpa-analyst

# Get skill system prompt
databridge skill prompt fpa-analyst

# Activate skill for session
databridge skill activate fpa-analyst
```

### MCP Usage (with Claude)

When using DataBridge AI as an MCP server with Claude, you can activate skills:

```
"Activate the FP&A Analyst skill"
→ AI uses get_skill_prompt('fpa-analyst') and adopts the persona

"Help me with month-end close as an Accountant"
→ AI responds with accounting precision and GAAP references

"I'm a CFO - give me the executive summary"
→ AI uses C-Suite communication style: headline first, strategic context
```

### Skill Prompt Structure

Each skill prompt includes:

1. **Role & Expertise** - Background and core competencies
2. **Communication Style** - How to speak, terminology to use
3. **Approach Methodology** - Step-by-step workflows for common tasks
4. **Industry Knowledge** - Sector-specific insights (O&G, Manufacturing, etc.)
5. **Tool Recommendations** - Which DataBridge tools to use when
6. **Pain Points** - Understanding of common frustrations
7. **Example Responses** - Detailed templates for typical questions
8. **Mindset** - Core beliefs and principles

## Skill Selection Guide

### By Task

| Task | Recommended Skill |
|------|-------------------|
| Build P&L hierarchy | `fpa-analyst` or `business-analyst` |
| Reconcile GL to subledger | `accountant` |
| Explain OEE to finance | `operations-manager` |
| Prepare board presentation | `c-suite-executive` |
| Profile and clean data | `business-analyst` |
| Optimize hierarchy queries | `database-developer` |
| Evaluate vendor solution | `it-manager-cio` |

### By Communication Need

| Need | Recommended Skill |
|------|-------------------|
| Business storytelling | `fpa-analyst` or `c-suite-executive` |
| Technical precision | `accountant` or `database-developer` |
| Action-oriented response | `operations-manager` |
| ROI justification | `it-manager-cio` |
| Step-by-step guidance | `business-analyst` |

### By Audience

| Presenting To | Use Skill |
|---------------|-----------|
| Board of Directors | `c-suite-executive` |
| External Auditors | `accountant` |
| Shop Floor Managers | `operations-manager` |
| IT Leadership | `it-manager-cio` |
| Finance Team | `fpa-analyst` |
| Implementation Team | `database-developer` |

## Skill Files

```
skills/
├── index.json                    # Skill registry with metadata
├── README.md                     # This file
├── fpa-analyst-prompt.txt        # FP&A Analyst system prompt
├── accountant-prompt.txt         # Accountant/Controller system prompt
├── operations-manager-prompt.txt # Operations Manager system prompt
├── c-suite-executive-prompt.txt  # C-Suite Executive system prompt
├── business-analyst-prompt.txt   # Business Analyst system prompt
├── database-developer-prompt.txt # Database Developer system prompt
└── it-manager-cio-prompt.txt     # IT Manager/CIO system prompt
```

## Customizing Skills

Skills can be extended with client-specific knowledge:

```bash
# Add custom prompt to client profile
databridge kb prompt add <client-id> "Always use IFRS instead of GAAP"

# Custom prompts are appended to skill prompts when that client is active
```

## Integration with Vector Embeddings

Skills are indexed in the vector store for semantic retrieval:

- When a user asks a question, the system can suggest relevant skills
- Industry patterns are linked to appropriate skill personas
- Cross-skill knowledge enables multi-perspective responses

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 3.0.0 | 2025-01-28 | Initial 7 skills based on whitepaper roles |

---

*Part of the Headless DataBridge AI Python CLI*
