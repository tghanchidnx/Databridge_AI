# DataBridge AI: Project Configuration & Rules

## 🎯 Purpose
A headless, MCP-native data reconciliation engine with **142 MCP tools** across eight major modules:

1. **Data Reconciliation Engine** - Bridges messy sources (OCR/PDF/SQL) with structured comparison pipelines
2. **Hierarchy Knowledge Base Builder** - Creates and manages hierarchical data structures for reporting systems
3. **Templates, Skills & Knowledge Base** - Pre-built templates, AI expertise definitions, and client-specific knowledge
4. **Git Automation** - Automated commits, PRs, and dbt project generation for CI/CD workflows
5. **SQL Discovery** - Extract hierarchies from SQL CASE statements automatically
6. **AI Orchestrator** - Multi-agent coordination with task queues and event-driven workflows
7. **PlannerAgent** - AI-powered workflow planning using Claude for intelligent task decomposition
8. **Smart Recommendation Engine** - Context-aware recommendations for CSV imports using skills, templates, and knowledge base

## 🔧 Available Tool Categories (142 Tools)

### Data Reconciliation (38 tools)
- **Data Loading**: `load_csv`, `load_json`, `query_database`
- **Profiling**: `profile_data`, `detect_schema_drift`
- **Comparison**: `compare_hashes`, `get_orphan_details`, `get_conflict_details`
- **Fuzzy Matching**: `fuzzy_match_columns`, `fuzzy_deduplicate`
- **OCR/PDF**: `extract_text_from_pdf`, `ocr_image`, `parse_table_from_text`
- **Transforms**: `transform_column`, `merge_sources`
- **Workflow**: `save_workflow_step`, `get_workflow`, `clear_workflow`, `get_audit_log`

### Hierarchy Knowledge Base (44 tools)
- **Projects**: `create_hierarchy_project`, `list_hierarchy_projects`, `delete_hierarchy_project`
- **Hierarchies**: `create_hierarchy`, `update_hierarchy`, `delete_hierarchy`, `get_hierarchy_tree`
- **Mappings**: `add_source_mapping`, `remove_source_mapping`, `get_inherited_mappings`, `get_mapping_summary`, `get_mappings_by_precedence`
- **Import/Export**: `export_hierarchy_csv`, `export_mapping_csv`, `import_hierarchy_csv`, `import_mapping_csv`, `export_project_json`
- **Flexible Import**: `detect_hierarchy_format`, `configure_project_defaults`, `get_project_defaults`, `preview_import`, `import_flexible_hierarchy`, `export_hierarchy_simplified`
- **Formulas**: `create_formula_group`, `add_formula_rule`, `list_formula_groups`
- **Deployment**: `generate_hierarchy_scripts`, `push_hierarchy_to_snowflake`, `get_deployment_history`
- **Backend Sync**: `sync_to_backend`, `sync_from_backend`, `configure_auto_sync`
- **Dashboard**: `get_dashboard_stats`, `get_recent_activities`, `search_hierarchies_backend`
- **Connections**: `list_backend_connections`, `get_connection_databases`, `get_connection_tables`
- **Schema Matching**: `compare_database_schemas`, `generate_merge_sql_script`
- **Data Matching**: `compare_table_data`, `get_data_comparison_summary`

#### Flexible Hierarchy Import System (Tiered)

Supports four complexity tiers for creating hierarchies from various input formats:

| Tier | Columns | Use Case | Auto-Generated |
|------|---------|----------|----------------|
| **Tier 1** | 2-3 (source_value, group_name) | Quick grouping, non-technical users | hierarchy_id, parent_id, flags, source_* |
| **Tier 2** | 5-7 (hierarchy_name, parent_name, sort_order) | Basic hierarchies with parent-child | hierarchy_id, parent_id from name lookup |
| **Tier 3** | 10-12 (explicit IDs, source info) | Full control without enterprise complexity | Minimal inference |
| **Tier 4** | 28+ (LEVEL_1-10, all flags, formulas) | Enterprise - current full format | None |

**Tier 1 Example (Ultra-Simple):**
```csv
source_value,group_name
4100,Revenue
4200,Revenue
5100,COGS
```

**Tier 2 Example (Basic with Parents):**
```csv
hierarchy_name,parent_name,source_value,sort_order
Revenue,,4%,1
Product Revenue,Revenue,41%,2
Service Revenue,Revenue,42%,3
```

**Tool Workflow:**
1. `detect_hierarchy_format(content)` - Analyze input, detect tier
2. `configure_project_defaults(project_id, database, schema, table, column)` - Set source defaults
3. `preview_import(content, source_defaults)` - Preview without creating
4. `import_flexible_hierarchy(project_id, content, source_defaults)` - Create hierarchies
5. `export_hierarchy_simplified(project_id, target_tier)` - Export in simpler format

#### CSV Import/Export Guidelines

**Hierarchy CSV** (`_HIERARCHY.CSV`) columns:
| Column | Description |
|--------|-------------|
| HIERARCHY_ID | Unique identifier |
| HIERARCHY_NAME | Display name |
| PARENT_ID | Parent hierarchy ID |
| DESCRIPTION | Optional description |
| LEVEL_1 - LEVEL_10 | Hierarchy level values |
| LEVEL_1_SORT - LEVEL_10_SORT | **Sort order for each level** |
| INCLUDE_FLAG, EXCLUDE_FLAG, etc. | Boolean flags |
| FORMULA_GROUP | Formula group name |
| SORT_ORDER | Overall sort order |

**Mapping CSV** (`_HIERARCHY_MAPPING.CSV`) columns:
| Column | Description |
|--------|-------------|
| HIERARCHY_ID | Links to hierarchy |
| MAPPING_INDEX | Order of mapping |
| SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN | Database references |
| SOURCE_UID | Specific value filter |
| PRECEDENCE_GROUP | Precedence grouping |
| INCLUDE_FLAG, EXCLUDE_FLAG, etc. | Boolean flags |

**IMPORTANT**: Sort orders (LEVEL_X_SORT) come from the HIERARCHY CSV, not the MAPPING CSV.

**Import Process**:
1. Always ask: "Is this an older/legacy version CSV format?"
2. Request TWO CSV files
3. Import hierarchy CSV first, then mapping CSV

### Templates, Skills & Knowledge Base (16 tools)
- **Templates**: `list_financial_templates`, `get_template_details`, `create_project_from_template`, `save_project_as_template`, `get_template_recommendations`
- **Skills**: `list_available_skills`, `get_skill_details`, `get_skill_prompt`
- **Knowledge Base**: `list_client_profiles`, `get_client_knowledge`, `update_client_knowledge`, `create_client_profile`, `add_client_custom_prompt`
- **Documentation**: `list_application_documentation`, `get_application_documentation`, `get_user_guide_section`

### Git Automation (4 tools)
- **Git Operations**: `commit_dbt_project`, `create_deployment_pr`, `commit_deployment_scripts`, `get_git_status`

### SQL Discovery (2 tools)
- **SQL to Hierarchy**: `sql_to_hierarchy` - Convert SQL CASE statements into hierarchies with mappings
- **SQL Analysis**: `analyze_sql_for_hierarchies` - Analyze SQL to identify potential hierarchy structures

#### SQL to Hierarchy Example
```sql
-- Input SQL with CASE statement
SELECT
  CASE
    WHEN account_code LIKE '4%' THEN 'Revenue'
    WHEN account_code LIKE '5%' THEN 'Cost of Goods Sold'
    WHEN account_code LIKE '6%' THEN 'Operating Expenses'
    ELSE 'Other'
  END AS account_category
FROM gl_transactions
```

**Tool call:**
```python
sql_to_hierarchy(
    sql=sql,
    project_id="my-project",
    source_database="WAREHOUSE",
    source_schema="FINANCE",
    source_table="GL_TRANSACTIONS",
    source_column="ACCOUNT_CODE"
)
```

**Result:** Creates hierarchies with source mappings:
| Hierarchy | Source Mapping |
|-----------|----------------|
| Revenue | ACCOUNT_CODE LIKE '4%' |
| Cost of Goods Sold | ACCOUNT_CODE LIKE '5%' |
| Operating Expenses | ACCOUNT_CODE LIKE '6%' |

### Smart SQL Analyzer (2 tools) - **RECOMMENDED**
The Smart SQL Analyzer properly processes SQL queries by respecting WHERE clause filters.

- **`smart_analyze_sql`** - Full SQL analysis with query plan execution
- **`parse_sql_query_plan`** - Preview query plan without executing

**IMPORTANT**: Unlike basic CASE extraction, this tool:
1. Parses WHERE clause filters (NOT IN, <>, NOT LIKE)
2. APPLIES filters BEFORE generating mappings
3. Excludes invalid GL categories and account codes
4. Uses COA reference data with filter awareness

**Example with WHERE Filtering:**
```sql
SELECT CASE WHEN account_code LIKE '8%' THEN 'G&A' ... END AS gl
FROM gl_entries
WHERE gl NOT IN ('Hedge Gains', 'DD&A', 'G&A')  -- G&A will be EXCLUDED
  AND account_code NOT LIKE '242%'              -- 242-xxx accounts excluded
```

**Tool call:**
```python
smart_analyze_sql(
    sql=sql,
    coa_path="C:/data/DIM_ACCOUNT.csv",
    output_dir="./result_export",
    export_name="los_analysis"
)
```

### Mapping Enrichment (5 tools)
Configurable reference data enrichment for mapping exports.

- **`configure_mapping_enrichment`** - Set up data sources with detail columns
- **`get_enrichment_config`** - View current configuration
- **`enrich_mapping_file`** - Expand mappings with reference data
- **`get_available_columns_for_enrichment`** - List available columns
- **`suggest_enrichment_after_hierarchy`** - AI prompt for enrichment

**Configurable Detail Columns:**
```python
configure_mapping_enrichment(
    project_id="my-project",
    source_id="coa",
    source_type="csv",
    source_path="C:/data/DIM_ACCOUNT.csv",
    table_name="DIM_ACCOUNT",
    key_column="ACCOUNT_CODE",
    detail_columns="ACCOUNT_ID,ACCOUNT_NAME,ACCOUNT_BILLING_CATEGORY_CODE"
)
```

### AI Orchestrator (16 tools)
Multi-agent coordination with task queues, agent messaging, and event-driven workflows.

**Task Management:**
- **`submit_orchestrated_task`** - Submit a task to the job queue with priority and dependencies
- **`get_task_status`** - Get task progress and checkpoints
- **`list_orchestrator_tasks`** - List tasks with status filters
- **`cancel_orchestrated_task`** - Cancel a pending or running task

**Agent Registration:**
- **`register_agent`** - Register an external agent with capabilities
- **`list_registered_agents`** - List agents with health status
- **`get_agent_details`** - Get agent info and capabilities

**Agent Messaging (AI-Link):**
- **`send_agent_message`** - Send message to another agent
- **`get_agent_messages`** - Get messages for an agent
- **`list_agent_conversations`** - List active conversations

**Workflows:**
- **`create_orchestrator_workflow`** - Create a multi-step workflow definition
- **`execute_orchestrator_workflow`** - Start workflow execution
- **`list_orchestrator_workflows`** - List available workflows
- **`get_workflow_execution_status`** - Get execution progress

**Events:**
- **`publish_orchestrator_event`** - Publish event to Event Bus (Redis Pub/Sub)
- **`get_orchestrator_health`** - Get orchestrator health status

### PlannerAgent - AI Workflow Planning (11 tools)
AI-powered workflow planning using Claude to decompose complex requests into executable steps.

- **`plan_workflow`** - Create a workflow plan from natural language request
- **`analyze_request`** - Analyze user request to understand intent and requirements
- **`suggest_agents`** - Get ranked agent suggestions for a task
- **`explain_plan`** - Generate human-readable explanation of a workflow plan
- **`optimize_plan`** - Optimize a plan for parallel execution
- **`get_workflow_definition`** - Convert plan to Orchestrator-compatible format
- **`list_available_agents`** - List all agents available for planning
- **`register_custom_agent`** - Register a custom agent for workflow planning
- **`get_planner_status`** - Get planner status
- **`get_planning_history`** - Get history of recent plans
- **`configure_planner`** - Configure planner settings (model, temperature, etc.)

**Example - Plan a Workflow:**
```python
plan_workflow(
    request="Extract hierarchies from SQL CASE statements and deploy to Snowflake",
    context='{"schema": "FINANCE", "database": "WAREHOUSE"}'
)
```

**Returns:** Structured workflow plan with:
- Ordered steps with agent assignments
- Input/output mappings between steps
- Dependency graph for parallel execution
- Confidence score and estimated duration
- Reasoning for each step

**Available Agents for Planning:**
| Agent | Capabilities | Description |
|-------|-------------|-------------|
| `schema_scanner` | scan_schema, extract_metadata, detect_keys | Scans database schemas |
| `logic_extractor` | parse_sql, extract_case, identify_calcs | Extracts logic from SQL |
| `warehouse_architect` | design_star_schema, generate_dims, dbt_models | Designs data warehouses |
| `deploy_validator` | execute_ddl, run_dbt, validate_counts | Deploys and validates |
| `hierarchy_builder` | create_hierarchy, import_hierarchy, add_properties | Manages hierarchies |
| `data_reconciler` | compare_sources, fuzzy_match, identify_orphans | Reconciles data |

### Smart Recommendation Engine (5 tools)
Context-aware recommendation system for CSV imports that leverages DataBridge's skills, templates, and knowledge base.

- **`get_smart_recommendations`** - Get comprehensive recommendations for CSV import
- **`get_llm_validation_prompt`** - Get formatted prompt for LLM to validate recommendations
- **`suggest_enrichment_after_hierarchy`** - Get enrichment suggestions post-import
- **`smart_import_csv`** - Intelligent CSV import with automatic recommendations
- **`get_recommendation_context`** - View available skills, templates, and knowledge base

**How It Works:**
```
User CSV + Context
        ↓
┌─────────────────────────────────────────────┐
│      DataBridge Recommendation Engine       │
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
└─────────────────────────────────────────────┘
```

**Example - Get Smart Recommendations:**
```python
get_smart_recommendations(
    file_path="C:/data/gl_accounts.csv",
    user_intent="Build a P&L hierarchy for upstream oil and gas",
    industry="oil_gas"
)
```

**Returns:**
- **data_profile**: Column analysis, detected patterns, industry/domain hints
- **import_tier**: Recommended tier (1-4) with reasoning
- **skills**: Top 3 skill recommendations with scores
- **templates**: Top 3 template recommendations with scores
- **knowledge**: Client-specific pattern matches
- **summary**: Human-readable recommendation summary

**Detected Industries:**
| Industry | Keywords |
|----------|----------|
| `oil_gas` | well, field, basin, lease, royalty, LOE, DD&A, BOE |
| `manufacturing` | plant, BOM, WIP, variance, standard_cost |
| `saas` | ARR, MRR, churn, LTV, CAC, cohort |
| `transportation` | fleet, lane, terminal, operating_ratio |

### Auto-Sync Feature
All hierarchy write operations (create, update, delete) **automatically sync** to the NestJS backend.
Use `configure_auto_sync(enabled=False)` to disable if needed.

## 📋 Available Templates (20 Templates)

Templates are organized by **domain** and **industry** for easy filtering.

### Accounting Domain (10 templates)
| Template ID | Name | Industry | Description |
|-------------|------|----------|-------------|
| `standard_pl` | Standard P&L | General | Standard income statement for most businesses |
| `standard_bs` | Standard Balance Sheet | General | Assets, liabilities, and equity structure |
| `oil_gas_los` | Oil & Gas LOS | Oil & Gas | Lease Operating Statement for upstream ops |
| `upstream_oil_gas_pl` | Upstream Oil & Gas P&L | Oil & Gas - E&P | Full E&P income statement with LOE breakdown |
| `midstream_oil_gas_pl` | Midstream Oil & Gas P&L | Oil & Gas - Midstream | Fee-based revenue and DCF calculations |
| `oilfield_services_pl` | Oilfield Services P&L | Oil & Gas - Services | Service line revenue and utilization |
| `manufacturing_pl` | Industrial Manufacturing P&L | Manufacturing | COGS breakdown with variances |
| `industrial_services_pl` | Industrial Services P&L | Industrial Services | Project/contract revenue model |
| `saas_pl` | SaaS Company P&L | SaaS | ARR/MRR tracking and unit economics |
| `transportation_pl` | Transportation & Logistics P&L | Transportation | Operating ratio and fleet metrics |

### Operations Domain (8 templates)
| Template ID | Name | Industry | Description |
|-------------|------|----------|-------------|
| `geographic_hierarchy` | Geographic Hierarchy | General | Global regions, countries, states, cities |
| `department_hierarchy` | Department Hierarchy | General | Organizational functions and teams |
| `asset_hierarchy` | Asset Class Hierarchy | General | Fixed asset classification |
| `legal_entity_hierarchy` | Legal Entity Hierarchy | General | Parent-subsidiary structure |
| `upstream_field_hierarchy` | Upstream Field Hierarchy | Oil & Gas - E&P | Basin → Field → Well structure |
| `midstream_asset_hierarchy` | Midstream Asset Hierarchy | Oil & Gas - Midstream | Processing plants and pipelines |
| `manufacturing_plant_hierarchy` | Manufacturing Plant Hierarchy | Manufacturing | Region → Plant → Line → Work Center |
| `fleet_hierarchy` | Fleet & Route Hierarchy | Transportation | Power units, trailers, terminals, lanes |

### Finance Domain (2 templates)
| Template ID | Name | Industry | Description |
|-------------|------|----------|-------------|
| `cost_center_hierarchy` | Cost Center Hierarchy | General | Expense allocation and responsibility |
| `profit_center_hierarchy` | Profit Center Hierarchy | General | Profitability by segment/product |

### Industry Categories
- **General** - Industry-agnostic, suitable for most businesses
- **Oil & Gas** - Parent category with sub-industries:
  - `oil_gas_upstream` - Exploration & Production (E&P)
  - `oil_gas_midstream` - Gathering, processing, transportation
  - `oil_gas_services` - Oilfield services (drilling, completions)
- **Manufacturing** - Discrete and process manufacturing
- **Industrial Services** - Maintenance, facility, equipment services
- **SaaS** - Software as a Service companies
- **Transportation** - Trucking, freight, 3PL, logistics

## 🎓 Available Skills (7 Skills)

Skills are organized by **domain** with industry specializations.

### Accounting Domain
| Skill ID | Name | Industries | Capabilities |
|----------|------|------------|--------------|
| `financial-analyst` | Financial Analyst | General | GL reconciliation, trial balance, bank rec, COA design |
| `manufacturing-analyst` | Manufacturing Analyst | Manufacturing | Standard costing, COGS, variances, inventory |

### Finance Domain
| Skill ID | Name | Industries | Capabilities |
|----------|------|------------|--------------|
| `fpa-oil-gas-analyst` | FP&A Oil & Gas Analyst | Oil & Gas | LOS analysis, JIB, reserves, hedge accounting |
| `fpa-cost-analyst` | FP&A Cost Analyst | General, Manufacturing, SaaS | Cost centers, budgets, allocations, FP&A |
| `saas-metrics-analyst` | SaaS Metrics Analyst | SaaS, Technology | ARR/MRR, cohorts, CAC/LTV, unit economics |

### Operations Domain
| Skill ID | Name | Industries | Capabilities |
|----------|------|------------|--------------|
| `operations-analyst` | Operations Analyst | General, Manufacturing, Transportation | Geographic, department, asset hierarchies |
| `transportation-analyst` | Transportation Analyst | Transportation, Logistics | Operating ratio, fleet, lanes, driver metrics |

## 🛠 Tech Stack

### Librarian/Researcher Architecture (Current)
- **Frontend:** React + TypeScript + Vite + Tailwind CSS (Docker: port 8000)
- **Backend:** NestJS + TypeScript + MySQL (Docker: port 8001)
- **MCP Server:** Python 3.10+ + FastMCP
- **Data Engine:** Pandas, SQLAlchemy, RapidFuzz
- **Database:** MySQL 8.0 (Docker: port 3308)
- **Cache:** Redis 7 (Docker: port 6381)

### Librarian/Researcher Service Ports
| Service | Port | Container |
|---------|------|-----------|
| Frontend | 8000 | databridge-librarian |
| Backend | 8001 | databridge-researcher |
| MySQL | 3308 | databridge-mysql-v2 |
| Redis | 6381 | databridge-redis-v2 |

### Librarian/Researcher API Keys
- `v2-dev-key-1` - Primary development key
- `v2-dev-key-2` - Secondary development key

## 📂 Folder Structure
```
C:\Users\telha\Databridge_AI\
├── /src                    # Core logic and FastMCP server
│   └── /templates          # Service layer for templates/skills/KB
├── /data                   # workflow.json and audit_trail.csv
├── /docs                   # Auto-generated MANIFEST.md
├── /tests                  # Pytest suite
├── /templates              # Hierarchy templates by domain
│   ├── index.json          # Template registry with metadata
│   ├── /accounting         # P&L, Balance Sheet, industry-specific
│   ├── /finance            # Cost center, profit center, budget
│   └── /operations         # Geographic, department, asset, fleet
├── /skills                 # AI expertise definitions
│   ├── index.json          # Skill registry with industry mappings
│   ├── *-prompt.txt        # System prompts for each skill
│   └── *.md                # Documentation for each skill
└── /knowledge_base         # Client-specific configurations
    ├── index.json          # Client registry
    └── /clients/{id}/      # Per-client config, prompts, mappings
```

## 📜 Development Rules
1. **Tool-First Design:** Every capability must be an @mcp.tool with detailed docstrings.
2. **Atomic Commits:** When using Claude Code, implement one tool at a time and verify via `fastmcp dev`.
3. **Context Sensitivity:** Never return more than 10 rows of raw data to the LLM. Use `df.describe()` or `row_hashes` for large tables.
4. **Living Docs:** After adding/modifying a tool, run the `update_manifest` tool to keep `docs/MANIFEST.md` current.
5. **Review Lessons Learned:** Before making changes, review `docs/LESSONS_LEARNED.md` for common pitfalls.

## 🔄 Librarian/Researcher Development Workflow

### Before Making Frontend Changes
1. Check if running in Docker: `docker ps | grep frontend`
2. Make code changes locally
3. Rebuild Docker image: `cd v2 && docker-compose build frontend-v2 --no-cache`
4. Restart container: `docker-compose up -d frontend-v2`
5. Hard refresh browser: `Ctrl+Shift+R`

### Common Issues (See docs/LESSONS_LEARNED.md)
- **Tree not showing children**: Check `parentId` references `hierarchyId`, not `id`
- **Hover effects not working**: Ensure parent has `group` class, rebuild Docker
- **AI changes not applying**: Check `data.changes` from backend response
- **API calls failing**: Verify correct port (8001 for Researcher) and API key

### Quick Commands
```bash
# Start all services
python start_services.py

# Rebuild and restart frontend
cd v2 && docker-compose build frontend-v2 --no-cache && docker-compose up -d frontend-v2

# Check logs
docker logs databridge-frontend-v2 --tail 50

# Test backend
curl http://localhost:8001/api/health
```

## ⚠️ Safety Guardrails
- Securely load keys from `.env` using Pydantic Settings.
- No PII or raw secrets in the `audit_trail.log`.
- Knowledge Base stores client data locally (not synced to backend) for security.
