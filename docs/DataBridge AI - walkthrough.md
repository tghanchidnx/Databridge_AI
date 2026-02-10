● DataBridge AI - Complete Application Walkthrough

  DataBridge AI is a headless, MCP-native data reconciliation engine with 96 MCP tools designed for financial data
  management, hierarchy building, and automated deployments.

  ---
  🏗️ Architecture Overview

  ┌─────────────────────────────────────────────────────────────────────┐
  │                        DataBridge AI Platform                        │
  ├─────────────────────────────────────────────────────────────────────┤
  │                                                                      │
  │  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐   │
  │  │   Frontend   │◄──►│   Backend    │◄──►│    MCP Server        │   │
  │  │  React/Vite  │    │   NestJS     │    │  (Python/FastMCP)    │   │
  │  │  Port: 8000  │    │  Port: 8001  │    │                      │   │
  │  └──────────────┘    └──────────────┘    └──────────────────────┘   │
  │         │                   │                      │                 │
  │         └───────────────────┼──────────────────────┘                 │
  │                             ▼                                        │
  │  ┌──────────────────────────────────────────────────────────────┐   │
  │  │                     Data Layer                                │   │
  │  │  ┌────────────┐  ┌────────────┐  ┌────────────────────────┐  │   │
  │  │  │   MySQL    │  │   Redis    │  │  Local SQLite + JSON   │  │   │
  │  │  │ Port: 3308 │  │ Port: 6381 │  │    (databridge.db)     │  │   │
  │  │  └────────────┘  └────────────┘  └────────────────────────┘  │   │
  │  └──────────────────────────────────────────────────────────────┘   │
  └─────────────────────────────────────────────────────────────────────┘

  ---
  📦 Four Major Modules

  1️⃣ Data Reconciliation Engine (38 tools)

  Purpose: Bridge messy data sources with structured comparison pipelines.
  ┌────────────────┬──────────────────────────────────────────────────────────┬────────────────────────────────────────┐
  │   Capability   │                          Tools                           │              Description               │
  ├────────────────┼──────────────────────────────────────────────────────────┼────────────────────────────────────────┤
  │ Data Loading   │ load_csv, load_json, query_database                      │ Ingest from CSV, JSON, or SQL          │
  ├────────────────┼──────────────────────────────────────────────────────────┼────────────────────────────────────────┤
  │ Profiling      │ profile_data, detect_schema_drift                        │ Analyze structure, find anomalies      │
  ├────────────────┼──────────────────────────────────────────────────────────┼────────────────────────────────────────┤
  │ Comparison     │ compare_hashes, get_orphan_details, get_conflict_details │ Row-level reconciliation               │
  ├────────────────┼──────────────────────────────────────────────────────────┼────────────────────────────────────────┤
  │ Fuzzy Matching │ fuzzy_match_columns, fuzzy_deduplicate                   │ RapidFuzz-powered matching             │
  ├────────────────┼──────────────────────────────────────────────────────────┼────────────────────────────────────────┤
  │ OCR/PDF        │ extract_text_from_pdf, ocr_image, parse_table_from_text  │ Extract data from unstructured sources │
  ├────────────────┼──────────────────────────────────────────────────────────┼────────────────────────────────────────┤
  │ Transforms     │ transform_column, merge_sources                          │ Clean and combine datasets             │
  ├────────────────┼──────────────────────────────────────────────────────────┼────────────────────────────────────────┤
  │ Workflow       │ save_workflow_step, get_workflow, get_audit_log          │ Track reconciliation steps             │
  └────────────────┴──────────────────────────────────────────────────────────┴────────────────────────────────────────┘
  Typical Flow:
  CSV/PDF/SQL → Load → Profile → Compare → Find Orphans/Conflicts → Transform → Merge

  ---
  2️⃣ Hierarchy Knowledge Base Builder (38 tools)

  Purpose: Create multi-level hierarchies for financial reporting systems.

                      ┌─────────────────┐
                      │  Total Revenue  │  ← Calculated (SUM of children)
                      └────────┬────────┘
             ┌─────────────────┼─────────────────┐
             ▼                 ▼                 ▼
      ┌──────────┐      ┌──────────┐      ┌──────────┐
      │ Product  │      │ Services │      │  Other   │
      │ Revenue  │      │ Revenue  │      │ Revenue  │
      └────┬─────┘      └──────────┘      └──────────┘
           │
      ┌────┴────┐
      ▼         ▼
  ┌───────┐ ┌───────┐
  │ Prod A│ │ Prod B│  ← Leaf nodes with source mappings
  └───────┘ └───────┘     (maps to GL accounts 4100-500, etc.)

  Key Concepts:
  - Projects - Container for related hierarchies
  - Hierarchies - Tree nodes up to 15 levels deep
  - Source Mappings - Link GL accounts/columns to hierarchy nodes
  - Formulas - Calculate parent values (SUM, SUBTRACT, MULTIPLY, DIVIDE)
  - Precedence Groups - Control mapping priority

  Deployment Pipeline:
  Create Project → Build Hierarchy → Add Mappings → Validate → Generate SQL → Deploy to Snowflake

  ---
  3️⃣ Templates, Skills & Knowledge Base (16 tools)

  Purpose: Accelerate work with pre-built structures and AI expertise.

  Templates (20 available)

  Pre-built hierarchy structures by domain:
  - Accounting: Standard P&L, Balance Sheet, Oil & Gas LOS, SaaS P&L, Manufacturing P&L
  - Operations: Geographic, Department, Asset, Fleet hierarchies
  - Finance: Cost Center, Profit Center hierarchies

  Skills (7 available)

  AI expertise definitions that configure analysis behavior:
  - financial-analyst - GL reconciliation, COA design
  - fpa-oil-gas-analyst - Lease Operating Statements, JIB, reserves
  - saas-metrics-analyst - ARR/MRR, cohorts, unit economics
  - manufacturing-analyst - Standard costing, variances
  - transportation-analyst - Operating ratios, fleet metrics

  Client Knowledge Base

  Store client-specific configurations:
  - COA patterns
  - Custom prompts
  - GL mapping rules
  - Preferred templates/skills

  ---
  4️⃣ Git Automation (4 tools)

  Purpose: Automated CI/CD for data deployments.
  ┌───────────────────────────┬───────────────────────────────────────┐
  │           Tool            │              Description              │
  ├───────────────────────────┼───────────────────────────────────────┤
  │ commit_dbt_project        │ Generate and commit dbt project files │
  ├───────────────────────────┼───────────────────────────────────────┤
  │ create_deployment_pr      │ Create PRs for hierarchy deployments  │
  ├───────────────────────────┼───────────────────────────────────────┤
  │ commit_deployment_scripts │ Commit generated SQL scripts          │
  ├───────────────────────────┼───────────────────────────────────────┤
  │ get_git_status            │ Check repository state                │
  └───────────────────────────┴───────────────────────────────────────┘
  ---
  🔄 Auto-Sync Feature

  All hierarchy write operations automatically sync between:
  - MCP Server (local SQLite) ↔ NestJS Backend (MySQL)

  This keeps the CLI and Web UI in sync without manual intervention.

  ---
  📁 Project Structure

  databridge_ai/
  ├── apps/
  │   ├── databridge-librarian/     # MCP Server (Python)
  │   │   ├── src/
  │   │   │   ├── mcp/              # MCP tools and server
  │   │   │   ├── hierarchy/        # Hierarchy service
  │   │   │   ├── source/           # Source discovery & analysis
  │   │   │   └── sql_generator/    # View/script generation
  │   │   └── data/                 # SQLite DB, workflows
  │   │
  │   └── databridge-researcher/    # Backend (NestJS)
  │       └── src/
  │           └── dynamic_tables/   # Aggregation & formulas
  │
  ├── libs/
  │   ├── databridge-core/          # Shared utilities
  │   ├── databridge-models/        # Data models
  │   └── databridge-discovery/     # Session management
  │
  ├── templates/                    # Hierarchy templates
  │   ├── accounting/
  │   ├── finance/
  │   └── operations/
  │
  ├── skills/                       # AI expertise definitions
  │   └── *-prompt.txt
  │
  └── knowledge_base/               # Client configurations
      └── clients/

  ---
  🚀 Typical Workflows

  Workflow 1: Data Reconciliation

  1. load_csv("source.csv") + load_csv("target.csv")
  2. profile_data() - understand structure
  3. compare_hashes(key_columns="ID") - find differences
  4. get_orphan_details() - see missing records
  5. get_conflict_details() - see value mismatches
  6. fuzzy_match_columns() - handle near-matches
  7. merge_sources() - create reconciled output

  Workflow 2: Build Financial Hierarchy

  1. list_financial_templates() - browse options
  2. create_project_from_template("upstream_oil_gas_pl", "Client X P&L")
  3. list_hierarchies() - see structure
  4. add_source_mapping() - link GL accounts to nodes
  5. validate_hierarchy_project() - check for issues
  6. generate_deployment_scripts() - create SQL
  7. push_hierarchy_to_snowflake() - deploy

  Workflow 3: Import Existing Hierarchy

  1. Ask user: "Is this a legacy CSV format?"
  2. import_hierarchy_csv(csv_content) - import structure
  3. import_mapping_csv(csv_content) - import mappings
  4. validate_hierarchy_project() - verify
  5. sync_to_backend() - push to Web UI

  ---
  🔑 Key API Details
  ┌──────────────┬────────────────────────────┐
  │     Item     │           Value            │
  ├──────────────┼────────────────────────────┤
  │ Frontend URL │ http://localhost:8000      │
  ├──────────────┼────────────────────────────┤
  │ Backend API  │ http://localhost:8001      │
  ├──────────────┼────────────────────────────┤
  │ API Keys     │ dev-key-1, dev-key-2       │
  ├──────────────┼────────────────────────────┤
  │ MySQL Port   │ 3308                       │
  ├──────────────┼────────────────────────────┤
  │ Redis Port   │ 6381                       │
  └──────────────┴────────────────────────────┘
  ---
  💡 Design Principles

  1. Tool-First - Every capability is an MCP tool with detailed docstrings
  2. Context Sensitivity - Never return >10 rows to LLM; use summaries
  3. Atomic Operations - One tool = one focused action
  4. Auto-Documentation - update_manifest keeps docs current
  5. Audit Trail - All operations logged for compliance