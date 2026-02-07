# Implementation Plans: Phases 23-25

## Phase 23: WebSocket Console Dashboard

### Overview
Real-time agent activity streaming with a web-based Communication Console for live reasoning loop visualization.

### Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                    WebSocket Console                            │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │           FastAPI WebSocket Server                       │   │
│  │  - /ws/console/{session_id}  - Live console stream      │   │
│  │  - /ws/agent/{agent_id}      - Agent-specific stream    │   │
│  │  - /ws/reasoning/{conv_id}   - Reasoning loop stream    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           ↓                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │           Event Bus (Redis Pub/Sub)                      │   │
│  │  - agent.message.*     - Agent messages                  │   │
│  │  - console.log.*       - Console entries                │   │
│  │  - reasoning.step.*    - Reasoning loop updates         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           ↓                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │           Web Dashboard (React/HTML)                     │   │
│  │  - Live console log viewer                              │   │
│  │  - Reasoning loop visualizer (step-by-step)             │   │
│  │  - Agent activity monitor                               │   │
│  │  - Interactive query panel                              │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Deliverables (8 files)

| File | Purpose |
|------|---------|
| `src/console_ws/server.py` | FastAPI WebSocket server |
| `src/console_ws/handlers.py` | WebSocket message handlers |
| `src/console_ws/broadcaster.py` | Redis pub/sub broadcaster |
| `src/console_ws/types.py` | WebSocket message types |
| `src/console_ws/mcp_tools.py` | 5 MCP tools for console control |
| `src/console_ws/__init__.py` | Module exports |
| `console_dashboard/index.html` | Single-page dashboard |
| `console_dashboard/app.js` | Dashboard JavaScript |

### MCP Tools (5)

| Tool | Description |
|------|-------------|
| `start_console_server` | Start WebSocket server on port |
| `stop_console_server` | Stop WebSocket server |
| `get_console_connections` | List active WebSocket connections |
| `broadcast_console_message` | Send message to all clients |
| `get_console_server_status` | Get server status and stats |

### Key Features

1. **Live Console Stream**
   - Real-time log entries as they happen
   - Filter by message type, agent, conversation
   - Color-coded by severity/type

2. **Reasoning Loop Visualizer**
   - Step-by-step visualization of OBSERVE → PLAN → EXECUTE → REFLECT
   - Expandable details for each step
   - Cortex query/response display
   - Progress indicator

3. **Agent Activity Monitor**
   - Active agents and their status
   - Message flow between agents
   - Task queue visualization

---

## Phase 24: dbt Integration

### Overview
Generate dbt models from DataBridge hierarchies with project scaffolding and CI/CD pipeline generation.

### Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                    dbt Integration Module                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │           DbtProjectGenerator                            │   │
│  │  - Generate dbt_project.yml                             │   │
│  │  - Create profiles.yml template                         │   │
│  │  - Scaffold directory structure                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           ↓                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │           DbtModelGenerator                              │   │
│  │  - Hierarchy → staging model                            │   │
│  │  - Hierarchy → dimension model                          │   │
│  │  - Mappings → source definitions                        │   │
│  │  - Formulas → metrics definitions                       │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           ↓                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │           CiCdGenerator                                  │   │
│  │  - GitHub Actions workflow                              │   │
│  │  - GitLab CI pipeline                                   │   │
│  │  - Azure DevOps pipeline                                │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
        ↓
┌─────────────────────────────────────────────────────────────────┐
│   Generated dbt Project                                         │
│   ├── dbt_project.yml                                          │
│   ├── profiles.yml.template                                    │
│   ├── models/                                                  │
│   │   ├── staging/                                             │
│   │   │   └── stg_*.sql                                       │
│   │   ├── intermediate/                                        │
│   │   │   └── int_*.sql                                       │
│   │   └── marts/                                               │
│   │       └── dim_*.sql, fct_*.sql                            │
│   ├── sources.yml                                              │
│   ├── schema.yml                                               │
│   └── .github/workflows/dbt_ci.yml                            │
└─────────────────────────────────────────────────────────────────┘
```

### Deliverables (7 files)

| File | Purpose |
|------|---------|
| `src/dbt_integration/project_generator.py` | dbt project scaffolding |
| `src/dbt_integration/model_generator.py` | SQL model generation |
| `src/dbt_integration/source_generator.py` | sources.yml generation |
| `src/dbt_integration/cicd_generator.py` | CI/CD pipeline templates |
| `src/dbt_integration/types.py` | Configuration types |
| `src/dbt_integration/mcp_tools.py` | 8 MCP tools |
| `src/dbt_integration/__init__.py` | Module exports |

### MCP Tools (8)

| Tool | Description |
|------|-------------|
| `create_dbt_project` | Scaffold new dbt project from hierarchy |
| `generate_dbt_model` | Generate dbt model for a hierarchy |
| `generate_dbt_sources` | Generate sources.yml from mappings |
| `generate_dbt_schema` | Generate schema.yml with tests |
| `generate_dbt_metrics` | Generate metrics from formulas |
| `generate_cicd_pipeline` | Generate CI/CD workflow |
| `validate_dbt_project` | Validate generated project structure |
| `export_dbt_project` | Export project to directory/zip |

### Model Generation Examples

**Hierarchy → Staging Model:**
```sql
-- models/staging/stg_gl_accounts.sql
{{ config(materialized='view') }}

SELECT
    ACCOUNT_CODE,
    ACCOUNT_NAME,
    CASE
        WHEN ACCOUNT_CODE LIKE '4%' THEN 'Revenue'
        WHEN ACCOUNT_CODE LIKE '5%' THEN 'COGS'
        ELSE 'Other'
    END AS hierarchy_category
FROM {{ source('finance', 'gl_accounts') }}
```

**Hierarchy → Dimension Model:**
```sql
-- models/marts/dim_account_hierarchy.sql
{{ config(materialized='table') }}

WITH hierarchy AS (
    SELECT * FROM {{ ref('int_account_hierarchy') }}
)
SELECT
    hierarchy_id,
    hierarchy_name,
    parent_id,
    level_1, level_2, level_3,
    sort_order
FROM hierarchy
```

---

## Phase 25: Great Expectations Integration

### Overview
Data quality validation with expectation suites generated from hierarchy definitions and automated data contracts.

### Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                Great Expectations Integration                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │           ExpectationSuiteGenerator                      │   │
│  │  - Hierarchy → column expectations                      │   │
│  │  - Mappings → referential integrity checks              │   │
│  │  - Formulas → aggregation expectations                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           ↓                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │           DataContractGenerator                          │   │
│  │  - Generate YAML data contracts                         │   │
│  │  - Define SLAs and quality thresholds                   │   │
│  │  - Version control integration                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           ↓                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │           ValidationRunner                               │   │
│  │  - Run expectation suites                               │   │
│  │  - Generate validation reports                          │   │
│  │  - Alert on failures                                    │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Deliverables (7 files)

| File | Purpose |
|------|---------|
| `src/great_expectations/suite_generator.py` | Generate expectation suites |
| `src/great_expectations/contract_generator.py` | Data contract generation |
| `src/great_expectations/validation_runner.py` | Run validations |
| `src/great_expectations/types.py` | Configuration types |
| `src/great_expectations/mcp_tools.py` | 7 MCP tools |
| `src/great_expectations/__init__.py` | Module exports |
| `tests/test_great_expectations.py` | Unit tests |

### MCP Tools (7)

| Tool | Description |
|------|-------------|
| `generate_expectation_suite` | Generate suite from hierarchy |
| `add_column_expectation` | Add expectation to suite |
| `create_data_contract` | Create data contract YAML |
| `run_validation` | Run expectation suite |
| `get_validation_results` | Get last validation results |
| `list_expectation_suites` | List available suites |
| `export_data_contract` | Export contract to file |

### Expectation Generation Examples

**From Hierarchy:**
```python
# Hierarchy with source_column = ACCOUNT_CODE, source_uid = "4%"
# Generates:
expect_column_values_to_match_regex(
    column="ACCOUNT_CODE",
    regex="^4.*"
)
```

**From Mapping:**
```python
# Mapping: SOURCE_TABLE.SOURCE_COLUMN -> HIERARCHY_ID
# Generates:
expect_column_values_to_be_in_set(
    column="ACCOUNT_CODE",
    value_set=["4100", "4200", "4300"]  # From hierarchy mappings
)
```

**Data Contract YAML:**
```yaml
contract:
  name: GL Accounts Data Contract
  version: 1.0.0
  owner: finance-team

schema:
  - column: ACCOUNT_CODE
    type: VARCHAR
    not_null: true
    unique: true
    pattern: "^[4-9][0-9]{3}$"

  - column: ACCOUNT_NAME
    type: VARCHAR
    not_null: true
    max_length: 100

quality:
  freshness:
    max_age_hours: 24
  completeness:
    min_percent: 99.5
  uniqueness:
    columns: [ACCOUNT_CODE]

sla:
  validation_schedule: "0 6 * * *"  # Daily at 6 AM
  alert_on_failure: true
  alert_channels: ["slack://finance-alerts"]
```

---

## Implementation Order

| Phase | Tools | Dependencies |
|-------|-------|--------------|
| Phase 23 | 5 | Redis, FastAPI |
| Phase 24 | 8 | None (file generation) |
| Phase 25 | 7 | great_expectations package |

**Total new tools: 20**
**Running total: 224 tools**

---

## Quick Start Commands

```bash
# Phase 23: WebSocket Console
start_console_server(port=8080)
# Open http://localhost:8080 in browser

# Phase 24: dbt Project
create_dbt_project(
    hierarchy_project_id="my-pl",
    output_dir="./dbt_project",
    include_cicd=True
)

# Phase 25: Great Expectations
generate_expectation_suite(
    hierarchy_project_id="my-pl",
    suite_name="gl_accounts_suite"
)
run_validation(
    suite_name="gl_accounts_suite",
    connection_id="snowflake-prod"
)
```
