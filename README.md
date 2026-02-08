# DataBridge AI

[![PyPI version](https://badge.fury.io/py/databridge-ai.svg)](https://pypi.org/project/databridge-ai/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**DataBridge AI** is a headless, MCP-native data reconciliation engine with **292 tools** for hierarchy management, data quality, and analytics.

## Features

| Module | Tools | Description |
|--------|-------|-------------|
| **Data Reconciliation** | 38 | Compare and validate data from CSV, SQL, PDF, JSON sources |
| **Hierarchy Builder** | 44 | Create and manage multi-level hierarchy projects (up to 15 levels) |
| **Cortex AI** | 25 | Snowflake Cortex AI with natural language to SQL |
| **Wright Module** | 18 | Hierarchy-driven data mart generation with 4-object pipeline |
| **Data Catalog** | 15 | Centralized metadata registry with business glossary |
| **Data Versioning** | 12 | Semantic versioning, snapshots, rollback, and diff |
| **Lineage Tracking** | 11 | Column-level lineage and impact analysis |
| **Git/CI-CD** | 12 | Automated workflows and GitHub integration |
| **dbt Integration** | 8 | Generate dbt projects from hierarchies |
| **Data Quality** | 7 | Expectation suites and data contracts |
| **Templates & Skills** | 16 | Pre-built templates and AI expertise definitions |
| **AI Orchestrator** | 27 | Multi-agent coordination with task queues |
| **And more...** | 54 | Diff utilities, recommendations, console, etc. |

## Installation

```bash
# Basic installation
pip install databridge-ai

# With PDF support
pip install databridge-ai[pdf]

# With Snowflake support
pip install databridge-ai[snowflake]

# Full installation
pip install databridge-ai[all]
```

## Quick Start

### As MCP Server (Claude Desktop)

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "DataBridge_AI": {
      "command": "python",
      "args": ["-m", "src.server"]
    }
  }
}
```

### Web UI Dashboard

```bash
cd databridge-ce/ui
python server.py
# Open http://127.0.0.1:5050
```

### Programmatic Usage

```python
from src.server import mcp

# Run as MCP server
mcp.run()
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Claude / LLM Client                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      MCP Protocol                            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   DataBridge MCP Server                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │  Hierarchy  │  │   Cortex    │  │   Wright    │         │
│  │   Builder   │  │   Agent     │  │   Module    │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │    Data     │  │   Lineage   │  │    Data     │         │
│  │   Catalog   │  │   Tracker   │  │   Quality   │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              Snowflake / CSV / SQL / PDF                     │
└─────────────────────────────────────────────────────────────┘
```

## Core Modules

### Hierarchy Builder
Create and manage multi-level hierarchy projects for financial reporting, organizational structures, and data classification.

```python
# Create a hierarchy project
create_hierarchy_project(name="Revenue P&L", description="Revenue hierarchy")

# Add hierarchies with source mappings
create_hierarchy(project_id="...", name="Product Revenue", parent_id="...")
add_source_mapping(hierarchy_id="...", source_column="ACCOUNT_CODE", source_uid="41%")

# Export and deploy
export_hierarchy_csv(project_id="...")
generate_hierarchy_scripts(project_id="...")
```

### Cortex AI Integration
Natural language to SQL and AI-powered data operations using Snowflake Cortex.

```python
# Configure Cortex
configure_cortex_agent(connection_id="snowflake-prod", cortex_model="mistral-large")

# Natural language query
analyst_ask(question="What was total revenue by region last quarter?",
            semantic_model_file="@ANALYTICS.PUBLIC.MODELS/sales.yaml")

# AI reasoning loop
cortex_reason(goal="Analyze data quality in PRODUCTS table")
```

### Wright Module (Data Mart Factory)
Generate data marts using the 4-object pipeline pattern: VW_1 → DT_2 → DT_3A → DT_3

```python
# Create mart configuration
create_mart_config(project_name="upstream_gross", report_type="GROSS",
                   hierarchy_table="TBL_0_GROSS_LOS_REPORT_HIERARCHY")

# Generate pipeline
generate_mart_pipeline(config_name="upstream_gross")
```

### Data Versioning
Track changes to hierarchies, catalog assets, and semantic models with semantic versioning.

```python
# Create version snapshot
version_create(object_type="hierarchy", object_id="revenue-pl",
               description="Added new cost centers", bump="minor")

# Compare versions
version_diff(object_type="hierarchy", object_id="revenue-pl",
             from_version="1.0.0", to_version="1.1.0")

# Rollback
version_rollback(object_type="hierarchy", object_id="revenue-pl", to_version="1.0.0")
```

## Configuration

Create a `.env` file:

```env
# Data directory
DATA_DIR=./data

# NestJS backend (optional)
NESTJS_BACKEND_URL=http://localhost:8001
NESTJS_API_KEY=your-api-key

# Snowflake (optional)
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-user
SNOWFLAKE_PASSWORD=your-password

# Cortex AI
CORTEX_DEFAULT_MODEL=mistral-large
```

## Documentation

- **[CLAUDE.md](CLAUDE.md)** - Complete tool reference and usage guide
- **[docs/MANIFEST.md](docs/MANIFEST.md)** - Auto-generated tool manifest
- **[Wiki](../../wiki)** - Architecture, getting started, and tutorials

## Community Edition

The `databridge-ce/` folder contains the open-source Community Edition with:
- Plugin architecture for custom tools
- Web UI dashboard
- Starter templates

## Contributing

Contributions are welcome! Please read our contributing guidelines before submitting PRs.

## License

MIT License - See [LICENSE](LICENSE) for details.

Copyright (c) 2024-2026 DataBridge AI Team
