# DataBridge AI

[![PyPI version](https://badge.fury.io/py/databridge-ai.svg)](https://badge.fury.io/py/databridge-ai)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**DataBridge AI** is a headless, MCP-native data reconciliation engine with **292 tools** for hierarchy management, data quality, and analytics.

## Features

- **Data Reconciliation** - Compare and validate data from CSV, SQL, PDF, and JSON sources
- **Hierarchy Builder** - Create and manage multi-level hierarchy projects (up to 15 levels)
- **Wright Module** - Hierarchy-driven data mart generation with 4-object pipeline
- **Cortex AI Integration** - Snowflake Cortex AI with natural language to SQL
- **Data Catalog** - Centralized metadata registry with business glossary
- **Data Quality** - Expectation suites and data contracts
- **Lineage Tracking** - Column-level lineage and impact analysis
- **Git/CI-CD** - Automated workflows and GitHub integration
- **dbt Integration** - Generate dbt projects from hierarchies

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

### Programmatic Usage

```python
from src.server import mcp

# Run as MCP server
mcp.run()
```

### Available Tools (292)

| Category | Count | Examples |
|----------|-------|----------|
| Data Reconciliation | 20+ | `load_csv`, `compare_hashes`, `fuzzy_match_columns` |
| Hierarchy Builder | 44 | `create_hierarchy_project`, `import_hierarchy_csv` |
| Wright (Mart Factory) | 18 | `create_mart_config`, `generate_mart_pipeline` |
| Cortex AI | 22 | `cortex_complete`, `analyst_ask`, `cortex_reason` |
| Data Catalog | 15 | `catalog_create_asset`, `catalog_search` |
| Versioning | 12 | `version_create`, `version_rollback` |
| Lineage | 11 | `track_column_lineage`, `analyze_change_impact` |
| Git/CI-CD | 12 | `git_commit`, `github_create_pr` |
| dbt Integration | 8 | `create_dbt_project`, `generate_dbt_model` |
| Data Quality | 7 | `generate_expectation_suite`, `run_validation` |

## Tool Categories

### Data Reconciliation
- Load and profile data from CSV, JSON, and SQL sources
- Compare datasets with hash-based matching
- Fuzzy matching for deduplication
- PDF text extraction and OCR

### Hierarchy Builder
- Create multi-level hierarchy projects
- Define source mappings to database columns
- Build calculation formulas (SUM, SUBTRACT, MULTIPLY, DIVIDE)
- Export to CSV/JSON and generate deployment scripts
- Deploy hierarchies to Snowflake

### Wright Module (Data Mart Factory)
- 4-object pipeline: VW_1 → DT_2 → DT_3A → DT_3
- 7 configuration variables for parameterization
- AI-powered hierarchy discovery via Cortex
- 5-level formula precedence engine

### Cortex AI Integration
- Snowflake Cortex functions (COMPLETE, SUMMARIZE, SENTIMENT, TRANSLATE)
- Natural language to SQL via semantic models
- Orchestrated reasoning loop (Observe → Plan → Execute → Reflect)

## Configuration

Create a `.env` file or set environment variables:

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
```

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Build package
python -m build

# Upload to PyPI
twine upload dist/*
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## Links

- [GitHub Repository](https://github.com/tghanchidnx/Databridge_AI)
- [Issue Tracker](https://github.com/tghanchidnx/Databridge_AI/issues)
