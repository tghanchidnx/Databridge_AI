# DataBridge AI - Community Edition

*Open-source, MCP-native data reconciliation engine for comparing, profiling, and managing data quality.*

[![PyPI version](https://badge.fury.io/py/databridge-ai.svg)](https://badge.fury.io/py/databridge-ai)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

---

## Overview

**DataBridge AI Community Edition** is a free, open-source data reconciliation toolkit built on the Model Context Protocol (MCP). It provides essential tools for:

- **Data Comparison** - Hash-based row comparison, orphan detection, conflict identification
- **Fuzzy Matching** - Find approximate matches between datasets using RapidFuzz
- **Data Profiling** - Statistical analysis and quality metrics for your data
- **PDF/OCR Extraction** - Extract text from PDFs and images
- **dbt Integration** - Generate dbt projects from your data
- **Data Quality** - Create and run data validation rules

## Installation

```bash
# Basic installation
pip install databridge-ai

# With PDF support
pip install databridge-ai[pdf]

# With OCR support
pip install databridge-ai[ocr]

# With all optional dependencies
pip install databridge-ai[all]
```

## Quick Start

### As an MCP Server

DataBridge AI works as an MCP server, making its tools available to AI assistants like Claude:

```bash
# Run the MCP server
databridge-mcp
```

### Using the Dashboard

```bash
# Start the web dashboard
databridge-ui
```

Then open `http://localhost:5050` in your browser.

### Python API

```python
from databridge_ai import load_csv, profile_data, fuzzy_match_columns

# Load and profile a CSV file
result = load_csv("data.csv")
profile = profile_data("data.csv")

# Find fuzzy matches between two files
matches = fuzzy_match_columns(
    source_file="source.csv",
    target_file="target.csv",
    source_column="name",
    target_column="customer_name",
    threshold=80
)
```

## Available Tools (~106 tools)

| Category | Tools | Description |
|----------|-------|-------------|
| **File Discovery** | 3 | `find_files`, `get_working_directory`, `stage_file` |
| **Data Loading** | 3 | `load_csv`, `load_json`, `query_database` |
| **Data Profiling** | 2 | `profile_data`, `profile_book_sources` |
| **Hashing & Comparison** | 3 | `compare_hashes`, `compare_table_data`, `get_data_comparison_summary` |
| **Fuzzy Matching** | 2 | `fuzzy_match_columns`, `fuzzy_deduplicate` |
| **PDF/OCR** | 3 | `extract_text_from_pdf`, `ocr_image`, `parse_table_from_text` |
| **Workflow** | 4 | `analyze_request`, `save_workflow_step`, `get_workflow`, `clear_workflow` |
| **Transform** | 2 | `transform_column`, `convert_sql_format` |
| **Documentation** | 1 | `get_application_documentation` |
| **Templates** | 10 | `list_financial_templates`, `get_template_details`, `get_skill_prompt`, etc. |
| **Diff Utilities** | 6 | `diff_text`, `diff_dicts`, `diff_lists`, `explain_diff`, `generate_patch`, `find_similar_strings` |
| **dbt Integration** | 8 | `create_dbt_project`, `generate_dbt_model`, `generate_dbt_schema`, `validate_dbt_project`, etc. |
| **Data Quality** | 7 | `generate_expectation_suite`, `run_validation`, `create_data_contract`, `add_column_expectation`, etc. |
| **License** | 1 | `get_license_status` |

## Editions

DataBridge AI is available in four editions:

| | **Community (CE)** | **Pro** | **Pro Examples** | **Enterprise** |
|---|:---:|:---:|:---:|:---:|
| **Tools** | ~106 | ~277 | Tests & Tutorials | 341+ |
| **Price** | Free | Licensed | Licensed Add-on | Custom |
| **Distribution** | Public PyPI | GitHub Packages | GitHub Packages | Private Deploy |
| Data Reconciliation | ✅ | ✅ | | ✅ |
| Fuzzy Matching | ✅ | ✅ | | ✅ |
| Data Profiling | ✅ | ✅ | | ✅ |
| PDF/OCR | ✅ | ✅ | | ✅ |
| dbt Basic | ✅ | ✅ | | ✅ |
| Data Quality | ✅ | ✅ | | ✅ |
| UI Dashboard | ✅ | ✅ | | ✅ |
| Diff Utilities | ✅ | ✅ | | ✅ |
| Templates (Basic) | ✅ | ✅ | | ✅ |
| **Hierarchy Builder** (44 tools) | | ✅ | | ✅ |
| **Wright Pipeline** (29 tools) | | ✅ | | ✅ |
| **Cortex AI Agent** (26 tools) | | ✅ | | ✅ |
| **Data Catalog** (19 tools) | | ✅ | | ✅ |
| **Faux Objects** (18 tools) | | ✅ | | ✅ |
| **Connections** (16 tools) | | ✅ | | ✅ |
| **AI Orchestrator** (16 tools) | | ✅ | | ✅ |
| **Data Observability** (15 tools) | | ✅ | | ✅ |
| **Data Versioning** (12 tools) | | ✅ | | ✅ |
| **Git/CI-CD** (12 tools) | | ✅ | | ✅ |
| **Lineage Tracking** (11 tools) | | ✅ | | ✅ |
| **PlannerAgent** (11 tools) | | ✅ | | ✅ |
| **GraphRAG Engine** (10 tools) | | ✅ | | ✅ |
| **Unified AI Agent** (10 tools) | | ✅ | | ✅ |
| **Console Dashboard** (5 tools) | | ✅ | | ✅ |
| **Schema Matcher** (5 tools) | | ✅ | | ✅ |
| **Data Matcher** (4 tools) | | ✅ | | ✅ |
| 47 Tests + 19 Tutorials | | | ✅ | |
| Custom Agents | | | | ✅ |
| White-label | | | | ✅ |
| SLA Support | | | | ✅ |
| On-premise Deploy | | | | ✅ |

## Upgrade to Pro

```bash
# Set your license key
export DATABRIDGE_LICENSE_KEY="DB-PRO-YOURKEY-20260101-signature"

# Install Pro (from GitHub Packages)
pip install databridge-ai-pro --extra-index-url https://ghp_TOKEN@raw.githubusercontent.com/tghanchidnx/Databridge_AI/main/

# Install Pro Examples (tests & tutorials, requires Pro key)
pip install databridge-ai-examples                # CE tests + beginner tutorials
pip install databridge-ai-examples[pro]           # + Pro tests + advanced tutorials
```

**License Key Format:** `DB-{TIER}-{CUSTOMER_ID}-{EXPIRY}-{SIGNATURE}`

Contact sales@databridge.ai for pricing and license keys.

## Configuration

Create a `.env` file in your project root:

```env
# Database connection (optional)
DATABRIDGE_DATABASE_URL=postgresql://user:pass@localhost/db

# OCR settings (optional)
DATABRIDGE_TESSERACT_PATH=/usr/bin/tesseract

# Fuzzy matching threshold (default: 80)
DATABRIDGE_FUZZY_THRESHOLD=80

# Max rows to display (default: 10)
DATABRIDGE_MAX_ROWS_DISPLAY=10
```

## Plugin System

Extend DataBridge AI with custom plugins:

```
plugins/
├── my_plugin/
│   ├── __init__.py
│   └── mcp_tools.py  # Must have register_tools(mcp)
```

```python
# plugins/my_plugin/mcp_tools.py
def register_tools(mcp):
    @mcp.tool()
    def my_custom_tool(param: str) -> str:
        """My custom tool description."""
        return f"Processed: {param}"
```

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License - see [LICENSE](LICENSE) for details.

## Links

- **Documentation**: [github.com/tghanchidnx/Databridge_AI/wiki](https://github.com/tghanchidnx/Databridge_AI/wiki)
- **Commercialization Guide**: [docs/COMMERCIALIZATION.md](../docs/COMMERCIALIZATION.md)
- **Issues**: [github.com/tghanchidnx/Databridge_AI/issues](https://github.com/tghanchidnx/Databridge_AI/issues)
- **Pro Features**: [Pro Features Wiki](https://github.com/tghanchidnx/Databridge_AI/wiki/Pro-Features)
