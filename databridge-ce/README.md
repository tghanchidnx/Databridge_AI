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
- **dbt Integration** - Generate dbt models from your data
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

## Available Tools (Community Edition)

| Category | Tools | Description |
|----------|-------|-------------|
| **File Discovery** | `find_files`, `get_working_directory` | Search for files across common directories |
| **Data Loading** | `load_csv`, `load_json`, `query_database` | Load data from various sources |
| **Data Profiling** | `profile_data` | Generate comprehensive data statistics |
| **Comparison** | `compare_hashes` | Hash-based row comparison with orphan/conflict detection |
| **Fuzzy Matching** | `fuzzy_match_columns` | Find approximate matches using RapidFuzz |
| **PDF/OCR** | `extract_text_from_pdf` | Extract text from PDF files |
| **Diff Utilities** | `diff_text` | Compare text strings |
| **License** | `get_license_status` | Check license tier and available features |

## Upgrade to Pro

DataBridge AI Pro unlocks advanced features:

| Feature | Community | Pro |
|---------|:---------:|:---:|
| Data Reconciliation | ✅ | ✅ |
| Fuzzy Matching | ✅ | ✅ |
| Data Profiling | ✅ | ✅ |
| PDF/OCR | ✅ | ✅ |
| dbt Basic | ✅ | ✅ |
| Cortex AI Agent | ❌ | ✅ |
| Wright Pipeline | ❌ | ✅ |
| GraphRAG Engine | ❌ | ✅ |
| Data Observability | ❌ | ✅ |
| Full Data Catalog | ❌ | ✅ |
| Column Lineage | ❌ | ✅ |
| AI Orchestrator | ❌ | ✅ |

```bash
# Install Pro (requires license)
pip install databridge-ai-pro --extra-index-url https://pypi.yourcompany.com/simple/

# Set your license key
export DATABRIDGE_LICENSE_KEY="DB-PRO-YOURKEY-20260101-signature"
```

Visit [databridge.ai/pro](https://databridge.ai/pro) for pricing and features.

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

- **Documentation**: [github.com/tghanchidnx/databridge-ai/wiki](https://github.com/tghanchidnx/databridge-ai/wiki)
- **Issues**: [github.com/tghanchidnx/databridge-ai/issues](https://github.com/tghanchidnx/databridge-ai/issues)
- **Pro Features**: [databridge.ai/pro](https://databridge.ai/pro)
