# DataBridge CLI

Unified command-line interface for DataBridge AI platform.

## Overview

DataBridge CLI provides a single entry point for accessing both:
- **Hierarchy Builder (V3)** - hierarchy management, mappings, formulas
- **Analytics Engine (V4)** - connectors, queries, dynamic tables

## Installation

```bash
# Install CLI only
pip install databridge-cli

# Install with V3 Hierarchy Builder
pip install databridge-cli[v3]

# Install with V4 Analytics Engine
pip install databridge-cli[v4]

# Install with both
pip install databridge-cli[all]
```

## Usage

```bash
# Show help
databridge --help

# Show version info
databridge version

# V3 Hierarchy Builder commands
databridge hierarchy --help
databridge hierarchy project list
databridge hierarchy create "My Project"

# V4 Analytics Engine commands
databridge analytics --help
databridge analytics connect list
databridge analytics query "Show me top 10 customers"
```

## Command Structure

```
databridge
├── version                   # Show version info
├── info                      # Show platform info
├── hierarchy                 # V3 Hierarchy Builder
│   ├── project              # Project management
│   ├── hierarchy            # Hierarchy operations
│   ├── mapping              # Source mappings
│   ├── formula              # Formula rules
│   └── deploy               # Deployment
└── analytics                # V4 Analytics Engine
    ├── connect              # Connector management
    ├── query                # Query execution
    ├── dynamic              # Dynamic tables
    └── workflow             # FP&A workflows
```

## Development

```bash
# Install for development
pip install -e ".[all,dev]"

# Run CLI
python -m databridge_cli.main
```
