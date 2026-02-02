# DataBridge CLI

Unified command-line interface for DataBridge AI platform.

## Overview

DataBridge CLI provides a single entry point for accessing both:
- **Hierarchy Builder (Librarian)** - hierarchy management, mappings, formulas
- **Analytics Engine (Researcher)** - connectors, queries, dynamic tables

## Installation

```bash
# Install CLI only
pip install databridge-cli

# Install with Librarian Hierarchy Builder
pip install databridge-cli[librarian]

# Install with Researcher Analytics Engine
pip install databridge-cli[researcher]

# Install with both
pip install databridge-cli[all]
```

## Usage

```bash
# Show help
databridge --help

# Show version info
databridge version

# Librarian Hierarchy Builder commands
databridge hierarchy --help
databridge hierarchy project list
databridge hierarchy create "My Project"

# Researcher Analytics Engine commands
databridge analytics --help
databridge analytics connect list
databridge analytics query "Show me top 10 customers"
```

## Command Structure

```
databridge
├── version                   # Show version info
├── info                      # Show platform info
├── hierarchy                 # Librarian Hierarchy Builder
│   ├── project              # Project management
│   ├── hierarchy            # Hierarchy operations
│   ├── mapping              # Source mappings
│   ├── formula              # Formula rules
│   └── deploy               # Deployment
└── analytics                # Researcher Analytics Engine
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
