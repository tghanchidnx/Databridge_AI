# Headless DataBridge AI - Python CLI Application

## Comprehensive Plan of Action

**Version:** 3.0.0
**Target:** Pure Python Command-Line Interface
**MCP Server Name:** Headless Databridge_AI - Python

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [Module Breakdown](#3-module-breakdown)
4. [Implementation Phases](#4-implementation-phases)
5. [Database Design](#5-database-design)
6. [CLI Command Structure](#6-cli-command-structure)
7. [MCP Server Integration](#7-mcp-server-integration)
8. [Python Libraries](#8-python-libraries)
9. [User Guide](#9-user-guide)
10. [Testing Strategy](#10-testing-strategy)
11. [Security Considerations](#11-security-considerations)
12. [Deployment Guide](#12-deployment-guide)

---

## 1. Executive Summary

### 1.1 Purpose
Create a **pure Python command-line application** that replicates all functionality of DataBridge AI v2, eliminating the need for:
- React/TypeScript frontend
- NestJS/Node.js backend
- Docker containers
- Redis cache
- MySQL database (optional - SQLite default)

### 1.2 Key Benefits
- **Zero UI dependency** - Pure terminal-based interaction
- **Single executable** - One Python package to rule them all
- **MCP Native** - Direct Claude AI integration via "Headless Databridge_AI - Python"
- **Portable** - Run anywhere Python 3.10+ is available
- **Offline capable** - Local SQLite storage, no backend required

### 1.3 Feature Parity Matrix

| Feature | V2 (Web) | V3 (CLI) | Notes |
|---------|----------|----------|-------|
| Hierarchy Management | Web UI | CLI + MCP | Full CRUD support |
| Data Reconciliation | API | CLI + MCP | Hash comparison, fuzzy matching |
| Templates (20) | Gallery | CLI + MCP | All templates included |
| Skills (7) | Prompts | CLI + MCP | All skills included |
| CSV Import/Export | Dialog | CLI + MCP | 2-file format preserved |
| Database Connections | UI | CLI + MCP | Snowflake, MySQL, PostgreSQL, SQL Server |
| Deployment | Dialog | CLI + MCP | Snowflake script generation |
| PDF/OCR | API | CLI + MCP | Text extraction |
| Knowledge Base | UI | CLI + MCP | Client profiles |
| Audit Trail | Log | CLI + MCP | CSV logging |

---

## 2. Architecture Overview

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    User Interface Layer                         │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │   CLI App    │  │ Interactive  │  │   MCP Server         │  │
│  │ (Click/Typer)│  │    REPL      │  │ "Headless DataBridge"│  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│                    Service Layer                                │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  Hierarchy   │  │ Reconcile    │  │   Template/Skill     │  │
│  │   Service    │  │   Service    │  │     Service          │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  Connection  │  │ Schema       │  │   Knowledge Base     │  │
│  │   Service    │  │  Matcher     │  │     Service          │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│                    Data Access Layer                            │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │   SQLite     │  │  JSON Files  │  │   External DBs       │  │
│  │  (Default)   │  │  (Legacy)    │  │ (Snowflake/MySQL/PG) │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Directory Structure

```
v3/
├── src/
│   ├── __init__.py
│   ├── main.py                      # Entry point
│   ├── cli/                         # CLI commands
│   │   ├── __init__.py
│   │   ├── app.py                   # Typer/Click app
│   │   ├── hierarchy_commands.py    # Hierarchy CLI
│   │   ├── reconcile_commands.py    # Reconciliation CLI
│   │   ├── template_commands.py     # Template CLI
│   │   ├── connection_commands.py   # Connection CLI
│   │   ├── deploy_commands.py       # Deployment CLI
│   │   └── interactive.py           # REPL mode
│   │
│   ├── core/                        # Core services
│   │   ├── __init__.py
│   │   ├── config.py                # Pydantic settings
│   │   ├── database.py              # SQLAlchemy models
│   │   └── audit.py                 # Audit trail logging
│   │
│   ├── hierarchy/                   # Hierarchy module
│   │   ├── __init__.py
│   │   ├── service.py               # Hierarchy service
│   │   ├── models.py                # Pydantic models
│   │   ├── csv_handler.py           # CSV import/export
│   │   ├── tree.py                  # Tree operations
│   │   └── formulas.py              # Formula engine
│   │
│   ├── reconciliation/              # Data reconciliation
│   │   ├── __init__.py
│   │   ├── service.py               # Reconciliation service
│   │   ├── loader.py                # CSV/JSON/DB loaders
│   │   ├── profiler.py              # Data profiling
│   │   ├── hasher.py                # Hash comparison
│   │   ├── fuzzy.py                 # RapidFuzz matching
│   │   ├── transforms.py            # Data transforms
│   │   └── ocr.py                   # PDF/OCR extraction
│   │
│   ├── templates/                   # Templates & skills
│   │   ├── __init__.py
│   │   ├── service.py               # Template service
│   │   ├── skills.py                # Skill management
│   │   └── knowledge_base.py        # Client KB
│   │
│   ├── connections/                 # Database connections
│   │   ├── __init__.py
│   │   ├── service.py               # Connection service
│   │   ├── snowflake.py             # Snowflake adapter
│   │   ├── mysql.py                 # MySQL adapter
│   │   ├── postgresql.py            # PostgreSQL adapter
│   │   └── sqlserver.py             # SQL Server adapter
│   │
│   ├── deployment/                  # Deployment module
│   │   ├── __init__.py
│   │   ├── service.py               # Deployment service
│   │   └── script_generator.py      # DDL generation
│   │
│   ├── mcp/                         # MCP Server
│   │   ├── __init__.py
│   │   ├── server.py                # FastMCP server
│   │   ├── hierarchy_tools.py       # 41 hierarchy tools
│   │   ├── reconcile_tools.py       # 20 reconciliation tools
│   │   ├── template_tools.py        # 16 template tools
│   │   ├── connection_tools.py      # 8 connection tools
│   │   ├── schema_tools.py          # 4 schema tools
│   │   └── data_tools.py            # 3 data tools
│   │
│   └── utils/                       # Utilities
│       ├── __init__.py
│       ├── formatters.py            # Output formatters
│       ├── validators.py            # Input validators
│       └── helpers.py               # Common helpers
│
├── data/                            # Data storage
│   ├── databridge.db                # SQLite database
│   ├── audit_trail.csv              # Audit log
│   └── workflow.json                # Workflow state
│
├── templates/                       # Template definitions
│   ├── index.json                   # Template registry
│   ├── accounting/                  # P&L, Balance Sheet
│   ├── finance/                     # Cost/Profit centers
│   └── operations/                  # Geographic, Asset hierarchies
│
├── skills/                          # Skill definitions
│   ├── index.json                   # Skill registry
│   ├── financial-analyst.md
│   ├── financial-analyst-prompt.txt
│   └── ... (7 skills total)
│
├── knowledge_base/                  # Client knowledge
│   ├── index.json                   # Client registry
│   └── clients/                     # Per-client configs
│
├── tests/                           # Test suite
│   ├── conftest.py
│   ├── test_hierarchy.py
│   ├── test_reconciliation.py
│   ├── test_templates.py
│   └── test_mcp.py
│
├── docs/                            # Documentation
│   ├── USER_GUIDE.md
│   ├── MCP_GUIDE.md
│   └── MANIFEST.md
│
├── pyproject.toml                   # Poetry/pip config
├── requirements.txt                 # Dependencies
├── setup.py                         # Setup script
├── README.md                        # Project README
├── PLAN.md                          # This file
└── .env.example                     # Environment template
```

---

## 3. Module Breakdown

### 3.1 Core Module (src/core/)

| Component | Purpose | Key Classes/Functions |
|-----------|---------|----------------------|
| `config.py` | Configuration management | `Settings(BaseSettings)` |
| `database.py` | SQLAlchemy ORM models | `Project`, `Hierarchy`, `Mapping`, `Connection` |
| `audit.py` | Audit trail logging | `AuditLogger`, `log_action()` |

### 3.2 Hierarchy Module (src/hierarchy/) - 41 Tools

| Component | Purpose | Tools Count |
|-----------|---------|-------------|
| `service.py` | CRUD operations | 10 |
| `csv_handler.py` | CSV import/export | 4 |
| `tree.py` | Tree navigation | 8 |
| `formulas.py` | Formula calculations | 3 |
| `mappings.py` | Source mappings | 6 |
| `sync.py` | Backend sync | 4 |
| `dashboard.py` | Statistics | 3 |
| `metadata.py` | Metadata queries | 3 |

### 3.3 Reconciliation Module (src/reconciliation/) - 20 Tools

| Component | Purpose | Tools Count |
|-----------|---------|-------------|
| `loader.py` | Data loading | 3 |
| `profiler.py` | Data profiling | 2 |
| `hasher.py` | Hash comparison | 3 |
| `fuzzy.py` | Fuzzy matching | 2 |
| `ocr.py` | PDF/OCR | 3 |
| `workflow.py` | Workflow management | 4 |
| `transforms.py` | Data transformation | 2 |
| `manifest.py` | Documentation | 1 |

### 3.4 Templates Module (src/templates/) - 16 Tools

| Component | Purpose | Tools Count |
|-----------|---------|-------------|
| `service.py` | Template CRUD | 5 |
| `skills.py` | Skill management | 3 |
| `knowledge_base.py` | Client KB | 5 |
| `docs.py` | Documentation | 3 |

### 3.5 Connections Module (src/connections/) - 8 Tools

| Component | Purpose | Tools Count |
|-----------|---------|-------------|
| `service.py` | Connection management | 3 |
| `browser.py` | Schema browsing | 5 |

### 3.6 Schema/Data Matchers - 7 Tools

| Component | Purpose | Tools Count |
|-----------|---------|-------------|
| `schema_matcher.py` | Schema comparison | 4 |
| `data_matcher.py` | Data comparison | 3 |

---

## 4. Implementation Phases

### Phase 1: Foundation (Week 1-2)
**Goal:** Core infrastructure and basic CLI

#### Tasks:
1. **Project Setup**
   - Initialize Python package structure
   - Configure pyproject.toml with dependencies
   - Set up pytest and pre-commit hooks

2. **Core Configuration**
   - Implement Pydantic Settings class
   - Create .env loader with validation
   - Set up logging configuration

3. **Database Layer**
   - Design SQLAlchemy models
   - Create migration scripts
   - Implement SQLite adapter

4. **Basic CLI Framework**
   - Set up Typer/Click application
   - Create command groups
   - Implement help system

#### Deliverables:
- [ ] Working CLI skeleton with `--help`
- [ ] SQLite database with schema
- [ ] Configuration loading from .env
- [ ] Basic test suite

---

### Phase 2: Hierarchy Module (Week 3-4)
**Goal:** Full hierarchy management

#### Tasks:
1. **Project Management**
   - `create_hierarchy_project` - Create new project
   - `list_hierarchy_projects` - List all projects
   - `get_hierarchy_project` - Get project details
   - `delete_hierarchy_project` - Delete project

2. **Hierarchy CRUD**
   - `create_hierarchy` - Create hierarchy node
   - `get_hierarchy` - Get hierarchy by ID
   - `update_hierarchy` - Update hierarchy
   - `delete_hierarchy` - Delete hierarchy
   - `list_hierarchies` - List all in project
   - `get_hierarchy_tree` - Full tree structure

3. **CSV Import/Export**
   - `import_hierarchy_csv` - Import hierarchies
   - `import_mapping_csv` - Import mappings
   - `export_hierarchy_csv` - Export hierarchies
   - `export_mapping_csv` - Export mappings

4. **Source Mappings**
   - `add_source_mapping` - Add mapping
   - `remove_source_mapping` - Remove mapping
   - `get_inherited_mappings` - Cascading mappings
   - `get_mapping_summary` - Mapping overview
   - `get_mappings_by_precedence` - Ordered mappings
   - `validate_mapping` - Check correctness

5. **Tree Operations**
   - `get_hierarchy_children` - Child nodes
   - `get_hierarchy_path` - Path from root
   - `get_hierarchy_ancestors` - All parents
   - `get_hierarchy_descendants` - All children
   - `get_hierarchy_siblings` - Same level
   - `get_hierarchy_level_info` - Level data
   - `validate_hierarchy` - Data quality

6. **Formulas**
   - `create_formula_group` - Formula group
   - `add_formula_rule` - Add rule
   - `list_formula_groups` - All groups

#### Deliverables:
- [ ] All 41 hierarchy tools implemented
- [ ] CLI commands for hierarchy management
- [ ] CSV import/export working
- [ ] Tree visualization in terminal

---

### Phase 3: Data Reconciliation (Week 5-6)
**Goal:** Full reconciliation engine

#### Tasks:
1. **Data Loading**
   - `load_csv` - Load CSV files
   - `load_json` - Load JSON data
   - `query_database` - SQL queries

2. **Data Profiling**
   - `profile_data` - Structure analysis
   - `detect_schema_drift` - Column differences

3. **Hash Comparison**
   - `compare_hashes` - Orphans and conflicts
   - `get_orphan_details` - Orphan records
   - `get_conflict_details` - Conflict details

4. **Fuzzy Matching**
   - `fuzzy_match_columns` - Column matching
   - `fuzzy_deduplicate` - Duplicate detection

5. **PDF/OCR**
   - `extract_text_from_pdf` - PDF extraction
   - `ocr_image` - Image OCR
   - `parse_table_from_text` - Table parsing

6. **Workflow**
   - `save_workflow_step` - Store step
   - `get_workflow` - Retrieve workflow
   - `clear_workflow` - Reset workflow
   - `get_audit_log` - View history

7. **Transforms**
   - `transform_column` - Column transforms
   - `merge_sources` - Join sources

#### Deliverables:
- [ ] All 20 reconciliation tools
- [ ] CLI commands for reconciliation
- [ ] PDF/OCR working (with optional deps)
- [ ] Workflow persistence

---

### Phase 4: Templates & Skills (Week 7)
**Goal:** Template and skill system

#### Tasks:
1. **Template Management**
   - Copy 20 templates from v2
   - Implement template registry
   - Create instantiation logic

2. **Skills System**
   - Copy 7 skills from v2
   - Implement skill loader
   - Create prompt generator

3. **Knowledge Base**
   - Client profile management
   - Custom prompt storage
   - Industry mappings

4. **Documentation**
   - Auto-generate MANIFEST.md
   - User guide sections
   - Application docs

#### Deliverables:
- [ ] All 16 template/skill tools
- [ ] CLI commands for templates
- [ ] Knowledge base working
- [ ] Documentation system

---

### Phase 5: Database Connections (Week 8)
**Goal:** Multi-database support

#### Tasks:
1. **Connection Management**
   - Store encrypted credentials
   - Test connectivity
   - Connection pooling

2. **Schema Browsing**
   - List databases
   - List schemas
   - List tables
   - List columns
   - Get distinct values

3. **Adapters**
   - Snowflake (OAuth, password, keypair)
   - MySQL
   - PostgreSQL
   - SQL Server

#### Deliverables:
- [ ] All 8 connection tools
- [ ] CLI for connection management
- [ ] Schema browser working
- [ ] Secure credential storage

---

### Phase 6: Schema & Data Matching (Week 9)
**Goal:** Comparison tools

#### Tasks:
1. **Schema Matcher**
   - Compare schemas between DBs
   - Generate merge SQL
   - Track comparison history

2. **Data Matcher**
   - Row-level comparison
   - Statistical summaries
   - Orphan/conflict detection

#### Deliverables:
- [ ] All 7 matcher tools
- [ ] CLI for schema/data comparison
- [ ] SQL script generation

---

### Phase 7: Deployment (Week 10)
**Goal:** Snowflake deployment

#### Tasks:
1. **Script Generation**
   - INSERT scripts
   - VIEW scripts
   - MAPPING scripts

2. **Deployment Execution**
   - Push to Snowflake
   - Track history
   - Error handling

#### Deliverables:
- [ ] Deployment tools
- [ ] CLI for deployment
- [ ] History tracking

---

### Phase 8: MCP Server (Week 11)
**Goal:** Claude integration

#### Tasks:
1. **Server Setup**
   - FastMCP server configuration
   - Tool registration (92 tools)
   - stdio transport

2. **Tool Implementation**
   - Wrap all services as MCP tools
   - JSON serialization
   - Error handling

3. **Claude Desktop Integration**
   - Configuration file
   - Testing with Claude

#### Deliverables:
- [ ] Working MCP server
- [ ] All 92 tools registered
- [ ] Claude Desktop config
- [ ] Integration tests

---

### Phase 9: Interactive REPL (Week 12)
**Goal:** Interactive mode

#### Tasks:
1. **REPL Shell**
   - Command history
   - Tab completion
   - Context preservation

2. **Visualization**
   - Tree rendering in terminal
   - Table formatting
   - Progress bars

3. **Batch Mode**
   - Script execution
   - Pipeline processing

#### Deliverables:
- [ ] Interactive REPL
- [ ] Beautiful terminal output
- [ ] Batch processing

---

### Phase 10: Testing & Documentation (Week 13-14)
**Goal:** Production readiness

#### Tasks:
1. **Comprehensive Testing**
   - Unit tests (>80% coverage)
   - Integration tests
   - MCP tool tests

2. **Documentation**
   - User guide
   - API reference
   - MCP guide

3. **Packaging**
   - PyPI publishing
   - Executable generation
   - Docker image (optional)

#### Deliverables:
- [ ] Full test suite
- [ ] Complete documentation
- [ ] Published package

---

## 5. Database Design

### 5.1 SQLAlchemy Models

```python
# core/database.py

from sqlalchemy import Column, String, Integer, JSON, Boolean, DateTime, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Project(Base):
    __tablename__ = 'projects'

    id = Column(String(36), primary_key=True)  # UUID
    name = Column(String(255), nullable=False)
    description = Column(Text)
    created_by = Column(String(255))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

    hierarchies = relationship("Hierarchy", back_populates="project")


class Hierarchy(Base):
    __tablename__ = 'hierarchies'

    id = Column(Integer, primary_key=True, autoincrement=True)
    hierarchy_id = Column(String(255), unique=True, nullable=False)
    project_id = Column(String(36), ForeignKey('projects.id'))
    hierarchy_name = Column(String(255), nullable=False)
    description = Column(Text)
    parent_id = Column(String(255))

    # Level columns (up to 15)
    level_1 = Column(String(255))
    level_2 = Column(String(255))
    # ... through level_15

    # Sort orders
    level_1_sort = Column(Integer, default=1)
    level_2_sort = Column(Integer, default=1)
    # ... through level_15_sort

    # Flags
    include_flag = Column(Boolean, default=True)
    exclude_flag = Column(Boolean, default=False)
    transform_flag = Column(Boolean, default=False)
    calculation_flag = Column(Boolean, default=False)
    active_flag = Column(Boolean, default=True)
    is_leaf_node = Column(Boolean, default=False)

    # JSON fields
    source_mappings = Column(JSON, default=[])
    formula_config = Column(JSON)
    filter_config = Column(JSON)
    pivot_config = Column(JSON)

    sort_order = Column(Integer, default=1)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

    project = relationship("Project", back_populates="hierarchies")


class Connection(Base):
    __tablename__ = 'connections'

    id = Column(String(36), primary_key=True)
    name = Column(String(255), nullable=False)
    type = Column(String(50))  # snowflake, mysql, postgresql, sqlserver
    host = Column(String(255))
    port = Column(Integer)
    database = Column(String(255))
    username = Column(String(255))
    password_encrypted = Column(Text)  # Encrypted storage
    extra_config = Column(JSON)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


class DeploymentHistory(Base):
    __tablename__ = 'deployment_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(String(36), ForeignKey('projects.id'))
    connection_id = Column(String(36), ForeignKey('connections.id'))
    script_type = Column(String(50))  # INSERT, VIEW, MAPPING
    script_content = Column(Text)
    status = Column(String(20))  # pending, success, failed
    error_message = Column(Text)
    executed_at = Column(DateTime)
    executed_by = Column(String(255))


class FormulaGroup(Base):
    __tablename__ = 'formula_groups'

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(String(36), ForeignKey('projects.id'))
    name = Column(String(255), nullable=False)
    description = Column(Text)
    rules = Column(JSON, default=[])
    created_at = Column(DateTime)


class ClientProfile(Base):
    __tablename__ = 'client_profiles'

    id = Column(String(36), primary_key=True)
    name = Column(String(255), nullable=False)
    industry = Column(String(100))
    custom_prompts = Column(JSON, default=[])
    mappings = Column(JSON, default={})
    preferences = Column(JSON, default={})
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


class AuditLog(Base):
    __tablename__ = 'audit_log'

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False)
    action = Column(String(100), nullable=False)
    entity_type = Column(String(50))
    entity_id = Column(String(255))
    user = Column(String(255))
    details = Column(JSON)
```

### 5.2 Migration Strategy

```python
# Use Alembic for migrations
# alembic init migrations
# alembic revision --autogenerate -m "Initial schema"
# alembic upgrade head
```

---

## 6. CLI Command Structure

### 6.1 Command Groups

```bash
# Main entry point
databridge --help

# Project commands
databridge project create <name> [--description]
databridge project list
databridge project show <project-id>
databridge project delete <project-id>

# Hierarchy commands
databridge hierarchy create <project-id> <name> [--parent-id] [--level-1] ...
databridge hierarchy list <project-id> [--tree]
databridge hierarchy show <hierarchy-id>
databridge hierarchy update <hierarchy-id> [--name] [--description] ...
databridge hierarchy delete <hierarchy-id>
databridge hierarchy tree <project-id> [--format=ascii|json]

# Mapping commands
databridge mapping add <hierarchy-id> --database --schema --table --column
databridge mapping remove <hierarchy-id> --index
databridge mapping list <hierarchy-id>
databridge mapping summary <project-id>

# CSV import/export
databridge csv import hierarchy <project-id> <file.csv> [--legacy]
databridge csv import mapping <project-id> <file.csv>
databridge csv export hierarchy <project-id> [--output]
databridge csv export mapping <project-id> [--output]

# Connection commands
databridge connection create <name> --type=snowflake|mysql|postgresql|sqlserver
databridge connection list
databridge connection test <connection-id>
databridge connection browse <connection-id> [--database] [--schema] [--table]

# Reconciliation commands
databridge reconcile load csv <file.csv> [--preview]
databridge reconcile load json <file.json>
databridge reconcile profile <source-name>
databridge reconcile compare <source-a> <source-b> --keys <col1,col2>
databridge reconcile fuzzy <source-name> <column> [--threshold=80]
databridge reconcile transform <source-name> <column> --operation=upper|lower|strip

# Template commands
databridge template list [--domain] [--industry]
databridge template show <template-id>
databridge template create-project <template-id> <project-name>

# Skill commands
databridge skill list
databridge skill show <skill-id>
databridge skill prompt <skill-id>

# Knowledge base commands
databridge kb client list
databridge kb client create <name> [--industry]
databridge kb client show <client-id>
databridge kb prompt add <client-id> <prompt-text>

# Deployment commands
databridge deploy generate <project-id> <connection-id> [--type=insert|view|mapping]
databridge deploy execute <project-id> <connection-id>
databridge deploy history <project-id>

# Interactive mode
databridge shell

# MCP server mode
databridge mcp serve
```

### 6.2 Typer Implementation

```python
# cli/app.py

import typer
from typing import Optional
from rich.console import Console
from rich.table import Table

app = typer.Typer(
    name="databridge",
    help="Headless DataBridge AI - Pure Python CLI",
    add_completion=True
)

console = Console()

# Import command groups
from .hierarchy_commands import hierarchy_app
from .reconcile_commands import reconcile_app
from .template_commands import template_app
from .connection_commands import connection_app
from .deploy_commands import deploy_app

app.add_typer(hierarchy_app, name="hierarchy")
app.add_typer(reconcile_app, name="reconcile")
app.add_typer(template_app, name="template")
app.add_typer(connection_app, name="connection")
app.add_typer(deploy_app, name="deploy")


@app.command()
def shell():
    """Start interactive REPL mode"""
    from .interactive import InteractiveShell
    shell = InteractiveShell()
    shell.run()


@app.command()
def mcp():
    """Start MCP server for Claude integration"""
    from ..mcp.server import run_server
    run_server()


if __name__ == "__main__":
    app()
```

---

## 7. MCP Server Integration

### 7.1 Server Configuration

```python
# mcp/server.py

from fastmcp import FastMCP
import json

mcp = FastMCP(
    name="Headless Databridge_AI - Python",
    version="3.0.0",
    description="Pure Python MCP server for DataBridge AI with 92 tools"
)

# Register all tool modules
from .hierarchy_tools import register_hierarchy_tools
from .reconcile_tools import register_reconcile_tools
from .template_tools import register_template_tools
from .connection_tools import register_connection_tools
from .schema_tools import register_schema_tools
from .data_tools import register_data_tools

register_hierarchy_tools(mcp)      # 41 tools
register_reconcile_tools(mcp)      # 20 tools
register_template_tools(mcp)       # 16 tools
register_connection_tools(mcp)     # 8 tools
register_schema_tools(mcp)         # 4 tools
register_data_tools(mcp)           # 3 tools

# Total: 92 tools


def run_server():
    """Run the MCP server via stdio"""
    mcp.run()
```

### 7.2 Claude Desktop Configuration

```json
// %APPDATA%\Claude\claude_desktop_config.json (Windows)
// ~/Library/Application Support/Claude/claude_desktop_config.json (macOS)

{
  "mcpServers": {
    "headless-databridge": {
      "command": "python",
      "args": ["-m", "databridge.mcp.server"],
      "env": {
        "DATABRIDGE_DB_PATH": "C:/Users/telha/Databridge_AI/v3/data/databridge.db",
        "DATABRIDGE_DATA_DIR": "C:/Users/telha/Databridge_AI/v3/data"
      }
    }
  }
}
```

### 7.3 Tool Implementation Pattern

```python
# mcp/hierarchy_tools.py

from fastmcp import FastMCP
import json
from ..hierarchy.service import HierarchyService
from ..core.config import get_settings


def register_hierarchy_tools(mcp: FastMCP):
    settings = get_settings()
    service = HierarchyService(settings.database_url)

    @mcp.tool()
    def create_hierarchy_project(name: str, description: str = "") -> str:
        """
        Create a new hierarchy project.

        Args:
            name: Project name
            description: Optional description

        Returns:
            JSON with project details including ID
        """
        try:
            project = service.create_project(name, description)
            return json.dumps({
                "success": True,
                "project": project.to_dict()
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def list_hierarchy_projects() -> str:
        """
        List all hierarchy projects.

        Returns:
            JSON array of projects with hierarchy counts
        """
        try:
            projects = service.list_projects()
            return json.dumps({
                "success": True,
                "projects": [p.to_dict() for p in projects],
                "count": len(projects)
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    # ... 39 more hierarchy tools
```

---

## 8. Python Libraries

### 8.1 Core Dependencies

```toml
# pyproject.toml

[project]
name = "databridge-cli"
version = "3.0.0"
description = "Headless DataBridge AI - Pure Python CLI with MCP Integration"
requires-python = ">=3.10"

[project.dependencies]
# CLI Framework
typer = ">=0.12.0"            # Modern CLI framework
click = ">=8.1.0"              # CLI utilities (Typer dependency)
rich = ">=13.7.0"              # Beautiful terminal output
prompt-toolkit = ">=3.0.0"     # Interactive REPL

# Data Processing
pandas = ">=2.2.0"             # DataFrame operations
numpy = ">=1.26.0"             # Numerical operations

# Database
sqlalchemy = ">=2.0.0"         # ORM and database abstraction
alembic = ">=1.13.0"           # Database migrations

# Configuration
pydantic = ">=2.6.0"           # Data validation
pydantic-settings = ">=2.2.0"  # Settings management
python-dotenv = ">=1.0.0"      # .env file loading

# MCP Server
fastmcp = ">=0.4.0"            # MCP framework

# Fuzzy Matching
rapidfuzz = ">=3.6.0"          # Fast fuzzy string matching

# File Handling
openpyxl = ">=3.1.0"           # Excel file support
xlsxwriter = ">=3.2.0"         # Excel export

# HTTP Client (for backend sync if needed)
httpx = ">=0.27.0"             # Async HTTP client

# Utilities
python-slugify = ">=8.0.0"     # Slug generation
uuid6 = ">=2024.1.0"           # UUID generation
arrow = ">=1.3.0"              # Date/time utilities
tabulate = ">=0.9.0"           # Table formatting
tqdm = ">=4.66.0"              # Progress bars

# Security
cryptography = ">=42.0.0"      # Encryption for credentials
```

### 8.2 Optional Dependencies

```toml
[project.optional-dependencies]
# Database Connectors
snowflake = [
    "snowflake-connector-python>=3.6.0",
    "snowflake-sqlalchemy>=1.5.0"
]
mysql = [
    "pymysql>=1.1.0",
    "mysqlclient>=2.2.0"
]
postgresql = [
    "psycopg2-binary>=2.9.9"
]
sqlserver = [
    "pyodbc>=5.1.0"
]

# OCR/PDF
ocr = [
    "pypdf>=4.0.0",
    "pytesseract>=0.3.10",
    "pillow>=10.2.0"
]

# Development
dev = [
    "pytest>=8.0.0",
    "pytest-cov>=4.1.0",
    "pytest-asyncio>=0.23.0",
    "black>=24.1.0",
    "ruff>=0.2.0",
    "mypy>=1.8.0",
    "pre-commit>=3.6.0"
]

# All database connectors
all-db = [
    "databridge-cli[snowflake,mysql,postgresql,sqlserver]"
]

# Full installation
all = [
    "databridge-cli[all-db,ocr,dev]"
]
```

### 8.3 Requirements.txt (Flat Version)

```
# Core
typer>=0.12.0
rich>=13.7.0
prompt-toolkit>=3.0.0

# Data
pandas>=2.2.0
numpy>=1.26.0

# Database
sqlalchemy>=2.0.0
alembic>=1.13.0

# Configuration
pydantic>=2.6.0
pydantic-settings>=2.2.0
python-dotenv>=1.0.0

# MCP
fastmcp>=0.4.0

# Fuzzy
rapidfuzz>=3.6.0

# Files
openpyxl>=3.1.0
xlsxwriter>=3.2.0

# HTTP
httpx>=0.27.0

# Utils
python-slugify>=8.0.0
uuid6>=2024.1.0
arrow>=1.3.0
tabulate>=0.9.0
tqdm>=4.66.0
cryptography>=42.0.0

# Optional: Database Connectors (uncomment as needed)
# snowflake-connector-python>=3.6.0
# snowflake-sqlalchemy>=1.5.0
# pymysql>=1.1.0
# psycopg2-binary>=2.9.9
# pyodbc>=5.1.0

# Optional: OCR/PDF (uncomment as needed)
# pypdf>=4.0.0
# pytesseract>=0.3.10
# pillow>=10.2.0

# Development (uncomment for dev)
# pytest>=8.0.0
# pytest-cov>=4.1.0
# black>=24.1.0
# ruff>=0.2.0
# mypy>=1.8.0
```

### 8.4 Library Purpose Summary

| Library | Purpose | Category |
|---------|---------|----------|
| **typer** | CLI framework with type hints | CLI |
| **rich** | Beautiful terminal output (tables, trees, progress) | CLI |
| **prompt-toolkit** | Interactive REPL with history/completion | CLI |
| **pandas** | DataFrame operations for reconciliation | Data |
| **numpy** | Numerical operations | Data |
| **sqlalchemy** | Database ORM abstraction | Database |
| **alembic** | Database migrations | Database |
| **pydantic** | Data validation and models | Config |
| **pydantic-settings** | Settings from .env | Config |
| **fastmcp** | MCP server framework | MCP |
| **rapidfuzz** | Fast fuzzy string matching | Matching |
| **openpyxl** | Excel file read/write | Files |
| **httpx** | Async HTTP client | Network |
| **cryptography** | Credential encryption | Security |
| **snowflake-connector-python** | Snowflake connectivity | DB Connector |
| **pymysql** | MySQL connectivity | DB Connector |
| **psycopg2-binary** | PostgreSQL connectivity | DB Connector |
| **pyodbc** | SQL Server connectivity | DB Connector |
| **pypdf** | PDF text extraction | OCR |
| **pytesseract** | Image OCR | OCR |

---

## 9. User Guide

### 9.1 Installation

```bash
# Clone repository
git clone https://github.com/your-org/databridge-cli.git
cd databridge-cli

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/macOS
venv\Scripts\activate     # Windows

# Install with pip
pip install -e .

# Or install with all optional dependencies
pip install -e ".[all]"

# Verify installation
databridge --version
databridge --help
```

### 9.2 Configuration

```bash
# Copy environment template
cp .env.example .env

# Edit configuration
# .env file contents:
DATABRIDGE_DB_PATH=data/databridge.db
DATABRIDGE_DATA_DIR=data
DATABRIDGE_AUDIT_LOG=data/audit_trail.csv
DATABRIDGE_WORKFLOW_FILE=data/workflow.json
DATABRIDGE_MAX_ROWS_DISPLAY=10
DATABRIDGE_FUZZY_THRESHOLD=80

# Optional: Backend sync (if using with v2)
NESTJS_BACKEND_URL=http://localhost:3002/api
NESTJS_API_KEY=v2-dev-key-1
NESTJS_SYNC_ENABLED=false

# Optional: Database connections
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
```

### 9.3 Quick Start

```bash
# 1. Create a project
databridge project create "My Financial Hierarchy" --description "FY2024 P&L"

# 2. Create from template
databridge template create-project standard_pl "Q1 P&L Report"

# 3. Import existing hierarchy from CSV
databridge csv import hierarchy <project-id> hierarchy.csv
databridge csv import mapping <project-id> mapping.csv

# 4. View hierarchy tree
databridge hierarchy tree <project-id>

# 5. Add source mapping
databridge mapping add <hierarchy-id> \
    --database ANALYTICS \
    --schema PUBLIC \
    --table DIM_PRODUCT \
    --column PRODUCT_ID

# 6. Export to CSV
databridge csv export hierarchy <project-id> --output export/

# 7. Start interactive mode
databridge shell
```

### 9.4 Interactive Mode (REPL)

```
$ databridge shell

Welcome to DataBridge AI Interactive Shell
Type 'help' for commands, 'exit' to quit

databridge> project list
┌──────────────────────────────────────┬─────────────────────────┬───────────┐
│ ID                                   │ Name                    │ Hierarchies│
├──────────────────────────────────────┼─────────────────────────┼───────────┤
│ 550e8400-e29b-41d4-a716-446655440000 │ My Financial Hierarchy  │ 15        │
│ 6fa459ea-ee8a-3ca4-894e-db77e160355e │ Q1 P&L Report           │ 42        │
└──────────────────────────────────────┴─────────────────────────┴───────────┘

databridge> hierarchy tree 550e8400-e29b-41d4-a716-446655440000
├── Revenue
│   ├── Product Sales
│   │   ├── Electronics
│   │   └── Software
│   └── Service Revenue
│       ├── Consulting
│       └── Support
├── Cost of Goods Sold
│   ├── Direct Materials
│   └── Direct Labor
└── Operating Expenses
    ├── Sales & Marketing
    └── G&A

databridge> reconcile load csv data/sales_2024.csv
Loaded 15,234 rows from sales_2024.csv
Columns: ['order_id', 'product_id', 'quantity', 'amount', 'date']

databridge> reconcile profile sales_2024
┌─────────────┬──────────┬───────────┬─────────┬──────────┐
│ Column      │ Type     │ Non-Null  │ Unique  │ Sample   │
├─────────────┼──────────┼───────────┼─────────┼──────────┤
│ order_id    │ string   │ 100%      │ 15,234  │ ORD-001  │
│ product_id  │ string   │ 100%      │ 1,234   │ PROD-A1  │
│ quantity    │ int64    │ 100%      │ 45      │ 10       │
│ amount      │ float64  │ 99.5%     │ 8,923   │ 149.99   │
│ date        │ datetime │ 100%      │ 365     │ 2024-01  │
└─────────────┴──────────┴───────────┴─────────┴──────────┘

databridge> exit
Goodbye!
```

### 9.5 Using with Claude (MCP)

```bash
# 1. Start MCP server
databridge mcp serve

# 2. Or configure Claude Desktop
# Add to claude_desktop_config.json:
{
  "mcpServers": {
    "headless-databridge": {
      "command": "python",
      "args": ["-m", "databridge.mcp.server"]
    }
  }
}

# 3. In Claude, you can now use:
# - "Create a new hierarchy project called 'Q2 Report'"
# - "Import this CSV file into my project"
# - "Show me the hierarchy tree"
# - "Compare these two data sources"
# - "Generate deployment scripts for Snowflake"
```

### 9.6 Common Workflows

#### Workflow 1: Create P&L Hierarchy from Template

```bash
# Step 1: List available P&L templates
databridge template list --domain accounting

# Step 2: View template details
databridge template show standard_pl

# Step 3: Create project from template
databridge template create-project standard_pl "FY2024 P&L"

# Step 4: Customize hierarchy
databridge hierarchy update <id> --name "Custom Revenue"

# Step 5: Add source mappings
databridge mapping add <id> --database FINANCE --schema GL --table TRANSACTIONS --column AMOUNT
```

#### Workflow 2: Data Reconciliation

```bash
# Step 1: Load source files
databridge reconcile load csv source_a.csv
databridge reconcile load csv source_b.csv

# Step 2: Profile data
databridge reconcile profile source_a
databridge reconcile profile source_b

# Step 3: Compare sources
databridge reconcile compare source_a source_b --keys order_id

# Step 4: View orphans and conflicts
databridge reconcile orphans
databridge reconcile conflicts

# Step 5: Fuzzy match for deduplication
databridge reconcile fuzzy source_a customer_name --threshold 85
```

#### Workflow 3: Deploy to Snowflake

```bash
# Step 1: Create connection
databridge connection create prod-snowflake \
    --type snowflake \
    --account xy12345.us-east-1 \
    --warehouse COMPUTE_WH

# Step 2: Test connection
databridge connection test prod-snowflake

# Step 3: Generate deployment scripts
databridge deploy generate <project-id> prod-snowflake --type all

# Step 4: Execute deployment
databridge deploy execute <project-id> prod-snowflake

# Step 5: View history
databridge deploy history <project-id>
```

---

## 10. Testing Strategy

### 10.1 Test Structure

```
tests/
├── conftest.py                 # Shared fixtures
├── unit/
│   ├── test_hierarchy_service.py
│   ├── test_reconciliation.py
│   ├── test_templates.py
│   └── test_connections.py
├── integration/
│   ├── test_csv_import_export.py
│   ├── test_database_operations.py
│   └── test_deployment.py
├── mcp/
│   ├── test_hierarchy_tools.py
│   ├── test_reconcile_tools.py
│   └── test_template_tools.py
└── e2e/
    ├── test_cli_commands.py
    └── test_interactive_shell.py
```

### 10.2 Test Commands

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific module
pytest tests/unit/test_hierarchy_service.py

# Run MCP tool tests
pytest tests/mcp/ -v
```

---

## 11. Security Considerations

### 11.1 Credential Management

```python
# Encrypt database passwords using Fernet
from cryptography.fernet import Fernet

def encrypt_password(password: str, key: bytes) -> str:
    f = Fernet(key)
    return f.encrypt(password.encode()).decode()

def decrypt_password(encrypted: str, key: bytes) -> str:
    f = Fernet(key)
    return f.decrypt(encrypted.encode()).decode()
```

### 11.2 Audit Trail

- All actions logged to `audit_trail.csv`
- No PII in logs (sanitized)
- Timestamps in UTC
- User identification (CLI user or MCP)

### 11.3 Database Access

- READ-ONLY queries by default
- SELECT statements only for `query_database`
- Connection credentials encrypted at rest

---

## 12. Deployment Guide

### 12.1 PyPI Installation (Future)

```bash
pip install databridge-cli
```

### 12.2 Standalone Executable

```bash
# Build with PyInstaller
pip install pyinstaller
pyinstaller --onefile src/main.py --name databridge

# Output: dist/databridge.exe (Windows) or dist/databridge (Unix)
```

### 12.3 Docker (Optional)

```dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY . .

RUN pip install -e ".[all]"

ENTRYPOINT ["databridge"]
CMD ["shell"]
```

---

## Approval Checklist

Please review and approve the following:

- [ ] **Architecture** - Module structure and separation of concerns
- [ ] **Database Design** - SQLAlchemy models and SQLite storage
- [ ] **CLI Commands** - Command structure and naming conventions
- [ ] **MCP Integration** - 92 tools with FastMCP
- [ ] **Library Selection** - Python dependencies
- [ ] **Implementation Timeline** - 14-week phased approach
- [ ] **Testing Strategy** - Unit, integration, and E2E tests
- [ ] **Security Approach** - Credential encryption and audit logging

---

**Next Steps After Approval:**
1. Initialize project structure
2. Set up pyproject.toml with dependencies
3. Implement Phase 1: Foundation
4. Begin iterative development

---

*Document Version: 1.0*
*Created: 2025-01-28*
*Author: Claude (Opus 4.5)*
