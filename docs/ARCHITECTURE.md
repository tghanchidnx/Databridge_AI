# DataBridge AI Architecture

This document describes the high-level architecture, component interactions, and design decisions of the DataBridge AI platform.

## System Overview

DataBridge AI is a headless, MCP-native data reconciliation and hierarchy management platform. It follows a layered architecture with clear separation between applications, libraries, and external services.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                 Clients                                          │
│                                                                                  │
│    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                    │
│    │ Claude Code  │    │Claude Desktop│    │   VS Code    │                    │
│    │   (MCP)      │    │    (MCP)     │    │   (MCP)      │                    │
│    └──────┬───────┘    └──────┬───────┘    └──────┬───────┘                    │
│           │                   │                   │                             │
└───────────┼───────────────────┼───────────────────┼─────────────────────────────┘
            │                   │                   │
            └───────────────────┴───────────────────┘
                                │
                         MCP Protocol
                                │
┌───────────────────────────────┴─────────────────────────────────────────────────┐
│                           Application Layer                                      │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    DataBridge Librarian (MCP Server)                     │   │
│  │                                                                          │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │   │
│  │  │    Data     │  │  Hierarchy  │  │   SQL/dbt   │  │     Git     │    │   │
│  │  │ Reconcile   │  │   Builder   │  │  Generator  │  │ Automation  │    │   │
│  │  │  38 tools   │  │  38 tools   │  │   8 tools   │  │   3 tools   │    │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │   │
│  │                                                                          │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                     │   │
│  │  │  Templates  │  │   Skills    │  │ Knowledge   │                     │   │
│  │  │  5 tools    │  │  3 tools    │  │    Base     │                     │   │
│  │  │             │  │             │  │  5 tools    │                     │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘                     │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  ┌────────────────────────────┐    ┌────────────────────────────┐              │
│  │  DataBridge Researcher    │    │      DataBridge CLI        │              │
│  │     (NestJS Backend)      │    │    (Python Typer)          │              │
│  │                           │    │                            │              │
│  │  - REST API (port 8001)   │    │  - Project management      │              │
│  │  - MySQL persistence      │    │  - Deployment commands     │              │
│  │  - Dynamic tables         │    │  - Scripting interface     │              │
│  │  - Deployment tracking    │    │                            │              │
│  └─────────────┬─────────────┘    └────────────────────────────┘              │
│                │                                                               │
└────────────────┼───────────────────────────────────────────────────────────────┘
                 │
┌────────────────┴───────────────────────────────────────────────────────────────┐
│                             Library Layer                                       │
│                                                                                  │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐    │
│  │   databridge-core   │  │  databridge-models  │  │databridge-discovery │    │
│  │                     │  │                     │  │                     │    │
│  │  - Git operations   │  │  - Pydantic models  │  │  - Source detection │    │
│  │  - Audit logging    │  │  - Hierarchy schema │  │  - Schema inference │    │
│  │  - Configuration    │  │  - Mapping schema   │  │  - Profile analysis │    │
│  │  - Utilities        │  │  - API contracts    │  │  - Discovery state  │    │
│  └─────────────────────┘  └─────────────────────┘  └─────────────────────┘    │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
┌───────────────────────────────────────┴─────────────────────────────────────────┐
│                           External Services                                      │
│                                                                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐                 │
│  │     MySQL       │  │     Redis       │  │   Snowflake     │                 │
│  │   (port 3308)   │  │   (port 6381)   │  │                 │                 │
│  │                 │  │                 │  │  - Deployment   │                 │
│  │  - Hierarchies  │  │  - Caching      │  │  - Data source  │                 │
│  │  - Mappings     │  │  - Sessions     │  │  - Schema info  │                 │
│  │  - Deployments  │  │  - Rate limits  │  │                 │                 │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘                 │
│                                                                                  │
│  ┌─────────────────┐  ┌─────────────────┐                                      │
│  │     GitHub      │  │  Local Files    │                                      │
│  │                 │  │                 │                                      │
│  │  - PR creation  │  │  - CSV/JSON     │                                      │
│  │  - dbt repos    │  │  - PDF/images   │                                      │
│  │  - CI/CD        │  │  - Templates    │                                      │
│  └─────────────────┘  └─────────────────┘                                      │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Component Details

### Application Layer

#### DataBridge Librarian (MCP Server)

The core MCP server providing 92+ tools through the Model Context Protocol.

**Technology Stack:**
- Python 3.10+
- FastMCP for MCP protocol handling
- Pandas for data manipulation
- RapidFuzz for fuzzy matching
- SQLAlchemy for database queries

**Key Modules:**

| Module | Purpose | Tools |
|--------|---------|-------|
| Data Reconciliation | Compare data sources | 38 |
| Hierarchy Builder | Manage hierarchies | 38 |
| SQL Generator | Generate deployment SQL | 8 |
| Git Automation | GitHub integration | 3 |
| Templates | Pre-built structures | 5 |
| Skills | AI expertise profiles | 3 |
| Knowledge Base | Client configurations | 5 |

#### DataBridge Researcher (NestJS Backend)

REST API backend for persistent storage and advanced features.

**Technology Stack:**
- NestJS (TypeScript)
- TypeORM with MySQL
- Redis for caching
- JWT authentication

**Key Features:**
- Hierarchy CRUD operations
- Deployment tracking
- Dynamic table generation
- Cross-project search
- Activity logging

#### DataBridge CLI

Command-line interface for scripting and automation.

**Technology Stack:**
- Python Typer
- Rich for terminal UI

### Library Layer

#### databridge-core

Shared utilities used across all applications.

```
databridge-core/
├── audit/
│   └── logger.py       # Audit trail logging
├── git/
│   ├── __init__.py
│   └── operations.py   # Git/GitHub CLI operations
├── config/
│   └── settings.py     # Pydantic settings
└── utils/
    └── helpers.py      # Common utilities
```

#### databridge-models

Pydantic models defining the data contracts.

```python
# Key models
HierarchyProject      # Project container
Hierarchy             # Hierarchy node
SourceMapping         # Database column mapping
FormulaGroup          # Calculated node definition
Template              # Hierarchy template
Skill                 # AI expertise profile
ClientProfile         # Client knowledge base
```

#### databridge-discovery

Source detection and schema inference engine.

**Capabilities:**
- Automatic file type detection
- Schema inference from samples
- Data quality profiling
- Relationship discovery

## Data Flow Diagrams

### Hierarchy Creation Flow

```
┌─────────┐     ┌──────────────┐     ┌──────────────┐     ┌─────────┐
│  User   │────▶│   MCP Tool   │────▶│   Service    │────▶│  MySQL  │
│         │     │create_project│     │ Layer        │     │         │
└─────────┘     └──────────────┘     └──────────────┘     └─────────┘
                                           │
                                           │ Auto-sync
                                           ▼
                                    ┌──────────────┐
                                    │   Backend    │
                                    │   (NestJS)   │
                                    └──────────────┘
```

### Data Reconciliation Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Source A  │────▶│   load_csv  │────▶│   Pandas    │
│   (CSV)     │     │             │     │  DataFrame  │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                               │
┌─────────────┐     ┌─────────────┐            │
│   Source B  │────▶│ query_db    │────────────┤
│   (SQL)     │     │             │            │
└─────────────┘     └─────────────┘            │
                                               ▼
                                    ┌──────────────────┐
                                    │  compare_hashes  │
                                    └────────┬─────────┘
                                             │
                    ┌────────────────────────┼────────────────────────┐
                    │                        │                        │
                    ▼                        ▼                        ▼
            ┌──────────────┐        ┌──────────────┐        ┌──────────────┐
            │   Matches    │        │   Orphans    │        │  Conflicts   │
            └──────────────┘        └──────────────┘        └──────────────┘
```

### Deployment Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Hierarchy  │────▶│  generate_  │────▶│   commit_   │────▶│   create_   │
│   Project   │     │  scripts    │     │   dbt_proj  │     │   PR        │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                                                   │
                                                                   ▼
                                                            ┌─────────────┐
                                                            │   GitHub    │
                                                            │   Actions   │
                                                            └──────┬──────┘
                                                                   │
                                                                   ▼
                                                            ┌─────────────┐
                                                            │  Snowflake  │
                                                            └─────────────┘
```

## MCP Protocol

DataBridge uses the Model Context Protocol (MCP) to expose functionality to AI assistants.

### Tool Registration

```python
from fastmcp import FastMCP

mcp = FastMCP("databridge-librarian")

@mcp.tool()
def create_hierarchy_project(name: str, description: str = "") -> dict:
    """Create a new hierarchy project.

    Args:
        name: Project name
        description: Optional description

    Returns:
        JSON with project ID and details
    """
    # Implementation
```

### Tool Categories

Tools are organized by domain:

1. **Data Reconciliation** - Working with raw data sources
2. **Hierarchy Management** - CRUD operations on hierarchies
3. **Source Mappings** - Linking data to hierarchies
4. **Templates & Skills** - Pre-built configurations
5. **Deployment** - Generating and deploying SQL
6. **Git Automation** - Version control integration

## Security Model

### Authentication

| Component | Method |
|-----------|--------|
| Backend API | API Key (`X-API-Key` header) |
| Snowflake | Username/Password or Key Pair |
| GitHub | Personal Access Token (`gh` CLI) |
| MCP Server | Local process (no auth needed) |

### Data Protection

- No PII in audit logs
- Secrets loaded from `.env` via Pydantic Settings
- Knowledge Base stored locally (not synced)
- Connection credentials encrypted in backend

### API Keys (Development)

```
dev-key-1  # Primary development key
dev-key-2  # Secondary development key
```

## Auto-Sync Feature

All hierarchy write operations automatically synchronize with the NestJS backend when enabled.

```python
# Enable/disable auto-sync
configure_auto_sync(enabled=True)

# Operations that trigger sync:
# - create_hierarchy_project
# - create_hierarchy
# - update_hierarchy
# - delete_hierarchy
# - add_source_mapping
```

## Database Schema

### Hierarchy Tables (MySQL)

```sql
-- Projects
CREATE TABLE hierarchy_projects (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Hierarchies
CREATE TABLE hierarchies (
    id VARCHAR(36) PRIMARY KEY,
    project_id VARCHAR(36) NOT NULL,
    hierarchy_id VARCHAR(100) NOT NULL,
    hierarchy_name VARCHAR(255) NOT NULL,
    parent_id VARCHAR(100),
    description TEXT,
    level_1 VARCHAR(255),
    -- ... level_2 through level_15
    include_flag BOOLEAN DEFAULT TRUE,
    calculation_flag BOOLEAN DEFAULT FALSE,
    active_flag BOOLEAN DEFAULT TRUE,
    is_leaf_node BOOLEAN DEFAULT FALSE,
    sort_order INT DEFAULT 0,
    FOREIGN KEY (project_id) REFERENCES hierarchy_projects(id)
);

-- Source Mappings
CREATE TABLE source_mappings (
    id VARCHAR(36) PRIMARY KEY,
    hierarchy_uuid VARCHAR(36) NOT NULL,
    source_database VARCHAR(100),
    source_schema VARCHAR(100),
    source_table VARCHAR(100),
    source_column VARCHAR(100),
    source_uid VARCHAR(255),
    precedence_group VARCHAR(10) DEFAULT '1',
    FOREIGN KEY (hierarchy_uuid) REFERENCES hierarchies(id)
);
```

## Service Ports

| Service | Port | Container Name |
|---------|------|----------------|
| Frontend | 8000 | databridge-librarian |
| Backend | 8001 | databridge-researcher |
| MySQL | 3308 | databridge-mysql |
| Redis | 6381 | databridge-redis |

## Development Guidelines

### Adding New Tools

1. Create tool function with `@mcp.tool()` decorator
2. Write detailed docstring (used for LLM context)
3. Add to appropriate module in `src/mcp/tools/`
4. Register in `src/mcp/server.py`
5. Run `update_manifest` to update documentation
6. Add tests in `tests/unit/mcp/`

### Testing

```bash
# Unit tests
pytest apps/databridge-librarian/tests/unit -v

# Integration tests
pytest apps/databridge-librarian/tests/integration -v

# With coverage
pytest --cov=src --cov-report=html
```

### Code Standards

- Type hints on all functions
- Docstrings with Args/Returns sections
- Maximum 10 rows returned to LLM
- Atomic commits per feature
- Review `docs/LESSONS_LEARNED.md` before changes

## Future Considerations

1. **Multi-tenant Support** - Separate projects by organization
2. **Real-time Collaboration** - WebSocket-based updates
3. **Advanced Formulas** - Custom expressions, cross-project references
4. **Data Lineage Visualization** - Graphical mapping view
5. **Automated Mapping Suggestions** - ML-based column matching
