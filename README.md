# DataBridge AI

A headless, MCP-native data reconciliation engine with **92+ MCP tools** for enterprise data management, hierarchy building, and automated deployment.

## Features

- **Data Reconciliation Engine** - Compare and validate data from CSV, SQL, PDF, JSON, and OCR sources
- **Hierarchy Knowledge Base Builder** - Create and manage multi-level hierarchical structures for financial reporting
- **Templates & Skills System** - 20 pre-built templates and 7 AI expertise profiles for domain-specific workflows
- **Database Connectivity** - Direct Snowflake integration with schema comparison and deployment automation
- **GitHub Automation** - Automated commits, PRs, and dbt project generation

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Applications                                    │
├─────────────────────┬─────────────────────┬─────────────────────────────────┤
│   Librarian (MCP)   │  Researcher (API)   │          CLI Tools              │
│   92 MCP Tools      │  NestJS Backend     │     databridge-cli              │
│   Python/FastMCP    │  REST API           │     Python Typer                │
└─────────┬───────────┴──────────┬──────────┴──────────────┬──────────────────┘
          │                      │                         │
┌─────────┴──────────────────────┴─────────────────────────┴──────────────────┐
│                              Libraries                                       │
├─────────────────────┬─────────────────────┬─────────────────────────────────┤
│   databridge-core   │  databridge-models  │      databridge-discovery       │
│   Git, Audit, Utils │  Pydantic Models    │      Source Discovery           │
└─────────────────────┴─────────────────────┴─────────────────────────────────┘
          │                      │                         │
┌─────────┴──────────────────────┴─────────────────────────┴──────────────────┐
│                           External Services                                  │
├─────────────────────┬─────────────────────┬─────────────────────────────────┤
│       MySQL         │       Redis         │          Snowflake              │
│   Hierarchy Store   │      Caching        │      Data Warehouse             │
└─────────────────────┴─────────────────────┴─────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.10+
- Docker & Docker Compose
- Git and GitHub CLI (`gh`)

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/databridge-ai.git
cd databridge-ai

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -e "libs/databridge-core[dev]"
pip install -e "libs/databridge-models[dev]"
pip install -e "apps/databridge-librarian[dev]"

# Start services
docker-compose up -d
```

### MCP Configuration

Add to your Claude Desktop or Claude Code MCP settings:

```json
{
  "mcpServers": {
    "databridge": {
      "command": "python",
      "args": ["-m", "fastmcp", "run", "apps/databridge-librarian/src/mcp/server.py"],
      "cwd": "/path/to/databridge-ai"
    }
  }
}
```

### CLI Usage

```bash
# List hierarchy projects
databridge projects list

# Create a project from template
databridge projects create --template standard_pl --name "Q4 2024 Reporting"

# Deploy to Snowflake
databridge deploy --project <project-id> --connection <connection-id>
```

## Project Structure

```
databridge-ai/
├── apps/
│   ├── databridge-librarian/     # MCP server with 92 tools
│   │   ├── src/
│   │   │   ├── mcp/              # MCP tool definitions
│   │   │   ├── hierarchy/        # Hierarchy management
│   │   │   ├── source/           # Source discovery
│   │   │   └── sql_generator/    # SQL/dbt generation
│   │   └── tests/
│   └── databridge-researcher/    # NestJS REST API backend
│       └── src/
├── libs/
│   ├── databridge-core/          # Shared utilities (git, audit, etc.)
│   ├── databridge-models/        # Pydantic data models
│   └── databridge-discovery/     # Source discovery engine
├── templates/                    # 20 hierarchy templates
│   ├── accounting/               # P&L, Balance Sheet, industry-specific
│   ├── finance/                  # Cost center, profit center
│   └── operations/               # Geographic, department, asset
├── skills/                       # 7 AI expertise profiles
├── knowledge_base/               # Client-specific configurations
├── docs/                         # Documentation
└── .github/workflows/            # CI/CD automation
```

## MCP Tool Categories

| Category | Tools | Description |
|----------|-------|-------------|
| Data Loading | 3 | Load CSV, JSON, query databases |
| Profiling | 2 | Profile data quality, detect schema drift |
| Comparison | 3 | Hash comparison, orphans, conflicts |
| Fuzzy Matching | 2 | Column matching, deduplication |
| OCR/PDF | 3 | Text extraction, table parsing |
| Transforms | 2 | Column transforms, source merging |
| Workflow | 4 | Save steps, audit trail |
| Hierarchy Projects | 4 | Create, list, delete projects |
| Hierarchy Nodes | 6 | CRUD operations, tree navigation |
| Source Mappings | 5 | Map database columns to hierarchies |
| Formulas | 3 | SUM, SUBTRACT, calculated nodes |
| Import/Export | 5 | CSV, JSON, SQL scripts |
| Backend Sync | 4 | MySQL synchronization |
| Connections | 8 | Database browsing, schema comparison |
| Templates | 5 | Financial statement templates |
| Skills | 3 | AI expertise profiles |
| Knowledge Base | 5 | Client-specific settings |
| Git Automation | 3 | Commits, PRs, dbt projects |

## Templates

Pre-built hierarchy structures for common use cases:

- **Accounting**: Standard P&L, Balance Sheet, Oil & Gas LOS, Manufacturing COGS
- **Operations**: Geographic, Department, Asset, Fleet hierarchies
- **Finance**: Cost Center, Profit Center structures

See [Librarian README](apps/databridge-librarian/README.md) for full template list.

## Development

### Running Tests

```bash
# All tests
pytest

# Specific app
pytest apps/databridge-librarian/tests -v

# With coverage
pytest --cov=apps/databridge-librarian/src --cov-report=html
```

### Starting Services

```bash
# Start all Docker services
python start_services.py

# Or manually
docker-compose up -d
```

### Service Ports

| Service | Port | Description |
|---------|------|-------------|
| Frontend | 8000 | React UI |
| Backend | 8001 | NestJS API |
| MySQL | 3308 | Database |
| Redis | 6381 | Cache |

## Documentation

- [Architecture Overview](docs/ARCHITECTURE.md)
- [Librarian MCP Tools](apps/databridge-librarian/README.md)
- [Lessons Learned](docs/LESSONS_LEARNED.md)
- [API Reference](docs/MANIFEST.md)

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

MIT License - see [LICENSE](LICENSE) for details.
