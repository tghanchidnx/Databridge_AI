# DataBridge Discovery Engine

Automated SQL parsing, CASE statement extraction, and hierarchy generation for data warehouse modeling.

## Features

- **SQL Parsing**: Multi-dialect SQL parsing using sqlglot (Snowflake, PostgreSQL, T-SQL, MySQL, BigQuery)
- **CASE Extraction**: Automatic extraction of CASE WHEN statements with hierarchy detection
- **Semantic Graph**: Graph-based semantic modeling with NetworkX
- **Entity Detection**: Detects 12 standard entity types (account, cost_center, department, etc.)
- **Librarian Integration**: Direct export to Librarian hierarchy project format

## Installation

```bash
# Basic installation
pip install databridge-discovery

# With embeddings support
pip install databridge-discovery[embeddings]

# With MCP tools
pip install databridge-discovery[mcp]

# Full installation
pip install databridge-discovery[all]
```

## Quick Start

```python
from databridge_discovery import SQLParser, CaseExtractor, DiscoverySession

# Parse SQL
parser = SQLParser(dialect="snowflake")
ast = parser.parse(sql_query)

# Extract CASE statements
extractor = CaseExtractor()
cases = extractor.extract(ast)

# Start discovery session
session = DiscoverySession()
session.add_sql_source(sql_query)
session.analyze()

# Get proposed hierarchies
hierarchies = session.get_proposed_hierarchies()
```

## MCP Tools

The library provides 50 MCP tools across 7 phases:

### Phase 1: SQL Parser & Session (6 tools)
- `parse_sql` - Parse SQL and return AST
- `extract_case_statements` - Extract CASE WHEN logic
- `analyze_sql_complexity` - Query complexity metrics
- `start_discovery_session` - Initialize discovery session
- `get_discovery_session` - Get session state
- `export_discovery_evidence` - Export evidence

### Phase 2: Semantic Graph (8 tools)
- `build_semantic_graph` - Build from schema
- `add_graph_relationship` - Add edge
- `find_join_paths` - Find join candidates
- And more...

### Phase 3-7: See full documentation

## Entity Types

The discovery engine detects 12 standard entity types:

| Entity | Description |
|--------|-------------|
| account | GL accounts, chart of accounts |
| cost_center | Cost centers, profit centers |
| department | Organizational departments |
| entity | Legal entities, companies |
| project | Projects, work orders |
| product | Products, SKUs |
| customer | Customers, clients |
| vendor | Vendors, suppliers |
| employee | Employees, workers |
| location | Geographic locations |
| time_period | Time periods, fiscal periods |
| currency | Currencies |

## License

MIT
