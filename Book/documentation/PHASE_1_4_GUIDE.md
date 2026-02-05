# DataBridge AI: Phase 1-4 Complete Guide

## Architecture Overview

The DataBridge AI ecosystem consists of three interconnected systems forming a "Trinity" architecture:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DataBridge AI Trinity                        │
├─────────────────┬──────────────────────┬────────────────────────────┤
│      Book       │      Librarian       │        Researcher          │
│   (Python)      │     (NestJS)         │        (NestJS)            │
│                 │                      │                            │
│ In-memory       │ Centralized project  │ Analytics, validation,     │
│ hierarchy       │ management, CRUD,    │ NL-to-SQL, schema          │
│ prototyping,    │ templates, skills,   │ embeddings, data           │
│ formulas,       │ deployment,          │ profiling                  │
│ AI agent,       │ version control      │                            │
│ vector search   │                      │                            │
└────────┬────────┴───────────┬──────────┴──────────────┬─────────────┘
         │                    │                         │
         │    promote/        │      validate/          │
         │    checkout        │      profile            │
         └────────────────────┴─────────────────────────┘
```

---

## Phase 1: Book Library Enhancements

### 1.1 Vector-Powered AIAgent

**Files:** `Book/book/ai_agent.py`, `Book/book/vector_db.py`, `Book/book/ai_agent_config.py`

The AIAgent uses SentenceTransformer embeddings (`all-MiniLM-L6-v2`) to semantically match user queries against a library of pre-defined skills.

**How it works:**
1. Skills are loaded from `skills/*.json` files
2. Skill descriptions are embedded into vectors using SentenceTransformer
3. When a user asks a question, the query is embedded and compared via cosine similarity
4. The best-matching skill is returned with suggestions

**API:**
```python
from book import AIAgent, AIAgentConfig

# Configure which skills to use
config = AIAgentConfig(skills_to_use=["financial-analyst", "pricing-analyst"])

# Initialize the agent
agent = AIAgent(
    databridge_project_path="C:/Users/telha/Databridge_AI",
    config=config
)

# Find the best skill for a query
best_skill = agent.find_best_skill("How do I reconcile my GL accounts?")

# Get enhancement suggestions for a Book
suggestions = agent.suggest_enhancements(my_book, "pricing strategy")
```

**Skill File Format** (`skills/financial-analyst.json`):
```json
{
    "name": "Financial Analyst",
    "description": "An AI skill for financial analysis.",
    "mappings": {
        "Sales": "Revenue",
        "Cost of Sales": "COGS"
    }
}
```

### 1.2 Persistent Graph Database (TinyDB + NetworkX)

**File:** `Book/book/graph_db.py`

Books can be converted to NetworkX directed graphs and persisted to TinyDB (a JSON-based document database) for durable storage.

**Round-trip flow:**
```
Book → NetworkX DiGraph → TinyDB (JSON file) → NetworkX DiGraph → Book
```

**API:**
```python
from book import book_to_graph, graph_to_book, save_graph_to_tinydb, load_graph_from_tinydb

# Convert Book to graph
graph = book_to_graph(my_book)

# Save to TinyDB
save_graph_to_tinydb(graph, "my_project.json")

# Load from TinyDB
loaded_graph = load_graph_from_tinydb("my_project.json")

# Convert back to Book
restored_book = graph_to_book(loaded_graph, "Restored Book")
```

**TinyDB Schema:**
- `nodes` table: `{id, name, properties, flags, ...}`
- `edges` table: `{source, target}`

---

## Phase 2: Librarian Integration

### 2.1 Promote Book to Librarian

**File:** `Book/use_cases/23_promote_to_librarian/promote_to_librarian.py`

Takes an in-memory Book (prototyped locally) and creates a Librarian project with all nodes preserved as hierarchies.

**Flow:**
```
Book (in-memory)
    ├── Create Librarian project record
    └── Recursively insert all nodes as hierarchy records
        └── Preserve parent-child relationships via parent_id
```

**API:**
```python
from promote_to_librarian import promote_book_to_librarian

project_id = promote_book_to_librarian(my_book, "librarian_db.json")
# Returns the new project UUID
```

### 2.2 Checkout Librarian to Book

**File:** `Book/use_cases/24_checkout_from_librarian/checkout_from_librarian.py`

Retrieves a Librarian project and converts it into an in-memory Book for manipulation.

**Flow:**
```
Librarian project
    ├── Query project by ID
    ├── Fetch all hierarchy nodes
    ├── Reconstruct parent-child relationships
    └── Return Book object
```

**API:**
```python
from checkout_from_librarian import checkout_librarian_hierarchy

book = checkout_librarian_hierarchy("proj-master-coa-001", "librarian_db.json")
# Returns a Book with full hierarchy
```

### 2.3 LibrarianBridge (Production Integration)

**File:** `src/agents/unified_agent/bridges/librarian_bridge.py`

The production-grade bridge for Book ↔ Librarian conversion that works with the NestJS backend via REST API.

**Capabilities:**
- `book_to_librarian_hierarchies()` - Convert Book nodes to Librarian API format
- `librarian_hierarchies_to_book()` - Convert Librarian records to Book
- `sync_book_and_librarian()` - Bidirectional sync with conflict resolution
- `diff_book_project()` - Compare Book vs Librarian project

---

## Phase 3: Researcher Integration

### 3.1 NL-to-SQL with Schema Embeddings

**File:** `Book/use_cases/25_nl_to_sql/nl_to_sql.py`

Translates natural language queries into SQL by using schema embeddings to identify relevant tables and columns.

**Flow:**
```
Natural Language Query
    ├── Tokenize and extract keywords
    ├── Match keywords against schema embeddings
    ├── Identify relevant tables and columns
    ├── Construct JOIN/WHERE/GROUP BY clauses
    └── Return SQL query
```

**Example:**
```
Input:  "show me the total order amount for each customer in the 'North' region"
Output: SELECT c.customer_name, SUM(o.total_amount) AS total_order_amount
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        WHERE c.region = 'North'
        GROUP BY c.customer_name;
```

### 3.2 ResearcherBridge (Production Integration)

**File:** `src/agents/unified_agent/bridges/researcher_bridge.py`

The production bridge for analytics and validation against live databases.

**Capabilities:**
- `extract_sources_from_book()` - Extract source mappings from Book nodes
- `analyze_book()` - Validate sources against target database
- `compare_hierarchy_data()` - Compare hierarchy values with live data
- `profile_sources()` - Get data distribution statistics

---

## Phase 4: Agentic & MCP Enhancements

### 4.1 MCP Tools for Book Library

**File:** `Book/use_cases/26_mcp_server/mcp_server.py`

Exposes Book operations as MCP tools that can be called from Claude Desktop or any MCP client.

**Registered Tools:**
| Tool | Description |
|------|-------------|
| `create_book_from_csv` | Creates a Book from CSV with parent-child relationships |
| `add_formula_to_book` | Adds a formula to a specific node |
| `get_book_as_json` | Returns the full Book as JSON |

**Usage with FastMCP:**
```python
from fastmcp import FastMCP
from mcp_server import register_book_mcp_tools

mcp = FastMCP("BookLib-MCP")
register_book_mcp_tools(mcp)
mcp.run()
```

### 4.2 Unified Agent MCP Tools (Production)

**File:** `src/agents/unified_agent/mcp_tools.py`

10 MCP tools registered on the main DataBridge server for cross-system operations:

| # | Tool | Systems | Description |
|---|------|---------|-------------|
| 1 | `checkout_librarian_to_book` | Librarian → Book | Convert project to Book |
| 2 | `promote_book_to_librarian` | Book → Librarian | Create/update project |
| 3 | `sync_book_and_librarian` | Book ↔ Librarian | Bidirectional sync |
| 4 | `diff_book_and_librarian` | Book ↔ Librarian | Show differences |
| 5 | `analyze_book_with_researcher` | Book → Researcher | Validate sources |
| 6 | `compare_book_to_database` | Book → Researcher | Compare to live data |
| 7 | `profile_book_sources` | Book → Researcher | Profile source columns |
| 8 | `create_unified_workflow` | All | Define multi-step workflow |
| 9 | `execute_unified_workflow` | All | Execute workflow |
| 10 | `get_unified_context` | All | Get current state |

### 4.3 Meta-Agent / Orchestrator

**File:** `Book/use_cases/27_meta_agent_orchestrator/meta_agent.py`

A workflow orchestrator that executes multi-step plans across Book, Librarian, and Researcher systems.

**Plan Structure:**
```python
plan = [
    {
        "name": "Step Name",
        "tool": "tool_name",          # Registered tool function
        "inputs": {"param": "value"}, # Input parameters (or context refs)
        "output": "context_key",      # Where to store the result
    }
]
```

**How it works:**
1. Define a plan as a list of steps
2. Each step references a tool from the registry
3. Input parameters can reference outputs from previous steps
4. Results are stored in a shared context dictionary
5. Steps execute sequentially with full error handling

---

## Component Summary

| Component | Location | Language | Purpose |
|-----------|----------|----------|---------|
| Book Models | `Book/book/models.py` | Python | Node, Book, Formula data structures |
| Hierarchy Builder | `Book/book/hierarchy.py` | Python | Build trees from flat data |
| Properties | `Book/book/properties.py` | Python | Add/propagate properties |
| Formulas | `Book/book/formula_engine.py` | Python | Execute node formulas |
| Graph DB | `Book/book/graph_db.py` | Python | NetworkX + TinyDB persistence |
| Vector DB | `Book/book/vector_db.py` | Python | ChromaDB semantic search |
| AI Agent | `Book/book/ai_agent.py` | Python | Skill recommendation |
| Linked Book | `Book/book/linked_book.py` | Python | Lightweight branching |
| Management | `Book/book/management.py` | Python | Copy, load, sync |
| Connectors | `Book/book/connectors/` | Python | CSV, JSON import |
| Librarian Bridge | `src/agents/unified_agent/bridges/` | Python | Book ↔ Librarian |
| Researcher Bridge | `src/agents/unified_agent/bridges/` | Python | Analytics |
| Unified Context | `src/agents/unified_agent/context.py` | Python | State management |
| Unified MCP Tools | `src/agents/unified_agent/mcp_tools.py` | Python | 10 MCP tools |

---

## Dependencies

```
# Core
pydantic>=2.0
tinydb>=4.8

# Graph
networkx>=3.0

# Vector/ML (optional - required for AIAgent and VectorDB)
sentence-transformers
chromadb
numpy

# MCP Server
fastmcp

# Data Processing
pandas (main DataBridge)
```

---

## Running Tests

```bash
# Book library tests (14 tests)
cd C:\Users\telha\Databridge_AI
python -m pytest Book/tests/test_book.py -v

# Main DataBridge tests (169 tests)
python -m pytest tests/ -q
```

---

## Validation Results

| Use Case | Script | Status | Notes |
|----------|--------|--------|-------|
| UC21 | Vector-Powered Agent | PASS | Loads embeddings, finds skills |
| UC22 | Persistent Graph DB | PASS | Full round-trip Book ↔ TinyDB |
| UC23 | Promote to Librarian | PASS | Creates project with 6 nodes |
| UC24 | Checkout from Librarian | PASS | Reconstructs hierarchy tree |
| UC25 | NL-to-SQL | PASS | Generates correct SQL |
| UC26 | MCP Server | PASS | Creates book, adds formula, exports JSON |
| UC27 | Meta-Agent Orchestrator | PASS | Executes multi-step plan |
| Book Tests | 14 unit tests | PASS | Models, hierarchy, properties, graph, vector, formulas |
| DataBridge Tests | 169 tests | PASS | Full regression suite |
