# Tutorial 4: Meta-Agent Orchestration & MCP Tools

## What You Will Learn

- How the **Meta-Agent** orchestrates multi-step workflows
- How to define a **workflow plan** with inputs, outputs, and tool references
- How Book operations are exposed as **MCP tools**
- How the orchestrator coordinates across Book, Librarian, and Researcher
- The full end-to-end pipeline: CSV → Book → Formulas → MCP → Orchestration

## Prerequisites

```bash
cd T:\Users\telha\Databridge_AI_Source
pip install tinydb networkx
```

## Step-by-Step Walkthrough

### Run the Tutorial

```bash
cd T:\Users\telha\Databridge_AI_Source
set PYTHONPATH=./Book
python Book/use_cases/tutorial_4_meta_agent_mcp/run_tutorial.py
```

### What the Script Does

#### Part A: MCP Tool Simulation

##### Step 1: Create a Book from CSV (MCP Tool)

The MCP server exposes `create_book_from_csv` as a tool. We simulate calling it:

```python
# Sample CSV data
id,name,parent_id,amount
1,Total Revenue,,
2,Product Sales,1,500000
3,Service Sales,1,200000
4,Total Expenses,,
5,COGS,4,300000
6,OpEx,4,150000
```

```python
result = create_book_from_csv("sample_data.csv", "Financial Report")
# Returns: "Successfully created book. Handle: book-0"
```

##### Step 2: Add Formula (MCP Tool)

Add a formula to calculate net income:

```python
result = add_formula_to_book("book-0", "Total Revenue", "total", "a + b", ["a", "b"])
```

##### Step 3: Get Book as JSON (MCP Tool)

Export the full Book:

```python
json_output = get_book_as_json("book-0")
```

#### Part B: Meta-Agent Orchestration

##### Step 4: Define a Workflow Plan

```python
plan = [
    {
        "name": "Fetch Master Hierarchy",
        "tool": "librarian_get_hierarchy",
        "inputs": {"name": "Consolidation Template"},
        "output": "master_hierarchy",
    },
    {
        "name": "Create Subsidiary A Book",
        "tool": "create_book",
        "inputs": {"name": "Subsidiary A", "data": [...]},
        "output": "sub_a",
    },
    {
        "name": "Create Subsidiary B Book",
        "tool": "create_book",
        "inputs": {"name": "Subsidiary B", "data": [...]},
        "output": "sub_b",
    },
    {
        "name": "Consolidate",
        "tool": "consolidate",
        "inputs": {"books": ["sub_a", "sub_b"], "template": "master_hierarchy"},
        "output": "consolidated",
    },
    {
        "name": "Validate Sources",
        "tool": "researcher_validate",
        "inputs": {"book": "consolidated"},
        "output": "validation_report",
    },
]
```

##### Step 5: Execute the Plan

The Meta-Agent executes each step in order:
1. Resolves input parameters from context (previous step outputs)
2. Calls the registered tool function
3. Stores the result in context for downstream steps
4. Logs success/failure

```python
agent = MetaAgent()
agent.execute_plan(plan)
```

##### Step 6: Inspect Results

```python
report = agent.context["consolidated"]
validation = agent.context["validation_report"]
```

### Expected Output

```
=== Tutorial 4: Meta-Agent Orchestration & MCP Tools ===

=== Part A: MCP Tool Simulation ===

--- Step 1: Create Book from CSV ---
MCP Tool Result: Successfully created book. Handle: book-0
Book has 2 root nodes: Total Revenue, Total Expenses

--- Step 2: Add Formula ---
MCP Tool Result: Successfully added formula.
Formula 'total' added to 'Total Revenue'

--- Step 3: Get Book JSON ---
Book JSON exported (X bytes)

=== Part B: Meta-Agent Orchestration ===

--- Step 4: Define Workflow Plan ---
Plan has 5 steps:
  1. Fetch Master Hierarchy (librarian_get_hierarchy)
  2. Create Subsidiary A Book (create_book)
  3. Create Subsidiary B Book (create_book)
  4. Consolidate (consolidate)
  5. Validate Sources (researcher_validate)

--- Step 5: Execute Plan ---
Executing: Fetch Master Hierarchy... OK
Executing: Create Subsidiary A Book... OK
Executing: Create Subsidiary B Book... OK
Executing: Consolidate... OK
Executing: Validate Sources... OK

--- Step 6: Results ---
Consolidated Report:
  Total Revenue: 1,500,000 (Sub A: 800K + Sub B: 700K)
  Total Expenses: 900,000 (Sub A: 500K + Sub B: 400K)
  Net Income: 600,000
Validation: All sources verified

All steps completed successfully!
```

## Key Concepts

| Concept | What It Does |
|---------|-------------|
| **MCP Tool** | A function exposed over the Model Context Protocol for AI clients |
| **Meta-Agent** | An orchestrator that executes multi-step plans across systems |
| **Tool Registry** | Maps tool names to Python functions |
| **Context** | A shared dictionary storing step outputs for downstream use |
| **Workflow Plan** | A list of steps with tool, inputs, and output definitions |

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────┐
│                     Meta-Agent                           │
│                                                          │
│  Step 1: Librarian    ─→ context["master_hierarchy"]     │
│  Step 2: Book (Sub A) ─→ context["sub_a"]                │
│  Step 3: Book (Sub B) ─→ context["sub_b"]                │
│  Step 4: Consolidate  ─→ context["consolidated"]         │
│  Step 5: Researcher   ─→ context["validation_report"]    │
│                                                          │
│  Tool Registry:                                          │
│  ┌─────────────────────────────────────────────────────┐ │
│  │ librarian_get_hierarchy  → Librarian API            │ │
│  │ create_book              → Book library             │ │
│  │ consolidate              → Researcher analytics     │ │
│  │ researcher_validate      → Researcher validation    │ │
│  └─────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

## Production MCP Tools

In production, the Unified Agent (`src/agents/unified_agent/mcp_tools.py`) registers 10 MCP tools:

| Tool | System Flow |
|------|------------|
| `checkout_librarian_to_book` | Librarian → Book |
| `promote_book_to_librarian` | Book → Librarian |
| `sync_book_and_librarian` | Book ↔ Librarian |
| `diff_book_and_librarian` | Book ↔ Librarian |
| `analyze_book_with_researcher` | Book → Researcher |
| `compare_book_to_database` | Book → Researcher |
| `profile_book_sources` | Book → Researcher |
| `create_unified_workflow` | All systems |
| `execute_unified_workflow` | All systems |
| `get_unified_context` | All systems |

## Summary

Across all 4 tutorials you have learned:

1. **Tutorial 1** - Book fundamentals: nodes, properties, formulas, branching
2. **Tutorial 2** - Persistence: NetworkX graphs, TinyDB, JSON/GML, SyncManager
3. **Tutorial 3** - Librarian integration: promote, checkout, modify, re-promote
4. **Tutorial 4** - Orchestration: MCP tools, Meta-Agent, multi-step workflows

These capabilities form the foundation of the DataBridge AI Trinity architecture,
enabling seamless data hierarchy management across in-memory prototyping (Book),
centralized management (Librarian), and analytics/validation (Researcher).
