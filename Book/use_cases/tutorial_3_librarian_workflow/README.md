# Tutorial 3: Librarian Promote & Checkout Workflow

## What You Will Learn

- Create a financial hierarchy in the Book library
- **Promote** the Book to a Librarian project (simulated with TinyDB)
- Verify the promotion by querying the Librarian database
- **Checkout** the Librarian project back into a Book
- Modify the checked-out Book
- Re-promote the modified Book (update cycle)
- Understand the full round-trip: Book → Librarian → Book → Librarian

## Prerequisites

```bash
cd C:\Users\telha\Databridge_AI
pip install tinydb
```

## Step-by-Step Walkthrough

### Run the Tutorial

```bash
cd C:\Users\telha\Databridge_AI
set PYTHONPATH=./Book
python Book/use_cases/tutorial_3_librarian_workflow/run_tutorial.py
```

### What the Script Does

#### Step 1: Create a Financial Hierarchy Book

```python
book = Book(
    name="FY2025 P&L Hierarchy",
    metadata={"description": "Profit & Loss hierarchy for fiscal year 2025"},
    root_nodes=[
        Node(name="Revenue", properties={"target": 10000000}, children=[
            Node(name="Product Revenue", properties={"target": 7000000}),
            Node(name="Service Revenue", properties={"target": 3000000}),
        ]),
        Node(name="Cost of Goods Sold", properties={"target": 4000000}, children=[
            Node(name="Materials", properties={"target": 2500000}),
            Node(name="Labor", properties={"target": 1500000}),
        ]),
        Node(name="Operating Expenses", properties={"target": 3000000}, children=[
            Node(name="R&D", properties={"target": 1500000}),
            Node(name="SG&A", properties={"target": 1000000}),
            Node(name="Depreciation", properties={"target": 500000}),
        ]),
    ]
)
```

**Tree:**
```
FY2025 P&L Hierarchy
├── Revenue (target: 10M)
│   ├── Product Revenue (7M)
│   └── Service Revenue (3M)
├── Cost of Goods Sold (target: 4M)
│   ├── Materials (2.5M)
│   └── Labor (1.5M)
└── Operating Expenses (target: 3M)
    ├── R&D (1.5M)
    ├── SG&A (1M)
    └── Depreciation (500K)
```

#### Step 2: Promote to Librarian

The promote function inserts the Book into a TinyDB database:
- Creates a project record with name and description
- Recursively inserts all nodes with parent-child relationships

```python
project_id = promote_book_to_librarian(book, db_path)
```

**Result:** A new project UUID is created. All 10 nodes are stored in the `hierarchies` table.

#### Step 3: Verify Promotion

Query the database to confirm:
```python
db = TinyDB(db_path)
project = projects_table.get(Query().name == "FY2025 P&L Hierarchy")
nodes = hierarchies_table.search(Query().project_id == project_id)
```

**Result:** Project found, 9 hierarchy nodes confirmed.

#### Step 4: Checkout from Librarian

Reverse the process - fetch the project and reconstruct a Book:

```python
checked_out = checkout_librarian_hierarchy(project_id, db_path)
```

**Result:** A Book with the full hierarchy is returned.

#### Step 5: Modify the Checked-Out Book

Add a new expense category:

```python
opex = checked_out.root_nodes[2]  # Operating Expenses
opex.children.append(Node(name="Marketing", properties={"target": 800000}))
```

#### Step 6: Re-Promote with Modifications

Promote the modified Book back, creating a new version:

```python
new_project_id = promote_book_to_librarian(checked_out, db_path)
```

**Result:** New project created with 10 nodes (the original 9 + Marketing).

### Expected Output

```
=== Tutorial 3: Librarian Promote & Checkout Workflow ===

--- Step 1: Create Financial Hierarchy ---
Book: FY2025 P&L Hierarchy (9 nodes)
  Revenue (target: 10000000)
    Product Revenue (target: 7000000)
    Service Revenue (target: 3000000)
  Cost of Goods Sold (target: 4000000)
    Materials (target: 2500000)
    Labor (target: 1500000)
  Operating Expenses (target: 3000000)
    R&D (target: 1500000)
    SG&A (target: 1000000)
    Depreciation (target: 500000)

--- Step 2: Promote to Librarian ---
Promoted to project: abc123-...
Nodes in Librarian DB: 9

--- Step 3: Verify Promotion ---
Project found: FY2025 P&L Hierarchy
All 10 nodes present in database.

--- Step 4: Checkout from Librarian ---
Checked out: FY2025 P&L Hierarchy (9 nodes)
  Revenue
    Product Revenue
    Service Revenue
  ...

--- Step 5: Modify Checked-Out Book ---
Added 'Marketing' under Operating Expenses
Node count: 10

--- Step 6: Re-Promote Modified Book ---
Re-promoted to new project: def456-...
Nodes in updated project: 10

Full round-trip completed successfully!
```

## Key Concepts

| Concept | What It Does |
|---------|-------------|
| **Promote** | Push a Book's hierarchy into the Librarian (central project store) |
| **Checkout** | Pull a Librarian project into a Book for in-memory manipulation |
| **Round-trip** | Promote → Checkout → Modify → Re-promote without data loss |
| **TinyDB simulation** | Simulates the Librarian's NestJS backend using a local JSON database |

## Production vs Simulation

| Feature | This Tutorial (Simulated) | Production |
|---------|--------------------------|------------|
| Database | TinyDB (JSON file) | NestJS + MySQL |
| API | Direct function calls | REST API via LibrarianBridge |
| MCP Tool | N/A | `promote_book_to_librarian` / `checkout_librarian_to_book` |
| Sync | Manual re-promote | `sync_book_and_librarian` with conflict resolution |

## Next Tutorial

Continue to [Tutorial 4: Meta-Agent Orchestration & MCP](../tutorial_4_meta_agent_mcp/README.md)
