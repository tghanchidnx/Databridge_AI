# Tutorial 1: Book Fundamentals - Build, Enrich, Calculate

## What You Will Learn

- Create a Book with hierarchical nodes from scratch
- Add properties to nodes and propagate them through the tree
- Attach formulas to nodes and execute calculations
- Use LinkedBook for lightweight branching
- Export the Book to JSON

## Prerequisites

```bash
cd C:\Users\telha\Databridge_AI
pip install tinydb networkx
```

## Step-by-Step Walkthrough

### Step 1: Run the Tutorial Script

```bash
cd C:\Users\telha\Databridge_AI
set PYTHONPATH=./Book
python Book/use_cases/tutorial_1_book_fundamentals/run_tutorial.py
```

### What the Script Does (Step by Step)

#### Step 1: Create a Book with Nodes

We create a financial hierarchy representing a simple P&L statement:

```python
book = Book(
    name="Q4 Budget",
    root_nodes=[
        Node(name="Revenue", children=[
            Node(name="Product Sales", properties={"amount": 500000}),
            Node(name="Service Revenue", properties={"amount": 200000}),
        ]),
        Node(name="Expenses", children=[
            Node(name="COGS", properties={"amount": 300000}),
            Node(name="Operating Expenses", properties={"amount": 150000}),
        ]),
    ]
)
```

**Result:** A tree structure:
```
Q4 Budget
├── Revenue
│   ├── Product Sales (amount: 500,000)
│   └── Service Revenue (amount: 200,000)
└── Expenses
    ├── COGS (amount: 300,000)
    └── Operating Expenses (amount: 150,000)
```

#### Step 2: Add Properties

We add a `department` property to Revenue and propagate it to all children:

```python
add_property(revenue_node, "department", "Sales")
propagate_to_children(revenue_node, "department", "Sales")
```

**Result:** Both "Product Sales" and "Service Revenue" now have `department: Sales`.

#### Step 3: Attach and Execute Formulas

We add a formula to calculate total revenue:

```python
formula = Formula(
    name="total_revenue",
    expression="product_sales + service_revenue",
    operands=["product_sales", "service_revenue"]
)
revenue_node.formulas.append(formula)
add_property(revenue_node, "product_sales", 500000)
add_property(revenue_node, "service_revenue", 200000)
execute_formulas(revenue_node, book)
```

**Result:** `revenue_node.properties["total_revenue"]` = 700,000

#### Step 4: Create a LinkedBook Branch

LinkedBook creates a lightweight copy with only the changes tracked:

```python
linked = LinkedBook(base_book=book)
linked.add_change(product_node.id, "amount", 600000)  # Increase sales

# Check the overridden value
assert linked.get_property(product_node.id, "amount") == 600000

# Materialize into a full Book
scenario_book = linked.to_book("Optimistic Scenario")
```

**Result:** Original book is unchanged. The linked book has Product Sales at 600,000.

#### Step 5: Export to JSON

```python
json_output = book.model_dump_json(indent=2)
```

**Result:** Full Book exported as JSON with all nodes, properties, and formulas.

### Expected Output

```
=== Tutorial 1: Book Fundamentals ===

--- Step 1: Create Book ---
Book: Q4 Budget
  Revenue
    Product Sales (amount: 500000)
    Service Revenue (amount: 200000)
  Expenses
    COGS (amount: 300000)
    Operating Expenses (amount: 150000)

--- Step 2: Properties ---
Revenue department: Sales
Product Sales department: Sales
Service Revenue department: Sales

--- Step 3: Formulas ---
Total Revenue calculated: 700000

--- Step 4: LinkedBook ---
Original Product Sales: 500000
LinkedBook Product Sales: 600000
Materialized scenario book: Optimistic Scenario

--- Step 5: JSON Export ---
Book exported to JSON (X bytes)

All steps completed successfully!
```

## Key Concepts

| Concept | What It Does |
|---------|-------------|
| **Node** | A single element in a hierarchy with name, properties, children, formulas |
| **Book** | A container holding root nodes with metadata and global properties |
| **Property** | A key-value pair on a node (e.g., `amount: 500000`) |
| **Propagation** | Automatically copying a property to all children or parents |
| **Formula** | A calculation expression attached to a node (uses node properties) |
| **LinkedBook** | A lightweight branch tracking only changes (deltas) from a base Book |

## Next Tutorial

Continue to [Tutorial 2: Persistent Storage & Graph Operations](../tutorial_2_persistence_graph/README.md)
