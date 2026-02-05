"""
Tutorial 4: Meta-Agent Orchestration & MCP Tools
==================================================
Demonstrates: MCP tool simulation, Meta-Agent workflow orchestration,
              multi-step plans across Book, Librarian, and Researcher.

Usage:
    cd C:\\Users\\telha\\Databridge_AI
    set PYTHONPATH=./Book
    python Book/use_cases/tutorial_4_meta_agent_mcp/run_tutorial.py
"""

import csv
import os
from book import Book, Node, Formula, from_list, add_property, execute_formulas, get_logger

logger = get_logger(__name__)


# ============================================================
# Part A: Simulated MCP Tools
# ============================================================

book_store = {}


def mcp_create_book_from_csv(file_path, book_name):
    """MCP Tool: Create a Book from a CSV file."""
    with open(file_path, "r", encoding="utf-8") as f:
        data = list(csv.DictReader(f))

    root_nodes = from_list(data, parent_col="parent_id", child_col="id", name_col="name")
    book = Book(name=book_name, root_nodes=root_nodes)

    handle = f"book-{len(book_store)}"
    book_store[handle] = book
    return f"Successfully created book. Handle: {handle}"


def mcp_add_formula(handle, node_name, formula_name, expression, operands):
    """MCP Tool: Add a formula to a node."""
    if handle not in book_store:
        return "Error: Book not found."

    book = book_store[handle]

    def find_node(nodes, name):
        for n in nodes:
            if n.name == name:
                return n
            found = find_node(n.children, name)
            if found:
                return found
        return None

    node = find_node(book.root_nodes, node_name)
    if not node:
        return f"Error: Node '{node_name}' not found."

    formula = Formula(name=formula_name, expression=expression, operands=operands)
    node.formulas.append(formula)
    for op in operands:
        add_property(node, op, 0)

    return "Successfully added formula."


def mcp_get_book_json(handle):
    """MCP Tool: Export a Book as JSON."""
    if handle not in book_store:
        return "Error: Book not found."
    return book_store[handle].model_dump_json(indent=2)


# ============================================================
# Part B: Meta-Agent Orchestrator
# ============================================================

def librarian_get_hierarchy(name):
    """Simulated Librarian: fetch a master hierarchy template."""
    return Book(name=name, root_nodes=[
        Node(name="Total Revenue"),
        Node(name="Total Expenses"),
        Node(name="Net Income"),
    ])


def create_book(name, revenue, expenses):
    """Create a subsidiary Book with financials."""
    net = revenue - expenses
    return Book(name=name, root_nodes=[
        Node(name="Total Revenue", properties={"amount": revenue}),
        Node(name="Total Expenses", properties={"amount": expenses}),
        Node(name="Net Income", properties={"amount": net}),
    ])


def consolidate(books, template):
    """Consolidate multiple subsidiary Books into one."""
    total_rev = sum(
        n.properties.get("amount", 0)
        for b in books
        for n in b.root_nodes
        if n.name == "Total Revenue"
    )
    total_exp = sum(
        n.properties.get("amount", 0)
        for b in books
        for n in b.root_nodes
        if n.name == "Total Expenses"
    )
    net = total_rev - total_exp

    return Book(name="Consolidated Report", root_nodes=[
        Node(name="Total Revenue", properties={"amount": total_rev}),
        Node(name="Total Expenses", properties={"amount": total_exp}),
        Node(name="Net Income", properties={"amount": net}),
    ])


def researcher_validate(book):
    """Simulated Researcher: validate sources."""
    issues = []
    for node in book.root_nodes:
        amt = node.properties.get("amount", 0)
        if amt < 0 and node.name != "Net Income":
            issues.append(f"{node.name} has negative amount: {amt}")

    return {
        "status": "PASS" if not issues else "FAIL",
        "nodes_checked": len(book.root_nodes),
        "issues": issues,
    }


class MetaAgent:
    """Orchestrates multi-step workflows across systems."""

    def __init__(self):
        self.context = {}
        self.tool_registry = {
            "librarian_get_hierarchy": librarian_get_hierarchy,
            "create_book": create_book,
            "consolidate": consolidate,
            "researcher_validate": researcher_validate,
        }

    def execute_step(self, step):
        tool_name = step["tool"]
        if tool_name not in self.tool_registry:
            print(f"  ERROR: Tool '{tool_name}' not found")
            return False

        # Resolve inputs from context or use directly
        raw_inputs = step.get("inputs", {})
        resolved = {}
        for key, val in raw_inputs.items():
            if isinstance(val, str) and val in self.context:
                resolved[key] = self.context[val]
            elif isinstance(val, list):
                resolved[key] = [self.context.get(v, v) for v in val]
            else:
                resolved[key] = val

        result = self.tool_registry[tool_name](**resolved)
        self.context[step["output"]] = result
        return True

    def execute_plan(self, plan):
        for step in plan:
            name = step["name"]
            print(f"  Executing: {name}...", end=" ")
            ok = self.execute_step(step)
            print("OK" if ok else "FAILED")


def main():
    print("=== Tutorial 4: Meta-Agent Orchestration & MCP Tools ===\n")

    # ================================================================
    # Part A: MCP Tool Simulation
    # ================================================================
    print("=== Part A: MCP Tool Simulation ===\n")

    # Step 1: Create CSV and Book
    print("--- Step 1: Create Book from CSV ---")
    csv_path = "tutorial_mcp_data.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "parent_id", "amount"])
        writer.writerow(["1", "Total Revenue", "", ""])
        writer.writerow(["2", "Product Sales", "1", "500000"])
        writer.writerow(["3", "Service Sales", "1", "200000"])
        writer.writerow(["4", "Total Expenses", "", ""])
        writer.writerow(["5", "COGS", "4", "300000"])
        writer.writerow(["6", "OpEx", "4", "150000"])

    result = mcp_create_book_from_csv(csv_path, "Financial Report")
    print(f"MCP Tool Result: {result}")
    handle = result.split(": ")[1]
    book = book_store[handle]
    root_names = [n.name for n in book.root_nodes]
    print(f"Book has {len(book.root_nodes)} root nodes: {', '.join(root_names)}")
    print()

    # Step 2: Add formula
    print("--- Step 2: Add Formula ---")
    result = mcp_add_formula(handle, "Total Revenue", "total", "a + b", ["a", "b"])
    print(f"MCP Tool Result: {result}")
    print(f"Formula 'total' added to 'Total Revenue'")
    print()

    # Step 3: Get JSON
    print("--- Step 3: Get Book JSON ---")
    json_out = mcp_get_book_json(handle)
    print(f"Book JSON exported ({len(json_out)} bytes)")
    print()

    # Clean up CSV
    os.remove(csv_path)

    # ================================================================
    # Part B: Meta-Agent Orchestration
    # ================================================================
    print("=== Part B: Meta-Agent Orchestration ===\n")

    # Step 4: Define workflow plan
    print("--- Step 4: Define Workflow Plan ---")
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
            "inputs": {"name": "Subsidiary A", "revenue": 800000, "expenses": 500000},
            "output": "sub_a",
        },
        {
            "name": "Create Subsidiary B Book",
            "tool": "create_book",
            "inputs": {"name": "Subsidiary B", "revenue": 700000, "expenses": 400000},
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

    print(f"Plan has {len(plan)} steps:")
    for i, step in enumerate(plan, 1):
        print(f"  {i}. {step['name']} ({step['tool']})")
    print()

    # Step 5: Execute
    print("--- Step 5: Execute Plan ---")
    agent = MetaAgent()
    agent.execute_plan(plan)
    print()

    # Step 6: Results
    print("--- Step 6: Results ---")
    consolidated = agent.context["consolidated"]
    validation = agent.context["validation_report"]

    print("Consolidated Report:")
    for node in consolidated.root_nodes:
        amt = node.properties.get("amount", 0)
        print(f"  {node.name}: ${amt:,.0f}")

    print(f"\nValidation: {validation['status']}")
    print(f"  Nodes checked: {validation['nodes_checked']}")
    if validation["issues"]:
        for issue in validation["issues"]:
            print(f"  Issue: {issue}")
    else:
        print("  All sources verified - no issues found")
    print()

    # Assertions
    rev = consolidated.root_nodes[0].properties["amount"]
    exp = consolidated.root_nodes[1].properties["amount"]
    net = consolidated.root_nodes[2].properties["amount"]
    assert rev == 1500000, f"Expected 1500000, got {rev}"
    assert exp == 900000, f"Expected 900000, got {exp}"
    assert net == 600000, f"Expected 600000, got {net}"
    assert validation["status"] == "PASS"

    print("All steps completed successfully!")


if __name__ == "__main__":
    main()
