"""
Tutorial 1: Book Fundamentals - Build, Enrich, Calculate
=========================================================
Demonstrates: Node creation, properties, formulas, LinkedBook, JSON export.

Usage:
    cd C:\\Users\\telha\\Databridge_AI
    set PYTHONPATH=./Book
    python Book/use_cases/tutorial_1_book_fundamentals/run_tutorial.py
"""

from book import (
    Book, Node, Formula,
    add_property, propagate_to_children, get_property,
    execute_formulas, LinkedBook,
)

def print_tree(nodes, indent=""):
    for node in nodes:
        amount = node.properties.get("amount", "")
        extra = f" (amount: {amount})" if amount else ""
        print(f"{indent}{node.name}{extra}")
        print_tree(node.children, indent + "  ")


def main():
    print("=== Tutorial 1: Book Fundamentals ===\n")

    # --- Step 1: Create a Book with hierarchical nodes ---
    print("--- Step 1: Create Book ---")
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

    print(f"Book: {book.name}")
    print_tree(book.root_nodes)
    print()

    # Grab references
    revenue_node = book.root_nodes[0]
    product_node = revenue_node.children[0]
    service_node = revenue_node.children[1]

    # --- Step 2: Add and propagate properties ---
    print("--- Step 2: Properties ---")
    add_property(revenue_node, "department", "Sales")
    propagate_to_children(revenue_node, "department", "Sales")

    print(f"Revenue department: {get_property(revenue_node, book, 'department')}")
    print(f"Product Sales department: {get_property(product_node, book, 'department')}")
    print(f"Service Revenue department: {get_property(service_node, book, 'department')}")
    print()

    # --- Step 3: Attach and execute formulas ---
    print("--- Step 3: Formulas ---")
    formula = Formula(
        name="total_revenue",
        expression="product_sales + service_revenue",
        operands=["product_sales", "service_revenue"]
    )
    revenue_node.formulas.append(formula)
    add_property(revenue_node, "product_sales", 500000)
    add_property(revenue_node, "service_revenue", 200000)

    execute_formulas(revenue_node, book)
    total = get_property(revenue_node, book, "total_revenue")
    print(f"Total Revenue calculated: {total}")
    assert total == 700000, f"Expected 700000, got {total}"
    print()

    # --- Step 4: LinkedBook for lightweight branching ---
    print("--- Step 4: LinkedBook ---")
    linked = LinkedBook(base_book=book)
    linked.add_change(product_node.id, "amount", 600000)

    original = get_property(product_node, book, "amount")
    overridden = linked.get_property(product_node.id, "amount")
    print(f"Original Product Sales: {original}")
    print(f"LinkedBook Product Sales: {overridden}")
    assert original == 500000
    assert overridden == 600000

    scenario_book = linked.to_book("Optimistic Scenario")
    print(f"Materialized scenario book: {scenario_book.name}")
    print()

    # --- Step 5: Export to JSON ---
    print("--- Step 5: JSON Export ---")
    json_output = book.model_dump_json(indent=2)
    print(f"Book exported to JSON ({len(json_output)} bytes)")
    print()

    print("All steps completed successfully!")


if __name__ == "__main__":
    main()
