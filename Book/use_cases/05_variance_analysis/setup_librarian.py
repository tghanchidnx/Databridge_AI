from book import Book, Node

def create_income_statement_hierarchy():
    """
    This function simulates the creation of an income statement hierarchy
    that would be stored in the Librarian.
    """
    revenue = Node(name="Revenue", children=[
        Node(name="Product Revenue"),
        Node(name="Service Revenue"),
    ])
    cogs = Node(name="Cost of Revenue", children=[
        Node(name="Product COGS"),
        Node(name="Service COGS"),
    ])
    gross_profit = Node(name="Gross Profit")
    operating_expenses = Node(name="Operating Expenses", children=[
        Node(name="Sales & Marketing"),
        Node(name="General & Administrative"),
    ])
    operating_income = Node(name="Operating Income")

    income_statement = Book(name="Standard Income Statement", root_nodes=[
        revenue,
        cogs,
        gross_profit,
        operating_expenses,
        operating_income,
    ])
    
    return income_statement

if __name__ == "__main__":
    # In a real scenario, you would use the Librarian's CLI or MCP tools
    # to create this hierarchy.
    # For example:
    # databridge hierarchy create-project "Standard CoA"
    # databridge template create-project standard_pl "Standard Income Statement"
    
    hierarchy = create_income_statement_hierarchy()
    print("Simulated creation of income statement hierarchy:")
    def print_hierarchy(nodes, indent=""):
        for node in nodes:
            print(f"{indent}{node.name}")
            print_hierarchy(node.children, indent + "  ")
    print_hierarchy(hierarchy.root_nodes)
