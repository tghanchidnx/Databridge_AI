from book import Book, Node

def create_consolidation_hierarchy():
    """
    Simulates the creation of a financial consolidation hierarchy.
    """
    consolidation_book = Book(name="Consolidation Hierarchy", root_nodes=[
        Node(name="Consolidated Revenue"),
        Node(name="Consolidated COGS"),
        Node(name="Consolidated Gross Profit"),
        Node(name="Consolidated Operating Expenses"),
        Node(name="Consolidated Operating Income"),
        Node(name="Intercompany Eliminations", children=[
            Node(name="Intercompany Revenue Elimination"),
            Node(name="Intercompany COGS Elimination"),
        ]),
    ])
    return consolidation_book

if __name__ == "__main__":
    hierarchy = create_consolidation_hierarchy()
    print("Simulated creation of Consolidation Hierarchy:")
    def print_hierarchy(nodes, indent=""):
        for node in nodes:
            print(f"{indent}{node.name}")
            print_hierarchy(node.children, indent + "  ")
    print_hierarchy(hierarchy.root_nodes)
