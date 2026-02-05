from book import Book, Node

def create_master_coa():
    """
    Simulates the creation of a master Chart of Accounts hierarchy.
    """
    coa_book = Book(name="Master Chart of Accounts", root_nodes=[
        Node(name="Assets", children=[
            Node(name="Current Assets", children=[
                Node(name="Cash"),
                Node(name="Accounts Receivable"),
            ]),
            Node(name="Fixed Assets"),
        ]),
        Node(name="Liabilities"),
        Node(name="Equity"),
    ])
    return coa_book

if __name__ == "__main__":
    hierarchy = create_master_coa()
    print("Simulated creation of Master CoA hierarchy:")
    def print_hierarchy(nodes, indent=""):
        for node in nodes:
            print(f"{indent}{node.name}")
            print_hierarchy(node.children, indent + "  ")
    print_hierarchy(hierarchy.root_nodes)
