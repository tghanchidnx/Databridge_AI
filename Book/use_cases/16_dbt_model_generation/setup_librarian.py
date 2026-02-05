from book import Book, Node

def create_product_hierarchy():
    """
    Simulates the creation of a product hierarchy.
    """
    product_book = Book(name="Product Hierarchy", root_nodes=[
        Node(name="All Products", children=[
            Node(name="Electronics", children=[
                Node(name="Laptops"),
                Node(name="Smartphones"),
            ]),
            Node(name="Software", children=[
                Node(name="Operating Systems"),
                Node(name="Productivity Suites"),
            ]),
        ]),
    ])
    return product_book

if __name__ == "__main__":
    hierarchy = create_product_hierarchy()
    print("Simulated creation of Product Hierarchy:")
    def print_hierarchy(nodes, indent=""):
        for node in nodes:
            print(f"{indent}{node.name}")
            print_hierarchy(node.children, indent + "  ")
    print_hierarchy(hierarchy.root_nodes)
