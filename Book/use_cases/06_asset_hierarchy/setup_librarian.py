from book import Book, Node

def create_asset_hierarchy():
    """
    This function simulates the creation of an asset hierarchy
    that would be stored in the Librarian.
    """
    building1 = Node(name="Building 1", children=[
        Node(name="Floor 1", properties={"location": "Ground", "status": "Active", "value": 1000000}, children=[
            Node(name="Office 101", properties={"location": "1st Floor", "status": "Active", "value": 50000}, children=[
                Node(name="Desk 1", properties={"location": "1st Floor", "status": "Active", "value": 500}),
                Node(name="Chair 1", properties={"location": "1st Floor", "status": "Active", "value": 100}),
            ])
        ]),
        Node(name="Floor 2", properties={"location": "2nd Floor", "status": "Active", "value": 800000}, children=[
            Node(name="Office 201", properties={"location": "2nd Floor", "status": "Active", "value": 40000}, children=[
                Node(name="Server Rack 1", properties={"location": "2nd Floor", "status": "Active", "value": 10000})
            ])
        ])
    ])

    asset_hierarchy_book = Book(name="Master Asset Hierarchy", root_nodes=[
        building1
    ])
    
    return asset_hierarchy_book

if __name__ == "__main__":
    hierarchy = create_asset_hierarchy()
    print("Simulated creation of asset hierarchy:")
    def print_hierarchy(nodes, indent=""):
        for node in nodes:
            print(f"{indent}{node.name}")
            print_hierarchy(node.children, indent + "  ")
    print_hierarchy(hierarchy.root_nodes)
