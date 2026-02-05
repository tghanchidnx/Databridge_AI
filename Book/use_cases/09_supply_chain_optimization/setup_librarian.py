from book import Book, Node, from_list
import csv

def load_csv(file_path: str) -> list:
    """Loads data from a CSV file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def create_supply_chain_hierarchy():
    """
    This function simulates the creation of a supply chain hierarchy
    that would be stored in the Librarian.
    """
    supply_chain_data = load_csv("supply_chain_data.csv")
    root_nodes = from_list(supply_chain_data, parent_col="parent_node_id", child_col="node_id", name_col="node_name")
    
    supply_chain_book = Book(name="Master Supply Chain", root_nodes=root_nodes)
    
    return supply_chain_book

if __name__ == "__main__":
    hierarchy = create_supply_chain_hierarchy()
    print("Simulated creation of supply chain hierarchy:")
    def print_hierarchy(nodes, indent=""):
        for node in nodes:
            print(f"{indent}{node.name}")
            print_hierarchy(node.children, indent + "  ")
    print_hierarchy(hierarchy.root_nodes)
