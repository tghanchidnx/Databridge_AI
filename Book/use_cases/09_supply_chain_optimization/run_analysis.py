from book import (
    Book,
    Node,
    get_logger,
    add_property,
    from_list
)
from setup_librarian import create_supply_chain_hierarchy
import csv

logger = get_logger(__name__)

def load_csv(file_path: str) -> list:
    """Loads data from a CSV file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def analyze_supply_chain(nodes: list, book: Book):
    """
    Recursively analyzes the supply chain for bottlenecks and calculates costs.
    """
    for node in nodes:
        node_type = node.properties.get("type")
        capacity_str = node.properties.get("capacity")
        cost_str = node.properties.get("cost_per_unit")
        lead_time_str = node.properties.get("lead_time_days")

        # Bottleneck detection
        if node_type in ["Supplier", "Factory"] and capacity_str:
            capacity = int(capacity_str)
            if capacity < 15000: # Arbitrary threshold for bottleneck
                add_property(node, "is_bottleneck", True)
                add_property(node, "bottleneck_reason", f"Low capacity: {capacity}")

        # Cost calculation (simple accumulation)
        current_cost = float(node.properties.get("accumulated_cost", 0.0))
        if cost_str:
            add_property(node, "accumulated_cost", current_cost + float(cost_str))
        else:
            add_property(node, "accumulated_cost", current_cost) # Propagate cost to parent
        
        # Propagate total cost to children (for flow-through cost)
        for child in node.children:
            add_property(child, "accumulated_cost", node.properties.get("accumulated_cost", 0.0))
            
        analyze_supply_chain(node.children, book)


def main():
    """
    This script performs a supply chain optimization analysis using a hierarchy
    from the Librarian and the Book library.
    """
    logger.info("Starting supply chain optimization use case...")

    # 1. Get the master supply chain hierarchy from the "Librarian"
    logger.info("Loading master supply chain hierarchy from Librarian (simulated)...")
    supply_chain_book = create_supply_chain_hierarchy()

    # 2. Perform analysis on the hierarchy (simulating Researcher)
    logger.info("Performing bottleneck and cost analysis (simulating Researcher)...")
    analyze_supply_chain(supply_chain_book.root_nodes, supply_chain_book)

    # 3. Print the analysis report
    logger.info("\n--- Supply Chain Analysis Report ---")
    
    def print_analysis(nodes, indent=""):
        for node in nodes:
            bottleneck_status = "BOTTLENECK!" if node.properties.get("is_bottleneck") else ""
            accumulated_cost = node.properties.get("accumulated_cost", 0.0)
            print(f"{indent}{node.name} (Type: {node.properties.get('type')}): {bottleneck_status} Accumulated Cost: ${accumulated_cost:.2f}")
            if bottleneck_status:
                print(f"{indent}  Reason: {node.properties.get('bottleneck_reason')}")
            print_analysis(node.children, indent + "  ")

    print_analysis(supply_chain_book.root_nodes)

    logger.info("\nSupply chain optimization use case completed.")

if __name__ == "__main__":
    main()
