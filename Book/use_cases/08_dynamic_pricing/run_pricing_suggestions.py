from book import (
    Book,
    Node,
    from_list,
    get_logger,
    add_property,
    AIAgent,
    AIAgentConfig,
)
import csv
import os

logger = get_logger(__name__)

def load_csv(file_path: str) -> list:
    """Loads data from a CSV file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def main():
    """
    This script demonstrates dynamic pricing suggestions using the Book library and AI Agent.
    """
    logger.info("Starting dynamic pricing suggestions use case...")

    # 1. Load product catalog into a Book
    logger.info("Loading product catalog from product_catalog.csv into a Book...")
    product_data_raw = load_csv("product_catalog.csv")
    product_nodes = from_list(product_data_raw, parent_col=None, child_col="product_id", name_col="product_name")
    product_book = Book(name="Product Pricing", root_nodes=product_nodes)

    # 2. Configure and initialize the AI Agent
    logger.info("Configuring and initializing the AI Agent with 'pricing-analyst' skill...")
    # Point the databridge_project_path to the parent directory of the `Book` project
    # where the `skills` directory is located.
    agent_config = AIAgentConfig(skills_to_use=["pricing-analyst"])
    agent = AIAgent(databridge_project_path="..", config=agent_config)

    # 3. Get pricing suggestions for each product
    logger.info("Getting pricing suggestions for each product...")
    print("\n--- Pricing Suggestions ---")
    for node in product_book.root_nodes:
        # Simulate passing relevant node data to the agent for context
        # In a real scenario, the agent would use more sophisticated analysis
        node_context = {
            "product_name": node.name,
            "category": node.properties.get("category"),
            "base_price": node.properties.get("base_price"),
            "market_demand": node.properties.get("market_demand"),
        }
        
        # This is a basic simulation of agent's suggestion logic
        suggestions = []
        if "pricing-analyst" in agent.skills and "rules" in agent.skills["pricing-analyst"]:
            for rule in agent.skills["pricing-analyst"]["rules"]:
                condition = rule["condition"]
                suggestion = rule["suggestion"]
                
                # Simple evaluation of condition (for demonstration purposes)
                # In a real agent, this would be a more robust inference engine
                if "market_demand == 'High'" in condition and node_context["market_demand"] == "High":
                    suggestions.append(suggestion)
                elif "market_demand == 'Medium'" in condition and node_context["market_demand"] == "Medium":
                    suggestions.append(suggestion)
                elif "market_demand == 'Low'" in condition and node_context["market_demand"] == "Low":
                    suggestions.append(suggestion)

        print(f"Product: {node.name} (Category: {node_context['category']}, Demand: {node_context['market_demand']})")
        print(f"  Current Price: ${float(node_context['base_price']):.2f}")
        if suggestions:
            for s in suggestions:
                print(f"  Suggestion: {s}")
        else:
            print("  No specific suggestions from AI Agent.")
        print("-" * 40)

    logger.info("Dynamic pricing suggestions use case completed.")

if __name__ == "__main__":
    main()
