from book import (
    Book,
    from_list,
    AIAgent,
    AIAgentConfig,
    get_logger
)
import os

# Get a logger for this module
logger = get_logger(__name__)

def main():
    """
    This script demonstrates how to use the AIAgent to suggest mappings for a new
    chart of accounts.
    """
    logger.info("Starting AI-powered CoA mapping use case...")

    # 1. Ingest the new CoA data
    logger.info("Ingesting data from new_coa.csv...")
    data = []
    with open("new_coa.csv", "r") as f:
        import csv
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)

    # 2. Build the hierarchy for the new CoA
    logger.info("Building the hierarchy for the new CoA...")
    root_nodes = from_list(data, parent_col="parent_account", child_col="account_id", name_col="account_name")
    new_coa_book = Book(name="New CoA", root_nodes=root_nodes)

    # 3. Configure and run the AI Agent
    logger.info("Configuring and running the AI Agent...")
    
    # Configure the agent to use the 'financial-analyst' skill
    # We point the databridge_project_path to the parent directory of the `Book` project
    # to simulate the project structure.
    agent_config = AIAgentConfig(skills_to_use=["financial-analyst"])
    agent = AIAgent(databridge_project_path="..", config=agent_config)

    # 4. Get suggestions
    logger.info("Getting suggestions from the AI Agent...")
    suggestions = agent.suggest_enhancements(new_coa_book)

    # 5. Print the suggestions
    logger.info("AI Agent Suggestions:")
    for suggestion in suggestions:
        print(f"- {suggestion}")

if __name__ == "__main__":
    main()
