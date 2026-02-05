from book import (
    Book,
    Node,
    get_logger,
    AIAgent,
)

logger = get_logger(__name__)

def main():
    """
    This script demonstrates how the AIAgent uses vector similarity search
    to find relevant skills and provide suggestions.
    """
    logger.info("Starting vector-powered AI agent use case...")

    # 1. Create a sample Book (the context for the agent)
    sample_book = Book(name="Sample Book")

    # 2. Initialize the AI Agent
    # The agent will load the skill embeddings from the 'skills' directory
    logger.info("Initializing AI Agent...")
    agent = AIAgent(databridge_project_path="..")

    # 3. Define a user query
    user_query = "How should I adjust my product prices?"

    # 4. Get suggestions from the agent
    logger.info(f"Getting suggestions for query: '{user_query}'")
    suggestions = agent.suggest_enhancements(sample_book, user_query)

    # 5. Print the suggestions
    logger.info("\n--- AI Agent Suggestions ---")
    for suggestion in suggestions:
        print(f"- {suggestion}")

    logger.info("\nVector-powered AI agent use case completed.")

if __name__ == "__main__":
    main()

