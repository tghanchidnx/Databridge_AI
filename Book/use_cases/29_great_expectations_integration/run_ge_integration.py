from book import (
    Book,
    Node,
    get_logger,
    great_expectations_integration,
    AIAgent,
)
import os
import shutil

logger = get_logger(__name__)

def main():
    """
    This script demonstrates the integration between the Book library and
    Great Expectations for data quality validation.
    """
    logger.info("Starting Great Expectations integration use case...")

    # --- Part 1: Setup ---
    logger.info("\n--- Part 1: Setting up the Book with data quality issues ---")
    
    # 1. Create a sample Book with some data quality issues
    data_with_issues = Book(name="Data With Issues", root_nodes=[
        Node(name="Record 1", properties={"id": 1, "product_name": "Laptop", "price": 1200}),
        Node(name="Record 2", properties={"id": 2, "product_name": "Mouse", "price": -25}), # Negative price
        Node(name="Record 3", properties={"id": 3, "product_name": None, "price": 20}), # Null product name
    ])

    # --- Part 2: Generate and Run Expectations ---
    logger.info("\n--- Part 2: Generating and running Great Expectations ---")
    
    # 2. Generate an Expectation Suite
    suite_name = "data_with_issues_suite"
    suite_path = great_expectations_integration.generate_expectations_from_book(data_with_issues, suite_name)

    # 3. Validate the Book with the generated suite
    great_expectations_integration.validate_book_with_expectations(data_with_issues, suite_name)

    # --- Part 3: AI-powered Analysis ---
    logger.info("\n--- Part 3: Analyzing validation results with the AIAgent ---")
    
    # 4. Use the AIAgent to analyze the validation results
    # The databridge_project_path needs to be set to the root of the project.
    # For this use case, we assume the script is run from the `Book` directory.
    agent = AIAgent(databridge_project_path="..")
    suggestions = agent.suggest_enhancements(data_with_issues, "Analyze data quality")

    # 5. Print the suggestions
    for suggestion in suggestions:
        print(suggestion)

    # Clean up
    if os.path.exists("great_expectations"):
        shutil.rmtree("great_expectations")
        logger.info("\nCleaned up great_expectations directory.")

    logger.info("\nGreat Expectations integration use case completed.")

if __name__ == "__main__":
    main()
