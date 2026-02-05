from book import get_logger
import re

logger = get_logger(__name__)

def main():
    """
    This script simulates a Natural Language to SQL (NL-to-SQL) engine
    enhanced with schema embeddings.
    """
    logger.info("Starting NL-to-SQL with schema embeddings use case...")

    # 1. Define a simple database schema
    db_schema = {
        "tables": {
            "customers": {
                "columns": ["customer_id", "customer_name", "region"],
                "description": "Information about customers."
            },
            "orders": {
                "columns": ["order_id", "customer_id", "order_date", "total_amount"],
                "description": "Customer orders."
            },
        }
    }

    # 2. Simulate schema embeddings (simple keyword matching)
    # In a real implementation, you would use a sentence transformer to create
    # vector embeddings of table and column descriptions.
    schema_embeddings = {
        "customers": "customers customer information region",
        "orders": "orders customer total amount date",
    }

    # 3. Take a natural language query
    nl_query = "show me the total order amount for each customer in the 'North' region"
    logger.info(f"Natural language query: '{nl_query}'")

    # 4. Find relevant tables and columns (simulated Researcher)
    logger.info("Finding relevant tables and columns using schema embeddings...")
    
    query_tokens = set(nl_query.lower().split())
    
    # Simple keyword matching to find relevant tables
    relevant_tables = []
    for table, keywords in schema_embeddings.items():
        if any(token in keywords for token in query_tokens):
            relevant_tables.append(table)
    
    logger.info(f"Relevant tables found: {relevant_tables}")

    # 5. Construct the SQL query
    logger.info("Constructing SQL query...")
    
    # This is a very simplified query builder for demonstration purposes
    sql_query = ""
    if "customers" in relevant_tables and "orders" in relevant_tables:
        sql_query = (
            "SELECT\n"
            "    c.customer_name,\n"
            "    SUM(o.total_amount) AS total_order_amount\n"
            "FROM customers c\n"
            "JOIN orders o ON c.customer_id = o.customer_id\n"
            "WHERE c.region = 'North'\n"
            "GROUP BY c.customer_name;"
        )

    # 6. Print the generated SQL
    logger.info("\n--- Generated SQL Query ---")
    if sql_query:
        print(sql_query)
    else:
        logger.warning("Could not generate SQL query from the natural language input.")

    logger.info("\nNL-to-SQL with schema embeddings use case completed.")

if __name__ == "__main__":
    main()
