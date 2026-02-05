from book import Book, Node, get_logger
from setup_librarian import create_product_hierarchy
from jinja2 import Template

logger = get_logger(__name__)

DBT_MODEL_TEMPLATE = """
-- This model is auto-generated from a Book hierarchy.
-- It unnests a hierarchical structure into a flat dimension table.

WITH base_hierarchy AS (
    -- This would typically be your source hierarchy data
    SELECT 1 AS id, 'All Products' AS name, NULL AS parent_id
    UNION ALL SELECT 2, 'Electronics', 1
    UNION ALL SELECT 3, 'Software', 1
    UNION ALL SELECT 4, 'Laptops', 2
    UNION ALL SELECT 5, 'Smartphones', 2
    UNION ALL SELECT 6, 'Operating Systems', 3
    UNION ALL SELECT 7, 'Productivity Suites', 3
),

-- Recursive CTE to build the full path for each node
hierarchy_paths AS (
    SELECT
        id,
        name,
        parent_id,
        name AS path
    FROM base_hierarchy
    WHERE parent_id IS NULL

    UNION ALL

    SELECT
        c.id,
        c.name,
        c.parent_id,
        p.path || ' -> ' || c.name
    FROM base_hierarchy c
    JOIN hierarchy_paths p ON c.parent_id = p.id
)

SELECT
    id,
    name,
    parent_id,
    path
FROM hierarchy_paths
"""

def generate_dbt_model_from_book(book: Book) -> str:
    """
    Generates a dbt model SQL file from a Book object.
    
    For simplicity, this example uses a static template. A more advanced
    implementation would dynamically generate the CTEs based on the Book's structure.
    """
    template = Template(DBT_MODEL_TEMPLATE)
    return template.render(book=book)

def main():
    """
    This script demonstrates generating a dbt model from a Book hierarchy.
    """
    logger.info("Starting dbt model generation use case...")

    # 1. Load product hierarchy from the "Librarian"
    logger.info("Loading product hierarchy from Librarian...")
    product_hierarchy = create_product_hierarchy()

    # 2. Generate the dbt model SQL
    logger.info("Generating dbt model SQL...")
    dbt_model_sql = generate_dbt_model_from_book(product_hierarchy)

    # 3. Save the dbt model to a file
    output_path = "dim_products_hierarchy.sql"
    with open(output_path, "w") as f:
        f.write(dbt_model_sql)
    logger.info(f"dbt model saved to {output_path}")

    # 4. Print the generated SQL
    logger.info("\n--- Generated dbt Model SQL ---")
    print(dbt_model_sql)

    logger.info("\ndbt model generation use case completed.")

if __name__ == "__main__":
    main()
