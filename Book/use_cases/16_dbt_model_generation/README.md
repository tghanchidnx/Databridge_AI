# Use Case 16: Building a dbt-compatible model from a Book

This use case demonstrates how a hierarchy defined in a `Book` can be used to programmatically generate a dbt (Data Build Tool) model. This is a powerful concept for data engineers who want to automate the creation of data warehouse dimensions based on master data hierarchies.

## Features Highlighted

*   **`Librarian` (Simulated):** Provides the master product hierarchy.
*   **`Book` Library:** Used to represent the hierarchy in-memory.
*   **Templating with Jinja2:** A Jinja2 template is used to generate the dbt model SQL.
*   **Infrastructure as Code:** Shows how the `Book` can be a source for generating data infrastructure (dbt models).

## Components Involved

*   **`Librarian` (Simulated):** Manages the master product hierarchy.
*   **`Book` Library:** Provides the in-memory representation of the hierarchy.

## Files

*   `setup_librarian.py`: A script that *simulates* the creation of the product hierarchy within the `Librarian`.
*   `generate_dbt_model.py`: The Python script that generates the dbt model SQL file.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the dbt Model Generation Script

Navigate to the `Book/use_cases/16_dbt_model_generation` directory and run the `generate_dbt_model.py` script:

```bash
python generate_dbt_model.py
```

### 3. What's Happening?

1.  **Load Hierarchy:** The script loads the product hierarchy from the simulated `Librarian`.
2.  **Generate SQL:** The `generate_dbt_model_from_book` function uses a Jinja2 template to generate the SQL for a dbt model. For simplicity, this example uses a static template with hardcoded values, but a more advanced implementation could dynamically generate the CTEs based on the `Book`'s structure.
3.  **Save SQL File:** The generated SQL is saved to a file named `dim_products_hierarchy.sql`. This file can then be added to a dbt project.
4.  **Print SQL:** The script prints the generated SQL to the console.

### Expected Output

You should see the following output in your terminal, and a new file `dim_products_hierarchy.sql` will be created in the directory.

```
INFO:__main__:Starting dbt model generation use case...
INFO:__main__:Loading product hierarchy from Librarian...
INFO:__main__:Generating dbt model SQL...
INFO:__main__:dbt model saved to dim_products_hierarchy.sql

--- Generated dbt Model SQL ---

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

INFO:__main__:
dbt model generation use case completed.
```

This use case demonstrates how the `Book` library can be a powerful tool for data engineers, enabling a "hierarchy-as-code" approach to building and maintaining data warehouse dimensions.
