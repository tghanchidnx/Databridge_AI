# Use Case 28: Bi-directional dbt Integration

This use case demonstrates the powerful, bi-directional integration between the `Book` library and a dbt project. It showcases a full round-trip workflow: from a `Book` to a dbt project, and from a dbt project's metadata back into a `Book`.

## Features Highlighted

*   **`Databridge AI` -> `dbt`:** Generating a dbt project from a `Book` hierarchy.
*   **`dbt` -> `Databridge AI`:** Creating a `Book` from a dbt `manifest.json` file to visualize and analyze the project's dependency graph.
*   **Infrastructure as Code:** A complete workflow for managing your data models and hierarchies as code.

## Components Involved

*   **`Book` Library:** Used to define the initial hierarchy and to represent the structure of the dbt project.
*   **dbt Integration Module:** The new module in the `Book` library that handles the bi-directional integration.

## Files

*   `run_dbt_integration.py`: The Python script that demonstrates the full round-trip workflow.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies, including `dbt-core` and `dbt-artifacts-parser`, installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Integration Script

Navigate to the `Book/use_cases/28_dbt_integration` directory and run the `run_dbt_integration.py` script:

```bash
python run_dbt_integration.py
```

### 3. What's Happening?

The script runs in two parts:

**Part 1: `Book` -> `dbt`**
1.  **Create a Book:** A sample `Book` representing a product hierarchy is created.
2.  **Generate dbt Project:** The `generate_dbt_project_from_book` function is called. This creates a new directory named after the `Book` ("Product Hierarchy"), which contains a `dbt_project.yml` file and a `models` directory with a `.sql` file for the hierarchy model.

**Part 2: `dbt` -> `Book`**
1.  **Simulate `dbt compile`:** The script creates a dummy `manifest.json` file inside a `target` directory within the newly created dbt project. In a real workflow, you would run `dbt compile` in the generated dbt project directory to create this file.
2.  **Create Book from Manifest:** The `create_book_from_dbt_manifest` function is called. This function parses the `manifest.json` file and creates a new `Book` object that represents the dbt project's dependency graph.
3.  **Print dbt Graph:** The script then prints the structure of this new `Book`, showing how the dbt models and sources are related.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting dbt integration use case...

--- Part 1: Generating a dbt project from a Book ---
INFO:book.dbt_integration.project:Generating dbt project for Book: Product Hierarchy...
INFO:book.dbt_integration.project:dbt project generated at: ./Product Hierarchy

--- Part 2: Creating a Book from a dbt manifest ---
INFO:__main__:Creating a dummy manifest.json file...
INFO:book.dbt_integration.manifest_parser:Creating Book from dbt manifest: Product Hierarchy/target/manifest.json...
INFO:book.dbt_integration.manifest_parser:Successfully created Book from dbt manifest. Found 2 nodes.

--- dbt Project as a Book ---
- my_table (source)
  - product_hierarchy (model)

INFO:__main__:
Cleaned up dbt project directory: Product Hierarchy

INFO:__main__:
dbt integration use case completed.
```

This use case demonstrates a complete, bi-directional workflow that allows you to manage your master data hierarchies and your dbt data models in a unified, code-driven way.