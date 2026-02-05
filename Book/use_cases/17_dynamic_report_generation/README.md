# Use Case 17: Dynamic Report Generation with Jinja2

This use case demonstrates how to use a `Book` object as a data source for a Jinja2 template to dynamically generate an HTML report. This is a powerful pattern for creating presentation-ready reports from your hierarchical data.

## Features Highlighted

*   Using a `Book` object as a context for a templating engine.
*   Generating a dynamic HTML report from a hierarchical data structure.
*   Recursive template inclusion for rendering nested data.

## Components Involved

*   **`Book` Library:** Provides the structured data (`Book` object) for the report.
*   **Jinja2:** A popular templating engine for Python.

## Files

*   `trial_balance.csv`: The input data for the report.
*   `report_template.html`: The main Jinja2 template for the HTML report.
*   `node.html`: A sub-template used recursively to render the nodes of the hierarchy.
*   `generate_report.py`: The Python script that loads the data, calculates formulas, and renders the report.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies, including `Jinja2`, installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Report Generation Script

Navigate to the `Book/use_cases/17_dynamic_report_generation` directory and run the `generate_report.py` script:

```bash
python generate_report.py
```

### 3. What's Happening?

1.  **Load and Prepare Data:** The script loads the `trial_balance.csv` file into a `Book` object and calculates the formula-based accounts ("Gross Margin" and "Operating Income").
2.  **Set up Jinja2:** It sets up a Jinja2 environment that loads templates from the current directory.
3.  **Render HTML:** The `template.render()` method is called with the `Book` object as the context. Jinja2 then processes the `report_template.html` file:
    *   It uses the `book.name` for the report title.
    *   It iterates through the `root_nodes` of the `Book`.
    *   For each node, it includes the `node.html` sub-template, which recursively renders the node's name, its calculated amount, and all of its children.
4.  **Save Report:** The generated HTML is saved to a file named `income_statement_report.html`.

### 4. View the Report

After running the script, open the `income_statement_report.html` file in your web browser. You will see a nicely formatted income statement, with the hierarchy and calculated values rendered correctly.

This use case demonstrates how the `Book` library can be seamlessly integrated with other tools, like templating engines, to create a wide variety of outputs from your hierarchical data.
