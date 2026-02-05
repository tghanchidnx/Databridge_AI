# Use Case 29: Great Expectations Integration for Data Quality

This use case demonstrates how to use the `Book` library's integration with Great Expectations to automatically generate data quality rules, validate your data, and get AI-powered suggestions for fixing data quality issues.

## Features Highlighted

*   **Automatic Expectation Generation:** Generating a Great Expectations "Expectation Suite" from a `Book` object.
*   **Data Validation:** Running the validation and attaching the results to the `Book`.
*   **AI-powered Suggestions:** Using the `AIAgent` to analyze the validation results and provide human-readable suggestions for fixing data quality issues.

## Components Involved

*   **`Book` Library:** Used to hold the data with quality issues.
*   **Great Expectations Integration Module:** The new module in the `Book` library that handles the integration with Great Expectations.
*   **`AIAgent`:** The AI agent that analyzes the validation results and provides suggestions.

## Files

*   `run_ge_integration.py`: The Python script that demonstrates the full workflow.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies, including `great-expectations`, installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Integration Script

Navigate to the `Book/use_cases/29_great_expectations_integration` directory and run the `run_ge_integration.py` script:

```bash
python run_ge_integration.py
```

### 3. What's Happening?

The script runs in three parts:

**Part 1: Setup**
1.  **Create a Book:** A sample `Book` is created with some known data quality issues (a negative price and a null product name).

**Part 2: Generate and Run Expectations**
1.  **Generate Expectation Suite:** The `generate_expectations_from_book` function is called. This function profiles the data in the `Book` and creates a Great Expectations suite with some basic expectations.
2.  **Validate Data:** The `validate_book_with_expectations` function is called. This function runs the validation and attaches the results to the `Book`'s nodes.

**Part 3: AI-powered Analysis**
1.  **Analyze Results:** The `AIAgent` is used to analyze the validation results stored in the `Book`.
2.  **Provide Suggestions:** The agent identifies the failed expectations and provides human-readable suggestions for how to fix the data quality issues.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting Great Expectations integration use case...

--- Part 1: Setting up the Book with data quality issues ---

--- Part 2: Generating and running Great Expectations ---
INFO:book.great_expectations_integration.generator:Generating Expectation Suite for Book: Data With Issues...
...
INFO:book.great_expectations_integration.validator:Validating Book: Data With Issues with suite: data_with_issues_suite...
...
WARNING:book.great_expectations_integration.validator:Validation failed.

--- Part 3: Analyzing validation results with the AIAgent ---
Using skill: Financial Analyst
Suggestion: Check for negative values in financial columns.
Suggestion: Ensure all financial records have a valid transaction date.

--- Data Quality Analysis ---
Data Quality Issue Found in column 'price': Expectation 'expect_column_values_to_be_positive' failed.
Data Quality Issue Found in column 'product_name': Expectation 'expect_column_values_to_not_be_null' failed.

INFO:__main__:
Cleaned up great_expectations directory.

INFO:__main__:
Great Expectations integration use case completed.
```

This use case demonstrates how the `Book` library, in combination with Great Expectations and the `AIAgent`, can create a powerful, automated data quality workflow.
