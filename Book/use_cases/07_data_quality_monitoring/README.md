# Use Case 7: Data Quality Monitoring (Book & Researcher)

This use case demonstrates how the `Book` library can be used to structure data for data quality monitoring, and how a script (simulating the `Researcher`) can analyze this data to identify and flag issues. It highlights how to:

*   Load raw data into a `Book` object.
*   Iterate through nodes to perform automated data quality checks.
*   Attach data quality flags and comments as properties to individual nodes.
*   Generate a data quality report.

## Components Involved

*   **`Book` Library:** Used to represent the product data, making it easy to navigate and attach metadata (quality flags and comments) to individual product nodes.
*   **`Researcher` (Simulated):** The `run_quality_check.py` script acts as a simplified "Researcher" component, performing the analytical task of checking data quality.

## Files

*   `product_data.csv`: A sample CSV file containing product information with simulated data quality issues (e.g., missing values, invalid data types, negative numbers).
*   `run_quality_check.py`: The Python script that loads the data, performs quality checks, and generates a report.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Quality Check Script

Navigate to the `Book/use_cases/07_data_quality_monitoring` directory and run the `run_quality_check.py` script:

```bash
python run_quality_check.py
```

### 3. What's Happening?

1.  **Load Data into Book:** The script loads `product_data.csv` and transforms it into a `Book` object, where each product is represented as a `Node`.
2.  **Automated Quality Checks:** The script then iterates through each `Node` (product) in the `Book` and performs various checks:
    *   **Missing Values:** It checks if `price`, `stock_level`, and `category` properties are present.
    *   **Data Type Validation:** It attempts to convert `price` to a float and `stock_level` to an integer, flagging errors if conversion fails.
    *   **Value Range Checks:** It verifies that `price` and `stock_level` are non-negative.
3.  **Flagging Issues:** For every data quality issue identified, the script adds a boolean flag (e.g., `missing_price=True`) to the node's `flags` dictionary and a descriptive `quality_comment` to its `properties`. This demonstrates how metadata can be attached directly to the data.
4.  **Generate Report:** Finally, the script prints a structured "Data Quality Report" listing all products with identified issues, along with the specific flags and comments.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting data quality monitoring use case...
INFO:__main__:Loading product data from product_data.csv into a Book...
INFO:__main__:Performing data quality checks...
INFO:__main__:--- Data Quality Report ---
Product: Wireless Mouse (ID: P003)
  - Issue: Missing Stock
  - Comment: Missing stock level.
------------------------------
Product: Monitor 4K (ID: P006)
  - Issue: Negative Price
  - Comment: Negative price detected.
------------------------------
Product: Software Suite (ID: P007)
  - Issue: Missing Category
  - Comment: Missing category.
------------------------------
Product: External SSD (ID: P009)
  - Issue: Invalid Stock Type
  - Comment: Invalid stock level data type.
------------------------------
INFO:__main__:Data quality monitoring use case completed.
```

This use case illustrates how the `Book` library provides a flexible and extensible framework for embedding data quality processes directly within your data structures, making it easier to identify, track, and manage data integrity.
