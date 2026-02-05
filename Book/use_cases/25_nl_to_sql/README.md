# Use Case 25: Enhanced NL-to-SQL with Schema Embeddings

This use case demonstrates how the `Researcher` component can be enhanced with schema embeddings to provide a more accurate and reliable Natural Language to SQL (NL-to-SQL) capability.

## Features Highlighted

*   **NL-to-SQL:** Converting a natural language query into a SQL query.
*   **Schema Embeddings (Simulated):** Using embeddings of the database schema (table names, column names, descriptions) to improve the accuracy of the NL-to-SQL conversion.
*   **`Researcher` (Simulated):** The `nl_to_sql.py` script acts as the `Researcher`, performing the NL-to-SQL translation.

## Components Involved

*   **`Researcher` (Simulated):** The core component for performing the NL-to-SQL translation.

## Files

*   `nl_to_sql.py`: The Python script that simulates the NL-to-SQL workflow.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the NL-to-SQL Script

Navigate to the `Book/use_cases/25_nl_to_sql` directory and run the `nl_to_sql.py` script:

```bash
python nl_to_sql.py
```

### 3. What's Happening?

1.  **Define Schema:** The script starts by defining a simple database schema in a Python dictionary.
2.  **Simulate Schema Embeddings:** It then creates a simple keyword-based "embedding" for each table. In a real implementation, this would be a sophisticated vector embedding created using a sentence transformer model.
3.  **Natural Language Query:** A natural language query is defined: "show me the total order amount for each customer in the 'North' region".
4.  **Find Relevant Tables:** The script (simulating the `Researcher`) performs a simple keyword search to find the most relevant tables for the query based on the simulated schema embeddings.
5.  **Construct SQL:** Based on the identified tables, the script constructs a SQL query. This is a very simplified query builder for demonstration purposes. A real NL-to-SQL engine would be much more complex.
6.  **Print SQL:** The script prints the generated SQL query.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting NL-to-SQL with schema embeddings use case...
INFO:__main__:Natural language query: 'show me the total order amount for each customer in the 'North' region'
INFO:__main__:Finding relevant tables and columns using schema embeddings...
INFO:__main__:Relevant tables found: ['customers', 'orders']
INFO:__main__:Constructing SQL query...

--- Generated SQL Query ---
SELECT
    c.customer_name,
    SUM(o.total_amount) AS total_order_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.region = 'North'
GROUP BY c.customer_name;

INFO:__main__:
NL-to-SQL with schema embeddings use case completed.
```

This use case demonstrates how schema embeddings can be used to significantly improve the accuracy and reliability of an NL-to-SQL engine. By understanding the semantic meaning of the database schema, the `Researcher` can make more intelligent decisions about which tables and columns to use in the generated SQL.
