# Use Case 14: Multi-Source Data Integration for a 360-Degree Customer View

This use case demonstrates how the `Book` library can be used to integrate data from multiple sources to create a unified, 360-degree view of a customer. It highlights:

*   Ingesting data from multiple CSV files.
*   Merging data from different sources into a single `Book` based on a common key.
*   Structuring the integrated data within `Node` properties for a holistic view.

## Components Involved

*   **`Book` Library:** Used to create the unified customer view. The flexibility of the `Book`'s property system is key to this use case.

## Files

*   `crm_data.csv`: A sample CSV file containing customer information from a CRM system.
*   `billing_data.csv`: A sample CSV file containing billing information for the same customers.
*   `run_integration.py`: The Python script that performs the data integration.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Integration Script

Navigate to the `Book/use_cases/14_customer_360` directory and run the `run_integration.py` script:

```bash
python run_integration.py
```

### 3. What's Happening?

1.  **Load Data from Sources:** The script loads the data from `crm_data.csv` and `billing_data.csv` into two separate dictionaries, using the `customer_id` as the key.
2.  **Create Unified Book:** A new `Book` object, "Customer 360 View," is created. The script then iterates through all unique customer IDs from both data sources.
3.  **Merge and Structure Data:** For each customer ID, a new `Node` is created. The script then adds the data from both the CRM and billing systems as properties to this node. This creates a single, unified record for each customer, containing information from both sources.
4.  **Print 360-Degree View:** Finally, the script prints a "Customer 360-Degree View," showing the integrated data for each customer.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting multi-source data integration use case...
INFO:__main__:Loading data from CRM and billing systems...
INFO:__main__:Creating a unified customer Book...

--- Customer 360-Degree View ---
Customer: Alpha Corp
  - CRM Info:
    - Region: North
    - Segment: Enterprise
    - Account Manager: John Smith
  - Billing Info:
    - Status: Active
    - Total Spend: $50,000.00
    - Last Invoice: 2026-01-15
----------------------------------------
Customer: Beta LLC
  - CRM Info:
    - Region: South
    - Segment: SMB
    - Account Manager: Jane Doe
  - Billing Info:
    - Status: Active
    - Total Spend: $15,000.00
    - Last Invoice: 2026-01-20
----------------------------------------
Customer: Gamma Inc
  - CRM Info:
    - Region: North
    - Segment: Enterprise
    - Account Manager: John Smith
  - Billing Info:
    - Status: Past Due
    - Total Spend: $75,000.00
    - Last Invoice: 2025-12-10
----------------------------------------

INFO:__main__:Multi-source data integration use case completed.
```

This use case demonstrates how the `Book` library can be used as a powerful tool for data integration, allowing you to easily combine data from disparate sources into a single, cohesive view for analysis and reporting.
