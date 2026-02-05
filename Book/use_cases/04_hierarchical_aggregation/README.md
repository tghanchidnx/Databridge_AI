# Use Case 4: Hierarchical Aggregation and Reporting with Global Overrides

This use case demonstrates a more complex scenario involving hierarchical data aggregation and the use of global properties with local overrides. It showcases how to build a global sales report that consolidates data from different regions with different currencies.

## Features Highlighted

*   Deep hierarchy creation.
*   Use of global properties for default values (e.g., currency conversion rates).
*   Overriding global properties at the node level for specific cases.
*   Recursive aggregation of data up the hierarchy.

## Files

*   `global_sales.csv`: The input data, representing sales from different entities in various local currencies.
*   `aggregate.py`: The Python script that performs the aggregation and reporting.

## Step-by-Step Instructions

### 1. Run the Script

Navigate to the `Book/use_cases/04_hierarchical_aggregation` directory and run the `aggregate.py` script:

```bash
python aggregate.py
```

### 2. What's Happening?

1.  **Load Data:** The script loads the sales data from `global_sales.csv`.
2.  **Create Book with Global Properties:** A `Book` is created with a `global_properties` dictionary that defines default currency conversion rates. The default `usd_conversion_rate` is set to `1.0`.
3.  **Build Hierarchy:** The script builds a multi-level hierarchy representing the corporate structure.
4.  **Set Local Overrides:** It then iterates through the hierarchy and sets a local `usd_conversion_rate` property on the nodes that do not use USD as their local currency. This demonstrates the override mechanism.
5.  **Calculate USD Sales:** A recursive function traverses the hierarchy and calculates the `sales_usd` for each entity by applying the appropriate conversion rate (either the global default or the local override).
6.  **Aggregate Sales:** Another recursive function aggregates the `sales_usd` from the leaf nodes up to the root of the hierarchy.
7.  **Print Report:** Finally, the script prints the fully aggregated sales report, with all figures consolidated into USD.

### Expected Output

```
INFO:__main__:Starting hierarchical aggregation use case...
INFO:__main__:Loading global sales data...
INFO:__main__:Creating Book with global USD conversion rates...
INFO:__main__:Building sales hierarchy...
INFO:__main__:Setting local currency conversion overrides...
INFO:__main__:Calculating USD sales for all nodes...
INFO:__main__:Aggregating sales up the hierarchy...
INFO:__main__:Aggregated Global Sales Report (in USD):
Global Corp: $2,544,000.00
  North America: $1,652,500.00
    USA: $1,250,000.00
      East: $500,000.00
      West: $750,000.00
    Canada: $401,500.00
      East: $182,500.00
      West: $219,000.00
  Europe: $891,500.00
    Germany: $432,000.00
    France: $378,000.00
    UK: $381,000.00
```

This use case demonstrates the power of the `Book` library for handling real-world business scenarios that involve complex hierarchies, data from multiple sources with different characteristics, and the need for both global assumptions and specific overrides.
