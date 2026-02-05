# Use Case 8: Dynamic Pricing Hierarchy (Book & AI Agent)

This use case demonstrates how to combine the hierarchical data structuring capabilities of the `Book` library with the intelligent suggestions of the `AIAgent` for dynamic pricing. It showcases how a `Book` can represent a product catalog and how an AI Agent, configured with specific skills, can provide actionable insights for pricing adjustments.

## Features Highlighted

*   Building a hierarchical product catalog in a `Book`.
*   Using `AIAgent` with a specific skill (`pricing-analyst`) to generate suggestions.
*   Simulating AI-driven analysis based on product properties.

## Components Involved

*   **`Book` Library:** Used to structure the product catalog and its properties.
*   **`AIAgent`:** Provides intelligent pricing suggestions based on defined rules in a "skill."

## Files

*   `product_catalog.csv`: A sample CSV file containing product information, including categories, base prices, and market demand.
*   `../skills/pricing-analyst.json`: A dummy AI skill definition that provides rules for pricing adjustments based on market demand.
*   `run_pricing_suggestions.py`: The Python script that implements the use case.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Pricing Suggestions Script

Navigate to the `Book/use_cases/08_dynamic_pricing` directory and run the `run_pricing_suggestions.py` script:

```bash
python run_pricing_suggestions.py
```

### 3. What's Happening?

1.  **Load Product Data:** The script loads the `product_catalog.csv` file into a `Book` object, where each product becomes a `Node` with its properties (category, base price, market demand).
2.  **Initialize AI Agent:** An `AIAgent` is initialized. It's configured to specifically use the `pricing-analyst` skill (defined in `../skills/pricing-analyst.json`). The `databridge_project_path` is set to point to the location where the `skills` directory is found.
3.  **Generate Suggestions:** The script then iterates through each product `Node` in the `Book`. For each product, it extracts relevant information and passes it to a simulated suggestion logic within the `run_pricing_suggestions.py` script. This simulation uses the rules defined in the `pricing-analyst` skill to generate a pricing adjustment suggestion based on the product's `market_demand`.
4.  **Print Report:** The script prints a report listing each product, its current price, and the AI agent's suggested pricing adjustment.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting dynamic pricing suggestions use case...
INFO:__main__:Loading product catalog from product_catalog.csv into a Book...
INFO:__main__:Configuring and initializing the AI Agent with 'pricing-analyst' skill...
INFO:__main__:Getting pricing suggestions for each product...

--- Pricing Suggestions ---
Product: Laptop Pro (Category: Electronics, Demand: High)
  Current Price: $1200.00
  Suggestion: Increase price by 10%
----------------------------------------
Product: Mechanical Keyboard (Category: Electronics, Demand: Medium)
  Current Price: $150.00
  Suggestion: Maintain current price
----------------------------------------
Product: Wireless Mouse (Category: Electronics, Demand: Low)
  Current Price: $25.00
  Suggestion: Decrease price by 5%
----------------------------------------
Product: Ergonomic Chair (Category: Office Furniture, Demand: Medium)
  Current Price: $400.00
  Suggestion: Maintain current price
----------------------------------------
Product: Desk Lamp (Category: Office Furniture, Demand: High)
  Current Price: $50.00
  Suggestion: Increase price by 10%
----------------------------------------
Product: Monitor 4K (Category: Electronics, Demand: High)
  Current Price: $500.00
  Suggestion: Increase price by 10%
----------------------------------------
INFO:__main__:Dynamic pricing suggestions use case completed.
```

This use case demonstrates how the `Book` library can serve as a powerful data representation for AI-driven applications, allowing for intelligent automation and decision support based on structured hierarchical data and configurable AI skills.
