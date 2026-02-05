from book import (
    Book,
    from_list,
    execute_formulas,
    Formula,
    get_logger,
    add_property,
)

# Get a logger for this module
logger = get_logger(__name__)

def main():
    """
    This script demonstrates how to build a simple financial report using the Book library.
    """
    logger.info("Starting financial report use case...")

    # 1. Ingest data from CSV
    logger.info("Ingesting data from trial_balance.csv...")
    data = []
    with open("trial_balance.csv", "r") as f:
        import csv
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    
    # 2. Build the hierarchy
    logger.info("Building the hierarchy from the ingested data...")
    root_nodes = from_list(data, parent_col="parent_account", child_col="account_id", name_col="account_name")
    
    # Create the book
    income_statement = Book(name="Income Statement", root_nodes=root_nodes)

    # 3. Add formulas
    logger.info("Adding formulas for Gross Margin and Operating Income...")
    
    # Find the nodes to attach formulas to
    gross_margin_node = next((n for n in root_nodes if n.name == "Gross Margin"), None)
    operating_income_node = next((n for n in root_nodes if n.name == "Operating Income"), None)
    
    if gross_margin_node:
        gross_margin_formula = Formula(
            name="amount",
            expression="revenue - cogs",
            operands=["revenue", "cogs"],
        )
        gross_margin_node.formulas.append(gross_margin_formula)
        # Add properties to the node so that the formula can be calculated
        add_property(gross_margin_node, "revenue", 600000) # Product Revenue + Service Revenue
        add_property(gross_margin_node, "cogs", 250000) # Product COGS + Service COGS

    if operating_income_node:
        operating_income_formula = Formula(
            name="amount",
            expression="gross_margin - operating_expenses",
            operands=["gross_margin", "operating_expenses"],
        )
        operating_income_node.formulas.append(operating_income_formula)
        # Add properties to the node so that the formula can be calculated
        add_property(operating_income_node, "gross_margin", 350000) # Calculated from the previous formula
        add_property(operating_income_node, "operating_expenses", 175000) # Sales & Marketing + General & Administrative
    
    # 4. Execute formulas
    logger.info("Executing formulas...")
    if gross_margin_node:
        execute_formulas(gross_margin_node, income_statement)
    if operating_income_node:
        execute_formulas(operating_income_node, income_statement)

    # 5. Print the results
    logger.info("Financial Report:")
    def print_hierarchy(nodes, indent=""):
        for node in nodes:
            amount = node.properties.get("amount", "")
            print(f"{indent}{node.name}: {amount}")
            print_hierarchy(node.children, indent + "  ")

    print_hierarchy(income_statement.root_nodes)

if __name__ == "__main__":
    main()
