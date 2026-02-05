from book import (
    Book,
    Node,
    get_logger,
    add_property,
    execute_formulas,
    Formula,
)
from setup_librarian import create_consolidation_hierarchy
import csv

logger = get_logger(__name__)

def load_csv_to_dict(file_path: str, key_column: str) -> dict:
    """Loads data from a CSV file into a dictionary keyed by a specific column."""
    data_dict = {}
    with open(file_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            data_dict[row[key_column]] = float(row["amount"])
    return data_dict

def main():
    """
    This script demonstrates a full-cycle financial consolidation.
    """
    logger.info("Starting financial consolidation use case...")

    # 1. Load master hierarchy from Librarian
    logger.info("Loading consolidation hierarchy from Librarian...")
    consolidation_hierarchy = create_consolidation_hierarchy()

    # 2. Load subsidiary trial balances into Book objects
    logger.info("Loading subsidiary trial balances...")
    sub_a_data = load_csv_to_dict("subsidiary_a_trial_balance.csv", "account")
    sub_b_data = load_csv_to_dict("subsidiary_b_trial_balance.csv", "account")

    # 3. Perform consolidation (simulating Researcher)
    logger.info("Performing financial consolidation...")
    
    # --- Aggregate data from subsidiaries ---
    consolidated_data = {
        "Revenue": sub_a_data.get("Revenue", 0) + sub_b_data.get("Revenue", 0),
        "COGS": sub_a_data.get("COGS", 0) + sub_b_data.get("COGS", 0),
        "Operating Expenses": sub_a_data.get("Operating Expenses", 0) + sub_b_data.get("Operating Expenses", 0),
        "Intercompany Revenue": sub_a_data.get("Intercompany Revenue", 0),
        "Intercompany COGS": sub_b_data.get("Intercompany COGS", 0),
    }

    # --- Perform eliminations ---
    elimination = consolidated_data["Intercompany Revenue"] + consolidated_data["Intercompany COGS"]
    
    consolidated_revenue = consolidated_data["Revenue"] + consolidated_data["Intercompany Revenue"]
    consolidated_cogs = consolidated_data["COGS"] - consolidated_data["Intercompany COGS"]
    
    # 4. Create the consolidated report Book
    logger.info("Creating consolidated report Book...")
    report_book = Book(name="Consolidated Financial Report")
    
    # Create nodes for the report
    report_nodes = {node.name: node for node in consolidation_hierarchy.root_nodes}
    
    add_property(report_nodes["Consolidated Revenue"], "amount", consolidated_revenue)
    add_property(report_nodes["Consolidated COGS"], "amount", consolidated_cogs)
    add_property(report_nodes["Consolidated Operating Expenses"], "amount", consolidated_data["Operating Expenses"])
    
    # Add formulas for calculated fields
    gross_profit_node = report_nodes["Consolidated Gross Profit"]
    gross_profit_node.formulas.append(Formula(name="amount", expression="rev - cogs", operands=["rev", "cogs"]))
    add_property(gross_profit_node, "rev", consolidated_revenue)
    add_property(gross_profit_node, "cogs", consolidated_cogs)
    
    op_income_node = report_nodes["Consolidated Operating Income"]
    op_income_node.formulas.append(Formula(name="amount", expression="gp - opex", operands=["gp", "opex"]))
    add_property(op_income_node, "opex", consolidated_data["Operating Expenses"])

    # 5. Execute formulas
    logger.info("Executing consolidation formulas...")
    execute_formulas(gross_profit_node, report_book)
    
    add_property(op_income_node, "gp", gross_profit_node.properties.get("amount", 0))
    execute_formulas(op_income_node, report_book)
    
    report_book.root_nodes = list(report_nodes.values())

    # 6. Print the consolidated report
    logger.info("\n--- Consolidated Financial Report ---")
    print(f"{'Account':<35} {'Amount':>15}")
    print("-" * 50)
    for node in report_book.root_nodes:
        amount = node.properties.get("amount")
        if amount is not None:
            print(f"{node.name:<35} ${amount:,.2f}")

    logger.info("\nFinancial consolidation use case completed.")

if __name__ == "__main__":
    main()
