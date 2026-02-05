from book import (
    Book,
    Node,
    from_list,
    get_logger,
    add_property,
    execute_formulas,
    Formula,
)
import csv

logger = get_logger(__name__)

def load_csv_to_dict(file_path: str, key_column: str) -> dict:
    """Loads data from a CSV file into a dictionary keyed by a specific column."""
    data_dict = {}
    with open(file_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            data_dict[row[key_column]] = row
    return data_dict

def main():
    """
    This script demonstrates creating a multi-layered financial report with
    dependent formula calculations.
    """
    logger.info("Starting multi-layered financial reporting use case...")

    # 1. Load trial balance data
    logger.info("Loading detailed trial balance data...")
    trial_balance_data = load_csv_to_dict("detailed_trial_balance.csv", "account_name")

    # 2. Define the multi-layered hierarchy
    logger.info("Defining the multi-layered hierarchy...")
    
    # Create the nodes
    nodes = {name: Node(name=name, properties=props) for name, props in trial_balance_data.items()}
    
    # Manually create the hierarchy and calculated nodes
    report_book = Book(name="Multi-layered Income Statement")
    
    # --- Level 1: Revenue & COGS -> Gross Profit ---
    revenue_node = Node(name="Total Revenue")
    cogs_node = Node(name="Total Cost of Goods Sold")
    gross_profit_node = Node(name="Gross Profit")
    
    revenue_node.children = [nodes["Product Revenue"], nodes["Service Revenue"]]
    cogs_node.children = [nodes["Product COGS"], nodes["Service COGS"]]
    
    # --- Level 2: Operating Expenses -> Operating Income ---
    op_ex_node = Node(name="Total Operating Expenses")
    op_income_node = Node(name="Operating Income")
    
    op_ex_node.children = [nodes["Sales & Marketing"], nodes["Research & Development"], nodes["General & Administrative"]]
    
    # --- Level 3: Non-operating -> Net Income ---
    non_op_node = Node(name="Total Non-operating Income/Expense")
    net_income_node = Node(name="Net Income")
    
    non_op_node.children = [nodes["Interest Income"], nodes["Interest Expense"]]

    # 3. Add formulas with dependencies
    logger.info("Adding formulas with dependencies...")
    
    # Gross Profit formula
    add_property(gross_profit_node, "total_revenue", 1250000)
    add_property(gross_profit_node, "total_cogs", 500000)
    gross_profit_node.formulas.append(Formula(name="amount", expression="total_revenue - total_cogs", operands=["total_revenue", "total_cogs"]))
    
    # Operating Income formula (depends on Gross Profit)
    add_property(op_income_node, "total_op_ex", 300000)
    op_income_node.formulas.append(Formula(name="amount", expression="gross_profit - total_op_ex", operands=["gross_profit", "total_op_ex"]))

    # Net Income formula (depends on Operating Income)
    add_property(net_income_node, "total_non_op", 5000)
    net_income_node.formulas.append(Formula(name="amount", expression="operating_income + total_non_op", operands=["operating_income", "total_non_op"]))

    # 4. Execute formulas in the correct order
    logger.info("Executing formulas in order...")
    execute_formulas(gross_profit_node, report_book)
    
    # Pass the calculated gross_profit to the next formula's scope
    add_property(op_income_node, "gross_profit", gross_profit_node.properties.get("amount", 0))
    execute_formulas(op_income_node, report_book)
    
    add_property(net_income_node, "operating_income", op_income_node.properties.get("amount", 0))
    execute_formulas(net_income_node, report_book)

    # 5. Assemble the final report book
    report_book.root_nodes = [revenue_node, cogs_node, gross_profit_node, op_ex_node, op_income_node, non_op_node, net_income_node]

    # 6. Print the report
    logger.info("\n--- Multi-layered Income Statement ---")
    def print_report(nodes, indent=""):
        for node in nodes:
            amount = node.properties.get('amount')
            if amount is not None:
                print(f"{indent}{node.name:<30} {'${:,.2f}'.format(amount)}")
            else:
                print(f"{indent}{node.name}:")
            
            if node.children:
                print_report(node.children, indent + "  ")

    print_report(report_book.root_nodes)

    logger.info("\nMulti-layered reporting use case completed.")

if __name__ == "__main__":
    main()
