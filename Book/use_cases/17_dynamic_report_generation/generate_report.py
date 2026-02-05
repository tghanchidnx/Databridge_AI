from book import (
    Book,
    Node,
    from_list,
    execute_formulas,
    Formula,
    get_logger,
    add_property,
)
from jinja2 import Environment, FileSystemLoader
import csv
import os

logger = get_logger(__name__)

def load_csv(file_path: str) -> list:
    """Loads data from a CSV file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def main():
    """
    This script demonstrates dynamic report generation using a Book and Jinja2.
    """
    logger.info("Starting dynamic report generation use case...")

    # 1. Load and prepare the data
    logger.info("Loading and preparing financial data...")
    data = load_csv("trial_balance.csv")
    root_nodes = from_list(data, parent_col="parent_account", child_col="account_id", name_col="account_name")
    income_statement = Book(name="Income Statement", root_nodes=root_nodes)
    
    # 2. Add and execute formulas
    logger.info("Calculating formulas...")
    gross_margin_node = next((n for n in root_nodes if n.name == "Gross Margin"), None)
    operating_income_node = next((n for n in root_nodes if n.name == "Operating Income"), None)
    
    if gross_margin_node:
        add_property(gross_margin_node, "revenue", 600000)
        add_property(gross_margin_node, "cogs", 250000)
        formula = Formula(name="amount", expression="revenue - cogs", operands=["revenue", "cogs"])
        gross_margin_node.formulas.append(formula)
        execute_formulas(gross_margin_node, income_statement)

    if operating_income_node:
        add_property(operating_income_node, "gross_margin", gross_margin_node.properties.get("amount", 0))
        add_property(operating_income_node, "operating_expenses", 175000)
        formula = Formula(name="amount", expression="gross_margin - operating_expenses", operands=["gross_margin", "operating_expenses"])
        operating_income_node.formulas.append(formula)
        execute_formulas(operating_income_node, income_statement)

    # 3. Set up Jinja2 environment
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template("report_template.html")

    # 4. Render the HTML report
    logger.info("Rendering HTML report...")
    html_output = template.render(book=income_statement)

    # 5. Save the report to a file
    output_path = "income_statement_report.html"
    with open(output_path, "w") as f:
        f.write(html_output)
    logger.info(f"HTML report saved to {output_path}")

    logger.info("Dynamic report generation use case completed.")

if __name__ == "__main__":
    main()
