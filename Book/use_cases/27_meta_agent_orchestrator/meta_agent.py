from book import Book, Node, get_logger
from typing import List, Dict, Any, Callable

logger = get_logger(__name__)

# --- Simulate other components of the DataBridge AI ecosystem ---

def librarian_get_hierarchy(name: str) -> Book:
    """
    Simulates the Librarian providing a master hierarchy.
    """
    logger.info(f"[Librarian] Fetching master hierarchy: '{name}'")
    if name == "Consolidation Hierarchy":
        return Book(name=name, root_nodes=[
            Node(name="Consolidated Revenue"),
            Node(name="Consolidated COGS"),
        ])
    return Book(name=name)

def researcher_run_consolidation(book_a: Book, book_b: Book, master_hierarchy: Book) -> Book:
    """
    Simulates the Researcher performing a financial consolidation.
    """
    logger.info(f"[Researcher] Running consolidation for '{book_a.name}' and '{book_b.name}'...")
    
    # Simplified consolidation logic
    consolidated_book = Book(name="Consolidated Report")
    
    # In a real scenario, this would involve complex logic for aggregation and eliminations
    # For now, we'll just create a summary node
    total_revenue = 800000 # Dummy value
    consolidated_book.root_nodes.append(
        Node(name="Total Consolidated Revenue", properties={"amount": total_revenue})
    )
    return consolidated_book

# --- Meta-Agent / Orchestrator ---

class MetaAgent:
    """
    A meta-agent that can orchestrate a multi-step workflow involving
    the Book library, Librarian, and Researcher.
    """
    def __init__(self):
        self.context = {}  # To store the results of each step
        self.tool_registry = {
            "get_hierarchy": librarian_get_hierarchy,
            "run_consolidation": researcher_run_consolidation,
        }

    def execute_step(self, step: Dict[str, Any]):
        tool_name = step["tool"]
        args = {k: self.context.get(v) for k, v in step.get("inputs", {}).items()}
        
        logger.info(f"\nExecuting step: '{step['name']}' with tool: '{tool_name}'")
        
        if tool_name in self.tool_registry:
            result = self.tool_registry[tool_name](**args)
            self.context[step["output"]] = result
            logger.info(f"  - Step successful. Output stored in context as '{step['output']}'.")
        else:
            logger.info(f"  - Error: Tool '{tool_name}' not found.")

    def execute_plan(self, plan: List[Dict[str, Any]]):
        """
        Executes a multi-step plan.
        """
        logger.info("--- Starting Execution of Meta-Agent Plan ---")
        for step in plan:
            self.execute_step(step)
        logger.info("\n--- Meta-Agent Plan Execution Completed ---")

def main():
    """
    This script demonstrates the Meta-Agent orchestrating a financial consolidation.
    """
    # 1. Define a multi-step plan
    consolidation_plan = [
        {
            "name": "Fetch Master Hierarchy",
            "tool": "get_hierarchy",
            "inputs": {"name": "Consolidation Hierarchy"},
            "output": "master_hierarchy",
        },
        {
            "name": "Load Subsidiary A Data",
            "tool": "create_book", # This tool would be in a real implementation
            "output": "sub_a_book",
        },
        {
            "name": "Load Subsidiary B Data",
            "tool": "create_book",
            "output": "sub_b_book",
        },
        {
            "name": "Run Consolidation",
            "tool": "run_consolidation",
            "inputs": {
                "book_a": "sub_a_book",
                "book_b": "sub_b_book",
                "master_hierarchy": "master_hierarchy",
            },
            "output": "consolidated_report",
        },
    ]

    # 2. Instantiate and run the Meta-Agent
    meta_agent = MetaAgent()
    
    # Manually add the book objects to the context for this simulation
    meta_agent.context["Consolidation Hierarchy"] = "Consolidation Hierarchy"
    meta_agent.context["sub_a_book"] = Book(name="Subsidiary A")
    meta_agent.context["sub_b_book"] = Book(name="Subsidiary B")

    meta_agent.execute_plan(consolidation_plan)

    # 3. Print the final result
    final_report = meta_agent.context.get("consolidated_report")
    if final_report:
        logger.info("\n--- Final Consolidated Report ---")
        print(final_report.model_dump_json(indent=2))

if __name__ == "__main__":
    main()
