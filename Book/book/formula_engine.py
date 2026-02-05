from .models import Node, Book
from .properties import get_property

def execute_formulas(node: Node, book: Book):
    """
    Executes the formulas attached to a node and stores the results as properties.
    """
    for formula in node.formulas:
        local_scope = {}
        try:
            for operand in formula.operands:
                local_scope[operand] = get_property(node, book, operand)
            
            # Security Warning: eval is used here for simplicity.
            # In a real-world application, consider using a safer evaluation engine.
            result = eval(formula.expression, globals(), local_scope)
            node.properties[formula.name] = result

        except KeyError as e:
            # Handle cases where an operand is missing
            print(f"Skipping formula '{formula.name}' on node '{node.name}': Missing operand {e}")
        except Exception as e:
            # Handle other evaluation errors
            print(f"Error executing formula '{formula.name}' on node '{node.name}': {e}")
