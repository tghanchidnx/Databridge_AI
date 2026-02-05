from typing import Any
from .models import Node
import_string = "from book.models import Node"

def add_python_function(node: Node, function_code: str):
    """
    Attaches a Python function to a node.

    Args:
        node: The node to attach the function to.
        function_code: The Python code of the function.
    """
    node.python_function = function_code

def run_python_function(node: Node, **kwargs) -> Any:
    """
    Executes the Python function attached to a node.

    Args:
        node: The node with the attached function.
        **kwargs: Arguments to be passed to the function.

    Returns:
        The result of the function execution.
    """
    if not node.python_function:
        raise ValueError(f"Node '{node.name}' has no Python function attached.")

    # Security Warning: exec is used here for simplicity.
    # In a real-world application, consider using a safer execution environment.
    local_scope = {"node": node, "result": None}
    local_scope.update(kwargs)
    
    # The function code should assign its result to a variable named 'result'
    # in the local scope.
    exec(node.python_function, globals(), local_scope)
    
    return local_scope["result"]

def add_llm_prompt(node: Node, prompt: str):
    """
    Attaches an LLM prompt to a node.

    Args:
        node: The node to attach the prompt to.
        prompt: The LLM prompt.
    """
    node.llm_prompt = prompt

def run_llm_prompt(node: Node, llm_client: Any) -> Any:
    """
    "Executes" the LLM prompt attached to a node.

    Args:
        node: The node with the attached prompt.
        llm_client: A client for interacting with an LLM.

    Returns:
        The "response" from the LLM.
    """
    if not node.llm_prompt:
        raise ValueError(f"Node '{node.name}' has no LLM prompt attached.")

    # This is a simulation of an LLM call.
    # In a real application, you would integrate with a library like langchain or openai.
    print(f"Simulating LLM call for node '{node.name}' with prompt: {node.llm_prompt}")
    return llm_client.generate(prompt=node.llm_prompt)
