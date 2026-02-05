from pydantic import BaseModel
from typing import List, Any, Dict

class Formula(BaseModel):
    """
    Represents a formula that can be attached to a node.
    """
    name: str
    expression: str
    operands: List[str]
