from __future__ import annotations
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
import uuid
from datetime import datetime, timezone
from .formulas import Formula

class Node(BaseModel):
    """
    Represents a node in the Book's hierarchy.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    schema_version: str = "1.0"
    name: str
    children: List[Node] = []
    properties: Dict[str, Any] = {}
    python_function: Optional[str] = None
    llm_prompt: Optional[str] = None
    flags: Dict[str, bool] = {}
    formulas: List[Formula] = []

class Book(BaseModel):
    """
    Represents a Book, which is a collection of hierarchical nodes.
    """
    name: str
    schema_version: str = "1.0"
    data_version: str = Field(default_factory=lambda: str(uuid.uuid4()))
    last_updated: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    root_nodes: List[Node] = []
    metadata: Dict[str, Any] = {}
    global_properties: Dict[str, Any] = {}

# Update forward reference
Node.model_rebuild()
