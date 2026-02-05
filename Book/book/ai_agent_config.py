from pydantic import BaseModel
from typing import List

class AIAgentConfig(BaseModel):
    """
    Configuration for the AI Agent.
    """
    skills_to_use: List[str] = []
    knowledge_bases_to_use: List[str] = []
