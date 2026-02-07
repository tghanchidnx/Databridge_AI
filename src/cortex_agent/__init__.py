"""
CortexAgent - Snowflake Cortex AI Integration for DataBridge.

This module provides:
1. Orchestrated Reasoning Loop - Observe → Plan → Execute → Reflect pattern
2. Direct Cortex Integration - Execute Cortex LLM functions via SQL
3. Communication Console - Full observability with CLI, File, Database outputs
4. State Management - Manage conversation state for stateless Cortex functions
5. Cortex Analyst - Natural language to SQL via semantic models (Phase 20)

Phase 19 & 20 of DataBridge AI.
"""

from .types import (
    MessageType,
    AgentState,
    CortexFunction,
    AgentMessage,
    ThinkingStep,
    ExecutionPlan,
    StepResult,
    CortexAgentConfig,
    Conversation,
    AgentResponse,
)
from .cortex_client import CortexClient
from .context import CortexAgentContext, get_context
from .console import (
    ConsoleOutput,
    CLIOutput,
    FileOutput,
    DatabaseOutput,
    CommunicationConsole,
)
from .reasoning_loop import CortexReasoningLoop

# Phase 20: Cortex Analyst types
from .analyst_types import (
    SemanticModelConfig,
    LogicalTable,
    Dimension,
    TimeDimension,
    Metric,
    Fact,
    BaseTableRef,
    TableRelationship,
    AnalystMessage,
    AnalystResponse,
    AnalystConversation,
    AnalystQueryResult,
    QueryResult,
)
from .analyst_client import AnalystClient
from .semantic_model import SemanticModelManager

__all__ = [
    # Phase 19 Types
    "MessageType",
    "AgentState",
    "CortexFunction",
    "AgentMessage",
    "ThinkingStep",
    "ExecutionPlan",
    "StepResult",
    "CortexAgentConfig",
    "Conversation",
    "AgentResponse",
    # Phase 19 Core classes
    "CortexClient",
    "CortexAgentContext",
    "get_context",
    "CortexReasoningLoop",
    # Phase 19 Console
    "ConsoleOutput",
    "CLIOutput",
    "FileOutput",
    "DatabaseOutput",
    "CommunicationConsole",
    # Phase 20 Analyst Types
    "SemanticModelConfig",
    "LogicalTable",
    "Dimension",
    "TimeDimension",
    "Metric",
    "Fact",
    "BaseTableRef",
    "TableRelationship",
    "AnalystMessage",
    "AnalystResponse",
    "AnalystConversation",
    "AnalystQueryResult",
    "QueryResult",
    # Phase 20 Analyst Classes
    "AnalystClient",
    "SemanticModelManager",
]
