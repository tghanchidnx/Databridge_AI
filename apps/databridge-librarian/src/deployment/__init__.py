"""
Deployment module for generating and executing deployment scripts.

This module provides:
- DeploymentExecutor: Executes DDL scripts against target databases
- DeploymentService: Orchestrates deployment workflows
- Models: Data classes for deployment state tracking
"""

from .models import (
    DeploymentStatus,
    DeploymentMode,
    ScriptType,
    DeploymentScript,
    ScriptExecutionResult,
    DeploymentPlan,
    DeploymentResult,
    DeploymentConfig,
)
from .executor import (
    DeploymentExecutor,
    DeploymentError,
    ValidationError,
    ExecutionError,
)
from .service import (
    DeploymentService,
    DeploymentServiceError,
    ProjectNotFoundError,
    ConnectionNotFoundError,
)

__all__ = [
    # Models
    "DeploymentStatus",
    "DeploymentMode",
    "ScriptType",
    "DeploymentScript",
    "ScriptExecutionResult",
    "DeploymentPlan",
    "DeploymentResult",
    "DeploymentConfig",
    # Executor
    "DeploymentExecutor",
    "DeploymentError",
    "ValidationError",
    "ExecutionError",
    # Service
    "DeploymentService",
    "DeploymentServiceError",
    "ProjectNotFoundError",
    "ConnectionNotFoundError",
]
