"""
Workflows module for DataBridge Discovery.

This module provides workflow definitions for automated discovery:
- Discovery workflow (E2E)
- Incremental sync workflow
- Validation workflow
"""

from databridge_discovery.workflows.discovery_workflow import (
    DiscoveryWorkflow,
    DiscoveryWorkflowConfig,
    DiscoveryWorkflowResult,
)
from databridge_discovery.workflows.incremental_sync import (
    IncrementalSyncWorkflow,
    SyncConfig,
    SyncResult,
)
from databridge_discovery.workflows.validation_workflow import (
    ValidationWorkflow,
    ValidationConfig,
    ValidationResult,
)

__all__ = [
    # Discovery Workflow
    "DiscoveryWorkflow",
    "DiscoveryWorkflowConfig",
    "DiscoveryWorkflowResult",
    # Incremental Sync
    "IncrementalSyncWorkflow",
    "SyncConfig",
    "SyncResult",
    # Validation
    "ValidationWorkflow",
    "ValidationConfig",
    "ValidationResult",
]
