"""
Deployment data models and enums.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any


class DeploymentStatus(str, Enum):
    """Deployment execution status."""
    PENDING = "pending"
    VALIDATING = "validating"
    EXECUTING = "executing"
    SUCCESS = "success"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"
    PARTIAL = "partial"


class DeploymentMode(str, Enum):
    """Deployment execution mode."""
    DRY_RUN = "dry_run"  # Validate only, no execution
    EXECUTE = "execute"  # Execute scripts
    EXECUTE_WITH_ROLLBACK = "execute_with_rollback"  # Execute with transaction


class ScriptType(str, Enum):
    """Type of deployment script."""
    CREATE_TABLE = "CREATE_TABLE"
    CREATE_VIEW = "CREATE_VIEW"
    CREATE_DYNAMIC_TABLE = "CREATE_DYNAMIC_TABLE"
    INSERT = "INSERT"
    MERGE = "MERGE"
    DROP = "DROP"
    ALTER = "ALTER"
    GRANT = "GRANT"


@dataclass
class DeploymentScript:
    """A single script to be deployed."""
    script_type: ScriptType
    object_name: str
    sql: str
    tier: Optional[str] = None
    schema_name: Optional[str] = None
    database_name: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)

    @property
    def full_name(self) -> str:
        """Get fully qualified object name."""
        parts = []
        if self.database_name:
            parts.append(self.database_name)
        if self.schema_name:
            parts.append(self.schema_name)
        parts.append(self.object_name)
        return ".".join(parts)


@dataclass
class ScriptExecutionResult:
    """Result of executing a single script."""
    script: DeploymentScript
    status: DeploymentStatus
    rows_affected: int = 0
    execution_time_ms: int = 0
    error_message: Optional[str] = None
    executed_at: Optional[datetime] = None

    @property
    def success(self) -> bool:
        """Check if execution was successful."""
        return self.status == DeploymentStatus.SUCCESS


@dataclass
class DeploymentPlan:
    """Plan for a deployment."""
    project_id: str
    project_name: str
    target_database: str
    target_schema: str
    scripts: List[DeploymentScript] = field(default_factory=list)
    execution_order: List[int] = field(default_factory=list)
    estimated_objects: int = 0
    validation_errors: List[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """Check if plan is valid for execution."""
        return len(self.validation_errors) == 0 and len(self.scripts) > 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "project_id": self.project_id,
            "project_name": self.project_name,
            "target_database": self.target_database,
            "target_schema": self.target_schema,
            "script_count": len(self.scripts),
            "estimated_objects": self.estimated_objects,
            "is_valid": self.is_valid,
            "validation_errors": self.validation_errors,
            "scripts": [
                {
                    "type": s.script_type.value,
                    "object": s.full_name,
                    "tier": s.tier,
                }
                for s in self.scripts
            ],
        }


@dataclass
class DeploymentResult:
    """Result of a deployment execution."""
    deployment_id: str
    project_id: str
    status: DeploymentStatus
    mode: DeploymentMode
    target_database: str
    target_schema: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    script_results: List[ScriptExecutionResult] = field(default_factory=list)
    error_message: Optional[str] = None
    rollback_executed: bool = False

    @property
    def success(self) -> bool:
        """Check if deployment was successful."""
        return self.status == DeploymentStatus.SUCCESS

    @property
    def total_scripts(self) -> int:
        """Total number of scripts."""
        return len(self.script_results)

    @property
    def successful_scripts(self) -> int:
        """Number of successful scripts."""
        return sum(1 for r in self.script_results if r.success)

    @property
    def failed_scripts(self) -> int:
        """Number of failed scripts."""
        return sum(1 for r in self.script_results if not r.success)

    @property
    def total_execution_time_ms(self) -> int:
        """Total execution time."""
        return sum(r.execution_time_ms for r in self.script_results)

    @property
    def total_rows_affected(self) -> int:
        """Total rows affected."""
        return sum(r.rows_affected for r in self.script_results)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "deployment_id": self.deployment_id,
            "project_id": self.project_id,
            "status": self.status.value,
            "mode": self.mode.value,
            "target_database": self.target_database,
            "target_schema": self.target_schema,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "total_scripts": self.total_scripts,
            "successful_scripts": self.successful_scripts,
            "failed_scripts": self.failed_scripts,
            "total_execution_time_ms": self.total_execution_time_ms,
            "total_rows_affected": self.total_rows_affected,
            "error_message": self.error_message,
            "rollback_executed": self.rollback_executed,
            "script_results": [
                {
                    "object": r.script.full_name,
                    "type": r.script.script_type.value,
                    "status": r.status.value,
                    "rows_affected": r.rows_affected,
                    "execution_time_ms": r.execution_time_ms,
                    "error_message": r.error_message,
                }
                for r in self.script_results
            ],
        }


@dataclass
class DeploymentConfig:
    """Configuration for deployment execution."""
    target_database: str
    target_schema: str
    mode: DeploymentMode = DeploymentMode.EXECUTE
    stop_on_error: bool = True
    use_transactions: bool = True
    create_schema_if_not_exists: bool = True
    drop_existing: bool = False
    grant_roles: List[str] = field(default_factory=list)
    executed_by: str = "system"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "target_database": self.target_database,
            "target_schema": self.target_schema,
            "mode": self.mode.value,
            "stop_on_error": self.stop_on_error,
            "use_transactions": self.use_transactions,
            "create_schema_if_not_exists": self.create_schema_if_not_exists,
            "drop_existing": self.drop_existing,
            "grant_roles": self.grant_roles,
            "executed_by": self.executed_by,
        }
