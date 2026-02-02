"""
Audit trail logging for DataBridge AI platform.

Provides a centralized audit logging system for tracking actions
across both Librarian and Researcher applications.
"""

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict
import threading


@dataclass
class AuditEntry:
    """Represents a single audit log entry."""
    timestamp: str
    action: str
    entity_type: str
    entity_id: Optional[str]
    user_id: Optional[str]
    source: str
    details: Optional[Dict[str, Any]]
    ip_address: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


class AuditLogger:
    """
    Audit trail logger.

    Logs actions to both a CSV file (for easy analysis) and optionally
    to a database table.
    """

    def __init__(
        self,
        log_path: Path,
        source: str = "mcp",
        buffer_size: int = 10,
    ):
        """
        Initialize the audit logger.

        Args:
            log_path: Path to the audit log CSV file.
            source: Default source identifier (e.g., "mcp", "cli", "api").
            buffer_size: Number of entries to buffer before flushing.
        """
        self._log_path = Path(log_path)
        self._source = source
        self._buffer_size = buffer_size
        self._buffer: List[AuditEntry] = []
        self._lock = threading.Lock()

        # Ensure directory exists
        self._log_path.parent.mkdir(parents=True, exist_ok=True)

        # Create file with headers if it doesn't exist
        if not self._log_path.exists():
            self._write_headers()

    def _write_headers(self) -> None:
        """Write CSV headers to the log file."""
        with open(self._log_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp",
                "action",
                "entity_type",
                "entity_id",
                "user_id",
                "source",
                "details",
                "ip_address",
            ])

    def log(
        self,
        action: str,
        entity_type: str,
        entity_id: Optional[str] = None,
        user_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        source: Optional[str] = None,
        ip_address: Optional[str] = None,
    ) -> AuditEntry:
        """
        Log an action to the audit trail.

        Args:
            action: Action performed (e.g., "create", "update", "delete").
            entity_type: Type of entity (e.g., "project", "hierarchy").
            entity_id: Optional entity identifier.
            user_id: Optional user identifier.
            details: Optional additional details.
            source: Override default source.
            ip_address: Optional IP address.

        Returns:
            AuditEntry: The logged entry.
        """
        entry = AuditEntry(
            timestamp=datetime.now(timezone.utc).isoformat(),
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            user_id=user_id,
            source=source or self._source,
            details=details,
            ip_address=ip_address,
        )

        with self._lock:
            self._buffer.append(entry)
            if len(self._buffer) >= self._buffer_size:
                self._flush()

        return entry

    def _flush(self) -> None:
        """Flush buffered entries to the log file."""
        if not self._buffer:
            return

        with open(self._log_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for entry in self._buffer:
                writer.writerow([
                    entry.timestamp,
                    entry.action,
                    entry.entity_type,
                    entry.entity_id or "",
                    entry.user_id or "",
                    entry.source,
                    json.dumps(entry.details) if entry.details else "",
                    entry.ip_address or "",
                ])

        self._buffer.clear()

    def flush(self) -> None:
        """Manually flush the buffer."""
        with self._lock:
            self._flush()

    def get_recent_entries(self, limit: int = 100) -> List[AuditEntry]:
        """
        Get recent audit entries.

        Args:
            limit: Maximum number of entries to return.

        Returns:
            List of recent audit entries.
        """
        entries = []

        if not self._log_path.exists():
            return entries

        with open(self._log_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                entries.append(AuditEntry(
                    timestamp=row["timestamp"],
                    action=row["action"],
                    entity_type=row["entity_type"],
                    entity_id=row.get("entity_id") or None,
                    user_id=row.get("user_id") or None,
                    source=row["source"],
                    details=json.loads(row["details"]) if row.get("details") else None,
                    ip_address=row.get("ip_address") or None,
                ))

        # Return most recent entries
        return entries[-limit:]

    def __del__(self):
        """Flush buffer on destruction."""
        try:
            self.flush()
        except Exception:
            pass


# =============================================================================
# Workflow Tracing
# =============================================================================

@dataclass
class WorkflowTrace:
    """Represents a single step in a workflow execution trace."""
    session_id: str
    step_id: str
    tool_name: str
    status: str  # e.g., "started", "success", "failure"
    start_time: str
    end_time: Optional[str] = None
    input_snapshots: Optional[List[str]] = None
    output_snapshot: Optional[str] = None
    data_shape: Optional[Dict[str, int]] = None  # e.g., {"rows": 100, "columns": 5}
    details: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


class WorkflowLogger:
    """
    Workflow execution trace logger.
    """

    def __init__(self, log_path: Path, buffer_size: int = 1):
        self._log_path = Path(log_path)
        self._buffer_size = buffer_size
        self._buffer: List[WorkflowTrace] = []
        self._lock = threading.Lock()

        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        if not self._log_path.exists():
            self._write_headers()

    def _write_headers(self) -> None:
        with open(self._log_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(WorkflowTrace.__dataclass_fields__.keys())

    def log(self, trace: WorkflowTrace):
        with self._lock:
            self._buffer.append(trace)
            if len(self._buffer) >= self._buffer_size:
                self._flush()

    def _flush(self):
        if not self._buffer:
            return
        with open(self._log_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=WorkflowTrace.__dataclass_fields__.keys())
            for trace in self._buffer:
                row = trace.to_dict()
                row["input_snapshots"] = json.dumps(row["input_snapshots"]) if row["input_snapshots"] else ""
                row["data_shape"] = json.dumps(row["data_shape"]) if row["data_shape"] else ""
                row["details"] = json.dumps(row["details"]) if row["details"] else ""
                writer.writerow(row)
        self._buffer.clear()

    def flush(self):
        with self._lock:
            self._flush()

    def __del__(self):
        try:
            self.flush()
        except Exception:
            pass


# =============================================================================
# Global Logger Management
# =============================================================================

_audit_logger: Optional[AuditLogger] = None
_workflow_logger: Optional[WorkflowLogger] = None


def set_audit_logger(logger: AuditLogger) -> None:
    """Set the global audit logger."""
    global _audit_logger
    _audit_logger = logger


def get_audit_logger() -> Optional[AuditLogger]:
    """Get the global audit logger."""
    return _audit_logger


def set_workflow_logger(logger: WorkflowLogger) -> None:
    """Set the global workflow logger."""
    global _workflow_logger
    _workflow_logger = logger


def get_workflow_logger() -> Optional[WorkflowLogger]:
    """Get the global workflow logger."""
    return _workflow_logger


def log_action(
    action: str,
    entity_type: str,
    entity_id: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Optional[AuditEntry]:
    """
    Log an action using the global audit logger.
    """
    logger = get_audit_logger()
    if logger is None:
        return None
    return logger.log(action, entity_type, entity_id, details=details, **kwargs)


def log_workflow_step(trace: WorkflowTrace) -> None:
    """
    Log a workflow step using the global workflow logger.
    """
    logger = get_workflow_logger()
    if logger is None:
        return
    logger.log(trace)