"""
Audit logging for DataBridge AI Librarian.

Provides comprehensive audit trail without storing PII.
"""

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

from .config import get_settings
from .database import AuditLog, session_scope


class AuditLogger:
    """
    Comprehensive audit logger for compliance tracking.

    Writes to both database and CSV file for redundancy.
    """

    def __init__(self, log_path: Optional[Path] = None):
        """
        Initialize the audit logger.

        Args:
            log_path: Path to CSV audit log file. If None, uses config default.
        """
        settings = get_settings()
        self.log_path = log_path or settings.data.audit_log
        self._ensure_csv_exists()

    def _ensure_csv_exists(self) -> None:
        """Create CSV file with headers if it doesn't exist."""
        if not self.log_path.exists():
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.log_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp",
                    "action",
                    "entity_type",
                    "entity_id",
                    "user_id",
                    "source",
                    "details",
                ])

    def log(
        self,
        action: str,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        user_id: Optional[str] = None,
        source: str = "cli",
        details: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None,
    ) -> int:
        """
        Log an audit event.

        Args:
            action: Action being performed (e.g., "create_project", "import_csv").
            entity_type: Type of entity affected (e.g., "project", "hierarchy").
            entity_id: ID of the affected entity.
            user_id: ID of the user performing the action.
            source: Source of the action (cli, mcp, api).
            details: Additional details as a dictionary.
            ip_address: IP address of the client.

        Returns:
            int: ID of the created audit log entry.
        """
        timestamp = datetime.utcnow()

        # Write to CSV
        self._write_csv(
            timestamp=timestamp,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            user_id=user_id,
            source=source,
            details=details,
        )

        # Write to database
        with session_scope() as session:
            log_entry = AuditLog(
                timestamp=timestamp,
                action=action,
                entity_type=entity_type,
                entity_id=entity_id,
                user_id=user_id,
                source=source,
                details=details,
                ip_address=ip_address,
            )
            session.add(log_entry)
            session.flush()
            return log_entry.id

    def _write_csv(
        self,
        timestamp: datetime,
        action: str,
        entity_type: Optional[str],
        entity_id: Optional[str],
        user_id: Optional[str],
        source: str,
        details: Optional[Dict[str, Any]],
    ) -> None:
        """Write audit entry to CSV file."""
        # Sanitize for CSV (prevent injection)
        def sanitize(val: Any) -> str:
            if val is None:
                return ""
            s = str(val)
            # Remove potential CSV injection characters
            if s.startswith(("=", "+", "-", "@")):
                s = "'" + s
            return s.replace("\n", " ").replace("\r", " ")

        with open(self.log_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                timestamp.isoformat(),
                sanitize(action),
                sanitize(entity_type),
                sanitize(entity_id),
                sanitize(user_id),
                sanitize(source),
                json.dumps(details) if details else "",
            ])

    def query(
        self,
        action: Optional[str] = None,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        user_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Query audit log entries.

        Args:
            action: Filter by action type.
            entity_type: Filter by entity type.
            entity_id: Filter by entity ID.
            user_id: Filter by user ID.
            start_date: Filter by start date.
            end_date: Filter by end date.
            limit: Maximum number of results.

        Returns:
            List of audit log entries as dictionaries.
        """
        with session_scope() as session:
            query = session.query(AuditLog)

            if action:
                query = query.filter(AuditLog.action == action)
            if entity_type:
                query = query.filter(AuditLog.entity_type == entity_type)
            if entity_id:
                query = query.filter(AuditLog.entity_id == entity_id)
            if user_id:
                query = query.filter(AuditLog.user_id == user_id)
            if start_date:
                query = query.filter(AuditLog.timestamp >= start_date)
            if end_date:
                query = query.filter(AuditLog.timestamp <= end_date)

            query = query.order_by(AuditLog.timestamp.desc()).limit(limit)

            return [
                {
                    "id": log.id,
                    "timestamp": log.timestamp.isoformat(),
                    "action": log.action,
                    "entity_type": log.entity_type,
                    "entity_id": log.entity_id,
                    "user_id": log.user_id,
                    "source": log.source,
                    "details": log.details,
                }
                for log in query.all()
            ]

    def get_recent(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get recent audit log entries.

        Args:
            limit: Maximum number of results.

        Returns:
            List of recent audit log entries.
        """
        return self.query(limit=limit)


# Module-level logger instance
_audit_logger: Optional[AuditLogger] = None


def get_audit_logger() -> AuditLogger:
    """
    Get the audit logger singleton.

    Returns:
        AuditLogger: The audit logger instance.
    """
    global _audit_logger
    if _audit_logger is None:
        _audit_logger = AuditLogger()
    return _audit_logger


def log_action(
    action: str,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    user_id: Optional[str] = None,
    source: str = "cli",
    details: Optional[Dict[str, Any]] = None,
) -> Optional[int]:
    """
    Convenience function to log an action.

    Args:
        action: Action being performed.
        entity_type: Type of entity affected.
        entity_id: ID of the affected entity.
        user_id: ID of the user.
        source: Source of the action.
        details: Additional details.

    Returns:
        int: ID of the created audit log entry, or None if audit disabled.
    """
    import os
    if os.environ.get("DATABRIDGE_DISABLE_AUDIT", "").lower() in ("true", "1", "yes"):
        return None
    return get_audit_logger().log(
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        user_id=user_id,
        source=source,
        details=details,
    )
