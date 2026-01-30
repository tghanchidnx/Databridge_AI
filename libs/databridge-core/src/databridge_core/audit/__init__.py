"""
DataBridge Core Audit Module.

Provides audit trail logging for compliance and debugging.
"""

from databridge_core.audit.logger import (
    AuditLogger,
    get_audit_logger,
    log_action,
)

__all__ = [
    "AuditLogger",
    "get_audit_logger",
    "log_action",
]
