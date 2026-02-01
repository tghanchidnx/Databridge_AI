"""
Incremental Sync Workflow for delta synchronization.

This workflow handles incremental updates:
1. Detect changes in source
2. Extract delta records
3. Merge with target
4. Validate sync
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class SyncMode(str, Enum):
    """Sync mode options."""

    FULL = "full"
    INCREMENTAL = "incremental"
    MERGE = "merge"


@dataclass
class SyncConfig:
    """Configuration for sync workflow."""

    mode: SyncMode = SyncMode.INCREMENTAL
    source_table: str = ""
    target_table: str = ""
    key_columns: list[str] = field(default_factory=list)
    timestamp_column: str | None = None
    last_sync_timestamp: datetime | None = None
    batch_size: int = 10000
    validate_after_sync: bool = True


@dataclass
class SyncResult:
    """Result of sync operation."""

    sync_id: str
    mode: SyncMode
    source_table: str
    target_table: str
    records_processed: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_deleted: int = 0
    records_unchanged: int = 0
    validation_passed: bool = True
    errors: list[str] = field(default_factory=list)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "sync_id": self.sync_id,
            "mode": self.mode.value,
            "source_table": self.source_table,
            "target_table": self.target_table,
            "records_processed": self.records_processed,
            "records_inserted": self.records_inserted,
            "records_updated": self.records_updated,
            "records_deleted": self.records_deleted,
            "records_unchanged": self.records_unchanged,
            "validation_passed": self.validation_passed,
            "error_count": len(self.errors),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
        }


class IncrementalSyncWorkflow:
    """
    Incremental sync workflow for delta updates.

    Supports:
    - Full refresh
    - Incremental based on timestamp
    - Merge (upsert) operations
    - Post-sync validation

    Example:
        workflow = IncrementalSyncWorkflow()

        result = workflow.execute(
            SyncConfig(
                mode=SyncMode.INCREMENTAL,
                source_table="staging.orders",
                target_table="analytics.dim_orders",
                key_columns=["order_id"],
                timestamp_column="updated_at",
            ),
            source_data=[...],
        )
    """

    def __init__(self):
        """Initialize sync workflow."""
        self._sync_history: list[SyncResult] = []

    def execute(
        self,
        config: SyncConfig,
        source_data: list[dict[str, Any]] | None = None,
        target_data: list[dict[str, Any]] | None = None,
    ) -> SyncResult:
        """
        Execute sync workflow.

        Args:
            config: Sync configuration
            source_data: Source records (for offline mode)
            target_data: Target records (for offline mode)

        Returns:
            SyncResult
        """
        import uuid

        result = SyncResult(
            sync_id=str(uuid.uuid4())[:8],
            mode=config.mode,
            source_table=config.source_table,
            target_table=config.target_table,
            started_at=datetime.now(),
        )

        try:
            if config.mode == SyncMode.FULL:
                self._full_sync(config, source_data, target_data, result)
            elif config.mode == SyncMode.INCREMENTAL:
                self._incremental_sync(config, source_data, target_data, result)
            elif config.mode == SyncMode.MERGE:
                self._merge_sync(config, source_data, target_data, result)

            # Validate if requested
            if config.validate_after_sync:
                self._validate_sync(config, source_data, target_data, result)

        except Exception as e:
            result.errors.append(str(e))
            result.validation_passed = False

        result.completed_at = datetime.now()
        if result.started_at:
            result.duration_seconds = (result.completed_at - result.started_at).total_seconds()

        self._sync_history.append(result)
        return result

    def _full_sync(
        self,
        config: SyncConfig,
        source_data: list[dict[str, Any]] | None,
        target_data: list[dict[str, Any]] | None,
        result: SyncResult,
    ) -> None:
        """Execute full sync (truncate and load)."""
        if source_data is None:
            result.errors.append("No source data provided")
            return

        # Full refresh - all source records are inserted
        result.records_processed = len(source_data)
        result.records_inserted = len(source_data)
        result.records_deleted = len(target_data) if target_data else 0

    def _incremental_sync(
        self,
        config: SyncConfig,
        source_data: list[dict[str, Any]] | None,
        target_data: list[dict[str, Any]] | None,
        result: SyncResult,
    ) -> None:
        """Execute incremental sync based on timestamp."""
        if source_data is None:
            result.errors.append("No source data provided")
            return

        # Filter by timestamp if configured
        if config.timestamp_column and config.last_sync_timestamp:
            filtered = [
                r for r in source_data
                if r.get(config.timestamp_column) and
                r.get(config.timestamp_column) > config.last_sync_timestamp
            ]
        else:
            filtered = source_data

        result.records_processed = len(filtered)

        # Determine inserts vs updates
        if target_data and config.key_columns:
            target_keys = set(
                tuple(r.get(k) for k in config.key_columns)
                for r in target_data
            )

            for record in filtered:
                record_key = tuple(record.get(k) for k in config.key_columns)
                if record_key in target_keys:
                    result.records_updated += 1
                else:
                    result.records_inserted += 1
        else:
            result.records_inserted = len(filtered)

    def _merge_sync(
        self,
        config: SyncConfig,
        source_data: list[dict[str, Any]] | None,
        target_data: list[dict[str, Any]] | None,
        result: SyncResult,
    ) -> None:
        """Execute merge (upsert) sync."""
        if source_data is None:
            result.errors.append("No source data provided")
            return

        result.records_processed = len(source_data)

        if not target_data:
            result.records_inserted = len(source_data)
            return

        if not config.key_columns:
            result.errors.append("Key columns required for merge")
            return

        # Build target key index
        target_index: dict[tuple, dict] = {}
        for record in target_data:
            key = tuple(record.get(k) for k in config.key_columns)
            target_index[key] = record

        # Process source records
        for record in source_data:
            key = tuple(record.get(k) for k in config.key_columns)

            if key in target_index:
                # Check if changed
                target_record = target_index[key]
                if self._records_differ(record, target_record, config.key_columns):
                    result.records_updated += 1
                else:
                    result.records_unchanged += 1
            else:
                result.records_inserted += 1

    def _validate_sync(
        self,
        config: SyncConfig,
        source_data: list[dict[str, Any]] | None,
        target_data: list[dict[str, Any]] | None,
        result: SyncResult,
    ) -> None:
        """Validate sync results."""
        # Basic validation: check record counts make sense
        total_changes = result.records_inserted + result.records_updated + result.records_deleted
        if total_changes > result.records_processed * 2:
            result.errors.append("Unexpected change count")
            result.validation_passed = False

        # For incremental, should have some records
        if config.mode == SyncMode.INCREMENTAL and result.records_processed == 0:
            # This might be OK if no changes
            pass

    def _records_differ(
        self,
        record1: dict[str, Any],
        record2: dict[str, Any],
        exclude_columns: list[str],
    ) -> bool:
        """Check if two records differ (excluding key columns)."""
        for key, value in record1.items():
            if key in exclude_columns:
                continue
            if key in record2 and record2[key] != value:
                return True
        return False

    def get_history(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get sync history."""
        return [r.to_dict() for r in self._sync_history[-limit:]]
