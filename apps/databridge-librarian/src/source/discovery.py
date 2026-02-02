"""
Source Discovery Service - Orchestrates the full discovery pipeline.

Wires together:
- SchemaScanner from discovery library
- SourceAnalyzer for entity inference
- SourceModelStore for persistence
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from .models import (
    CanonicalModel,
    SourceTable,
    SourceColumn,
    SourceEntity,
    SourceRelationship,
    EntityType,
    RelationshipType,
    ColumnRole,
)
from .analyzer import SourceAnalyzer
from .store import SourceModelStore


class DiscoveryPhase(str, Enum):
    """Phases of the source discovery process."""

    CONNECTING = "connecting"
    SCANNING = "scanning"
    ANALYZING = "analyzing"
    INFERRING_ENTITIES = "inferring_entities"
    DETECTING_RELATIONSHIPS = "detecting_relationships"
    CONSOLIDATING = "consolidating"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class DiscoveryProgress:
    """Tracks progress of a discovery operation."""

    phase: DiscoveryPhase = DiscoveryPhase.CONNECTING
    phase_progress: float = 0.0
    total_tables: int = 0
    tables_processed: int = 0
    total_columns: int = 0
    entities_found: int = 0
    relationships_found: int = 0
    started_at: datetime = field(default_factory=datetime.utcnow)
    current_table: str = ""
    error_message: str = ""

    @property
    def overall_progress(self) -> float:
        """Calculate overall progress (0.0 to 1.0)."""
        phase_weights = {
            DiscoveryPhase.CONNECTING: 0.05,
            DiscoveryPhase.SCANNING: 0.25,
            DiscoveryPhase.ANALYZING: 0.35,
            DiscoveryPhase.INFERRING_ENTITIES: 0.15,
            DiscoveryPhase.DETECTING_RELATIONSHIPS: 0.15,
            DiscoveryPhase.CONSOLIDATING: 0.05,
            DiscoveryPhase.COMPLETED: 1.0,
            DiscoveryPhase.FAILED: 0.0,
        }

        # Sum weights of completed phases
        completed = 0.0
        for p, w in phase_weights.items():
            if self._phase_order(p) < self._phase_order(self.phase):
                completed += w
            elif p == self.phase:
                completed += w * self.phase_progress
                break

        return min(completed, 1.0)

    def _phase_order(self, phase: DiscoveryPhase) -> int:
        """Get ordering index of a phase."""
        order = [
            DiscoveryPhase.CONNECTING,
            DiscoveryPhase.SCANNING,
            DiscoveryPhase.ANALYZING,
            DiscoveryPhase.INFERRING_ENTITIES,
            DiscoveryPhase.DETECTING_RELATIONSHIPS,
            DiscoveryPhase.CONSOLIDATING,
            DiscoveryPhase.COMPLETED,
        ]
        return order.index(phase) if phase in order else -1

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "phase": self.phase.value,
            "phase_progress": round(self.phase_progress, 2),
            "overall_progress": round(self.overall_progress, 2),
            "total_tables": self.total_tables,
            "tables_processed": self.tables_processed,
            "total_columns": self.total_columns,
            "entities_found": self.entities_found,
            "relationships_found": self.relationships_found,
            "started_at": self.started_at.isoformat(),
            "current_table": self.current_table,
            "error_message": self.error_message,
            "elapsed_seconds": (datetime.now(timezone.utc) - self.started_at).total_seconds(),
        }


@dataclass
class DiscoveryConfig:
    """Configuration for source discovery."""

    # Schema options
    include_views: bool = True
    include_system_tables: bool = False
    table_filter: str = ""  # Regex pattern to filter tables

    # Analysis options
    sample_size: int = 100  # Sample rows for profiling
    detect_scd: bool = True  # Detect SCD Type 2 patterns
    infer_relationships: bool = True

    # Quality thresholds
    min_entity_confidence: float = 0.5
    min_relationship_confidence: float = 0.5

    # Entity type overrides (table_name -> EntityType)
    entity_overrides: Dict[str, EntityType] = field(default_factory=dict)


@dataclass
class DiscoveryResult:
    """Result of a source discovery operation."""

    model_id: str
    model_name: str
    status: str  # "completed", "failed", "partial"
    progress: DiscoveryProgress

    # Summary stats
    tables_discovered: int = 0
    columns_discovered: int = 0
    entities_inferred: int = 0
    relationships_inferred: int = 0

    # Quality metrics
    high_confidence_entities: int = 0
    low_confidence_entities: int = 0
    tables_needing_review: List[str] = field(default_factory=list)

    # Timing
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    duration_seconds: float = 0.0

    # Errors
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "model_id": self.model_id,
            "model_name": self.model_name,
            "status": self.status,
            "progress": self.progress.to_dict(),
            "tables_discovered": self.tables_discovered,
            "columns_discovered": self.columns_discovered,
            "entities_inferred": self.entities_inferred,
            "relationships_inferred": self.relationships_inferred,
            "high_confidence_entities": self.high_confidence_entities,
            "low_confidence_entities": self.low_confidence_entities,
            "tables_needing_review": self.tables_needing_review,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "errors": self.errors,
            "warnings": self.warnings,
        }


class SourceDiscoveryService:
    """
    Orchestrates the full source discovery pipeline.

    Integrates:
    - Database adapters for connection
    - SchemaScanner for metadata extraction
    - SourceAnalyzer for entity inference
    - SourceModelStore for persistence

    Example:
        from src.connections.adapters import SnowflakeAdapter

        adapter = SnowflakeAdapter(
            account="xxx",
            username="user",
            password="pass",
            warehouse="COMPUTE_WH",
        )

        service = SourceDiscoveryService(store=store)

        result = service.discover(
            adapter=adapter,
            database="ANALYTICS",
            schema="RAW",
            model_name="Analytics Discovery",
        )

        print(f"Discovered {result.tables_discovered} tables")
        print(f"Inferred {result.entities_inferred} entities")
    """

    def __init__(
        self,
        store: Optional[SourceModelStore] = None,
        config: Optional[DiscoveryConfig] = None,
    ):
        """
        Initialize the discovery service.

        Args:
            store: Model store for persistence. Creates default if None.
            config: Discovery configuration.
        """
        self.store = store or SourceModelStore()
        self.config = config or DiscoveryConfig()
        self._progress: Optional[DiscoveryProgress] = None
        self._progress_callback: Optional[Callable[[DiscoveryProgress], None]] = None

    def set_progress_callback(
        self,
        callback: Callable[[DiscoveryProgress], None],
    ) -> None:
        """
        Set a callback for progress updates.

        Args:
            callback: Function called with DiscoveryProgress on updates.
        """
        self._progress_callback = callback

    def _update_progress(
        self,
        phase: Optional[DiscoveryPhase] = None,
        phase_progress: Optional[float] = None,
        current_table: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Update progress and notify callback."""
        if not self._progress:
            return

        if phase is not None:
            self._progress.phase = phase
        if phase_progress is not None:
            self._progress.phase_progress = phase_progress
        if current_table is not None:
            self._progress.current_table = current_table

        for key, value in kwargs.items():
            if hasattr(self._progress, key):
                setattr(self._progress, key, value)

        if self._progress_callback:
            self._progress_callback(self._progress)

    def discover(
        self,
        adapter,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        model_name: str = "Discovered Model",
        model_description: str = "",
        connection_id: Optional[str] = None,
    ) -> DiscoveryResult:
        """
        Run full source discovery on a database schema.

        Args:
            adapter: Database adapter (must implement AbstractDatabaseAdapter).
            database: Database to scan.
            schema: Schema to scan.
            model_name: Name for the resulting model.
            model_description: Description for the model.
            connection_id: Optional connection ID for tracking.

        Returns:
            DiscoveryResult with the discovered model and statistics.
        """
        # Initialize progress
        self._progress = DiscoveryProgress()
        result = DiscoveryResult(
            model_id="",
            model_name=model_name,
            status="running",
            progress=self._progress,
        )

        try:
            # Phase 1: Connect
            self._update_progress(phase=DiscoveryPhase.CONNECTING, phase_progress=0.0)

            if not adapter.connect():
                raise RuntimeError("Failed to connect to database")

            self._update_progress(phase_progress=1.0)

            # Phase 2: Scan schema
            self._update_progress(phase=DiscoveryPhase.SCANNING, phase_progress=0.0)

            tables_info = adapter.list_tables(database=database, schema=schema)
            self._progress.total_tables = len(tables_info)

            self._update_progress(phase_progress=0.5)

            # Phase 3: Analyze tables
            self._update_progress(phase=DiscoveryPhase.ANALYZING, phase_progress=0.0)

            analyzer = SourceAnalyzer(adapter)
            model = analyzer.analyze_schema(
                database=database,
                schema=schema,
                name=model_name,
                description=model_description,
                include_views=self.config.include_views,
                sample_size=self.config.sample_size,
            )

            # Set connection info
            model.connection_id = connection_id
            model.connection_name = adapter.name if hasattr(adapter, "name") else ""

            # Update progress with table counts
            self._progress.tables_processed = len(model.tables)
            self._progress.total_columns = sum(len(t.columns) for t in model.tables)

            self._update_progress(phase_progress=1.0)

            # Phase 4: Entity inference (already done by analyzer)
            self._update_progress(
                phase=DiscoveryPhase.INFERRING_ENTITIES,
                phase_progress=1.0,
                entities_found=len(model.entities),
            )

            # Phase 5: Relationship detection (already done by analyzer)
            self._update_progress(
                phase=DiscoveryPhase.DETECTING_RELATIONSHIPS,
                phase_progress=1.0,
                relationships_found=len(model.relationships),
            )

            # Phase 6: Consolidate and save
            self._update_progress(phase=DiscoveryPhase.CONSOLIDATING, phase_progress=0.0)

            # Apply entity overrides
            self._apply_entity_overrides(model)

            # Filter by confidence thresholds
            self._apply_confidence_filters(model, result)

            # Save to store
            self.store.save_model(model)

            self._update_progress(phase_progress=1.0)

            # Complete
            self._update_progress(phase=DiscoveryPhase.COMPLETED, phase_progress=1.0)

            result.model_id = model.id
            result.status = "completed"
            result.tables_discovered = len(model.tables)
            result.columns_discovered = sum(len(t.columns) for t in model.tables)
            result.entities_inferred = len(model.entities)
            result.relationships_inferred = len(model.relationships)
            result.completed_at = datetime.now(timezone.utc)
            result.duration_seconds = (
                result.completed_at - result.started_at
            ).total_seconds()

        except Exception as e:
            self._update_progress(
                phase=DiscoveryPhase.FAILED,
                error_message=str(e),
            )
            result.status = "failed"
            result.errors.append(str(e))

        finally:
            # Disconnect adapter
            try:
                adapter.disconnect()
            except Exception:
                pass

        return result

    def discover_from_metadata(
        self,
        tables: List[Dict[str, Any]],
        model_name: str = "Discovered Model",
        model_description: str = "",
    ) -> DiscoveryResult:
        """
        Run discovery from pre-extracted metadata (offline mode).

        Useful when you have metadata from SchemaScanner or other sources
        and want to run entity/relationship inference.

        Args:
            tables: List of table metadata dictionaries with columns.
            model_name: Name for the resulting model.
            model_description: Description for the model.

        Returns:
            DiscoveryResult with the discovered model.
        """
        # Initialize progress
        self._progress = DiscoveryProgress()
        result = DiscoveryResult(
            model_id="",
            model_name=model_name,
            status="running",
            progress=self._progress,
        )

        try:
            self._progress.total_tables = len(tables)

            # Phase: Analyzing
            self._update_progress(phase=DiscoveryPhase.ANALYZING, phase_progress=0.0)

            # Create model
            model = CanonicalModel(
                name=model_name,
                description=model_description,
            )

            # Convert metadata to SourceTable objects
            for i, table_data in enumerate(tables):
                table = self._metadata_to_source_table(table_data)
                model.tables.append(table)

                self._update_progress(
                    phase_progress=(i + 1) / len(tables),
                    current_table=table.name,
                    tables_processed=i + 1,
                )

            self._progress.total_columns = sum(len(t.columns) for t in model.tables)

            # Phase: Entity inference
            self._update_progress(phase=DiscoveryPhase.INFERRING_ENTITIES, phase_progress=0.0)

            analyzer = SourceAnalyzer()
            model.entities = analyzer._infer_entities(model.tables)

            self._update_progress(
                phase_progress=1.0,
                entities_found=len(model.entities),
            )

            # Phase: Relationship detection
            self._update_progress(phase=DiscoveryPhase.DETECTING_RELATIONSHIPS, phase_progress=0.0)

            model.relationships = analyzer._infer_relationships(model.tables)

            self._update_progress(
                phase_progress=1.0,
                relationships_found=len(model.relationships),
            )

            # Phase: Consolidate
            self._update_progress(phase=DiscoveryPhase.CONSOLIDATING, phase_progress=0.0)

            self._apply_entity_overrides(model)
            self._apply_confidence_filters(model, result)

            # Save
            model.analyzed_at = datetime.now(timezone.utc)
            self.store.save_model(model)

            self._update_progress(phase_progress=1.0)

            # Complete
            self._update_progress(phase=DiscoveryPhase.COMPLETED, phase_progress=1.0)

            result.model_id = model.id
            result.status = "completed"
            result.tables_discovered = len(model.tables)
            result.columns_discovered = sum(len(t.columns) for t in model.tables)
            result.entities_inferred = len(model.entities)
            result.relationships_inferred = len(model.relationships)
            result.completed_at = datetime.now(timezone.utc)
            result.duration_seconds = (
                result.completed_at - result.started_at
            ).total_seconds()

        except Exception as e:
            self._update_progress(
                phase=DiscoveryPhase.FAILED,
                error_message=str(e),
            )
            result.status = "failed"
            result.errors.append(str(e))

        return result

    def _metadata_to_source_table(self, table_data: Dict[str, Any]) -> SourceTable:
        """Convert metadata dictionary to SourceTable."""
        table = SourceTable(
            name=table_data.get("name", ""),
            schema=table_data.get("schema", ""),
            database=table_data.get("database", ""),
            table_type=table_data.get("table_type", "TABLE"),
            row_count=table_data.get("row_count"),
        )

        # Convert columns
        for col_data in table_data.get("columns", []):
            col = SourceColumn(
                name=col_data.get("name", ""),
                data_type=col_data.get("data_type", "VARCHAR"),
                source_table=table.name,
                source_schema=table.schema,
                source_database=table.database,
                nullable=col_data.get("nullable", True),
                is_primary_key=col_data.get("is_primary_key", False),
                is_foreign_key=col_data.get("is_foreign_key", False),
                foreign_key_reference=col_data.get("foreign_key_ref"),
            )

            # Infer role from data type and name
            analyzer = SourceAnalyzer()
            col.role, col.confidence = analyzer._infer_column_role(
                col.name, col.data_type
            )

            # Infer entity type
            entity_type, entity_conf = analyzer._infer_entity_type(col.name)
            if entity_type:
                col.entity_type = entity_type
                col.confidence = max(col.confidence, entity_conf)

            table.columns.append(col)

        # Infer table entity type
        analyzer = SourceAnalyzer()
        table.entity_type, table.confidence = analyzer._infer_table_entity_type(table)
        table.scd_type = analyzer._detect_scd_type(table)

        return table

    def _apply_entity_overrides(self, model: CanonicalModel) -> None:
        """Apply configured entity type overrides."""
        for table in model.tables:
            if table.name in self.config.entity_overrides:
                table.user_entity_type = self.config.entity_overrides[table.name]
                table.confidence = 1.0

    def _apply_confidence_filters(
        self,
        model: CanonicalModel,
        result: DiscoveryResult,
    ) -> None:
        """Apply confidence thresholds and populate quality metrics."""
        # Count entities by confidence
        for entity in model.entities:
            if entity.confidence >= self.config.min_entity_confidence:
                result.high_confidence_entities += 1
            else:
                result.low_confidence_entities += 1

        # Flag tables needing review
        for table in model.tables:
            if table.confidence < self.config.min_entity_confidence:
                result.tables_needing_review.append(table.name)
            elif not table.entity_type:
                result.tables_needing_review.append(table.name)

        # Add warnings for low confidence items
        if result.low_confidence_entities > 0:
            result.warnings.append(
                f"{result.low_confidence_entities} entities have confidence below "
                f"{self.config.min_entity_confidence}"
            )

        if result.tables_needing_review:
            result.warnings.append(
                f"{len(result.tables_needing_review)} tables need manual review"
            )

    def get_discovery_status(self) -> Optional[Dict[str, Any]]:
        """
        Get current discovery progress.

        Returns:
            Progress dictionary or None if no discovery in progress.
        """
        if self._progress:
            return self._progress.to_dict()
        return None
