"""
Pydantic models and types for Data Catalog.

The Data Catalog provides centralized metadata management including:
- Data asset registry (tables, columns, hierarchies)
- Business glossary with term definitions
- Data ownership and stewardship
- Tags and classifications
- Quality metrics and statistics
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from pydantic import BaseModel, Field
import uuid


# =============================================================================
# Enumerations
# =============================================================================

class AssetType(str, Enum):
    """Types of data assets in the catalog."""
    DATABASE = "database"
    SCHEMA = "schema"
    TABLE = "table"
    VIEW = "view"
    COLUMN = "column"
    HIERARCHY = "hierarchy"
    HIERARCHY_PROJECT = "hierarchy_project"
    SEMANTIC_MODEL = "semantic_model"
    DBT_MODEL = "dbt_model"
    DATA_QUALITY_SUITE = "data_quality_suite"
    PIPELINE = "pipeline"
    DASHBOARD = "dashboard"
    REPORT = "report"


class DataClassification(str, Enum):
    """Data sensitivity classifications."""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    PII = "pii"
    PHI = "phi"
    PCI = "pci"


class DataQualityTier(str, Enum):
    """Data quality tiers."""
    GOLD = "gold"          # Production-ready, validated
    SILVER = "silver"      # Curated, some validation
    BRONZE = "bronze"      # Raw, minimal processing
    UNKNOWN = "unknown"


class OwnershipRole(str, Enum):
    """Roles for data ownership."""
    OWNER = "owner"              # Ultimate accountability
    STEWARD = "steward"          # Day-to-day management
    CUSTODIAN = "custodian"      # Technical management
    CONSUMER = "consumer"        # Read access
    CONTRIBUTOR = "contributor"  # Write access


class TermStatus(str, Enum):
    """Status of glossary terms."""
    DRAFT = "draft"
    PENDING_REVIEW = "pending_review"
    APPROVED = "approved"
    DEPRECATED = "deprecated"


# =============================================================================
# Core Asset Models
# =============================================================================

class Tag(BaseModel):
    """A tag for categorizing assets."""
    name: str = Field(..., description="Tag name")
    category: Optional[str] = Field(default=None, description="Tag category")
    color: Optional[str] = Field(default=None, description="Display color")

    model_config = {"extra": "allow"}


class Owner(BaseModel):
    """An owner or steward of a data asset."""
    user_id: str = Field(..., description="User identifier")
    name: str = Field(..., description="Display name")
    email: Optional[str] = Field(default=None)
    role: OwnershipRole = Field(default=OwnershipRole.OWNER)
    assigned_at: datetime = Field(default_factory=datetime.now)

    model_config = {"extra": "allow"}


class QualityMetrics(BaseModel):
    """Quality metrics for a data asset."""
    completeness: Optional[float] = Field(default=None, ge=0, le=100, description="Percentage of non-null values")
    uniqueness: Optional[float] = Field(default=None, ge=0, le=100, description="Percentage of unique values")
    accuracy: Optional[float] = Field(default=None, ge=0, le=100, description="Accuracy score")
    timeliness: Optional[float] = Field(default=None, ge=0, le=100, description="Data freshness score")
    validity: Optional[float] = Field(default=None, ge=0, le=100, description="Percentage passing validation")
    last_measured: Optional[datetime] = Field(default=None)

    model_config = {"extra": "allow"}

    @property
    def overall_score(self) -> Optional[float]:
        """Calculate overall quality score."""
        scores = [s for s in [self.completeness, self.uniqueness, self.accuracy,
                              self.timeliness, self.validity] if s is not None]
        return sum(scores) / len(scores) if scores else None


class ColumnProfile(BaseModel):
    """Profile information for a column."""
    column_name: str = Field(..., description="Column name")
    data_type: str = Field(..., description="Data type")
    nullable: bool = Field(default=True)
    is_primary_key: bool = Field(default=False)
    is_foreign_key: bool = Field(default=False)
    foreign_key_ref: Optional[str] = Field(default=None, description="Referenced table.column")
    description: Optional[str] = Field(default=None)
    sample_values: List[str] = Field(default_factory=list)
    distinct_count: Optional[int] = Field(default=None)
    null_count: Optional[int] = Field(default=None)
    min_value: Optional[str] = Field(default=None)
    max_value: Optional[str] = Field(default=None)
    patterns: List[str] = Field(default_factory=list, description="Detected patterns")
    glossary_term_id: Optional[str] = Field(default=None, description="Linked glossary term")
    tags: List[Tag] = Field(default_factory=list)
    classification: Optional[DataClassification] = Field(default=None)

    model_config = {"extra": "allow"}


class DataAsset(BaseModel):
    """A data asset in the catalog."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(..., description="Asset name")
    asset_type: AssetType = Field(..., description="Type of asset")
    description: Optional[str] = Field(default=None)

    # Location
    database: Optional[str] = Field(default=None)
    schema_name: Optional[str] = Field(default=None, alias="schema")
    fully_qualified_name: Optional[str] = Field(default=None)

    # Metadata
    tags: List[Tag] = Field(default_factory=list)
    classification: DataClassification = Field(default=DataClassification.INTERNAL)
    quality_tier: DataQualityTier = Field(default=DataQualityTier.UNKNOWN)

    # Ownership
    owners: List[Owner] = Field(default_factory=list)

    # Quality
    quality_metrics: Optional[QualityMetrics] = Field(default=None)

    # Columns (for tables/views)
    columns: List[ColumnProfile] = Field(default_factory=list)
    row_count: Optional[int] = Field(default=None)
    size_bytes: Optional[int] = Field(default=None)

    # Relationships
    parent_id: Optional[str] = Field(default=None, description="Parent asset ID")
    upstream_assets: List[str] = Field(default_factory=list, description="Upstream dependency IDs")
    downstream_assets: List[str] = Field(default_factory=list, description="Downstream dependency IDs")

    # Lineage
    lineage_graph_id: Optional[str] = Field(default=None, description="Associated lineage graph")

    # Source tracking
    source_connection_id: Optional[str] = Field(default=None)
    source_system: Optional[str] = Field(default=None)

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    last_scanned_at: Optional[datetime] = Field(default=None)
    last_accessed_at: Optional[datetime] = Field(default=None)

    # Custom metadata
    custom_properties: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "allow", "populate_by_name": True}

    def add_tag(self, tag: Tag) -> None:
        """Add a tag to the asset."""
        if not any(t.name == tag.name for t in self.tags):
            self.tags.append(tag)
            self.updated_at = datetime.now()

    def remove_tag(self, tag_name: str) -> bool:
        """Remove a tag by name."""
        original_len = len(self.tags)
        self.tags = [t for t in self.tags if t.name != tag_name]
        if len(self.tags) < original_len:
            self.updated_at = datetime.now()
            return True
        return False

    def add_owner(self, owner: Owner) -> None:
        """Add an owner to the asset."""
        if not any(o.user_id == owner.user_id for o in self.owners):
            self.owners.append(owner)
            self.updated_at = datetime.now()


# =============================================================================
# Business Glossary Models
# =============================================================================

class GlossaryTerm(BaseModel):
    """A business glossary term."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(..., description="Term name")
    definition: str = Field(..., description="Business definition")

    # Organization
    domain: Optional[str] = Field(default=None, description="Business domain")
    category: Optional[str] = Field(default=None, description="Term category")

    # Status
    status: TermStatus = Field(default=TermStatus.DRAFT)

    # Relationships
    parent_term_id: Optional[str] = Field(default=None, description="Parent term for hierarchy")
    related_term_ids: List[str] = Field(default_factory=list, description="Related terms")
    synonym_ids: List[str] = Field(default_factory=list, description="Synonymous terms")

    # Technical mappings
    linked_asset_ids: List[str] = Field(default_factory=list, description="Linked data assets")
    linked_column_refs: List[str] = Field(default_factory=list, description="database.schema.table.column refs")

    # Ownership
    owner: Optional[Owner] = Field(default=None)
    approved_by: Optional[str] = Field(default=None)
    approved_at: Optional[datetime] = Field(default=None)

    # Examples and context
    examples: List[str] = Field(default_factory=list)
    abbreviations: List[str] = Field(default_factory=list)
    synonyms: List[str] = Field(default_factory=list, description="Text synonyms")

    # Metadata
    tags: List[Tag] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    model_config = {"extra": "allow"}


class GlossaryDomain(BaseModel):
    """A domain/category in the business glossary."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(..., description="Domain name")
    description: Optional[str] = Field(default=None)
    parent_domain_id: Optional[str] = Field(default=None)
    owner: Optional[Owner] = Field(default=None)
    term_count: int = Field(default=0)
    created_at: datetime = Field(default_factory=datetime.now)

    model_config = {"extra": "allow"}


# =============================================================================
# Search and Discovery Models
# =============================================================================

class SearchQuery(BaseModel):
    """A search query for the catalog."""
    query: str = Field(..., description="Search text")
    asset_types: Optional[List[AssetType]] = Field(default=None, description="Filter by asset types")
    tags: Optional[List[str]] = Field(default=None, description="Filter by tag names")
    classifications: Optional[List[DataClassification]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None, description="Filter by owner user_ids")
    databases: Optional[List[str]] = Field(default=None)
    schemas: Optional[List[str]] = Field(default=None)
    quality_tier: Optional[DataQualityTier] = Field(default=None)
    min_quality_score: Optional[float] = Field(default=None, ge=0, le=100)
    include_columns: bool = Field(default=True, description="Search within columns")
    include_glossary: bool = Field(default=True, description="Include glossary terms")
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)

    model_config = {"extra": "allow"}


class SearchResult(BaseModel):
    """A search result item."""
    asset_id: str
    asset_type: AssetType
    name: str
    fully_qualified_name: Optional[str] = None
    description: Optional[str] = None
    match_score: float = Field(default=0.0, description="Relevance score 0-1")
    match_highlights: List[str] = Field(default_factory=list, description="Matched text snippets")
    tags: List[str] = Field(default_factory=list)
    owners: List[str] = Field(default_factory=list)
    quality_tier: Optional[DataQualityTier] = None

    model_config = {"extra": "allow"}


class SearchResults(BaseModel):
    """Search results with pagination."""
    query: str
    total_count: int
    results: List[SearchResult]
    offset: int
    limit: int
    took_ms: int = Field(default=0, description="Search time in milliseconds")

    model_config = {"extra": "allow"}


# =============================================================================
# Scan and Discovery Models
# =============================================================================

class ScanConfig(BaseModel):
    """Configuration for scanning a data source."""
    connection_id: str = Field(..., description="Connection to scan")
    database: Optional[str] = Field(default=None, description="Specific database")
    schema_pattern: Optional[str] = Field(default=None, description="Schema name pattern")
    table_pattern: Optional[str] = Field(default=None, description="Table name pattern")
    include_views: bool = Field(default=True)
    include_columns: bool = Field(default=True)
    profile_columns: bool = Field(default=False, description="Collect column statistics")
    sample_size: int = Field(default=1000, description="Rows to sample for profiling")
    detect_pii: bool = Field(default=True, description="Detect PII columns")
    detect_patterns: bool = Field(default=True, description="Detect data patterns")

    model_config = {"extra": "allow"}


class ScanResult(BaseModel):
    """Result of a catalog scan."""
    scan_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    connection_id: str
    started_at: datetime = Field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    status: str = Field(default="running")  # running, completed, failed

    # Counts
    databases_scanned: int = Field(default=0)
    schemas_scanned: int = Field(default=0)
    tables_scanned: int = Field(default=0)
    columns_scanned: int = Field(default=0)

    # Results
    assets_created: int = Field(default=0)
    assets_updated: int = Field(default=0)
    pii_columns_detected: int = Field(default=0)

    # Errors
    errors: List[str] = Field(default_factory=list)

    model_config = {"extra": "allow"}


# =============================================================================
# Catalog Statistics
# =============================================================================

class CatalogStats(BaseModel):
    """Statistics about the data catalog."""
    total_assets: int = Field(default=0)
    assets_by_type: Dict[str, int] = Field(default_factory=dict)
    assets_by_classification: Dict[str, int] = Field(default_factory=dict)
    assets_by_quality_tier: Dict[str, int] = Field(default_factory=dict)

    total_glossary_terms: int = Field(default=0)
    terms_by_status: Dict[str, int] = Field(default_factory=dict)
    terms_by_domain: Dict[str, int] = Field(default_factory=dict)

    total_tags: int = Field(default=0)
    most_used_tags: List[Dict[str, Any]] = Field(default_factory=list)

    total_owners: int = Field(default=0)
    assets_without_owners: int = Field(default=0)
    assets_without_descriptions: int = Field(default=0)

    last_scan: Optional[datetime] = Field(default=None)
    catalog_updated_at: datetime = Field(default_factory=datetime.now)

    model_config = {"extra": "allow"}
