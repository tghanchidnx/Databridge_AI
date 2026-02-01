"""
Pydantic models for discovery session state management.

These models track the state of a discovery session, including
sources analyzed, proposed hierarchies, and evidence collected.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class SourceType(str, Enum):
    """Types of sources that can be analyzed."""
    SQL_FILE = "sql_file"
    SQL_QUERY = "sql_query"
    CSV_FILE = "csv_file"
    JSON_FILE = "json_file"
    DATABASE_SCHEMA = "database_schema"
    DATABASE_QUERY_RESULT = "database_query_result"


class SessionStatus(str, Enum):
    """Status of a discovery session."""
    CREATED = "created"
    ANALYZING = "analyzing"
    REVIEWED = "reviewed"
    COMMITTED = "committed"
    EXPORTED = "exported"
    FAILED = "failed"


class EvidenceType(str, Enum):
    """Types of evidence collected during discovery."""
    CASE_STATEMENT = "case_statement"
    JOIN_RELATIONSHIP = "join_relationship"
    FOREIGN_KEY = "foreign_key"
    NAMING_PATTERN = "naming_pattern"
    DATA_SAMPLE = "data_sample"
    COLUMN_STATISTICS = "column_statistics"
    USER_ANNOTATION = "user_annotation"


class SessionSource(BaseModel):
    """
    Represents a source analyzed in a discovery session.

    Tracks what was analyzed and when.
    """

    id: str = Field(default_factory=lambda: str(uuid4()), description="Unique source ID")
    source_type: SourceType = Field(..., description="Type of source")
    source_path: str | None = Field(None, description="File path if applicable")
    source_name: str = Field(..., description="Name/identifier of source")

    # For database sources
    database: str | None = Field(None, description="Database name")
    schema_name: str | None = Field(None, description="Schema name")
    table: str | None = Field(None, description="Table name")

    # Content
    content_hash: str | None = Field(None, description="Hash of content for change detection")
    content_preview: str | None = Field(None, description="First 500 chars of content")
    row_count: int | None = Field(None, description="Row count if applicable")
    column_count: int | None = Field(None, description="Column count if applicable")

    # Analysis status
    analyzed_at: datetime | None = Field(None, description="When analysis completed")
    analysis_duration_ms: float = Field(default=0.0, description="Analysis time in ms")
    errors: list[str] = Field(default_factory=list, description="Any errors during analysis")

    model_config = {"extra": "allow"}


class ProposedHierarchy(BaseModel):
    """
    A hierarchy proposed from discovery analysis.

    Users can review, modify, and approve proposed hierarchies
    before committing them to The Librarian.
    """

    id: str = Field(default_factory=lambda: str(uuid4()), description="Unique proposal ID")
    name: str = Field(..., description="Proposed hierarchy name")
    description: str | None = Field(None, description="Description of what this hierarchy represents")

    # Source information
    source_id: str = Field(..., description="ID of source this came from")
    source_case_id: str | None = Field(None, description="CASE statement ID if from SQL")
    source_column: str | None = Field(None, description="Column this is based on")

    # Structure
    level_count: int = Field(default=0, description="Number of hierarchy levels")
    node_count: int = Field(default=0, description="Total nodes in hierarchy")
    levels: list[dict[str, Any]] = Field(default_factory=list, description="Level definitions")
    nodes: list[dict[str, Any]] = Field(default_factory=list, description="Hierarchy nodes")

    # Entity detection
    detected_entity_type: str = Field(default="unknown", description="Detected entity type")
    entity_confidence: float = Field(default=0.0, description="Confidence in entity detection")

    # Review status
    status: str = Field(default="proposed", description="Status: proposed, approved, rejected, modified")
    reviewed_by: str | None = Field(None, description="Who reviewed this")
    reviewed_at: datetime | None = Field(None, description="When it was reviewed")
    user_notes: str | None = Field(None, description="User notes/feedback")

    # Modifications
    user_modifications: dict[str, Any] = Field(default_factory=dict, description="User changes to proposal")

    model_config = {"extra": "allow"}


class DiscoveryEvidence(BaseModel):
    """
    Evidence collected during discovery to support hierarchy proposals.

    Evidence helps users understand why certain hierarchies were proposed.
    """

    id: str = Field(default_factory=lambda: str(uuid4()), description="Evidence ID")
    evidence_type: EvidenceType = Field(..., description="Type of evidence")
    source_id: str = Field(..., description="Source this evidence came from")
    hierarchy_id: str | None = Field(None, description="Hierarchy this supports")

    # Content
    title: str = Field(..., description="Brief title for this evidence")
    description: str = Field(..., description="Detailed description")
    raw_content: str | None = Field(None, description="Raw content (SQL, data sample, etc.)")

    # Scoring
    confidence: float = Field(default=0.0, description="Confidence score (0-1)")
    relevance: float = Field(default=0.0, description="Relevance score (0-1)")

    # Metadata
    collected_at: datetime = Field(default_factory=datetime.utcnow, description="When collected")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    model_config = {"extra": "allow"}


class DiscoverySessionState(BaseModel):
    """
    Complete state of a discovery session.

    This is the main state object that tracks everything about
    an active or completed discovery session.
    """

    id: str = Field(default_factory=lambda: str(uuid4()), description="Session ID")
    name: str = Field(default="Untitled Discovery", description="Session name")
    description: str | None = Field(None, description="Session description")

    # Status
    status: SessionStatus = Field(default=SessionStatus.CREATED, description="Current status")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="When created")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Last update")

    # Sources
    sources: list[SessionSource] = Field(default_factory=list, description="Sources analyzed")
    total_sources: int = Field(default=0, description="Total source count")

    # Proposed hierarchies
    proposed_hierarchies: list[ProposedHierarchy] = Field(
        default_factory=list,
        description="Proposed hierarchies from analysis"
    )
    approved_hierarchies: list[str] = Field(default_factory=list, description="IDs of approved hierarchies")
    rejected_hierarchies: list[str] = Field(default_factory=list, description="IDs of rejected hierarchies")

    # Evidence
    evidence: list[DiscoveryEvidence] = Field(default_factory=list, description="Collected evidence")

    # Analysis results
    case_statements_found: int = Field(default=0, description="Total CASE statements found")
    join_relationships_found: int = Field(default=0, description="Join relationships discovered")
    entity_types_detected: list[str] = Field(default_factory=list, description="Entity types found")

    # User preferences
    target_dialect: str = Field(default="snowflake", description="Target SQL dialect")
    auto_detect_entity_types: bool = Field(default=True, description="Auto-detect entity types")
    min_confidence_threshold: float = Field(default=0.5, description="Min confidence for proposals")

    # Export tracking
    exports: list[dict[str, Any]] = Field(default_factory=list, description="Export history")
    last_export_at: datetime | None = Field(None, description="Last export timestamp")

    model_config = {"extra": "allow"}

    def add_source(self, source: SessionSource) -> None:
        """Add a source to the session."""
        self.sources.append(source)
        self.total_sources = len(self.sources)
        self.updated_at = datetime.utcnow()

    def add_proposal(self, proposal: ProposedHierarchy) -> None:
        """Add a proposed hierarchy to the session."""
        self.proposed_hierarchies.append(proposal)
        self.updated_at = datetime.utcnow()

    def approve_hierarchy(self, hierarchy_id: str) -> bool:
        """Approve a proposed hierarchy."""
        for proposal in self.proposed_hierarchies:
            if proposal.id == hierarchy_id:
                proposal.status = "approved"
                proposal.reviewed_at = datetime.utcnow()
                if hierarchy_id not in self.approved_hierarchies:
                    self.approved_hierarchies.append(hierarchy_id)
                if hierarchy_id in self.rejected_hierarchies:
                    self.rejected_hierarchies.remove(hierarchy_id)
                self.updated_at = datetime.utcnow()
                return True
        return False

    def reject_hierarchy(self, hierarchy_id: str, reason: str | None = None) -> bool:
        """Reject a proposed hierarchy."""
        for proposal in self.proposed_hierarchies:
            if proposal.id == hierarchy_id:
                proposal.status = "rejected"
                proposal.reviewed_at = datetime.utcnow()
                if reason:
                    proposal.user_notes = reason
                if hierarchy_id not in self.rejected_hierarchies:
                    self.rejected_hierarchies.append(hierarchy_id)
                if hierarchy_id in self.approved_hierarchies:
                    self.approved_hierarchies.remove(hierarchy_id)
                self.updated_at = datetime.utcnow()
                return True
        return False

    def get_pending_proposals(self) -> list[ProposedHierarchy]:
        """Get proposals that haven't been reviewed yet."""
        return [
            p for p in self.proposed_hierarchies
            if p.status == "proposed"
        ]

    def get_approved_proposals(self) -> list[ProposedHierarchy]:
        """Get approved proposals."""
        return [
            p for p in self.proposed_hierarchies
            if p.status == "approved"
        ]

    def add_evidence(self, evidence: DiscoveryEvidence) -> None:
        """Add evidence to the session."""
        self.evidence.append(evidence)
        self.updated_at = datetime.utcnow()

    def get_evidence_for_hierarchy(self, hierarchy_id: str) -> list[DiscoveryEvidence]:
        """Get all evidence supporting a specific hierarchy."""
        return [e for e in self.evidence if e.hierarchy_id == hierarchy_id]

    def to_summary(self) -> dict[str, Any]:
        """Return a summary of the session state."""
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "total_sources": self.total_sources,
            "case_statements_found": self.case_statements_found,
            "proposed_hierarchies": len(self.proposed_hierarchies),
            "approved": len(self.approved_hierarchies),
            "rejected": len(self.rejected_hierarchies),
            "pending": len(self.get_pending_proposals()),
            "evidence_count": len(self.evidence),
        }
