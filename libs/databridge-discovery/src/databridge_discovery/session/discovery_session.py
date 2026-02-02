"""
Discovery Session management for coordinating discovery workflows.

This module provides the main DiscoverySession class that orchestrates
SQL parsing, CASE extraction, hierarchy detection, and export operations.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from databridge_discovery.models.case_statement import CaseStatement, ExtractedHierarchy
from databridge_discovery.models.parsed_query import ParsedQuery
from databridge_discovery.models.session_state import (
    DiscoveryEvidence,
    DiscoverySessionState,
    EvidenceType,
    ProposedHierarchy,
    SessionSource,
    SessionStatus,
    SourceType,
)
from databridge_discovery.parser.case_extractor import CaseExtractor
from databridge_discovery.parser.column_resolver import ColumnResolver
from databridge_discovery.parser.sql_parser import SQLParser
from databridge_discovery.session.result_cache import ResultCache


class DiscoverySession:
    """
    Main class for managing discovery sessions.

    A discovery session tracks all sources analyzed, CASE statements found,
    proposed hierarchies, and user decisions. Sessions can be persisted
    to SQLite for resumption.

    Example:
        session = DiscoverySession()
        session.add_sql_file("path/to/queries.sql")
        session.analyze()

        for proposal in session.get_proposed_hierarchies():
            print(f"Proposed: {proposal.name}")

        session.approve_hierarchy(proposal.id)
        session.export_to_librarian("output/")
    """

    def __init__(
        self,
        session_id: str | None = None,
        name: str = "Untitled Discovery",
        dialect: str = "snowflake",
        persist_path: str | None = None,
        cache_enabled: bool = True,
    ):
        """
        Initialize a discovery session.

        Args:
            session_id: Existing session ID to resume, or None for new
            name: Session name
            dialect: SQL dialect for parsing
            persist_path: Path to SQLite for persistence
            cache_enabled: Enable result caching
        """
        self.dialect = dialect
        self.persist_path = persist_path

        # Initialize components
        self._parser = SQLParser(dialect=dialect)
        self._case_extractor = CaseExtractor(dialect=dialect)
        self._column_resolver = ColumnResolver(dialect=dialect)

        # Cache
        self._cache = ResultCache(persist_path=persist_path) if cache_enabled else None

        # Thread safety
        self._lock = threading.RLock()

        # Initialize or load state
        if session_id and persist_path:
            self._state = self._load_state(session_id, persist_path)
        else:
            self._state = DiscoverySessionState(
                id=session_id or str(uuid4()),
                name=name,
                target_dialect=dialect,
            )

        # Store parsed queries and case statements
        self._parsed_queries: dict[str, ParsedQuery] = {}
        self._case_statements: dict[str, CaseStatement] = {}
        self._extracted_hierarchies: dict[str, ExtractedHierarchy] = {}

    @property
    def id(self) -> str:
        """Get session ID."""
        return self._state.id

    @property
    def state(self) -> DiscoverySessionState:
        """Get session state."""
        return self._state

    @property
    def status(self) -> SessionStatus:
        """Get session status."""
        return self._state.status

    def add_sql_source(
        self,
        sql: str,
        source_name: str = "inline_sql",
    ) -> SessionSource:
        """
        Add SQL query as a source for analysis.

        Args:
            sql: SQL query string
            source_name: Name for this source

        Returns:
            SessionSource object
        """
        with self._lock:
            # Generate content hash
            content_hash = hashlib.sha256(sql.encode()).hexdigest()[:16]

            source = SessionSource(
                source_type=SourceType.SQL_QUERY,
                source_name=source_name,
                content_hash=content_hash,
                content_preview=sql[:500] if sql else None,
            )

            self._state.add_source(source)

            # Parse immediately
            parsed = self._parser.parse(sql)
            self._parsed_queries[source.id] = parsed

            return source

    def add_sql_file(self, file_path: str) -> SessionSource:
        """
        Add SQL file as a source for analysis.

        Args:
            file_path: Path to SQL file

        Returns:
            SessionSource object
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"SQL file not found: {file_path}")

        sql = path.read_text(encoding="utf-8")

        with self._lock:
            content_hash = hashlib.sha256(sql.encode()).hexdigest()[:16]

            source = SessionSource(
                source_type=SourceType.SQL_FILE,
                source_name=path.name,
                source_path=str(path.absolute()),
                content_hash=content_hash,
                content_preview=sql[:500],
            )

            self._state.add_source(source)

            # Parse the file (may contain multiple statements)
            parsed_list = self._parser.parse_multiple(sql)
            for idx, parsed in enumerate(parsed_list):
                key = f"{source.id}_{idx}"
                self._parsed_queries[key] = parsed

            return source

    def add_csv_source(
        self,
        file_path: str,
        has_header: bool = True,
    ) -> SessionSource:
        """
        Add CSV file as a source for analysis.

        Args:
            file_path: Path to CSV file
            has_header: Whether CSV has header row

        Returns:
            SessionSource object
        """
        import csv

        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        with self._lock:
            # Read file to get metadata
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
                content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]

            # Count rows and columns
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                rows = list(reader)
                row_count = len(rows) - (1 if has_header else 0)
                column_count = len(rows[0]) if rows else 0

            source = SessionSource(
                source_type=SourceType.CSV_FILE,
                source_name=path.name,
                source_path=str(path.absolute()),
                content_hash=content_hash,
                content_preview=content[:500],
                row_count=row_count,
                column_count=column_count,
            )

            self._state.add_source(source)
            return source

    def analyze(self) -> dict[str, Any]:
        """
        Analyze all sources and extract hierarchies.

        Returns:
            Analysis summary with counts and findings
        """
        with self._lock:
            self._state.status = SessionStatus.ANALYZING

            total_cases = 0
            total_hierarchies = 0

            # Process all parsed queries
            for source_key, parsed in self._parsed_queries.items():
                source_id = source_key.split("_")[0]

                # Extract CASE statements
                cases = self._case_extractor.extract_from_sql(parsed.sql)

                for case in cases:
                    self._case_statements[case.id] = case
                    total_cases += 1

                    # Add evidence
                    evidence = DiscoveryEvidence(
                        evidence_type=EvidenceType.CASE_STATEMENT,
                        source_id=source_id,
                        title=f"CASE statement: {case.source_column}",
                        description=f"Detected {case.detected_entity_type.value} entity with {case.condition_count} conditions",
                        raw_content=case.raw_case_sql,
                        confidence=0.8,
                        metadata={
                            "entity_type": case.detected_entity_type.value,
                            "pattern": case.detected_pattern,
                            "condition_count": case.condition_count,
                        },
                    )
                    self._state.add_evidence(evidence)

                    # Extract hierarchy
                    hierarchy = self._case_extractor.extract_hierarchy(case)
                    if hierarchy:
                        self._extracted_hierarchies[hierarchy.id] = hierarchy
                        total_hierarchies += 1

                        # Create proposal
                        proposal = self._hierarchy_to_proposal(hierarchy, source_id)
                        self._state.add_proposal(proposal)

                        # Link evidence to proposal
                        evidence.hierarchy_id = proposal.id

            # Update state
            self._state.case_statements_found = total_cases
            self._state.entity_types_detected = list(
                set(c.detected_entity_type.value for c in self._case_statements.values())
            )
            self._state.status = SessionStatus.REVIEWED

            # Persist if enabled
            if self.persist_path:
                self._save_state()

            return {
                "session_id": self.id,
                "status": self._state.status.value,
                "sources_analyzed": len(self._state.sources),
                "case_statements_found": total_cases,
                "hierarchies_proposed": total_hierarchies,
                "entity_types": self._state.entity_types_detected,
            }

    def _hierarchy_to_proposal(
        self,
        hierarchy: ExtractedHierarchy,
        source_id: str,
    ) -> ProposedHierarchy:
        """Convert ExtractedHierarchy to ProposedHierarchy."""
        # Build level definitions
        levels = []
        for level in hierarchy.levels:
            levels.append({
                "level_number": level.level_number,
                "level_name": level.level_name,
                "value_count": len(level.values),
                "sort_order_map": level.sort_order_map,
            })

        # Build nodes list
        nodes = []
        for level in hierarchy.levels:
            for idx, value in enumerate(level.values):
                nodes.append({
                    "id": f"{hierarchy.id}_{level.level_number}_{idx}",
                    "name": value,
                    "level": level.level_number,
                    "sort_order": level.sort_order_map.get(value, idx),
                })

        return ProposedHierarchy(
            name=hierarchy.name,
            description=f"Extracted from CASE statement on {hierarchy.source_column}",
            source_id=source_id,
            source_case_id=hierarchy.source_case_id,
            source_column=hierarchy.source_column,
            level_count=hierarchy.total_levels,
            node_count=hierarchy.total_nodes,
            levels=levels,
            nodes=nodes,
            detected_entity_type=hierarchy.entity_type.value,
            entity_confidence=hierarchy.confidence_score,
        )

    def get_proposed_hierarchies(self) -> list[ProposedHierarchy]:
        """
        Get all proposed hierarchies.

        Returns:
            List of ProposedHierarchy objects
        """
        return self._state.proposed_hierarchies

    def get_pending_proposals(self) -> list[ProposedHierarchy]:
        """
        Get proposals that haven't been reviewed.

        Returns:
            List of pending ProposedHierarchy objects
        """
        return self._state.get_pending_proposals()

    def approve_hierarchy(self, hierarchy_id: str) -> bool:
        """
        Approve a proposed hierarchy.

        Args:
            hierarchy_id: ID of hierarchy to approve

        Returns:
            True if approved successfully
        """
        with self._lock:
            result = self._state.approve_hierarchy(hierarchy_id)
            if result and self.persist_path:
                self._save_state()
            return result

    def reject_hierarchy(self, hierarchy_id: str, reason: str | None = None) -> bool:
        """
        Reject a proposed hierarchy.

        Args:
            hierarchy_id: ID of hierarchy to reject
            reason: Optional rejection reason

        Returns:
            True if rejected successfully
        """
        with self._lock:
            result = self._state.reject_hierarchy(hierarchy_id, reason)
            if result and self.persist_path:
                self._save_state()
            return result

    def get_case_statements(self) -> list[CaseStatement]:
        """
        Get all extracted CASE statements.

        Returns:
            List of CaseStatement objects
        """
        return list(self._case_statements.values())

    def get_evidence(self, hierarchy_id: str | None = None) -> list[DiscoveryEvidence]:
        """
        Get collected evidence.

        Args:
            hierarchy_id: Filter by hierarchy ID (optional)

        Returns:
            List of DiscoveryEvidence objects
        """
        if hierarchy_id:
            return self._state.get_evidence_for_hierarchy(hierarchy_id)
        return self._state.evidence

    def export_to_librarian_csv(self, output_dir: str) -> dict[str, str]:
        """
        Export approved hierarchies to Librarian-compatible CSV files.

        Args:
            output_dir: Directory to write CSV files

        Returns:
            Dictionary of generated file paths
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        files_created: dict[str, str] = {}

        for proposal in self._state.get_approved_proposals():
            # Generate HIERARCHY CSV
            hierarchy_file = output_path / f"{proposal.name}_HIERARCHY.CSV"
            hierarchy_rows = self._proposal_to_hierarchy_csv(proposal)
            self._write_csv(hierarchy_file, hierarchy_rows)
            files_created[f"{proposal.name}_hierarchy"] = str(hierarchy_file)

            # Generate MAPPING CSV if we have the original case statement
            if proposal.source_case_id and proposal.source_case_id in self._case_statements:
                case_stmt = self._case_statements[proposal.source_case_id]
                mapping_file = output_path / f"{proposal.name}_HIERARCHY_MAPPING.CSV"
                mapping_rows = self._case_to_mapping_csv(case_stmt, proposal)
                self._write_csv(mapping_file, mapping_rows)
                files_created[f"{proposal.name}_mapping"] = str(mapping_file)

        # Track export
        self._state.exports.append({
            "type": "librarian_csv",
            "output_dir": str(output_path),
            "files": files_created,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        self._state.last_export_at = datetime.now(timezone.utc)

        if self.persist_path:
            self._save_state()

        return files_created

    def _proposal_to_hierarchy_csv(self, proposal: ProposedHierarchy) -> list[dict[str, Any]]:
        """Convert proposal to Librarian hierarchy CSV rows."""
        rows = []

        for node in proposal.nodes:
            row = {
                "HIERARCHY_ID": node["id"],
                "HIERARCHY_NAME": node["name"],
                "PARENT_ID": None,  # Would need parent-child detection
                "DESCRIPTION": None,
                "INCLUDE_FLAG": True,
                "EXCLUDE_FLAG": False,
                "FORMULA_GROUP": None,
                "SORT_ORDER": node.get("sort_order", 0),
            }

            # Add level columns
            level = node.get("level", 1)
            row[f"LEVEL_{level}"] = node["name"]
            row[f"LEVEL_{level}_SORT"] = node.get("sort_order", 0)

            rows.append(row)

        return rows

    def _case_to_mapping_csv(
        self,
        case_stmt: CaseStatement,
        proposal: ProposedHierarchy,
    ) -> list[dict[str, Any]]:
        """Convert CASE statement to Librarian mapping CSV rows."""
        rows = []

        for idx, when in enumerate(case_stmt.when_clauses):
            # Find the corresponding hierarchy node
            hierarchy_id = None
            for node in proposal.nodes:
                if node["name"] == when.result_value:
                    hierarchy_id = node["id"]
                    break

            if not hierarchy_id:
                continue

            for value in when.condition.values:
                row = {
                    "HIERARCHY_ID": hierarchy_id,
                    "MAPPING_INDEX": idx,
                    "SOURCE_DATABASE": None,
                    "SOURCE_SCHEMA": None,
                    "SOURCE_TABLE": case_stmt.input_table,
                    "SOURCE_COLUMN": case_stmt.input_column,
                    "SOURCE_UID": value,
                    "PRECEDENCE_GROUP": 1,
                    "INCLUDE_FLAG": True,
                    "EXCLUDE_FLAG": False,
                }
                rows.append(row)

        return rows

    def _write_csv(self, path: Path, rows: list[dict[str, Any]]) -> None:
        """Write rows to CSV file."""
        import csv

        if not rows:
            return

        fieldnames = list(rows[0].keys())

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def export_evidence(self, output_path: str) -> str:
        """
        Export all evidence to JSON file.

        Args:
            output_path: Path to output JSON file

        Returns:
            Path to created file
        """
        evidence_data = {
            "session_id": self.id,
            "session_name": self._state.name,
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "summary": self._state.to_summary(),
            "evidence": [
                {
                    "id": e.id,
                    "type": e.evidence_type.value,
                    "title": e.title,
                    "description": e.description,
                    "confidence": e.confidence,
                    "hierarchy_id": e.hierarchy_id,
                    "raw_content": e.raw_content,
                    "metadata": e.metadata,
                }
                for e in self._state.evidence
            ],
            "case_statements": [
                {
                    "id": c.id,
                    "source_column": c.source_column,
                    "input_column": c.input_column,
                    "entity_type": c.detected_entity_type.value,
                    "pattern": c.detected_pattern,
                    "condition_count": c.condition_count,
                    "unique_results": c.unique_result_values,
                }
                for c in self._case_statements.values()
            ],
        }

        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(evidence_data, f, indent=2, default=str)

        return str(path)

    def get_summary(self) -> dict[str, Any]:
        """
        Get session summary.

        Returns:
            Summary dictionary
        """
        return self._state.to_summary()

    def _save_state(self) -> None:
        """Save session state to SQLite."""
        if not self.persist_path:
            return

        path = Path(self.persist_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        db = sqlite3.connect(self.persist_path)
        db.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                state TEXT,
                updated_at REAL
            )
        """)

        state_json = self._state.model_dump_json()
        db.execute(
            "INSERT OR REPLACE INTO sessions (id, state, updated_at) VALUES (?, ?, ?)",
            (self.id, state_json, datetime.now(timezone.utc).timestamp()),
        )
        db.commit()
        db.close()

    def _load_state(self, session_id: str, persist_path: str) -> DiscoverySessionState:
        """Load session state from SQLite."""
        db = sqlite3.connect(persist_path)
        cursor = db.execute(
            "SELECT state FROM sessions WHERE id = ?",
            (session_id,),
        )
        row = cursor.fetchone()
        db.close()

        if row:
            return DiscoverySessionState.model_validate_json(row[0])

        return DiscoverySessionState(id=session_id)

    @classmethod
    def list_sessions(cls, persist_path: str) -> list[dict[str, Any]]:
        """
        List all saved sessions.

        Args:
            persist_path: Path to SQLite database

        Returns:
            List of session summaries
        """
        path = Path(persist_path)
        if not path.exists():
            return []

        db = sqlite3.connect(persist_path)
        cursor = db.execute("SELECT id, state, updated_at FROM sessions")
        sessions = []

        for row in cursor.fetchall():
            try:
                state = DiscoverySessionState.model_validate_json(row[1])
                sessions.append({
                    "id": row[0],
                    "name": state.name,
                    "status": state.status.value,
                    "updated_at": datetime.fromtimestamp(row[2]).isoformat(),
                    "sources": len(state.sources),
                    "proposals": len(state.proposed_hierarchies),
                })
            except Exception:
                pass

        db.close()
        return sessions
