"""
Researcher Bridge - Analytics and validation via Researcher API.

This bridge provides analytics capabilities for Book and Librarian data:
- Validate source mappings against live database schemas
- Compare hierarchy data with live database data
- Profile data for mapped columns
- Run schema comparisons

The Researcher API provides schema matching, data comparison, and query execution.
"""

import logging
import requests
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict

logger = logging.getLogger("researcher_bridge")


@dataclass
class SourceMapping:
    """Represents a source mapping extracted from Book/Librarian."""

    database: str
    schema: str
    table: str
    column: str
    uid: Optional[str] = None
    hierarchy_id: Optional[str] = None
    hierarchy_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def full_path(self) -> str:
        """Return fully qualified path: database.schema.table.column"""
        return f"{self.database}.{self.schema}.{self.table}.{self.column}"


@dataclass
class ValidationResult:
    """Result of validating source mappings."""

    valid: List[SourceMapping]
    invalid: List[Dict[str, Any]]  # Mapping with error info
    warnings: List[Dict[str, Any]]
    summary: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "valid": [m.to_dict() for m in self.valid],
            "invalid": self.invalid,
            "warnings": self.warnings,
            "summary": self.summary,
        }


@dataclass
class ComparisonResult:
    """Result of comparing hierarchy data with database data."""

    hierarchy_id: str
    matches: int
    mismatches: int
    orphans_in_hierarchy: int  # Values in hierarchy not in database
    orphans_in_database: int  # Values in database not in hierarchy
    sample_mismatches: List[Dict[str, Any]]
    sample_orphans: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ResearcherBridge:
    """
    Bridge for analytics and validation via the Researcher API.

    The Researcher API (schema-matcher module) provides:
    - Schema comparison between databases
    - Table column analysis
    - Query execution for data comparison
    - Merge script generation

    Usage:
        bridge = ResearcherBridge(base_url="http://localhost:8001/api", api_key="v2-dev-key-1")

        # Validate source mappings
        result = bridge.validate_sources(sources, connection_id)

        # Compare hierarchy data
        comparison = bridge.compare_hierarchy_data(hierarchy_id, connection_id, book_data)

        # Profile source columns
        profile = bridge.profile_sources(sources, connection_id)
    """

    def __init__(self, base_url: str, api_key: str, timeout: int = 60):
        """
        Initialize the Researcher bridge.

        Args:
            base_url: Researcher API base URL (e.g., 'http://localhost:8001/api')
            api_key: API key for authentication
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self.headers = {
            'X-API-Key': api_key,
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {api_key}',  # Some endpoints use Bearer
        }

    # =========================================================================
    # Source Extraction
    # =========================================================================

    def extract_sources_from_book(self, book: Any) -> List[SourceMapping]:
        """
        Extract all source mappings from a Book instance.

        Args:
            book: Book instance

        Returns:
            List of SourceMapping objects
        """
        sources = []

        def extract_from_node(node: Any):
            """Recursively extract sources from a node."""
            # Get mappings from properties
            mappings = node.properties.get("source_mappings", [])
            for m in mappings:
                if m.get("database") and m.get("table") and m.get("column"):
                    sources.append(SourceMapping(
                        database=m.get("database", ""),
                        schema=m.get("schema", ""),
                        table=m.get("table", ""),
                        column=m.get("column", ""),
                        uid=m.get("uid"),
                        hierarchy_id=node.id,
                        hierarchy_name=node.name,
                    ))

            # Process children
            for child in node.children:
                extract_from_node(child)

        # Process all root nodes
        for root in book.root_nodes:
            extract_from_node(root)

        logger.info(f"Extracted {len(sources)} source mappings from Book '{book.name}'")
        return sources

    def extract_sources_from_librarian(
        self,
        hierarchies: List[Dict[str, Any]],
    ) -> List[SourceMapping]:
        """
        Extract all source mappings from Librarian hierarchies.

        Args:
            hierarchies: List of SmartHierarchyMaster records

        Returns:
            List of SourceMapping objects
        """
        sources = []

        for h in hierarchies:
            hier_id = h.get("hierarchyId", h.get("hierarchy_id"))
            hier_name = h.get("hierarchyName", h.get("hierarchy_name"))
            mappings = h.get("mapping", [])

            for m in mappings:
                db = m.get("source_database", "")
                schema = m.get("source_schema", "")
                table = m.get("source_table", "")
                column = m.get("source_column", "")

                if db and table and column:
                    sources.append(SourceMapping(
                        database=db,
                        schema=schema,
                        table=table,
                        column=column,
                        uid=m.get("source_uid"),
                        hierarchy_id=hier_id,
                        hierarchy_name=hier_name,
                    ))

        logger.info(f"Extracted {len(sources)} source mappings from {len(hierarchies)} hierarchies")
        return sources

    # =========================================================================
    # Validation
    # =========================================================================

    def validate_sources(
        self,
        sources: List[SourceMapping],
        connection_id: str,
    ) -> ValidationResult:
        """
        Validate source mappings against a live database connection.

        Args:
            sources: List of source mappings to validate
            connection_id: Database connection ID

        Returns:
            ValidationResult with valid/invalid mappings
        """
        valid = []
        invalid = []
        warnings = []

        # Group sources by table for efficient validation
        table_sources: Dict[str, List[SourceMapping]] = {}
        for s in sources:
            key = f"{s.database}.{s.schema}.{s.table}"
            if key not in table_sources:
                table_sources[key] = []
            table_sources[key].append(s)

        # Validate each table
        for table_key, table_mappings in table_sources.items():
            parts = table_key.split(".")
            if len(parts) != 3:
                for m in table_mappings:
                    invalid.append({
                        **m.to_dict(),
                        "error": f"Invalid table path: {table_key}",
                    })
                continue

            database, schema, table = parts

            # Get table columns from Researcher
            columns_result = self.get_table_columns(
                connection_id=connection_id,
                database=database,
                schema=schema,
                table=table,
            )

            if columns_result.get("error"):
                # Table might not exist
                for m in table_mappings:
                    invalid.append({
                        **m.to_dict(),
                        "error": f"Table not found or not accessible: {table_key}",
                        "details": columns_result.get("message"),
                    })
                continue

            # Extract column names from result
            column_data = columns_result.get("data", columns_result)
            if isinstance(column_data, list):
                available_columns = {
                    c.get("COLUMN_NAME", c.get("column_name", c.get("name", ""))).upper()
                    for c in column_data
                }
            else:
                available_columns = set()

            # Validate each mapping
            for m in table_mappings:
                if m.column.upper() in available_columns:
                    valid.append(m)
                else:
                    # Check for close matches (typos)
                    close_matches = [
                        col for col in available_columns
                        if self._similar(m.column.upper(), col)
                    ]
                    error_info = {
                        **m.to_dict(),
                        "error": f"Column '{m.column}' not found in table",
                        "available_columns": list(available_columns)[:20],  # Limit for readability
                    }
                    if close_matches:
                        error_info["suggestions"] = close_matches
                        warnings.append({
                            "mapping": m.to_dict(),
                            "warning": f"Column '{m.column}' not found, did you mean: {close_matches}?",
                        })
                    invalid.append(error_info)

        # Build summary
        summary = {
            "total_sources": len(sources),
            "valid_count": len(valid),
            "invalid_count": len(invalid),
            "warning_count": len(warnings),
            "tables_checked": len(table_sources),
            "validation_rate": f"{len(valid) / len(sources) * 100:.1f}%" if sources else "N/A",
        }

        return ValidationResult(
            valid=valid,
            invalid=invalid,
            warnings=warnings,
            summary=summary,
        )

    def _similar(self, a: str, b: str, threshold: float = 0.8) -> bool:
        """Check if two strings are similar using simple ratio."""
        if not a or not b:
            return False
        # Simple character overlap ratio
        common = len(set(a) & set(b))
        total = len(set(a) | set(b))
        return common / total >= threshold if total > 0 else False

    # =========================================================================
    # Data Comparison
    # =========================================================================

    def compare_hierarchy_data(
        self,
        hierarchy_id: str,
        connection_id: str,
        hierarchy_values: List[str],
        source_mapping: SourceMapping,
        limit: int = 1000,
    ) -> ComparisonResult:
        """
        Compare hierarchy values with database values.

        Args:
            hierarchy_id: ID of the hierarchy being compared
            connection_id: Database connection ID
            hierarchy_values: List of values from the hierarchy (e.g., UIDs)
            source_mapping: Source mapping defining where to get database values
            limit: Maximum rows to fetch from database

        Returns:
            ComparisonResult with match statistics
        """
        # Build query to get distinct values from source
        query = f"""
        SELECT DISTINCT "{source_mapping.column}" as value
        FROM "{source_mapping.database}"."{source_mapping.schema}"."{source_mapping.table}"
        WHERE "{source_mapping.column}" IS NOT NULL
        LIMIT {limit}
        """

        # Execute query via Researcher
        query_result = self.execute_query(connection_id, query, limit=limit)

        if query_result.get("error"):
            return ComparisonResult(
                hierarchy_id=hierarchy_id,
                matches=0,
                mismatches=0,
                orphans_in_hierarchy=len(hierarchy_values),
                orphans_in_database=0,
                sample_mismatches=[],
                sample_orphans=[{
                    "error": query_result.get("message"),
                    "query": query,
                }],
            )

        # Extract database values
        rows = query_result.get("rows", query_result.get("data", []))
        db_values = set()
        for row in rows:
            val = row.get("value", row.get("VALUE"))
            if val is not None:
                db_values.add(str(val))

        # Compare
        hier_set = set(hierarchy_values)

        matches = hier_set & db_values
        orphans_hier = hier_set - db_values
        orphans_db = db_values - hier_set

        return ComparisonResult(
            hierarchy_id=hierarchy_id,
            matches=len(matches),
            mismatches=0,  # No type mismatches in this simple comparison
            orphans_in_hierarchy=len(orphans_hier),
            orphans_in_database=len(orphans_db),
            sample_mismatches=[],
            sample_orphans=[
                {"source": "hierarchy", "value": v}
                for v in list(orphans_hier)[:10]
            ] + [
                {"source": "database", "value": v}
                for v in list(orphans_db)[:10]
            ],
        )

    # =========================================================================
    # Data Profiling
    # =========================================================================

    def profile_sources(
        self,
        sources: List[SourceMapping],
        connection_id: str,
    ) -> Dict[str, Any]:
        """
        Profile data for source mappings.

        Args:
            sources: Source mappings to profile
            connection_id: Database connection ID

        Returns:
            Profile data including distinct counts, sample values, etc.
        """
        profiles = []

        for source in sources:
            profile = self._profile_single_source(source, connection_id)
            profiles.append(profile)

        return {
            "profiles": profiles,
            "summary": {
                "total_sources": len(sources),
                "profiled": sum(1 for p in profiles if not p.get("error")),
                "failed": sum(1 for p in profiles if p.get("error")),
            },
        }

    def _profile_single_source(
        self,
        source: SourceMapping,
        connection_id: str,
    ) -> Dict[str, Any]:
        """Profile a single source mapping."""
        profile = {
            "source": source.to_dict(),
            "full_path": source.full_path(),
        }

        # Query for basic profile
        query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT "{source.column}") as distinct_count,
            COUNT(CASE WHEN "{source.column}" IS NULL THEN 1 END) as null_count
        FROM "{source.database}"."{source.schema}"."{source.table}"
        """

        result = self.execute_query(connection_id, query, limit=1)

        if result.get("error"):
            profile["error"] = result.get("message")
            return profile

        rows = result.get("rows", result.get("data", []))
        if rows:
            row = rows[0]
            profile["total_rows"] = row.get("total_rows", row.get("TOTAL_ROWS", 0))
            profile["distinct_count"] = row.get("distinct_count", row.get("DISTINCT_COUNT", 0))
            profile["null_count"] = row.get("null_count", row.get("NULL_COUNT", 0))

        # Get sample values
        sample_query = f"""
        SELECT DISTINCT "{source.column}" as value
        FROM "{source.database}"."{source.schema}"."{source.table}"
        WHERE "{source.column}" IS NOT NULL
        LIMIT 10
        """

        sample_result = self.execute_query(connection_id, sample_query, limit=10)
        if not sample_result.get("error"):
            sample_rows = sample_result.get("rows", sample_result.get("data", []))
            profile["sample_values"] = [
                row.get("value", row.get("VALUE"))
                for row in sample_rows
            ]

        return profile

    # =========================================================================
    # HTTP Operations
    # =========================================================================

    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make an HTTP request to the Researcher API."""
        url = f"{self.base_url}{endpoint}"

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=data,
                params=params,
                timeout=self.timeout,
            )

            if response.status_code >= 400:
                return {
                    "error": True,
                    "status_code": response.status_code,
                    "message": response.text,
                }

            return response.json() if response.text else {"success": True}

        except requests.exceptions.ConnectionError:
            return {"error": True, "message": "Researcher backend not reachable"}
        except requests.exceptions.Timeout:
            return {"error": True, "message": "Request timed out"}
        except Exception as e:
            return {"error": True, "message": str(e)}

    def get_table_columns(
        self,
        connection_id: str,
        database: str,
        schema: str,
        table: str,
    ) -> Dict[str, Any]:
        """Get columns for a specific table."""
        return self._request(
            "GET",
            "/schema-matcher/table/columns",
            params={
                "connectionId": connection_id,
                "database": database,
                "schema": schema,
                "table": table,
            },
        )

    def get_tables(
        self,
        connection_id: str,
        database: str,
        schema: str,
    ) -> Dict[str, Any]:
        """Get all tables in a schema."""
        return self._request(
            "GET",
            "/schema-matcher/tables",
            params={
                "connectionId": connection_id,
                "database": database,
                "schema": schema,
            },
        )

    def execute_query(
        self,
        connection_id: str,
        query: str,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """Execute a SQL query via the preview endpoint."""
        return self._request(
            "POST",
            "/smart-hierarchy/preview-query",
            data={
                "connectionId": connection_id,
                "query": query,
                "limit": limit,
            },
        )

    def compare_schemas(
        self,
        source_connection_id: str,
        source_database: str,
        source_schema: str,
        target_connection_id: str,
        target_database: str,
        target_schema: str,
    ) -> Dict[str, Any]:
        """Compare schemas between two connections."""
        return self._request(
            "POST",
            "/schema-matcher/compare",
            data={
                "sourceConnectionId": source_connection_id,
                "sourceDatabase": source_database,
                "sourceSchema": source_schema,
                "targetConnectionId": target_connection_id,
                "targetDatabase": target_database,
                "targetSchema": target_schema,
            },
        )

    def get_comparison_result(self, job_id: str) -> Dict[str, Any]:
        """Get schema comparison result."""
        return self._request("GET", f"/schema-matcher/jobs/{job_id}")

    def list_connections(self) -> List[Dict[str, Any]]:
        """List available database connections."""
        result = self._request("GET", "/connections")
        if isinstance(result, dict) and result.get("error"):
            return []
        if isinstance(result, dict) and "data" in result:
            return result.get("data", [])
        return result if isinstance(result, list) else []

    # =========================================================================
    # Convenience Methods
    # =========================================================================

    def analyze_book(
        self,
        book: Any,
        connection_id: str,
        analysis_type: str = "validate_sources",
    ) -> Dict[str, Any]:
        """
        Analyze a Book using Researcher capabilities.

        Args:
            book: Book instance
            connection_id: Database connection ID for validation
            analysis_type: Type of analysis:
                - "validate_sources": Validate all source mappings
                - "profile_sources": Profile all source columns
                - "full": Both validation and profiling

        Returns:
            Analysis results
        """
        sources = self.extract_sources_from_book(book)

        result = {
            "book_name": book.name,
            "source_count": len(sources),
            "connection_id": connection_id,
            "analysis_type": analysis_type,
        }

        if analysis_type in ("validate_sources", "full"):
            validation = self.validate_sources(sources, connection_id)
            result["validation"] = validation.to_dict()

        if analysis_type in ("profile_sources", "full"):
            profile = self.profile_sources(sources, connection_id)
            result["profile"] = profile

        return result
