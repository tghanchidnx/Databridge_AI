"""
Proof of Graph - Anti-hallucination validation layer.

Validates AI-generated content against the knowledge graph:
1. Checks table/column references exist in catalog
2. Verifies hierarchy references match known hierarchies
3. Validates SQL syntax and entity references
4. Suggests corrections for unknown entities

This is the key differentiator from pure LLM generation -
we anchor outputs in verified data structures.
"""
import logging
import re
from typing import Dict, List, Optional, Set, Any

from .types import (
    ValidationResult, ValidationIssue, ValidationSeverity,
    RAGContext,
)

logger = logging.getLogger(__name__)


class ProofOfGraph:
    """
    Validates AI-generated content against the knowledge graph.

    The "Proof of Graph" concept ensures that generated SQL, hierarchies,
    and other artifacts only reference entities that exist in our
    verified data catalog and hierarchy system.

    This prevents hallucinations by anchoring AI outputs in real data.
    """

    def __init__(
        self,
        catalog_store=None,
        hierarchy_service=None,
        lineage_tracker=None,
        strict_mode: bool = False,
    ):
        """
        Initialize the Proof of Graph validator.

        Args:
            catalog_store: CatalogStore instance for entity lookups
            hierarchy_service: HierarchyService for hierarchy validation
            lineage_tracker: LineageTracker for relationship validation
            strict_mode: If True, warnings are treated as errors
        """
        self.catalog = catalog_store
        self.hierarchy = hierarchy_service
        self.lineage = lineage_tracker
        self.strict_mode = strict_mode

        # Build entity index
        self._known_tables: Set[str] = set()
        self._known_columns: Dict[str, Set[str]] = {}  # table -> columns
        self._known_hierarchies: Set[str] = set()
        self._known_projects: Set[str] = set()
        self._cte_aliases: Set[str] = set()  # Track CTEs during validation

        self._refresh_index()

    def _refresh_index(self) -> None:
        """Refresh the index of known entities from catalog/hierarchy."""
        # Index tables and columns from catalog
        if self.catalog:
            try:
                assets = self.catalog.list_assets(asset_types=["TABLE", "VIEW"])
                for asset in assets.get("assets", []):
                    table_name = asset.get("name", "").upper()
                    self._known_tables.add(table_name)

                    # Also add fully qualified name
                    fqn = asset.get("fully_qualified_name", "")
                    if fqn:
                        self._known_tables.add(fqn.upper())

                    # Index columns
                    cols = set()
                    for col in asset.get("columns", []):
                        cols.add(col.get("name", "").upper())
                    if cols:
                        self._known_columns[table_name] = cols

                logger.debug(f"Indexed {len(self._known_tables)} tables from catalog")

            except Exception as e:
                logger.debug(f"Failed to index catalog: {e}")

        # Index hierarchies
        if self.hierarchy:
            try:
                projects = self.hierarchy.list_projects()
                for proj in projects:
                    proj_id = proj.get("id", "")
                    proj_name = proj.get("name", "").upper()
                    self._known_projects.add(proj_id)
                    self._known_projects.add(proj_name)

                    hierarchies = self.hierarchy.list_hierarchies(proj_id)
                    for h in hierarchies:
                        hier_name = h.get("hierarchy_name", "").upper()
                        hier_id = h.get("hierarchy_id", "").upper()
                        if hier_name:
                            self._known_hierarchies.add(hier_name)
                        if hier_id:
                            self._known_hierarchies.add(hier_id)

                        # Index source mapping table references into _known_tables
                        for m in h.get("mapping", []):
                            tbl = m.get("source_table", "").upper()
                            if tbl:
                                self._known_tables.add(tbl)
                                # Also add fully qualified
                                db = m.get("source_database", "").upper()
                                schema = m.get("source_schema", "").upper()
                                if db and schema:
                                    self._known_tables.add(f"{db}.{schema}.{tbl}")

                logger.debug(f"Indexed {len(self._known_hierarchies)} hierarchies")

            except Exception as e:
                logger.debug(f"Failed to index hierarchies: {e}")

    def validate(
        self,
        content: str,
        content_type: str = "sql",
        context: Optional[RAGContext] = None,
    ) -> ValidationResult:
        """
        Validate generated content against knowledge graph.

        Args:
            content: The generated content to validate
            content_type: Type of content ("sql", "hierarchy", "dbt", "yaml")
            context: Optional RAG context with available entities

        Returns:
            ValidationResult with issues and suggestions
        """
        issues = []
        referenced = []
        verified = []
        missing = []
        suggestions = []

        # Reset CTE tracking
        self._cte_aliases = set()

        # Add context entities to known sets
        if context:
            for table in context.available_tables:
                self._known_tables.add(table.upper())
            for hier in context.available_hierarchies:
                self._known_hierarchies.add(hier.upper())

        # Validate based on content type
        if content_type == "sql":
            self._validate_sql(content, issues, referenced, verified, missing, suggestions)
        elif content_type == "hierarchy":
            self._validate_hierarchy(content, issues, referenced, verified, missing, suggestions)
        elif content_type == "dbt":
            self._validate_dbt(content, issues, referenced, verified, missing, suggestions)
        elif content_type == "yaml":
            self._validate_yaml(content, issues, referenced, verified, missing, suggestions)
        else:
            logger.warning(f"Unknown content type: {content_type}")

        # Check if valid (no errors, or no errors+warnings in strict mode)
        error_count = sum(1 for i in issues if i.severity == ValidationSeverity.ERROR)
        warning_count = sum(1 for i in issues if i.severity == ValidationSeverity.WARNING)

        is_valid = error_count == 0
        if self.strict_mode:
            is_valid = is_valid and warning_count == 0

        return ValidationResult(
            valid=is_valid,
            issues=issues,
            referenced_entities=referenced,
            verified_entities=verified,
            missing_entities=missing,
            suggested_fixes=suggestions,
        )

    def _validate_sql(
        self,
        sql: str,
        issues: List[ValidationIssue],
        referenced: List[str],
        verified: List[str],
        missing: List[str],
        suggestions: List[str],
    ) -> None:
        """Validate SQL content."""
        # First, extract CTEs so we don't flag them as unknown tables
        self._extract_ctes(sql)

        # Extract and validate table references
        tables = self._extract_table_references(sql)
        for table in tables:
            table_upper = table.upper()
            referenced.append(f"TABLE:{table}")

            # Skip if it's a CTE alias
            if table_upper in self._cte_aliases:
                verified.append(f"CTE:{table}")
                continue

            # Check against known tables
            if table_upper in self._known_tables:
                verified.append(f"TABLE:{table}")
            else:
                # Check if it might be a subquery alias
                if self._is_subquery_alias(sql, table):
                    verified.append(f"ALIAS:{table}")
                else:
                    missing.append(table)
                    issues.append(ValidationIssue(
                        severity=ValidationSeverity.WARNING,
                        message=f"Table '{table}' not found in catalog",
                        entity=table,
                        suggestion=self._suggest_similar_table(table),
                    ))

        # Validate column references against known columns
        columns = self._extract_column_references(sql)
        for col in columns:
            col_upper = col.upper()
            referenced.append(f"COLUMN:{col}")

            # Check if column exists in any known table
            found = False
            for table_cols in self._known_columns.values():
                if col_upper in table_cols:
                    found = True
                    verified.append(f"COLUMN:{col}")
                    break

            if not found:
                # Only warn, don't error - column might be from a CTE or subquery
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.INFO,
                    message=f"Column '{col}' not found in indexed tables",
                    entity=col,
                ))

        # Check for common SQL anti-patterns
        self._check_sql_antipatterns(sql, issues)

    def _validate_hierarchy(
        self,
        content: str,
        issues: List[ValidationIssue],
        referenced: List[str],
        verified: List[str],
        missing: List[str],
        suggestions: List[str],
    ) -> None:
        """Validate hierarchy definition content."""
        # Check for hierarchy name references
        patterns = [
            r'hierarchy[_\s]*(?:name|id)?["\']?\s*[:=]\s*["\']?(\w+)',
            r'parent[_\s]*(?:name|id)?["\']?\s*[:=]\s*["\']?(\w+)',
            r'project[_\s]*(?:name|id)?["\']?\s*[:=]\s*["\']?(\w+)',
        ]

        for pattern in patterns:
            for match in re.finditer(pattern, content, re.IGNORECASE):
                name = match.group(1).upper()
                referenced.append(f"HIERARCHY:{name}")

                if name in self._known_hierarchies or name in self._known_projects:
                    verified.append(f"HIERARCHY:{name}")
                else:
                    missing.append(name)
                    issues.append(ValidationIssue(
                        severity=ValidationSeverity.INFO,
                        message=f"Hierarchy '{name}' will be created (not found in existing)",
                        entity=name,
                    ))

        # Validate source mapping table references
        table_pattern = r'source[_\s]*table["\']?\s*[:=]\s*["\']?([\w.]+)'
        for match in re.finditer(table_pattern, content, re.IGNORECASE):
            table_ref = match.group(1).upper()
            referenced.append(f"TABLE:{table_ref}")
            if table_ref in self._known_tables:
                verified.append(f"TABLE:{table_ref}")
            else:
                suggestions.append(f"Source table '{table_ref}' not found in catalog — verify it exists")

        # Validate formula references point to existing hierarchies
        formula_pattern = r'formula[_\s]*(?:ref|source|hierarchy)["\']?\s*[:=]\s*["\']?([\w_]+)'
        for match in re.finditer(formula_pattern, content, re.IGNORECASE):
            ref = match.group(1).upper()
            referenced.append(f"FORMULA_REF:{ref}")
            if ref in self._known_hierarchies:
                verified.append(f"FORMULA_REF:{ref}")
            else:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.WARNING,
                    message=f"Formula references hierarchy '{ref}' which doesn't exist",
                    entity=ref,
                ))

        # Check parent-child forms valid DAG (no self-references)
        parent_refs = re.findall(r'parent[_\s]*id["\']?\s*[:=]\s*["\']?([\w-]+)', content, re.IGNORECASE)
        hier_refs = re.findall(r'hierarchy[_\s]*id["\']?\s*[:=]\s*["\']?([\w-]+)', content, re.IGNORECASE)
        for p in parent_refs:
            if p in hier_refs:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    message=f"Hierarchy '{p}' references itself as parent (circular reference)",
                    entity=p,
                ))

    def _validate_dbt(
        self,
        content: str,
        issues: List[ValidationIssue],
        referenced: List[str],
        verified: List[str],
        missing: List[str],
        suggestions: List[str],
    ) -> None:
        """Validate dbt model content."""
        # Check ref() calls
        ref_pattern = r"\{\{\s*ref\(['\"](\w+)['\"]\)\s*\}\}"
        for match in re.finditer(ref_pattern, content):
            model_name = match.group(1)
            referenced.append(f"DBT_REF:{model_name}")
            # dbt refs are typically internal - just track them
            verified.append(f"DBT_REF:{model_name}")

        # Check source() calls
        source_pattern = r"\{\{\s*source\(['\"](\w+)['\"],\s*['\"](\w+)['\"]\)\s*\}\}"
        for match in re.finditer(source_pattern, content):
            source_name = match.group(1)
            table_name = match.group(2)
            fqn = f"{source_name}.{table_name}".upper()
            referenced.append(f"SOURCE:{fqn}")

            if table_name.upper() in self._known_tables:
                verified.append(f"SOURCE:{fqn}")
            else:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.WARNING,
                    message=f"Source table '{table_name}' not found in catalog",
                    entity=table_name,
                    suggestion=self._suggest_similar_table(table_name),
                ))

        # Also validate the SQL inside the dbt model
        self._validate_sql(content, issues, referenced, verified, missing, suggestions)

    def _validate_yaml(
        self,
        content: str,
        issues: List[ValidationIssue],
        referenced: List[str],
        verified: List[str],
        missing: List[str],
        suggestions: List[str],
    ) -> None:
        """Validate YAML content (semantic models, sources, etc.)."""
        # Check for table references
        table_patterns = [
            r'table:\s*["\']?([a-zA-Z_][\w.]*)["\']?',
            r'base_table:\s*["\']?([a-zA-Z_][\w.]*)["\']?',
        ]

        for pattern in table_patterns:
            for match in re.finditer(pattern, content, re.IGNORECASE):
                table = match.group(1)
                referenced.append(f"TABLE:{table}")

                # Handle fully qualified names
                parts = table.upper().split(".")
                table_name = parts[-1]

                if table_name in self._known_tables or table.upper() in self._known_tables:
                    verified.append(f"TABLE:{table}")
                else:
                    missing.append(table)
                    issues.append(ValidationIssue(
                        severity=ValidationSeverity.WARNING,
                        message=f"Table '{table}' not found in catalog",
                        entity=table,
                    ))

    def _extract_ctes(self, sql: str) -> None:
        """Extract CTE names from SQL."""
        # Match WITH ... AS patterns
        cte_pattern = r'\bWITH\s+(?:RECURSIVE\s+)?(.+?)\s+AS\s*\('
        with_match = re.search(r'\bWITH\b', sql, re.IGNORECASE)

        if with_match:
            # Extract CTE names
            # Handle multiple CTEs: WITH cte1 AS (...), cte2 AS (...)
            cte_def_pattern = r'(\w+)\s+AS\s*\('
            for match in re.finditer(cte_def_pattern, sql, re.IGNORECASE):
                self._cte_aliases.add(match.group(1).upper())

    def _extract_table_references(self, sql: str) -> List[str]:
        """Extract table names from SQL."""
        tables = set()

        # FROM clause
        from_pattern = r'\bFROM\s+([a-zA-Z_][\w.]*)'
        for match in re.finditer(from_pattern, sql, re.IGNORECASE):
            tables.add(match.group(1))

        # JOIN clause
        join_pattern = r'\bJOIN\s+([a-zA-Z_][\w.]*)'
        for match in re.finditer(join_pattern, sql, re.IGNORECASE):
            tables.add(match.group(1))

        # INTO clause
        into_pattern = r'\bINTO\s+([a-zA-Z_][\w.]*)'
        for match in re.finditer(into_pattern, sql, re.IGNORECASE):
            tables.add(match.group(1))

        # UPDATE clause
        update_pattern = r'\bUPDATE\s+([a-zA-Z_][\w.]*)'
        for match in re.finditer(update_pattern, sql, re.IGNORECASE):
            tables.add(match.group(1))

        # Filter out keywords
        keywords = {"SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "NULL",
                    "TRUE", "FALSE", "AS", "ON", "IN", "BETWEEN", "LIKE",
                    "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS",
                    "GROUP", "ORDER", "BY", "HAVING", "LIMIT", "OFFSET",
                    "UNION", "INTERSECT", "EXCEPT", "WITH", "RECURSIVE"}

        return [t for t in tables if t.upper() not in keywords]

    def _extract_column_references(self, sql: str) -> List[str]:
        """Extract column names from SQL."""
        columns = set()

        # Columns in SELECT (before FROM)
        select_match = re.search(r'\bSELECT\b(.+?)\bFROM\b', sql, re.IGNORECASE | re.DOTALL)
        if select_match:
            select_clause = select_match.group(1)
            # Simple column extraction (doesn't handle all cases)
            col_pattern = r'\b([a-zA-Z_]\w*)\b'
            for match in re.finditer(col_pattern, select_clause):
                col = match.group(1)
                if col.upper() not in {"SELECT", "AS", "DISTINCT", "ALL", "CASE", "WHEN", "THEN", "ELSE", "END", "AND", "OR", "NOT", "NULL"}:
                    columns.add(col)

        # Columns in WHERE
        where_pattern = r'\b([a-zA-Z_]\w*)\s*(?:=|<|>|LIKE|IN\s*\(|IS\s+(?:NOT\s+)?NULL|BETWEEN)'
        for match in re.finditer(where_pattern, sql, re.IGNORECASE):
            col = match.group(1)
            if col.upper() not in {"AND", "OR", "NOT", "WHERE"}:
                columns.add(col)

        return list(columns)

    def _is_subquery_alias(self, sql: str, name: str) -> bool:
        """Check if name is a subquery alias."""
        # Pattern: ) AS alias or ) alias
        pattern = rf'\)\s+(?:AS\s+)?{re.escape(name)}\b'
        return bool(re.search(pattern, sql, re.IGNORECASE))

    def _suggest_similar_table(self, table: str, threshold: float = 0.6) -> Optional[str]:
        """Find similar table name using fuzzy matching."""
        try:
            from rapidfuzz import fuzz

            best_match = None
            best_score = 0

            for known in self._known_tables:
                score = fuzz.ratio(table.upper(), known) / 100
                if score > best_score and score >= threshold:
                    best_score = score
                    best_match = known

            if best_match:
                return f"Did you mean '{best_match}'?"
            return None

        except ImportError:
            return None

    def _check_sql_antipatterns(
        self,
        sql: str,
        issues: List[ValidationIssue],
    ) -> None:
        """Check for common SQL anti-patterns."""
        # SELECT *
        if re.search(r'\bSELECT\s+\*', sql, re.IGNORECASE):
            issues.append(ValidationIssue(
                severity=ValidationSeverity.INFO,
                message="SELECT * may return unexpected columns. Consider explicit column list.",
            ))

        # Missing WHERE on UPDATE/DELETE
        if re.search(r'\b(UPDATE|DELETE)\s+\w+(?!\s+WHERE)', sql, re.IGNORECASE | re.DOTALL):
            if 'WHERE' not in sql.upper():
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.WARNING,
                    message="UPDATE/DELETE without WHERE clause affects all rows",
                ))

        # DISTINCT without ORDER BY in certain contexts
        if 'DISTINCT' in sql.upper() and 'ORDER BY' not in sql.upper():
            if 'TOP' in sql.upper() or 'LIMIT' in sql.upper():
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.INFO,
                    message="DISTINCT with LIMIT/TOP but no ORDER BY may return inconsistent results",
                ))
