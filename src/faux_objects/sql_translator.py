"""SQL Translation Layer for Faux Objects.

This module provides bidirectional translation between SQL formats:
- Parse any SQL (CREATE VIEW, SELECT query, CREATE SEMANTIC VIEW DDL)
- Reverse-engineer it into a SemanticViewDefinition + FauxObjectConfig
- Convert between formats (view → semantic view DDL, semantic view DDL → view, etc.)
"""
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any, Set, Tuple

try:
    import sqlglot
    from sqlglot import exp
    from sqlglot.errors import ParseError
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False

from .types import (
    FauxProject,
    FauxObjectConfig,
    FauxObjectType,
    SemanticViewDefinition,
    SemanticColumn,
    SemanticColumnType,
    SemanticTable,
    SemanticRelationship,
    SnowflakeDataType,
)


class SQLInputFormat(str, Enum):
    """Detected SQL input format."""
    CREATE_VIEW = "create_view"
    SELECT_QUERY = "select_query"
    CREATE_SEMANTIC_VIEW = "create_semantic_view"
    UNKNOWN = "unknown"


@dataclass
class TranslationResult:
    """Result of translating SQL into a semantic view definition."""
    semantic_view: SemanticViewDefinition
    faux_config: Optional[FauxObjectConfig]
    input_format: SQLInputFormat
    warnings: List[str] = field(default_factory=list)
    original_sql: str = ""


class SQLTranslator:
    """Translator between SQL formats and SemanticViewDefinition."""

    # Aggregation functions that indicate a METRIC
    AGGREGATION_FUNCTIONS = {'SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'STDDEV', 'VARIANCE', 'MEDIAN'}

    def __init__(self):
        if not SQLGLOT_AVAILABLE:
            raise ImportError("sqlglot is required for SQL translation. Install with: pip install sqlglot")

    # =========================================================================
    # Format Detection
    # =========================================================================

    def detect_format(self, sql: str) -> SQLInputFormat:
        """Detect the SQL input format.

        Args:
            sql: SQL statement to analyze

        Returns:
            SQLInputFormat enum value
        """
        if not sql or not sql.strip():
            return SQLInputFormat.UNKNOWN

        # Strip comments and normalize whitespace
        cleaned = self._strip_sql_comments(sql).strip().upper()

        # Check for CREATE SEMANTIC VIEW first (more specific)
        if re.match(r'CREATE\s+(OR\s+REPLACE\s+)?SEMANTIC\s+VIEW', cleaned):
            return SQLInputFormat.CREATE_SEMANTIC_VIEW

        # Check for CREATE VIEW
        if re.match(r'CREATE\s+(OR\s+REPLACE\s+)?VIEW', cleaned):
            return SQLInputFormat.CREATE_VIEW

        # Check for SELECT
        if cleaned.startswith('SELECT') or cleaned.startswith('WITH'):
            return SQLInputFormat.SELECT_QUERY

        return SQLInputFormat.UNKNOWN

    def _strip_sql_comments(self, sql: str) -> str:
        """Remove SQL comments from a string."""
        # Remove single-line comments
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        # Remove multi-line comments
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        return sql

    # =========================================================================
    # Main Translation Entry Points
    # =========================================================================

    def translate(
        self,
        sql: str,
        name: Optional[str] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> TranslationResult:
        """Parse SQL into a SemanticViewDefinition.

        Args:
            sql: SQL statement (CREATE VIEW, SELECT, or CREATE SEMANTIC VIEW)
            name: Override name for the semantic view
            database: Override database
            schema_name: Override schema

        Returns:
            TranslationResult with semantic_view, optional faux_config, and warnings
        """
        input_format = self.detect_format(sql)
        warnings: List[str] = []

        if input_format == SQLInputFormat.UNKNOWN:
            raise ValueError("Could not detect SQL format. Expected CREATE VIEW, SELECT, or CREATE SEMANTIC VIEW.")

        if input_format == SQLInputFormat.SELECT_QUERY:
            sv, faux, warns = self._parse_select_query(sql, name, database, schema_name)
        elif input_format == SQLInputFormat.CREATE_VIEW:
            sv, faux, warns = self._parse_create_view(sql, name, database, schema_name)
        elif input_format == SQLInputFormat.CREATE_SEMANTIC_VIEW:
            sv, faux, warns = self._parse_semantic_view_ddl(sql, name, database, schema_name)
        else:
            raise ValueError(f"Unsupported SQL format: {input_format}")

        warnings.extend(warns)

        return TranslationResult(
            semantic_view=sv,
            faux_config=faux,
            input_format=input_format,
            warnings=warnings,
            original_sql=sql,
        )

    def translate_to_project(
        self,
        sql: str,
        project_name: str,
        service,  # FauxObjectsService
        description: str = "",
        faux_type: Optional[str] = None,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
    ) -> FauxProject:
        """Parse SQL and create a complete FauxProject.

        Args:
            sql: SQL statement to parse
            project_name: Name for the new project
            service: FauxObjectsService instance
            description: Project description
            faux_type: Optional faux object type to create (view, stored_procedure, etc.)
            target_database: Target database for faux object
            target_schema: Target schema for faux object

        Returns:
            Created FauxProject with semantic view and optional faux object
        """
        result = self.translate(sql)
        sv = result.semantic_view

        # Create the project
        project = service.create_project(project_name, description)

        # Define the semantic view
        service.define_semantic_view(
            project.id,
            sv.name,
            sv.database,
            sv.schema_name,
            comment=sv.comment,
            ai_sql_generation=sv.ai_sql_generation,
        )

        # Add tables
        for table in sv.tables:
            service.add_semantic_table(
                project.id,
                table.alias,
                table.fully_qualified_name,
                primary_key=table.primary_key,
            )

        # Add relationships
        for rel in sv.relationships:
            service.add_semantic_relationship(
                project.id,
                rel.from_table,
                rel.from_column,
                rel.to_table,
                to_column=rel.to_column,
            )

        # Add columns
        for dim in sv.dimensions:
            service.add_semantic_column(
                project.id,
                dim.name,
                "dimension",
                data_type=dim.data_type,
                table_alias=dim.table_alias,
                expression=dim.expression,
                synonyms=dim.synonyms or None,
                comment=dim.comment,
            )

        for fact in sv.facts:
            service.add_semantic_column(
                project.id,
                fact.name,
                "fact",
                data_type=fact.data_type,
                table_alias=fact.table_alias,
                expression=fact.expression,
                synonyms=fact.synonyms or None,
                comment=fact.comment,
            )

        for metric in sv.metrics:
            service.add_semantic_column(
                project.id,
                metric.name,
                "metric",
                data_type=metric.data_type,
                table_alias=metric.table_alias,
                expression=metric.expression,
                synonyms=metric.synonyms or None,
                comment=metric.comment,
            )

        # Add faux object if requested
        if faux_type:
            target_db = target_database or sv.database
            target_sch = target_schema or sv.schema_name

            service.add_faux_object(
                project.id,
                f"V_{sv.name.upper()}",
                faux_type,
                target_db,
                target_sch,
            )

        return service.get_project(project.id)

    def convert(
        self,
        sql: str,
        target_format: str,
        name: Optional[str] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
    ) -> str:
        """Convert SQL from one format to another.

        Args:
            sql: Source SQL statement
            target_format: Target format - "semantic_view_ddl", "create_view", or "select_query"
            name: Override semantic view name
            database: Override database
            schema_name: Override schema
            target_database: Target database for faux objects
            target_schema: Target schema for faux objects

        Returns:
            SQL in the target format
        """
        from .service import FauxObjectsService

        # Parse the input
        result = self.translate(sql, name, database, schema_name)
        sv = result.semantic_view

        # Create a temporary service for generation
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            service = FauxObjectsService(data_dir=tmpdir)

            if target_format == "semantic_view_ddl":
                return service.generate_semantic_view_ddl(sv)

            elif target_format == "create_view":
                # Create a faux object config for the view
                config = FauxObjectConfig(
                    name=f"V_{sv.name.upper()}",
                    faux_type=FauxObjectType.VIEW,
                    target_database=target_database or sv.database,
                    target_schema=target_schema or sv.schema_name,
                    selected_dimensions=[d.name for d in sv.dimensions],
                    selected_metrics=[m.name for m in sv.metrics],
                    selected_facts=[f.name for f in sv.facts],
                )
                return service.generate_view_sql(sv, config)

            elif target_format == "select_query":
                # Build a basic SELECT query from the semantic view
                return self._generate_select_from_sv(sv)

            else:
                raise ValueError(f"Unknown target format: {target_format}. Expected: semantic_view_ddl, create_view, select_query")

    def _generate_select_from_sv(self, sv: SemanticViewDefinition) -> str:
        """Generate a SELECT query from a SemanticViewDefinition."""
        lines = ["SELECT"]

        columns = []

        # Add dimensions
        for dim in sv.dimensions:
            if dim.table_alias:
                columns.append(f"    {dim.table_alias}.{dim.name}")
            else:
                columns.append(f"    {dim.name}")

        # Add facts
        for fact in sv.facts:
            if fact.table_alias:
                columns.append(f"    {fact.table_alias}.{fact.name}")
            else:
                columns.append(f"    {fact.name}")

        # Add metrics with their expressions
        for metric in sv.metrics:
            if metric.expression:
                columns.append(f"    {metric.expression} AS {metric.name}")
            elif metric.table_alias:
                columns.append(f"    {metric.table_alias}.{metric.name}")
            else:
                columns.append(f"    {metric.name}")

        lines.append(",\n".join(columns))

        # FROM clause
        if sv.tables:
            main_table = sv.tables[0]
            lines.append(f"FROM {main_table.fully_qualified_name} AS {main_table.alias}")

            # Add JOINs based on relationships
            joined_tables: Set[str] = {main_table.alias}
            for rel in sv.relationships:
                # Find the target table
                target_table = None
                for t in sv.tables:
                    if t.alias == rel.to_table:
                        target_table = t
                        break

                if target_table and target_table.alias not in joined_tables:
                    to_col = rel.to_column or target_table.primary_key or rel.from_column
                    lines.append(f"LEFT JOIN {target_table.fully_qualified_name} AS {target_table.alias}")
                    lines.append(f"    ON {rel.from_table}.{rel.from_column} = {rel.to_table}.{to_col}")
                    joined_tables.add(target_table.alias)

        # GROUP BY if we have metrics
        if sv.metrics and sv.dimensions:
            group_cols = []
            for dim in sv.dimensions:
                if dim.table_alias:
                    group_cols.append(f"{dim.table_alias}.{dim.name}")
                else:
                    group_cols.append(dim.name)
            lines.append(f"GROUP BY {', '.join(group_cols)}")

        return "\n".join(lines)

    # =========================================================================
    # SELECT Query Parser
    # =========================================================================

    def _parse_select_query(
        self,
        sql: str,
        name: Optional[str] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> Tuple[SemanticViewDefinition, Optional[FauxObjectConfig], List[str]]:
        """Parse a SELECT query into a SemanticViewDefinition.

        Uses sqlglot to parse the AST and extract:
        - Tables from FROM/JOIN clauses
        - Relationships from JOIN conditions
        - Dimensions from GROUP BY columns
        - Metrics from aggregation expressions
        - Facts from non-aggregated, non-grouped columns
        """
        warnings: List[str] = []

        try:
            parsed = sqlglot.parse_one(sql, dialect="snowflake")
        except ParseError as e:
            raise ValueError(f"Failed to parse SQL: {e}")

        # Extract tables
        tables: List[SemanticTable] = []
        table_alias_map: Dict[str, str] = {}  # alias -> fully_qualified_name

        for table_node in parsed.find_all(exp.Table):
            fqn = self._get_table_fqn(table_node)
            alias = table_node.alias or table_node.name

            if alias not in table_alias_map:
                table_alias_map[alias] = fqn
                tables.append(SemanticTable(
                    alias=alias,
                    fully_qualified_name=fqn,
                    primary_key=None,
                ))

        # Extract relationships from JOINs
        relationships: List[SemanticRelationship] = []
        for join_node in parsed.find_all(exp.Join):
            rel = self._extract_relationship_from_join(join_node, table_alias_map)
            if rel:
                relationships.append(rel)

        # Get GROUP BY columns
        group_by_columns: Set[str] = set()
        group_by_node = parsed.find(exp.Group)
        if group_by_node:
            for expr in group_by_node.expressions:
                col_name = self._extract_column_name(expr)
                if col_name:
                    group_by_columns.add(col_name.upper())

        # Extract columns from SELECT
        dimensions: List[SemanticColumn] = []
        metrics: List[SemanticColumn] = []
        facts: List[SemanticColumn] = []

        select_node = parsed.find(exp.Select)
        if select_node:
            for col_expr in select_node.expressions:
                # Check for SELECT *
                if isinstance(col_expr, exp.Star):
                    warnings.append("SELECT * detected - cannot classify columns. Consider using explicit column list.")
                    continue

                col_info = self._classify_select_column(col_expr, group_by_columns, table_alias_map)
                if col_info:
                    col_name, col_type, data_type, table_alias, expression = col_info

                    col = SemanticColumn(
                        name=col_name,
                        column_type=col_type,
                        data_type=data_type,
                        table_alias=table_alias,
                        expression=expression,
                    )

                    if col_type == SemanticColumnType.DIMENSION:
                        dimensions.append(col)
                    elif col_type == SemanticColumnType.METRIC:
                        metrics.append(col)
                    else:
                        facts.append(col)

        # Build the semantic view definition
        sv = SemanticViewDefinition(
            name=name or "parsed_query",
            database=database or "DATABASE",
            schema_name=schema_name or "SCHEMA",
            tables=tables,
            relationships=relationships,
            dimensions=dimensions,
            metrics=metrics,
            facts=facts,
        )

        return sv, None, warnings

    def _get_table_fqn(self, table_node: exp.Table) -> str:
        """Extract fully qualified name from a table node."""
        parts = []
        if table_node.catalog:
            parts.append(table_node.catalog)
        if table_node.db:
            parts.append(table_node.db)
        parts.append(table_node.name)
        return ".".join(parts)

    def _extract_relationship_from_join(
        self,
        join_node: exp.Join,
        table_alias_map: Dict[str, str],
    ) -> Optional[SemanticRelationship]:
        """Extract a relationship from a JOIN node."""
        on_clause = join_node.find(exp.EQ)
        if not on_clause:
            return None

        left_col = on_clause.left
        right_col = on_clause.right

        if isinstance(left_col, exp.Column) and isinstance(right_col, exp.Column):
            from_table = left_col.table or ""
            from_column = left_col.name
            to_table = right_col.table or ""
            to_column = right_col.name

            if from_table and to_table:
                return SemanticRelationship(
                    from_table=from_table,
                    from_column=from_column,
                    to_table=to_table,
                    to_column=to_column,
                )

        return None

    def _extract_column_name(self, expr) -> Optional[str]:
        """Extract column name from an expression."""
        if isinstance(expr, exp.Column):
            return expr.name
        if isinstance(expr, exp.Alias):
            return self._extract_column_name(expr.this)
        if hasattr(expr, 'name'):
            return expr.name
        return None

    def _classify_select_column(
        self,
        col_expr,
        group_by_columns: Set[str],
        table_alias_map: Dict[str, str],
    ) -> Optional[Tuple[str, SemanticColumnType, str, Optional[str], Optional[str]]]:
        """Classify a SELECT column as dimension, metric, or fact.

        Returns: (name, type, data_type, table_alias, expression)
        """
        # Get the alias or derive a name
        if isinstance(col_expr, exp.Alias):
            col_name = col_expr.alias
            inner_expr = col_expr.this
        elif isinstance(col_expr, exp.Column):
            col_name = col_expr.name
            inner_expr = col_expr
        else:
            # Complex expression without alias
            col_name = str(col_expr)[:30].replace(" ", "_")
            inner_expr = col_expr

        # Check for aggregation function
        has_aggregation = self._contains_aggregation(inner_expr)

        # Determine classification
        table_alias = None
        expression = None

        if isinstance(inner_expr, exp.Column):
            table_alias = inner_expr.table or None

        col_name_upper = col_name.upper()

        if has_aggregation:
            # It's a METRIC
            expression = str(inner_expr)
            data_type = self._infer_aggregation_type(inner_expr)
            return (col_name, SemanticColumnType.METRIC, data_type, table_alias, expression)

        elif col_name_upper in group_by_columns or self._column_in_group_by(inner_expr, group_by_columns):
            # It's a DIMENSION (in GROUP BY)
            data_type = self._infer_column_type(inner_expr)
            return (col_name, SemanticColumnType.DIMENSION, data_type, table_alias, None)

        else:
            # It's a FACT (raw column, not grouped)
            # But if there's no GROUP BY at all, treat all columns as FACT
            data_type = self._infer_column_type(inner_expr)
            return (col_name, SemanticColumnType.FACT, data_type, table_alias, None)

    def _contains_aggregation(self, expr) -> bool:
        """Check if expression contains an aggregation function."""
        for func in expr.find_all(exp.Func):
            func_name = func.name.upper() if hasattr(func, 'name') else type(func).__name__.upper()
            if func_name in self.AGGREGATION_FUNCTIONS:
                return True

        # Also check specific aggregation expression types
        agg_types = (exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max, exp.Stddev, exp.Variance)
        for _ in expr.find_all(*agg_types):
            return True

        return False

    def _column_in_group_by(self, expr, group_by_columns: Set[str]) -> bool:
        """Check if column expression matches a GROUP BY column."""
        if isinstance(expr, exp.Column):
            # Check qualified name
            if expr.table:
                qualified = f"{expr.table}.{expr.name}".upper()
                if qualified in group_by_columns:
                    return True
            return expr.name.upper() in group_by_columns
        return False

    def _infer_aggregation_type(self, expr) -> str:
        """Infer data type from aggregation expression."""
        # COUNT returns INT
        for _ in expr.find_all(exp.Count):
            return "INT"

        # SUM, AVG, etc. return FLOAT
        return "FLOAT"

    def _infer_column_type(self, expr) -> str:
        """Infer data type from column expression."""
        # Default to VARCHAR for dimensions, FLOAT for numeric-looking things
        expr_str = str(expr).upper()

        if any(kw in expr_str for kw in ['AMOUNT', 'PRICE', 'COST', 'TOTAL', 'SUM', 'BALANCE', 'RATE']):
            return "FLOAT"
        if any(kw in expr_str for kw in ['COUNT', 'QTY', 'QUANTITY', 'NUMBER', 'ID', 'YEAR']):
            return "INT"
        if any(kw in expr_str for kw in ['DATE', 'TIME', 'TIMESTAMP']):
            return "DATE"

        return "VARCHAR"

    # =========================================================================
    # CREATE VIEW Parser
    # =========================================================================

    def _parse_create_view(
        self,
        sql: str,
        name: Optional[str] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> Tuple[SemanticViewDefinition, Optional[FauxObjectConfig], List[str]]:
        """Parse a CREATE VIEW statement into a SemanticViewDefinition.

        Handles two cases:
        1. View with SEMANTIC_VIEW() call inside - extract from regex
        2. Regular view with SELECT - delegate to SELECT parser
        """
        warnings: List[str] = []

        # Extract view name from the CREATE statement
        view_match = re.search(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([^\s(]+)',
            sql,
            re.IGNORECASE,
        )

        view_name = name
        view_db = database
        view_schema = schema_name

        if view_match:
            full_view_name = view_match.group(1)
            parts = full_view_name.split(".")
            if len(parts) == 3:
                view_db = view_db or parts[0]
                view_schema = view_schema or parts[1]
                view_name = view_name or parts[2]
            elif len(parts) == 2:
                view_schema = view_schema or parts[0]
                view_name = view_name or parts[1]
            else:
                view_name = view_name or parts[0]

        # Extract comment if present
        comment = None
        comment_match = re.search(r"COMMENT\s*=\s*'([^']*)'", sql, re.IGNORECASE)
        if comment_match:
            comment = comment_match.group(1).replace("''", "'")

        # Check if this contains SEMANTIC_VIEW()
        if 'SEMANTIC_VIEW(' in sql.upper():
            # Parse the SEMANTIC_VIEW() call
            sv, faux, warns = self._parse_semantic_view_call(
                sql, view_name, view_db, view_schema, comment
            )
            warnings.extend(warns)

            # Create a faux config for this view
            faux_config = FauxObjectConfig(
                name=view_name or "parsed_view",
                faux_type=FauxObjectType.VIEW,
                target_database=view_db or "DATABASE",
                target_schema=view_schema or "SCHEMA",
                selected_dimensions=[d.name for d in sv.dimensions],
                selected_metrics=[m.name for m in sv.metrics],
                selected_facts=[f.name for f in sv.facts],
                comment=comment,
            )

            return sv, faux_config, warnings

        # Regular view - extract the SELECT and parse it
        as_match = re.search(r'\bAS\s+(SELECT\b.*)', sql, re.IGNORECASE | re.DOTALL)
        if as_match:
            select_sql = as_match.group(1)
            sv, _, warns = self._parse_select_query(
                select_sql, view_name, view_db, view_schema
            )
            warnings.extend(warns)

            sv.comment = comment

            # Create a faux config
            faux_config = FauxObjectConfig(
                name=view_name or "parsed_view",
                faux_type=FauxObjectType.VIEW,
                target_database=view_db or "DATABASE",
                target_schema=view_schema or "SCHEMA",
                selected_dimensions=[d.name for d in sv.dimensions],
                selected_metrics=[m.name for m in sv.metrics],
                selected_facts=[f.name for f in sv.facts],
                comment=comment,
            )

            return sv, faux_config, warnings

        raise ValueError("Could not extract SELECT from CREATE VIEW statement")

    def _parse_semantic_view_call(
        self,
        sql: str,
        name: Optional[str] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> Tuple[SemanticViewDefinition, Optional[FauxObjectConfig], List[str]]:
        """Parse a SEMANTIC_VIEW() function call within a view."""
        warnings: List[str] = []

        # Extract the semantic view reference
        sv_match = re.search(
            r'SEMANTIC_VIEW\s*\(\s*([^\s,)]+)',
            sql,
            re.IGNORECASE,
        )

        sv_name = name or "semantic_view"
        sv_db = database
        sv_schema = schema_name

        if sv_match:
            full_sv_name = sv_match.group(1)
            parts = full_sv_name.split(".")
            if len(parts) == 3:
                sv_db = sv_db or parts[0]
                sv_schema = sv_schema or parts[1]
                sv_name = parts[2]
            elif len(parts) == 2:
                sv_schema = sv_schema or parts[0]
                sv_name = parts[1]
            else:
                sv_name = parts[0]

        # Extract DIMENSIONS clause
        dimensions: List[SemanticColumn] = []
        dims_match = re.search(
            r'DIMENSIONS\s+([^)]*?)(?:FACTS|METRICS|WHERE|\)|$)',
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if dims_match:
            dims_str = dims_match.group(1).strip().rstrip(',')
            for dim_name in dims_str.split(','):
                dim_name = dim_name.strip()
                if dim_name:
                    table_alias = None
                    col_name = dim_name
                    if '.' in dim_name:
                        parts = dim_name.split('.')
                        table_alias = parts[0]
                        col_name = parts[1]

                    dimensions.append(SemanticColumn(
                        name=col_name,
                        column_type=SemanticColumnType.DIMENSION,
                        data_type="VARCHAR",
                        table_alias=table_alias,
                    ))

        # Extract FACTS clause
        facts: List[SemanticColumn] = []
        facts_match = re.search(
            r'FACTS\s+([^)]*?)(?:DIMENSIONS|METRICS|WHERE|\)|$)',
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if facts_match:
            facts_str = facts_match.group(1).strip().rstrip(',')
            for fact_name in facts_str.split(','):
                fact_name = fact_name.strip()
                if fact_name:
                    table_alias = None
                    col_name = fact_name
                    if '.' in fact_name:
                        parts = fact_name.split('.')
                        table_alias = parts[0]
                        col_name = parts[1]

                    facts.append(SemanticColumn(
                        name=col_name,
                        column_type=SemanticColumnType.FACT,
                        data_type="FLOAT",
                        table_alias=table_alias,
                    ))

        # Extract METRICS clause
        metrics: List[SemanticColumn] = []
        mets_match = re.search(
            r'METRICS\s+([^)]*?)(?:DIMENSIONS|FACTS|WHERE|\)|$)',
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if mets_match:
            mets_str = mets_match.group(1).strip().rstrip(',')
            for met_name in mets_str.split(','):
                met_name = met_name.strip()
                if met_name:
                    table_alias = None
                    col_name = met_name
                    if '.' in met_name:
                        parts = met_name.split('.')
                        table_alias = parts[0]
                        col_name = parts[1]

                    metrics.append(SemanticColumn(
                        name=col_name,
                        column_type=SemanticColumnType.METRIC,
                        data_type="FLOAT",
                        table_alias=table_alias,
                    ))

        sv = SemanticViewDefinition(
            name=sv_name,
            database=sv_db or "DATABASE",
            schema_name=sv_schema or "SCHEMA",
            comment=comment,
            dimensions=dimensions,
            metrics=metrics,
            facts=facts,
        )

        return sv, None, warnings

    # =========================================================================
    # CREATE SEMANTIC VIEW DDL Parser
    # =========================================================================

    def _parse_semantic_view_ddl(
        self,
        sql: str,
        name: Optional[str] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> Tuple[SemanticViewDefinition, Optional[FauxObjectConfig], List[str]]:
        """Parse a CREATE SEMANTIC VIEW DDL statement.

        Uses pure regex since sqlglot doesn't support this syntax.
        """
        warnings: List[str] = []

        # Extract semantic view name
        sv_match = re.search(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?SEMANTIC\s+VIEW\s+([^\s(]+)',
            sql,
            re.IGNORECASE,
        )

        sv_name = name
        sv_db = database
        sv_schema = schema_name

        if sv_match:
            full_name = sv_match.group(1)
            parts = full_name.split(".")
            if len(parts) == 3:
                sv_db = sv_db or parts[0]
                sv_schema = sv_schema or parts[1]
                sv_name = sv_name or parts[2]
            elif len(parts) == 2:
                sv_schema = sv_schema or parts[0]
                sv_name = sv_name or parts[1]
            else:
                sv_name = sv_name or parts[0]

        # Extract COMMENT
        comment = None
        comment_match = re.search(r"COMMENT\s*=\s*'([^']*(?:''[^']*)*)'", sql, re.IGNORECASE)
        if comment_match:
            comment = comment_match.group(1).replace("''", "'")

        # Extract AI_SQL_GENERATION
        ai_sql = None
        ai_match = re.search(r"AI_SQL_GENERATION\s*=\s*'([^']*(?:''[^']*)*)'", sql, re.IGNORECASE)
        if ai_match:
            ai_sql = ai_match.group(1).replace("''", "'")

        # Parse TABLES block
        tables = self._parse_tables_block(sql)

        # Parse RELATIONSHIPS block
        relationships = self._parse_relationships_block(sql)

        # Parse FACTS block
        facts = self._parse_column_block(sql, "FACTS", SemanticColumnType.FACT)

        # Parse DIMENSIONS block
        dimensions = self._parse_column_block(sql, "DIMENSIONS", SemanticColumnType.DIMENSION)

        # Parse METRICS block
        metrics = self._parse_column_block(sql, "METRICS", SemanticColumnType.METRIC)

        sv = SemanticViewDefinition(
            name=sv_name or "semantic_view",
            database=sv_db or "DATABASE",
            schema_name=sv_schema or "SCHEMA",
            comment=comment,
            ai_sql_generation=ai_sql,
            tables=tables,
            relationships=relationships,
            dimensions=dimensions,
            metrics=metrics,
            facts=facts,
        )

        return sv, None, warnings

    def _parse_tables_block(self, sql: str) -> List[SemanticTable]:
        """Parse the TABLES(...) block from SEMANTIC VIEW DDL."""
        tables: List[SemanticTable] = []

        # Find TABLES block
        tables_match = re.search(r'\bTABLES\s*\(', sql, re.IGNORECASE)
        if not tables_match:
            return tables

        # Extract content between balanced parens
        start = tables_match.end()
        content = self._extract_balanced_parens(sql[start-1:])
        if not content:
            return tables

        # Parse each table entry
        # Format: alias AS database.schema.table [PRIMARY KEY (col)]
        table_pattern = re.compile(
            r'(\w+)\s+AS\s+([^\s,()]+)(?:\s+PRIMARY\s+KEY\s*\(\s*(\w+)\s*\))?',
            re.IGNORECASE,
        )

        for match in table_pattern.finditer(content):
            alias = match.group(1)
            fqn = match.group(2)
            pk = match.group(3)

            tables.append(SemanticTable(
                alias=alias,
                fully_qualified_name=fqn,
                primary_key=pk,
            ))

        return tables

    def _parse_relationships_block(self, sql: str) -> List[SemanticRelationship]:
        """Parse the RELATIONSHIPS(...) block from SEMANTIC VIEW DDL."""
        relationships: List[SemanticRelationship] = []

        # Find RELATIONSHIPS block
        rel_match = re.search(r'\bRELATIONSHIPS\s*\(', sql, re.IGNORECASE)
        if not rel_match:
            return relationships

        # Extract content between balanced parens
        start = rel_match.end()
        content = self._extract_balanced_parens(sql[start-1:])
        if not content:
            return relationships

        # Parse each relationship entry
        # Format: from_table (from_col) REFERENCES to_table [(to_col)]
        rel_pattern = re.compile(
            r'(\w+)\s*\(\s*(\w+)\s*\)\s+REFERENCES\s+(\w+)(?:\s*\(\s*(\w+)\s*\))?',
            re.IGNORECASE,
        )

        for match in rel_pattern.finditer(content):
            from_table = match.group(1)
            from_col = match.group(2)
            to_table = match.group(3)
            to_col = match.group(4)  # May be None

            relationships.append(SemanticRelationship(
                from_table=from_table,
                from_column=from_col,
                to_table=to_table,
                to_column=to_col,
            ))

        return relationships

    def _parse_column_block(
        self,
        sql: str,
        block_name: str,
        col_type: SemanticColumnType,
    ) -> List[SemanticColumn]:
        """Parse a column block (FACTS, DIMENSIONS, or METRICS) from SEMANTIC VIEW DDL."""
        columns: List[SemanticColumn] = []

        # Find the block
        block_match = re.search(rf'\b{block_name}\s*\(', sql, re.IGNORECASE)
        if not block_match:
            return columns

        # Extract content between balanced parens
        start = block_match.end()
        content = self._extract_balanced_parens(sql[start-1:])
        if not content:
            return columns

        # Parse column entries
        # Handle multiple formats:
        # - simple: column_name
        # - with alias: table.column AS alias
        # - with expression: column AS expression
        # - with synonyms: column WITH SYNONYMS = ('a', 'b')
        # - with comment: column COMMENT = 'desc'

        # Split by comma, but be careful about nested parens
        entries = self._split_by_comma_respecting_parens(content)

        for entry in entries:
            entry = entry.strip()
            if not entry:
                continue

            col = self._parse_column_entry(entry, col_type)
            if col:
                columns.append(col)

        return columns

    def _parse_column_entry(self, entry: str, col_type: SemanticColumnType) -> Optional[SemanticColumn]:
        """Parse a single column entry from a SEMANTIC VIEW DDL block."""
        # Extract synonyms first (to remove from processing)
        synonyms: List[str] = []
        syn_match = re.search(r'WITH\s+SYNONYMS\s*=\s*\(([^)]+)\)', entry, re.IGNORECASE)
        if syn_match:
            syn_str = syn_match.group(1)
            synonyms = [s.strip().strip("'\"") for s in syn_str.split(',')]
            entry = entry[:syn_match.start()] + entry[syn_match.end():]

        # Extract comment
        comment = None
        com_match = re.search(r"COMMENT\s*=\s*'([^']*(?:''[^']*)*)'", entry, re.IGNORECASE)
        if com_match:
            comment = com_match.group(1).replace("''", "'")
            entry = entry[:com_match.start()] + entry[com_match.end():]

        # Clean up entry
        entry = entry.strip().rstrip(',')

        # Parse the main part: [table.]column [AS expression/alias]
        table_alias = None
        col_name = None
        expression = None

        # Check for AS clause
        as_match = re.match(r'([^\s]+)\s+AS\s+(.+)', entry, re.IGNORECASE)
        if as_match:
            col_ref = as_match.group(1).strip()
            expr_or_alias = as_match.group(2).strip()

            # Parse column reference
            if '.' in col_ref:
                parts = col_ref.split('.')
                table_alias = parts[0]
                col_name = parts[-1]
            else:
                col_name = col_ref

            # For metrics, the AS part is likely an expression
            if col_type == SemanticColumnType.METRIC:
                # If expression contains aggregation, it's an expression
                if any(agg in expr_or_alias.upper() for agg in self.AGGREGATION_FUNCTIONS):
                    expression = expr_or_alias
                else:
                    # It might just be an alias
                    col_name = expr_or_alias
            else:
                # For facts/dimensions, AS is usually an alias
                col_name = expr_or_alias
        else:
            # No AS clause - just column reference
            col_ref = entry.strip()
            if '.' in col_ref:
                parts = col_ref.split('.')
                table_alias = parts[0]
                col_name = parts[-1]
            else:
                col_name = col_ref

        if not col_name:
            return None

        # Infer data type
        if col_type == SemanticColumnType.METRIC:
            data_type = "FLOAT"
        elif col_type == SemanticColumnType.FACT:
            data_type = "FLOAT"
        else:
            data_type = "VARCHAR"

        return SemanticColumn(
            name=col_name,
            column_type=col_type,
            data_type=data_type,
            table_alias=table_alias,
            expression=expression,
            synonyms=synonyms,
            comment=comment,
        )

    def _extract_balanced_parens(self, sql: str) -> str:
        """Extract content between balanced parentheses."""
        if not sql.startswith('('):
            return ""

        depth = 0
        start = 0
        for i, char in enumerate(sql):
            if char == '(':
                if depth == 0:
                    start = i + 1
                depth += 1
            elif char == ')':
                depth -= 1
                if depth == 0:
                    return sql[start:i]

        return ""

    def _split_by_comma_respecting_parens(self, content: str) -> List[str]:
        """Split by comma but respect nested parentheses."""
        parts: List[str] = []
        current = []
        depth = 0

        for char in content:
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                parts.append(''.join(current))
                current = []
            else:
                current.append(char)

        if current:
            parts.append(''.join(current))

        return parts
