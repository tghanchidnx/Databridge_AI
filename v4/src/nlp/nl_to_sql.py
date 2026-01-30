"""
NL-to-SQL Engine for DataBridge AI V4 Analytics Engine.

Translates natural language queries into SQL using intent classification
and entity extraction.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

from .intent import IntentClassifier, Intent, IntentType
from .entity import EntityExtractor, Entity, EntityType, CatalogEntry
from ..query.builder import QueryBuilder, Query
from ..query.dialects import SQLDialect, get_dialect


@dataclass
class NLQueryResult:
    """Result of a natural language to SQL translation."""

    success: bool
    query: Optional[Query] = None
    intent: Optional[Intent] = None
    entities: List[Entity] = field(default_factory=list)
    confidence: float = 0.0
    explanation: str = ""
    suggestions: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "query": self.query.to_dict() if self.query else None,
            "intent": self.intent.to_dict() if self.intent else None,
            "entities": [e.to_dict() for e in self.entities],
            "confidence": round(self.confidence, 2),
            "explanation": self.explanation,
            "suggestions": self.suggestions,
            "errors": self.errors,
        }


class NLToSQLEngine:
    """
    Translates natural language queries into SQL.

    Combines intent classification and entity extraction to generate
    appropriate SQL queries for different types of analytical questions.
    """

    def __init__(
        self,
        catalog: Optional[List[CatalogEntry]] = None,
        dialect: str = "postgresql",
        default_table: Optional[str] = None,
        confidence_threshold: float = 0.5,
    ):
        """
        Initialize the NL-to-SQL engine.

        Args:
            catalog: Metadata catalog for entity extraction.
            dialect: SQL dialect to generate.
            default_table: Default table if none specified.
            confidence_threshold: Minimum confidence to generate SQL.
        """
        self.intent_classifier = IntentClassifier()
        self.entity_extractor = EntityExtractor(catalog=catalog)
        self.dialect = get_dialect(dialect)
        self.default_table = default_table
        self.confidence_threshold = confidence_threshold

    def load_catalog(self, catalog: List[Dict[str, Any]]) -> None:
        """
        Load metadata catalog for entity extraction.

        Args:
            catalog: List of catalog entry dictionaries.
        """
        self.entity_extractor.load_catalog_from_dict(catalog)

    def translate(self, query: str) -> NLQueryResult:
        """
        Translate a natural language query to SQL.

        Args:
            query: Natural language query string.

        Returns:
            NLQueryResult with generated SQL and metadata.
        """
        errors = []
        suggestions = []

        # Classify intent
        intent = self.intent_classifier.classify(query)

        # Extract entities
        entities = self.entity_extractor.extract(query)

        # Calculate overall confidence
        entity_confidence = (
            sum(e.confidence for e in entities) / len(entities)
            if entities else 0.0
        )
        overall_confidence = (intent.confidence + entity_confidence) / 2

        # Check confidence threshold
        if overall_confidence < self.confidence_threshold:
            suggestions = self.intent_classifier.get_suggestions(query)
            return NLQueryResult(
                success=False,
                intent=intent,
                entities=entities,
                confidence=overall_confidence,
                explanation="Confidence too low to generate SQL",
                suggestions=suggestions,
                errors=["Could not understand query with sufficient confidence"],
            )

        # Determine table
        table = self._determine_table(entities)
        if not table and self.default_table:
            table = self.default_table
        if not table:
            return NLQueryResult(
                success=False,
                intent=intent,
                entities=entities,
                confidence=overall_confidence,
                explanation="Could not determine target table",
                errors=["No table specified and no default table configured"],
            )

        # Generate SQL based on intent type
        try:
            sql_query = self._generate_sql(intent, entities, table)
            explanation = self._generate_explanation(intent, entities, table)

            return NLQueryResult(
                success=True,
                query=sql_query,
                intent=intent,
                entities=entities,
                confidence=overall_confidence,
                explanation=explanation,
            )

        except Exception as e:
            return NLQueryResult(
                success=False,
                intent=intent,
                entities=entities,
                confidence=overall_confidence,
                explanation="Error generating SQL",
                errors=[str(e)],
            )

    def _determine_table(self, entities: List[Entity]) -> Optional[str]:
        """Determine the primary table from extracted entities."""
        # Look for explicit table entity
        for entity in entities:
            if entity.entity_type == EntityType.TABLE:
                return entity.name

        # Look for table from column entities
        for entity in entities:
            if entity.table:
                return entity.table

        return None

    def _generate_sql(
        self,
        intent: Intent,
        entities: List[Entity],
        table: str,
    ) -> Query:
        """Generate SQL query based on intent and entities."""
        builder = QueryBuilder(dialect=self.dialect)
        builder.from_table(table)

        # Get metrics and dimensions from entities
        metrics = [e for e in entities if e.entity_type == EntityType.METRIC]
        dimensions = [e for e in entities if e.entity_type == EntityType.DIMENSION]
        time_columns = [e for e in entities if e.entity_type == EntityType.TIME_COLUMN]

        # Generate based on intent type
        if intent.intent_type == IntentType.AGGREGATION:
            self._build_aggregation_query(builder, intent, metrics, dimensions)

        elif intent.intent_type == IntentType.TREND:
            self._build_trend_query(builder, intent, metrics, time_columns, dimensions)

        elif intent.intent_type == IntentType.RANKING:
            self._build_ranking_query(builder, intent, metrics, dimensions)

        elif intent.intent_type == IntentType.COMPARISON:
            self._build_comparison_query(builder, intent, metrics, dimensions)

        elif intent.intent_type == IntentType.DISTRIBUTION:
            self._build_distribution_query(builder, intent, dimensions)

        elif intent.intent_type == IntentType.DETAIL:
            self._build_detail_query(builder, entities)

        else:
            # Default: select all columns
            builder.select("*")
            if intent.limit:
                builder.limit(intent.limit)

        # Add time filters if present
        if intent.time_filter:
            self._add_time_filter(builder, intent.time_filter, time_columns)

        return builder.build()

    def _build_aggregation_query(
        self,
        builder: QueryBuilder,
        intent: Intent,
        metrics: List[Entity],
        dimensions: List[Entity],
    ) -> None:
        """Build an aggregation query."""
        agg_func = intent.aggregation or "sum"

        # Select aggregated metrics
        if metrics:
            for metric in metrics:
                builder.select_aggregate(agg_func, metric.name, f"{agg_func}_{metric.name}")
        else:
            # No metrics found, use COUNT(*)
            builder.select("COUNT(*) AS record_count")

        # Group by dimensions
        if dimensions:
            for dim in dimensions:
                builder.select(dim.name)
                builder.group_by(dim.name)
        elif intent.dimensions:
            for dim_name in intent.dimensions:
                builder.select(dim_name)
                builder.group_by(dim_name)

    def _build_trend_query(
        self,
        builder: QueryBuilder,
        intent: Intent,
        metrics: List[Entity],
        time_columns: List[Entity],
        dimensions: List[Entity],
    ) -> None:
        """Build a trend (time series) query."""
        agg_func = intent.aggregation or "sum"

        # Determine time column
        time_col = time_columns[0].name if time_columns else "date"
        granularity = intent.time_filter.get("granularity", "month") if intent.time_filter else "month"

        # Add time dimension
        time_expr = self.dialect.format_date_trunc(time_col, granularity)
        builder.select(f"{time_expr} AS {granularity}")
        builder.group_by(time_expr)
        builder.order_by(time_expr)

        # Add metrics
        if metrics:
            for metric in metrics:
                builder.select_aggregate(agg_func, metric.name, f"{agg_func}_{metric.name}")
        else:
            builder.select("COUNT(*) AS record_count")

        # Add other dimensions
        for dim in dimensions:
            if dim.name != time_col:
                builder.select(dim.name)
                builder.group_by(dim.name)

    def _build_ranking_query(
        self,
        builder: QueryBuilder,
        intent: Intent,
        metrics: List[Entity],
        dimensions: List[Entity],
    ) -> None:
        """Build a ranking (TOP N) query."""
        agg_func = intent.aggregation or "sum"

        # Select dimensions
        if dimensions:
            for dim in dimensions:
                builder.select(dim.name)
                builder.group_by(dim.name)

        # Select aggregated metrics
        if metrics:
            metric = metrics[0]  # Use first metric for ranking
            alias = f"{agg_func}_{metric.name}"
            builder.select_aggregate(agg_func, metric.name, alias)
            builder.order_by(alias, intent.order_direction or "DESC")
        else:
            builder.select("COUNT(*) AS record_count")
            builder.order_by("record_count", intent.order_direction or "DESC")

        # Apply limit
        builder.limit(intent.limit or 10)

    def _build_comparison_query(
        self,
        builder: QueryBuilder,
        intent: Intent,
        metrics: List[Entity],
        dimensions: List[Entity],
    ) -> None:
        """Build a comparison query (BvA, YoY, etc.)."""
        # This is simplified - real implementation would need period columns
        agg_func = intent.aggregation or "sum"

        # Select dimensions
        for dim in dimensions:
            builder.select(dim.name)
            builder.group_by(dim.name)

        # Select metrics
        if metrics:
            for metric in metrics:
                builder.select_aggregate(agg_func, metric.name, f"{agg_func}_{metric.name}")

        # Note: Full comparison would require CTEs or subqueries
        # This is a simplified version

    def _build_distribution_query(
        self,
        builder: QueryBuilder,
        intent: Intent,
        dimensions: List[Entity],
    ) -> None:
        """Build a distribution/count query."""
        if dimensions:
            for dim in dimensions:
                builder.select(dim.name)
                builder.group_by(dim.name)
        elif intent.dimensions:
            for dim_name in intent.dimensions:
                builder.select(dim_name)
                builder.group_by(dim_name)

        builder.select("COUNT(*) AS count")
        builder.order_by("count", "DESC")

    def _build_detail_query(
        self,
        builder: QueryBuilder,
        entities: List[Entity],
    ) -> None:
        """Build a detail query (show specific records)."""
        columns = [e for e in entities if e.entity_type in [
            EntityType.COLUMN, EntityType.METRIC, EntityType.DIMENSION
        ]]

        if columns:
            for col in columns:
                builder.select(col.name)
        else:
            builder.select("*")

        builder.limit(100)  # Default limit for detail queries

    def _add_time_filter(
        self,
        builder: QueryBuilder,
        time_filter: Dict[str, Any],
        time_columns: List[Entity],
    ) -> None:
        """Add time-based WHERE conditions."""
        time_col = time_columns[0].name if time_columns else "date"

        reference = time_filter.get("reference")
        if reference:
            # Add appropriate date filter based on reference
            current_ts = self.dialect.format_current_timestamp()

            if reference == "this_year":
                builder.where(f"YEAR({time_col}) = YEAR({current_ts})")
            elif reference == "last_year":
                builder.where(f"YEAR({time_col}) = YEAR({current_ts}) - 1")
            elif reference == "this_month":
                builder.where(f"YEAR({time_col}) = YEAR({current_ts})")
                builder.where(f"MONTH({time_col}) = MONTH({current_ts})")
            elif reference == "last_month":
                # Simplified - real implementation would handle year boundary
                builder.where(f"MONTH({time_col}) = MONTH({current_ts}) - 1")

        # Specific year filter
        if "year" in time_filter:
            builder.where(f"YEAR({time_col}) = {time_filter['year']}")

        # Specific month filter
        if "month" in time_filter:
            builder.where(f"MONTH({time_col}) = {time_filter['month']}")

    def _generate_explanation(
        self,
        intent: Intent,
        entities: List[Entity],
        table: str,
    ) -> str:
        """Generate a human-readable explanation of the query."""
        parts = []

        # Intent description
        intent_descriptions = {
            IntentType.AGGREGATION: "Aggregating data",
            IntentType.TREND: "Analyzing trends over time",
            IntentType.RANKING: f"Finding top {intent.limit or 10} records",
            IntentType.COMPARISON: "Comparing values",
            IntentType.DISTRIBUTION: "Showing distribution",
            IntentType.DETAIL: "Retrieving detailed records",
            IntentType.FILTER: "Filtering data",
        }
        parts.append(intent_descriptions.get(intent.intent_type, "Querying data"))

        # Table
        parts.append(f"from {table}")

        # Metrics
        metrics = [e.name for e in entities if e.entity_type == EntityType.METRIC]
        if metrics and intent.aggregation:
            parts.append(f"calculating {intent.aggregation.upper()} of {', '.join(metrics)}")

        # Dimensions
        dimensions = [e.name for e in entities if e.entity_type == EntityType.DIMENSION]
        if dimensions:
            parts.append(f"grouped by {', '.join(dimensions)}")

        # Time filter
        if intent.time_filter:
            ref = intent.time_filter.get("reference", "")
            if ref:
                parts.append(f"for {ref.replace('_', ' ')}")

        return " ".join(parts)

    def suggest_questions(self, context: Optional[str] = None) -> List[str]:
        """
        Suggest sample questions based on available catalog.

        Args:
            context: Optional context to tailor suggestions.

        Returns:
            List of suggested questions.
        """
        suggestions = []

        tables = self.entity_extractor.get_tables()
        metrics = self.entity_extractor.get_metrics()
        dimensions = self.entity_extractor.get_dimensions()

        if metrics and dimensions:
            metric = metrics[0] if metrics else "amount"
            dimension = dimensions[0] if dimensions else "category"

            suggestions.extend([
                f"What is the total {metric} by {dimension}?",
                f"Show me the top 10 {dimension}s by {metric}",
                f"How has {metric} changed over time?",
                f"Compare {metric} year over year",
            ])

        if tables:
            table = tables[0]
            suggestions.append(f"Show me all records from {table}")

        return suggestions[:5]
