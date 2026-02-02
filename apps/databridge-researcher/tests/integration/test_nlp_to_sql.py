"""Integration tests for NL-to-SQL flow.

Tests the complete pipeline:
Intent Classification → Entity Extraction → Query Building → SQL Safety Validation
"""

import pytest

from src.nlp.intent import IntentClassifier, IntentType
from src.nlp.entity import EntityExtractor, EntityType, CatalogEntry
from src.nlp.nl_to_sql import NLToSQLEngine, NLQueryResult
from src.query.builder import QueryBuilder
from src.query.safety import (
    SQLSanitizer,
    QueryValidator,
    QueryAuditor,
    SQLRiskLevel,
    get_sanitizer,
    get_validator,
    get_auditor,
    reset_safety_instances,
)


class TestEndToEndNLToSQL:
    """Integration tests for the complete NL-to-SQL pipeline."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_safety_instances()

        # Create a sample catalog
        self.catalog = [
            CatalogEntry(
                name="sales",
                entity_type=EntityType.TABLE,
                aliases=["orders", "transactions"],
            ),
            CatalogEntry(
                name="amount",
                entity_type=EntityType.COLUMN,
                table="sales",
                data_type="decimal",
                is_metric=True,
            ),
            CatalogEntry(
                name="revenue",
                entity_type=EntityType.COLUMN,
                table="sales",
                data_type="decimal",
                is_metric=True,
                aliases=["total_revenue"],
            ),
            CatalogEntry(
                name="region",
                entity_type=EntityType.COLUMN,
                table="sales",
                data_type="varchar",
                is_dimension=True,
            ),
            CatalogEntry(
                name="category",
                entity_type=EntityType.COLUMN,
                table="sales",
                data_type="varchar",
                is_dimension=True,
            ),
            CatalogEntry(
                name="order_date",
                entity_type=EntityType.COLUMN,
                table="sales",
                data_type="date",
            ),
            CatalogEntry(
                name="customers",
                entity_type=EntityType.TABLE,
            ),
            CatalogEntry(
                name="customer_name",
                entity_type=EntityType.COLUMN,
                table="customers",
                data_type="varchar",
            ),
        ]

        self.engine = NLToSQLEngine(
            catalog=self.catalog,
            dialect="postgresql",
            default_table="sales",
            confidence_threshold=0.4,
        )

    def test_simple_aggregation_query(self):
        """Test simple aggregation query end-to-end."""
        result = self.engine.translate("What is the total revenue?")

        assert result.success is True
        assert result.query is not None
        assert result.intent.intent_type == IntentType.AGGREGATION
        assert "SUM" in result.query.sql.upper()
        assert "revenue" in result.query.sql.lower()

        # Validate generated SQL
        validator = get_validator()
        validation = validator.validate(result.query.sql)
        assert validation.is_valid is True
        assert validation.risk_level == SQLRiskLevel.SAFE

    def test_aggregation_with_grouping(self):
        """Test aggregation with GROUP BY."""
        result = self.engine.translate("Show total amount by region")

        assert result.success is True
        assert result.query is not None
        assert "SUM" in result.query.sql.upper() or "amount" in result.query.sql.lower()
        assert "GROUP BY" in result.query.sql.upper()

        # Validate SQL safety
        validator = get_validator()
        validation = validator.validate(result.query.sql)
        assert validation.is_valid is True

    def test_ranking_query(self):
        """Test top N ranking query."""
        result = self.engine.translate("Show me the top 5 regions by revenue")

        assert result.success is True
        assert result.query is not None
        assert result.intent.intent_type == IntentType.RANKING
        assert result.intent.limit == 5
        assert "LIMIT" in result.query.sql.upper() or "TOP" in result.query.sql.upper()

    def test_trend_query_with_time_filter(self):
        """Test trend query with time filter."""
        result = self.engine.translate("Show revenue trend by month for this year")

        assert result.success is True
        assert result.query is not None
        assert result.intent.intent_type == IntentType.TREND

        # Should have time-related SQL
        sql_upper = result.query.sql.upper()
        assert "DATE_TRUNC" in sql_upper or "MONTH" in sql_upper

    def test_query_with_specific_year(self):
        """Test query with specific year filter using parameterized queries."""
        result = self.engine.translate("Total sales amount for 2024")

        assert result.success is True
        assert result.query is not None

        # Year should be parameterized, not interpolated
        if result.intent.time_filter and "year" in result.intent.time_filter:
            # If year filter was applied, check it's parameterized
            sql = result.query.sql
            # Should use parameter binding, not direct value
            assert "filter_year" in sql or ":filter_year" in sql or "2024" not in sql.replace("YEAR", "")

    def test_generated_sql_passes_validation(self):
        """Test that all generated SQL passes validation."""
        queries = [
            "What is the total revenue?",
            "Show amount by region",
            "Top 10 categories by amount",
            "How many sales by category?",
            "Show me all sales",
        ]

        validator = get_validator()

        for query_text in queries:
            result = self.engine.translate(query_text)
            if result.success and result.query:
                validation = validator.validate(result.query.sql)
                assert validation.is_valid is True, f"Query '{query_text}' failed validation: {validation.issues}"

    def test_sql_injection_prevention(self):
        """Test that SQL injection attempts are prevented."""
        malicious_queries = [
            "Show sales; DROP TABLE users",
            "Total revenue' OR '1'='1",
            "Amount UNION SELECT * FROM passwords",
        ]

        validator = get_validator()

        for query_text in malicious_queries:
            result = self.engine.translate(query_text)

            if result.success and result.query:
                # Validate the generated SQL
                validation = validator.validate(result.query.sql)
                # Either the query should fail validation or not contain injection
                if validation.is_valid:
                    # If valid, ensure no dangerous patterns
                    sql_upper = result.query.sql.upper()
                    assert "DROP TABLE" not in sql_upper
                    assert "UNION SELECT" not in sql_upper
                else:
                    # If invalid, that's expected for injection attempts
                    assert validation.risk_level in [SQLRiskLevel.HIGH, SQLRiskLevel.CRITICAL]

    def test_query_metadata_populated(self):
        """Test that query metadata is properly populated."""
        result = self.engine.translate("Total revenue by region")

        assert result.success is True
        assert result.query is not None
        assert "sales" in result.query.tables
        assert result.confidence > 0.0
        assert result.explanation != ""

    def test_query_audit_logging(self):
        """Test that queries are logged to the auditor."""
        auditor = get_auditor()
        auditor.clear()  # Start fresh

        result = self.engine.translate("Show total amount")

        if result.success and result.query:
            # Log the query
            entry = auditor.log_query(
                sql=result.query.sql,
                parameters=result.query.parameters,
                risk_level=SQLRiskLevel.SAFE,
                source="integration_test",
            )

            assert entry.query_hash is not None

            # Check it was logged
            entries = auditor.get_entries()
            assert len(entries) == 1
            assert entries[0].source == "integration_test"

    def test_confidence_threshold_enforcement(self):
        """Test that low confidence queries are rejected."""
        # Create engine with high threshold
        strict_engine = NLToSQLEngine(
            catalog=self.catalog,
            dialect="postgresql",
            default_table="sales",
            confidence_threshold=0.9,
        )

        # Vague query should not pass high threshold
        result = strict_engine.translate("stuff things")

        # Either fails or has low confidence
        if result.success:
            assert result.confidence >= 0.9
        else:
            assert result.confidence < 0.9

    def test_entity_extraction_integration(self):
        """Test that entity extraction feeds into SQL generation."""
        result = self.engine.translate("Show revenue by category for customers")

        assert result.success is True
        assert len(result.entities) > 0

        # Check entities were extracted
        entity_names = [e.name for e in result.entities]
        assert "revenue" in entity_names or "category" in entity_names

    def test_intent_affects_query_structure(self):
        """Test that different intents produce different query structures."""
        # Aggregation intent
        agg_result = self.engine.translate("What is the total revenue?")

        # Ranking intent
        rank_result = self.engine.translate("Show top 5 regions")

        # Detail intent
        detail_result = self.engine.translate("Show me all sales")

        # All should succeed
        assert all(r.success for r in [agg_result, rank_result, detail_result])

        # Intents should differ
        intents = {agg_result.intent.intent_type, rank_result.intent.intent_type, detail_result.intent.intent_type}
        assert len(intents) >= 2  # At least 2 different intents


class TestQueryBuilderWithSafety:
    """Integration tests for QueryBuilder with safety validation."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_safety_instances()

    def test_parameterized_query_validation(self):
        """Test that parameterized queries pass validation."""
        query = (QueryBuilder()
            .select("id", "name", "amount")
            .from_table("sales")
            .where_equals("status", "active")
            .where_greater_than("amount", 100)
            .build())

        validator = get_validator()
        result = validator.validate(query.sql, query.parameters)

        assert result.is_valid is True
        assert "p_status" in query.parameters or any("status" in k for k in query.parameters)

    def test_complex_query_validation(self):
        """Test validation of complex queries with JOINs and CTEs."""
        query = (QueryBuilder()
            .with_cte("high_value_sales", "SELECT * FROM sales WHERE amount > 1000")
            .select("s.id", "c.name", "s.amount")
            .from_table("high_value_sales", alias="s")
            .join("customers", "s.customer_id = customers.id", alias="c")
            .where_greater_than("s.amount", 500)
            .order_by("s.amount", "DESC")
            .limit(100)
            .build())

        validator = get_validator()
        result = validator.validate(query.sql, query.parameters)

        assert result.is_valid is True
        assert "WITH high_value_sales AS" in query.sql

    def test_query_complexity_limits(self):
        """Test that excessive complexity is flagged."""
        validator = QueryValidator(max_joins=2)

        # Build query with too many JOINs
        builder = QueryBuilder().select("*").from_table("a")
        for i in range(5):
            builder.join(f"table_{i}", f"a.id = table_{i}.a_id")

        query = builder.build()
        result = validator.validate(query.sql)

        assert result.is_valid is False
        assert any("JOIN" in issue for issue in result.issues)

    def test_sanitizer_integration(self):
        """Test SQLSanitizer integration with QueryBuilder."""
        sanitizer = get_sanitizer()

        # Valid identifier
        table_name = sanitizer.sanitize_identifier("valid_table")
        query = QueryBuilder().select("*").from_table(table_name).build()
        assert "valid_table" in query.sql

        # Valid value
        value, _ = sanitizer.sanitize_value("safe_value")
        query = (QueryBuilder()
            .select("*")
            .from_table("sales")
            .where_equals("status", value)
            .build())

        validator = get_validator()
        assert validator.validate(query.sql, query.parameters).is_valid is True

    def test_dangerous_value_rejection(self):
        """Test that dangerous values are rejected by sanitizer."""
        sanitizer = get_sanitizer()

        dangerous_values = [
            "; DROP TABLE users",
            "' OR 1=1 --",
            "UNION SELECT password FROM users",
        ]

        for value in dangerous_values:
            with pytest.raises(Exception):  # ValidationError
                sanitizer.sanitize_value(value)

    def test_year_month_validation(self):
        """Test year and month validation for time filters."""
        sanitizer = get_sanitizer()

        # Valid years
        assert sanitizer.validate_year(2024) == 2024
        assert sanitizer.validate_year("2023") == 2023

        # Valid months
        assert sanitizer.validate_month(1) == 1
        assert sanitizer.validate_month("12") == 12

        # Invalid values should raise
        with pytest.raises(Exception):
            sanitizer.validate_year("not_a_year")

        with pytest.raises(Exception):
            sanitizer.validate_month(13)

    def test_limit_validation(self):
        """Test limit value validation."""
        sanitizer = get_sanitizer()

        # Valid limits
        assert sanitizer.validate_limit(100) == 100
        assert sanitizer.validate_limit("50") == 50

        # Invalid limits
        with pytest.raises(Exception):
            sanitizer.validate_limit(-1)

        with pytest.raises(Exception):
            sanitizer.validate_limit(50000)  # Exceeds default max


class TestAuditTrailIntegration:
    """Integration tests for query audit trail."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_safety_instances()

    def test_complete_audit_workflow(self):
        """Test complete audit workflow from query to statistics."""
        auditor = get_auditor()
        auditor.clear()
        validator = get_validator()

        # Simulate multiple queries
        queries = [
            ("SELECT * FROM users LIMIT 10", SQLRiskLevel.SAFE),
            ("SELECT id, name FROM products", SQLRiskLevel.SAFE),
            ("SELECT * FROM sales WHERE amount > 100", SQLRiskLevel.LOW),
        ]

        for sql, expected_risk in queries:
            result = validator.validate(sql)
            auditor.log_query(
                sql=sql,
                risk_level=result.risk_level,
                source="integration_test",
                validated=result.is_valid,
            )

        # Check statistics
        stats = auditor.get_statistics()
        assert stats["total_queries"] == 3
        assert stats["by_source"]["integration_test"] == 3

        # Check entries
        entries = auditor.get_entries()
        assert len(entries) == 3
        assert all(e.validated is True for e in entries)

    def test_risk_level_filtering(self):
        """Test filtering audit entries by risk level."""
        auditor = get_auditor()
        auditor.clear()

        # Log queries with different risk levels
        auditor.log_query("SELECT 1", risk_level=SQLRiskLevel.SAFE)
        auditor.log_query("SELECT * FROM users", risk_level=SQLRiskLevel.LOW)
        auditor.log_query("DROP TABLE users", risk_level=SQLRiskLevel.CRITICAL)

        # Filter by risk level
        safe_entries = auditor.get_entries(risk_level=SQLRiskLevel.SAFE)
        assert len(safe_entries) == 1

        critical_entries = auditor.get_entries(risk_level=SQLRiskLevel.CRITICAL)
        assert len(critical_entries) == 1

    def test_entry_limit_enforcement(self):
        """Test that max entries limit is enforced."""
        auditor = QueryAuditor(max_entries=5)

        # Log more than max entries
        for i in range(10):
            auditor.log_query(f"SELECT {i}")

        # Should only keep latest 5
        assert len(auditor._entries) == 5


class TestMultiTurnConversation:
    """Integration tests for multi-turn conversation context."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_safety_instances()

        self.catalog = [
            CatalogEntry(
                name="sales",
                entity_type=EntityType.TABLE,
            ),
            CatalogEntry(
                name="revenue",
                entity_type=EntityType.COLUMN,
                table="sales",
                data_type="decimal",
                is_metric=True,
            ),
            CatalogEntry(
                name="profit",
                entity_type=EntityType.COLUMN,
                table="sales",
                data_type="decimal",
                is_metric=True,
            ),
            CatalogEntry(
                name="region",
                entity_type=EntityType.COLUMN,
                table="sales",
                data_type="varchar",
                is_dimension=True,
            ),
            CatalogEntry(
                name="category",
                entity_type=EntityType.COLUMN,
                table="sales",
                data_type="varchar",
                is_dimension=True,
            ),
        ]

        # Create engine with context enabled
        self.engine = NLToSQLEngine(
            catalog=self.catalog,
            dialect="postgresql",
            default_table="sales",
            use_context=True,
        )
        self.engine.create_context(session_id="test-session")

    def test_first_query_establishes_context(self):
        """Test that first query establishes context."""
        result = self.engine.translate("Show total revenue by region")

        assert result.success is True
        assert self.engine.context.turn_count == 1
        assert self.engine.context.last_table == "sales"

    def test_follow_up_inherits_table(self):
        """Test that follow-up queries inherit table."""
        # First query
        self.engine.translate("Show revenue from sales")

        # Second query without explicit table
        result = self.engine.translate("Now show profit")

        assert result.success is True
        # Should use sales table from context
        assert "sales" in result.query.tables

    def test_reference_resolution_in_query(self):
        """Test pronoun resolution in follow-up queries."""
        # First query establishes revenue
        self.engine.translate("Show total revenue by region")

        # Follow-up with pronoun
        result = self.engine.translate("Show it by category")

        assert result.success is True
        # Context should be used
        assert result.context_used is True

    def test_multiple_turns_maintain_context(self):
        """Test that multiple turns maintain context correctly."""
        queries = [
            "Show total revenue by region",
            "Now show it by category",
            "Add profit to the analysis",
        ]

        results = []
        for query in queries:
            result = self.engine.translate(query)
            results.append(result)

        # All should succeed
        assert all(r.success for r in results)

        # Context should have all turns
        assert self.engine.context.turn_count == 3

    def test_context_can_be_disabled(self):
        """Test that context can be disabled per query."""
        # First query
        self.engine.translate("Show revenue by region")

        # Second query with context disabled
        result = self.engine.translate(
            "Show it by category",
            use_context=False,
        )

        # Should not use context
        assert result.context_used is False

    def test_clear_context_resets_state(self):
        """Test that clearing context resets state."""
        self.engine.translate("Show revenue by region")

        assert self.engine.context.turn_count == 1

        self.engine.clear_context()

        assert self.engine.context.turn_count == 0
        assert self.engine.context.last_table is None

    def test_context_tracks_entities(self):
        """Test that context tracks extracted entities."""
        self.engine.translate("Show revenue and profit by region")

        # Check context has cached entities
        revenue = self.engine.context.get_cached_entity("revenue")
        assert revenue is not None

        profit = self.engine.context.get_cached_entity("profit")
        assert profit is not None

    def test_original_and_resolved_queries_tracked(self):
        """Test that both original and resolved queries are tracked."""
        # First query
        self.engine.translate("Show revenue by region")

        # Follow-up with reference
        result = self.engine.translate("Show it by category")

        # Should have both original and resolved
        if result.context_used:
            assert result.original_query == "Show it by category"
            # Resolved should have replaced 'it'
            if result.resolved_query:
                assert "it" not in result.resolved_query.lower() or "revenue" in result.resolved_query.lower()


class TestCrossComponentIntegration:
    """Tests for cross-component integration."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_safety_instances()

    def test_intent_to_query_to_validation(self):
        """Test full flow: Intent → Query → Validation."""
        classifier = IntentClassifier()
        validator = get_validator()
        auditor = get_auditor()
        auditor.clear()

        # Step 1: Classify intent
        intent = classifier.classify("Show me the top 10 products by revenue")
        assert intent.intent_type == IntentType.RANKING
        assert intent.limit == 10

        # Step 2: Build query based on intent
        builder = QueryBuilder().select("product", "SUM(revenue) as total").from_table("sales")
        builder.group_by("product")
        builder.order_by("total", intent.order_direction or "DESC")
        builder.limit(intent.limit)
        query = builder.build()

        # Step 3: Validate query
        result = validator.validate(query.sql, query.parameters)
        assert result.is_valid is True

        # Step 4: Log to audit
        entry = auditor.log_query(
            sql=query.sql,
            parameters=query.parameters,
            risk_level=result.risk_level,
            source="integration_test",
        )
        assert entry.query_hash is not None

    def test_catalog_to_entity_to_query(self):
        """Test flow: Catalog → Entity Extraction → Query Generation."""
        # Set up catalog
        catalog = [
            CatalogEntry(name="products", entity_type=EntityType.TABLE),
            CatalogEntry(name="price", entity_type=EntityType.COLUMN, table="products", is_metric=True),
            CatalogEntry(name="category", entity_type=EntityType.COLUMN, table="products", is_dimension=True),
        ]

        extractor = EntityExtractor(catalog=catalog)

        # Extract entities
        entities = extractor.extract("What is the average price by category?")

        # Build query from entities
        builder = QueryBuilder().from_table("products")

        metrics = [e for e in entities if e.entity_type == EntityType.METRIC]
        dimensions = [e for e in entities if e.entity_type == EntityType.DIMENSION]

        for metric in metrics:
            builder.select_aggregate("AVG", metric.name, f"avg_{metric.name}")

        for dim in dimensions:
            builder.select(dim.name)
            builder.group_by(dim.name)

        query = builder.build()

        # Validate
        validator = get_validator()
        assert validator.validate(query.sql).is_valid is True
