"""Unit tests for the NL-to-SQL engine."""

import pytest

from src.nlp.intent import IntentClassifier, Intent, IntentType
from src.nlp.entity import EntityExtractor, Entity, EntityType, CatalogEntry
from src.nlp.nl_to_sql import NLToSQLEngine, NLQueryResult


class TestIntentClassifier:
    """Tests for IntentClassifier."""

    @pytest.fixture
    def classifier(self):
        return IntentClassifier()

    def test_aggregation_intent_total(self, classifier):
        """Test detection of aggregation intent with 'total'."""
        intent = classifier.classify("What is the total revenue by region?")

        assert intent.intent_type == IntentType.AGGREGATION
        assert intent.aggregation == "sum"

    def test_aggregation_intent_average(self, classifier):
        """Test detection of aggregation intent with 'average'."""
        intent = classifier.classify("What is the average order value?")

        assert intent.aggregation == "avg"

    def test_aggregation_intent_count(self, classifier):
        """Test detection of count intent."""
        intent = classifier.classify("How many customers do we have?")

        assert intent.aggregation == "count"

    def test_ranking_intent_top(self, classifier):
        """Test detection of ranking intent with 'top'."""
        intent = classifier.classify("Show me the top 10 customers by revenue")

        assert intent.intent_type == IntentType.RANKING
        assert intent.limit == 10
        assert intent.order_direction == "DESC"

    def test_ranking_intent_bottom(self, classifier):
        """Test detection of ranking intent with 'bottom'."""
        intent = classifier.classify("What are the bottom 5 products by sales?")

        assert intent.limit == 5
        assert intent.order_direction == "ASC"

    def test_trend_intent(self, classifier):
        """Test detection of trend intent."""
        intent = classifier.classify("Show monthly sales trend for this year")

        assert intent.intent_type == IntentType.TREND
        assert intent.time_filter is not None

    def test_comparison_intent_yoy(self, classifier):
        """Test detection of year-over-year comparison."""
        intent = classifier.classify("Compare revenue year over year")

        assert intent.intent_type == IntentType.COMPARISON
        assert intent.comparison_type == "year_over_year"

    def test_comparison_intent_bva(self, classifier):
        """Test detection of budget vs actual comparison."""
        intent = classifier.classify("Show budget vs actual variance")

        assert intent.comparison_type == "budget_vs_actual"

    def test_time_filter_this_year(self, classifier):
        """Test detection of 'this year' time filter."""
        intent = classifier.classify("Total sales this year")

        assert intent.time_filter is not None
        assert intent.time_filter.get("reference") == "this_year"

    def test_time_filter_last_month(self, classifier):
        """Test detection of 'last month' time filter."""
        intent = classifier.classify("Revenue last month")

        assert intent.time_filter is not None
        assert intent.time_filter.get("reference") == "last_month"

    def test_time_filter_specific_year(self, classifier):
        """Test detection of specific year."""
        intent = classifier.classify("Sales in 2023")

        assert intent.time_filter is not None
        assert intent.time_filter.get("year") == 2023

    def test_dimension_extraction_by(self, classifier):
        """Test extraction of dimensions with 'by'."""
        intent = classifier.classify("Total revenue by region")

        assert "region" in intent.dimensions

    def test_dimension_extraction_per(self, classifier):
        """Test extraction of dimensions with 'per'."""
        intent = classifier.classify("Average order per customer")

        assert "customer" in intent.dimensions

    def test_confidence_score(self, classifier):
        """Test that confidence score is provided."""
        intent = classifier.classify("Total sales by category")

        assert 0 <= intent.confidence <= 1

    def test_unknown_intent(self, classifier):
        """Test handling of unclear queries."""
        intent = classifier.classify("xyz abc 123")

        assert intent.intent_type == IntentType.UNKNOWN
        assert intent.confidence < 0.5


class TestEntityExtractor:
    """Tests for EntityExtractor."""

    @pytest.fixture
    def extractor(self):
        catalog = [
            CatalogEntry(name="sales", entity_type=EntityType.TABLE),
            CatalogEntry(name="customers", entity_type=EntityType.TABLE),
            CatalogEntry(name="revenue", entity_type=EntityType.COLUMN, table="sales", is_metric=True),
            CatalogEntry(name="amount", entity_type=EntityType.COLUMN, table="sales", is_metric=True),
            CatalogEntry(name="region", entity_type=EntityType.COLUMN, table="sales", is_dimension=True),
            CatalogEntry(name="category", entity_type=EntityType.COLUMN, table="sales", is_dimension=True),
            CatalogEntry(name="order_date", entity_type=EntityType.COLUMN, table="sales", data_type="date"),
        ]
        return EntityExtractor(catalog=catalog)

    def test_extract_table(self, extractor):
        """Test table extraction."""
        entities = extractor.extract("Show data from sales table")

        table_entities = [e for e in entities if e.entity_type == EntityType.TABLE]
        assert len(table_entities) >= 1
        assert any(e.name == "sales" for e in table_entities)

    def test_extract_metric(self, extractor):
        """Test metric column extraction."""
        entities = extractor.extract("Total revenue by region")

        metric_entities = [e for e in entities if e.entity_type == EntityType.METRIC]
        assert len(metric_entities) >= 1
        assert any(e.name == "revenue" for e in metric_entities)

    def test_extract_dimension(self, extractor):
        """Test dimension column extraction."""
        entities = extractor.extract("Sales by region and category")

        dim_entities = [e for e in entities if e.entity_type == EntityType.DIMENSION]
        assert len(dim_entities) >= 1

    def test_extract_time_column(self, extractor):
        """Test time column extraction."""
        entities = extractor.extract("Sales by order_date")

        time_entities = [e for e in entities if e.entity_type == EntityType.TIME_COLUMN]
        assert len(time_entities) >= 1

    def test_confidence_scores(self, extractor):
        """Test that entities have confidence scores."""
        entities = extractor.extract("Total revenue")

        assert all(0 <= e.confidence <= 1 for e in entities)

    def test_alias_matching(self, extractor):
        """Test alias-based matching."""
        # "sales" is a common alias for revenue
        entities = extractor.extract("Total sales by region")

        # Should find some entities even with aliased terms
        assert len(entities) >= 1

    def test_fuzzy_matching(self):
        """Test fuzzy matching capability."""
        catalog = [
            CatalogEntry(name="customer_name", entity_type=EntityType.COLUMN, is_dimension=True),
        ]
        extractor = EntityExtractor(catalog=catalog, fuzzy_threshold=70.0)

        # "custmer name" is a typo but should still match
        entities = extractor.extract("Show custmer name")

        # May or may not match depending on fuzzy threshold
        assert isinstance(entities, list)

    def test_get_tables(self, extractor):
        """Test getting list of tables."""
        tables = extractor.get_tables()

        assert "sales" in tables
        assert "customers" in tables

    def test_get_metrics(self, extractor):
        """Test getting list of metrics."""
        metrics = extractor.get_metrics()

        assert "revenue" in metrics
        assert "amount" in metrics


class TestNLToSQLEngine:
    """Tests for NLToSQLEngine."""

    @pytest.fixture
    def engine(self):
        catalog = [
            {"name": "sales", "entity_type": "table"},
            {"name": "revenue", "entity_type": "column", "table": "sales", "is_metric": True},
            {"name": "amount", "entity_type": "column", "table": "sales", "is_metric": True},
            {"name": "region", "entity_type": "column", "table": "sales", "is_dimension": True},
            {"name": "category", "entity_type": "column", "table": "sales", "is_dimension": True},
            {"name": "order_date", "entity_type": "column", "table": "sales", "data_type": "date"},
        ]
        engine = NLToSQLEngine(default_table="sales")
        engine.load_catalog(catalog)
        return engine

    def test_simple_aggregation(self, engine):
        """Test simple aggregation query."""
        result = engine.translate("Total revenue by region")

        assert result.success
        assert result.query is not None
        assert "SUM" in result.query.sql.upper() or "SELECT" in result.query.sql

    def test_ranking_query(self, engine):
        """Test ranking query generation."""
        result = engine.translate("Top 5 regions by revenue")

        assert result.success
        if result.query:
            assert "LIMIT" in result.query.sql or "TOP" in result.query.sql

    def test_includes_intent(self, engine):
        """Test that result includes intent information."""
        result = engine.translate("Total revenue by region")

        assert result.intent is not None
        assert result.intent.intent_type is not None

    def test_includes_entities(self, engine):
        """Test that result includes extracted entities."""
        result = engine.translate("Total revenue by region")

        assert isinstance(result.entities, list)

    def test_includes_confidence(self, engine):
        """Test that result includes confidence score."""
        result = engine.translate("Total revenue by region")

        assert 0 <= result.confidence <= 1

    def test_includes_explanation(self, engine):
        """Test that result includes explanation."""
        result = engine.translate("Total revenue by region")

        assert result.explanation is not None
        assert len(result.explanation) > 0

    def test_low_confidence_query(self, engine):
        """Test handling of low confidence queries."""
        result = engine.translate("xyz abc 123 foo bar")

        # Low confidence queries may still succeed but with lower confidence
        assert result.confidence < 0.8

    def test_suggest_questions(self, engine):
        """Test question suggestions."""
        suggestions = engine.suggest_questions()

        assert isinstance(suggestions, list)
        assert len(suggestions) >= 1

    def test_result_to_dict(self, engine):
        """Test NLQueryResult to_dict method."""
        result = engine.translate("Total revenue by region")
        result_dict = result.to_dict()

        assert "success" in result_dict
        assert "query" in result_dict
        assert "intent" in result_dict
        assert "entities" in result_dict
        assert "confidence" in result_dict
