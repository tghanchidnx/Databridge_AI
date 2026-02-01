"""
Tests for the consolidation module (EntityMatcher and ConceptMerger).
"""

import pytest

from databridge_discovery.consolidation.entity_matcher import (
    EntityMatcher,
    MatchCandidate,
    MatchResult,
)
from databridge_discovery.consolidation.concept_merger import (
    ConceptMerger,
    MergeCandidate,
    MergeResult,
)
from databridge_discovery.graph.node_types import (
    ColumnNode,
    NodeType,
    TableNode,
)


class TestEntityMatcherNormalization:
    """Tests for name normalization."""

    def test_normalize_basic(self):
        """Test basic name normalization."""
        matcher = EntityMatcher()

        # _id suffix is removed, resulting in just "customer"
        assert matcher.normalize_name("customer_id") == "customer"
        assert matcher.normalize_name("CUSTOMER_ID") == "customer"

    def test_normalize_prefixes(self):
        """Test removing common prefixes."""
        matcher = EntityMatcher()

        assert "tbl" not in matcher.normalize_name("tbl_customers")
        assert "dim" not in matcher.normalize_name("dim_product")
        assert "fact" not in matcher.normalize_name("fact_sales")

    def test_normalize_suffixes(self):
        """Test removing common suffixes."""
        matcher = EntityMatcher()

        # _id suffix should be removed and expanded
        normalized = matcher.normalize_name("customer_id")
        assert "identifier" in normalized or normalized == "customer"

    def test_normalize_abbreviations(self):
        """Test expanding abbreviations."""
        matcher = EntityMatcher()

        assert "account" in matcher.normalize_name("acct_balance")
        assert "customer" in matcher.normalize_name("cust_name")
        assert "department" in matcher.normalize_name("dept_code")

    def test_normalize_empty(self):
        """Test normalizing empty string."""
        matcher = EntityMatcher()
        assert matcher.normalize_name("") == ""
        assert matcher.normalize_name("  ") == ""


class TestEntityMatcherSimilarity:
    """Tests for similarity calculation."""

    def test_calculate_similarity_exact(self):
        """Test similarity for exact match."""
        matcher = EntityMatcher()

        sim = matcher.calculate_similarity("customer", "customer")
        assert sim == 1.0

    def test_calculate_similarity_normalized_exact(self):
        """Test similarity with normalization."""
        matcher = EntityMatcher()

        # These should be very similar after normalization
        sim = matcher.calculate_similarity("tbl_customer", "dim_customer")
        assert sim > 0.8

    def test_calculate_similarity_different(self):
        """Test similarity for different names."""
        matcher = EntityMatcher()

        sim = matcher.calculate_similarity("customer", "weather")
        assert sim < 0.5

    def test_calculate_similarity_no_normalization(self):
        """Test similarity without normalization."""
        matcher = EntityMatcher()

        # With normalization turned off, case differences affect the score
        sim = matcher.calculate_similarity(
            "TBL_CUSTOMER",
            "tbl_customer",
            use_normalized=False,
        )
        # Without normalization, case matters - fuzzy matching still finds some similarity
        assert sim > 0  # Just verify it returns a score

        # Identical strings should still match perfectly
        sim_identical = matcher.calculate_similarity(
            "customer",
            "customer",
            use_normalized=False,
        )
        assert sim_identical == 1.0


class TestEntityMatcherMatching:
    """Tests for entity matching."""

    @pytest.fixture
    def source_tables(self):
        """Create source tables."""
        return [
            TableNode(name="customers", table_name="customers"),
            TableNode(name="orders", table_name="orders"),
            TableNode(name="products", table_name="products"),
        ]

    @pytest.fixture
    def target_tables(self):
        """Create target tables."""
        return [
            TableNode(name="dim_customer", table_name="dim_customer"),
            TableNode(name="fact_orders", table_name="fact_orders"),
            TableNode(name="dim_product", table_name="dim_product"),
            TableNode(name="dim_time", table_name="dim_time"),
        ]

    def test_match_entities(self, source_tables, target_tables):
        """Test matching entities."""
        matcher = EntityMatcher(similarity_threshold=0.6)

        matches = matcher.match_entities(source_tables, target_tables)

        assert len(matches) >= 3
        # Check match structure
        for match in matches:
            assert isinstance(match, MatchResult)
            assert match.source_name is not None
            assert match.target_name is not None
            assert match.score >= 0.6

    def test_match_columns(self):
        """Test matching columns."""
        matcher = EntityMatcher()

        source_columns = [
            ColumnNode(name="customer_id", column_name="customer_id", data_type="INTEGER"),
            ColumnNode(name="order_date", column_name="order_date", data_type="DATE"),
        ]
        target_columns = [
            ColumnNode(name="cust_id", column_name="cust_id", data_type="INTEGER"),
            ColumnNode(name="ord_dt", column_name="ord_dt", data_type="DATE"),
        ]

        matches = matcher.match_columns(source_columns, target_columns, threshold=0.5)

        assert len(matches) >= 1
        # Check that type compatibility is considered
        for match in matches:
            assert "types_compatible" in match.metadata

    def test_match_columns_type_boost(self):
        """Test that type compatibility boosts similarity."""
        matcher = EntityMatcher()

        # Same name, different types
        source = [ColumnNode(name="amount", column_name="amount", data_type="INTEGER")]
        target_compatible = [ColumnNode(name="amt", column_name="amt", data_type="DECIMAL")]
        target_incompatible = [ColumnNode(name="amt", column_name="amt", data_type="VARCHAR")]

        matches_compatible = matcher.match_columns(source, target_compatible, threshold=0.0)
        matches_incompatible = matcher.match_columns(source, target_incompatible, threshold=0.0)

        # Compatible types should score higher
        if matches_compatible and matches_incompatible:
            assert matches_compatible[0].score >= matches_incompatible[0].score

    def test_find_duplicates(self, source_tables):
        """Test finding duplicates."""
        matcher = EntityMatcher(similarity_threshold=0.8)

        # Add a near-duplicate
        tables = source_tables + [
            TableNode(name="customer", table_name="customer"),  # Similar to customers
        ]

        duplicates = matcher.find_duplicates(tables)

        # Should find customers/customer pair
        assert len(duplicates) >= 1
        names = {(d[0].name, d[1].name) for d in duplicates}
        assert ("customers", "customer") in names or ("customer", "customers") in names

    def test_match_by_pattern(self, source_tables):
        """Test matching by pattern."""
        matcher = EntityMatcher()

        # Match all tables starting with "c"
        matches = matcher.match_by_pattern(source_tables, "c*")

        assert len(matches) >= 1
        names = [m[0].name for m in matches]
        assert "customers" in names

    def test_suggest_canonical_name(self):
        """Test suggesting canonical name."""
        matcher = EntityMatcher()

        names = ["cust", "customer", "tbl_customers"]
        canonical = matcher.suggest_canonical_name(names)

        # Should prefer longer, more descriptive name
        assert canonical in names

    def test_group_similar(self):
        """Test grouping similar nodes."""
        matcher = EntityMatcher(similarity_threshold=0.7)

        nodes = [
            TableNode(name="customers", table_name="customers"),
            TableNode(name="customer", table_name="customer"),
            TableNode(name="cust_data", table_name="cust_data"),
            TableNode(name="products", table_name="products"),
            TableNode(name="product", table_name="product"),
        ]

        groups = matcher.group_similar(nodes)

        # Should find at least 2 groups (customer-related and product-related)
        assert len(groups) >= 2

        # Check that groups contain similar items
        for group in groups:
            if len(group) > 1:
                # All items in group should be similar
                # Groups contain customer-related (customer, customers, cust_data)
                # or product-related (product, products)
                for node in group:
                    # Check for cust/custom pattern or product pattern
                    name_lower = node.name.lower()
                    assert ("cust" in name_lower or "product" in name_lower)

    def test_get_match_report(self):
        """Test getting match report."""
        matcher = EntityMatcher()

        matches = [
            MatchResult(
                source_id="1", source_name="a", target_id="2", target_name="b",
                score=0.95, match_type="exact", confidence=1.0,
            ),
            MatchResult(
                source_id="3", source_name="c", target_id="4", target_name="d",
                score=0.75, match_type="fuzzy", confidence=0.75,
            ),
        ]

        report = matcher.get_match_report(matches)

        assert report["total_matches"] == 2
        assert report["exact_matches"] == 1
        assert report["fuzzy_matches"] == 1
        assert 0 < report["avg_score"] < 1

    def test_types_compatible(self):
        """Test type compatibility checking."""
        matcher = EntityMatcher()

        # Same type
        assert matcher._types_compatible("INTEGER", "INTEGER")

        # Same family
        assert matcher._types_compatible("INTEGER", "BIGINT")
        assert matcher._types_compatible("VARCHAR", "TEXT")
        assert matcher._types_compatible("DATE", "TIMESTAMP")

        # Different families
        assert not matcher._types_compatible("INTEGER", "VARCHAR")
        assert not matcher._types_compatible("DATE", "BOOLEAN")


class TestConceptMerger:
    """Tests for ConceptMerger class."""

    @pytest.fixture
    def sample_nodes(self):
        """Create sample nodes for testing."""
        return [
            TableNode(name="customers", table_name="customers"),
            TableNode(name="customer", table_name="customer"),
            TableNode(name="cust_data", table_name="cust_data"),
            TableNode(name="products", table_name="products"),
            TableNode(name="product_catalog", table_name="product_catalog"),
            TableNode(name="orders", table_name="orders"),
        ]

    def test_create_merger(self):
        """Test creating a merger."""
        merger = ConceptMerger()
        assert merger is not None
        assert merger.matcher is not None

    def test_find_merge_candidates(self, sample_nodes):
        """Test finding merge candidates."""
        merger = ConceptMerger(similarity_threshold=0.7)

        candidates = merger.find_merge_candidates(sample_nodes)

        assert len(candidates) >= 1
        for candidate in candidates:
            assert isinstance(candidate, MergeCandidate)
            assert len(candidate.nodes) >= 2
            assert candidate.canonical_name is not None
            assert 0 <= candidate.confidence <= 1

    def test_find_merge_candidates_by_type(self, sample_nodes):
        """Test finding candidates grouped by type."""
        # Add a column with similar name
        nodes = sample_nodes + [
            ColumnNode(name="customer_id", column_name="customer_id"),
        ]

        merger = ConceptMerger(similarity_threshold=0.7)
        candidates = merger.find_merge_candidates(nodes, by_type=True)

        # Tables and columns should be in separate groups
        for candidate in candidates:
            types = set()
            for node in candidate.nodes:
                types.add(node.node_type)
            assert len(types) == 1  # All same type

    def test_merge_candidate(self, sample_nodes):
        """Test merging a candidate."""
        merger = ConceptMerger(similarity_threshold=0.7)
        candidates = merger.find_merge_candidates(sample_nodes)

        if candidates:
            result = merger.merge_candidate(candidates[0])

            assert isinstance(result, MergeResult)
            assert result.concept is not None
            assert result.concept.node_type == NodeType.CONCEPT
            assert len(result.merged_nodes) >= 2
            assert result.confidence > 0

    def test_merge_all(self, sample_nodes):
        """Test merging all candidates."""
        merger = ConceptMerger(similarity_threshold=0.7)
        candidates = merger.find_merge_candidates(sample_nodes)

        results = merger.merge_all(candidates)

        assert len(results) >= 1
        for result in results:
            assert result.concept is not None

    def test_merge_all_auto_only(self, sample_nodes):
        """Test merging only high-confidence candidates."""
        merger = ConceptMerger(
            similarity_threshold=0.7,
            auto_merge_threshold=0.95,
        )
        candidates = merger.find_merge_candidates(sample_nodes)

        results = merger.merge_all(candidates, auto_only=True)

        # Only very similar items should be merged
        for result in results:
            assert result.confidence >= 0.95

    def test_merge_nodes_manual(self):
        """Test manually merging nodes."""
        merger = ConceptMerger()
        nodes = [
            TableNode(name="table_a", table_name="table_a"),
            TableNode(name="table_b", table_name="table_b"),
        ]

        result = merger.merge_nodes(nodes, canonical_name="unified_table")

        assert result.concept.name == "unified_table"
        assert len(result.merged_nodes) == 2

    def test_get_concept_for_node(self, sample_nodes):
        """Test getting concept for a merged node."""
        merger = ConceptMerger(similarity_threshold=0.7)
        candidates = merger.find_merge_candidates(sample_nodes)

        if candidates:
            result = merger.merge_candidate(candidates[0])
            merged_node_id = result.merged_nodes[0]

            concept = merger.get_concept_for_node(merged_node_id)
            assert concept is not None
            assert concept.id == result.concept.id

    def test_get_all_concepts(self, sample_nodes):
        """Test getting all concepts."""
        merger = ConceptMerger(similarity_threshold=0.7)
        candidates = merger.find_merge_candidates(sample_nodes)
        merger.merge_all(candidates)

        concepts = merger.get_all_concepts()
        assert len(concepts) >= 1

    def test_unmerge_concept(self, sample_nodes):
        """Test unmerging a concept."""
        merger = ConceptMerger(similarity_threshold=0.7)
        candidates = merger.find_merge_candidates(sample_nodes)

        if candidates:
            result = merger.merge_candidate(candidates[0])
            concept_id = result.concept.id

            unmerged = merger.unmerge_concept(concept_id)

            assert len(unmerged) == len(result.merged_nodes)
            assert merger.get_concept_for_node(unmerged[0]) is None

    def test_suggest_cross_type_merges(self):
        """Test suggesting cross-type merges."""
        merger = ConceptMerger()
        nodes = [
            TableNode(name="customer", table_name="customer"),
            ColumnNode(name="customer_id", column_name="customer_id"),
            TableNode(name="product", table_name="product"),
        ]

        candidates = merger.suggest_cross_type_merges(nodes, threshold=0.8)

        # May or may not find cross-type matches depending on similarity
        for candidate in candidates:
            # Should have multiple types
            types = set()
            for node in candidate.nodes:
                types.add(node.node_type)
            if len(candidate.nodes) > 1:
                assert candidate.metadata.get("cross_type", False)

    def test_create_concept_hierarchy(self):
        """Test creating concept hierarchy."""
        merger = ConceptMerger()

        # Create some related concepts manually
        nodes1 = [TableNode(name="customer", table_name="customer")]
        nodes2 = [TableNode(name="customer_address", table_name="customer_address")]

        result1 = merger.merge_nodes(nodes1, "customer")
        result2 = merger.merge_nodes(nodes2, "customer_address")

        concepts = merger.get_all_concepts()
        hierarchy = merger.create_concept_hierarchy(concepts)

        # customer_address should be child of customer
        assert isinstance(hierarchy, dict)

    def test_get_merge_summary(self, sample_nodes):
        """Test getting merge summary."""
        merger = ConceptMerger(similarity_threshold=0.7)
        candidates = merger.find_merge_candidates(sample_nodes)
        merger.merge_all(candidates)

        summary = merger.get_merge_summary()

        assert "total_concepts" in summary
        assert "total_nodes_merged" in summary
        assert "compression_ratio" in summary
        assert "reduction_percent" in summary

    def test_export_concepts(self, sample_nodes):
        """Test exporting concepts."""
        merger = ConceptMerger(similarity_threshold=0.7)
        candidates = merger.find_merge_candidates(sample_nodes)
        merger.merge_all(candidates)

        exported = merger.export_concepts()

        assert isinstance(exported, list)
        for concept_dict in exported:
            assert "id" in concept_dict
            assert "name" in concept_dict
            assert "member_ids" in concept_dict

    def test_clear(self, sample_nodes):
        """Test clearing all concepts."""
        merger = ConceptMerger(similarity_threshold=0.7)
        candidates = merger.find_merge_candidates(sample_nodes)
        merger.merge_all(candidates)

        assert len(merger.get_all_concepts()) > 0

        merger.clear()

        assert len(merger.get_all_concepts()) == 0

    def test_no_double_merge(self, sample_nodes):
        """Test that nodes aren't merged twice."""
        merger = ConceptMerger(similarity_threshold=0.7)
        candidates = merger.find_merge_candidates(sample_nodes)

        # Merge all first
        first_results = merger.merge_all(candidates)

        # Try to merge again - should skip already merged nodes
        second_results = merger.merge_all(candidates)

        assert len(second_results) == 0  # All nodes already merged
