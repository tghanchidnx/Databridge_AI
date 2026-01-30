"""
Unit tests for TreeBuilder and TreeNavigator.
"""

import pytest


class TestTreeBuilder:
    """Tests for TreeBuilder class."""

    def test_build_simple_tree(self):
        """Test building a simple tree."""
        from src.hierarchy.tree import TreeBuilder

        items = [
            {"hierarchy_id": "ROOT", "hierarchy_name": "Root", "parent_id": None},
            {"hierarchy_id": "CHILD1", "hierarchy_name": "Child 1", "parent_id": "ROOT"},
            {"hierarchy_id": "CHILD2", "hierarchy_name": "Child 2", "parent_id": "ROOT"},
        ]

        builder = TreeBuilder()
        roots = builder.build(items)

        assert len(roots) == 1
        assert roots[0].hierarchy_id == "ROOT"
        assert len(roots[0].children) == 2

    def test_build_multi_level_tree(self):
        """Test building a multi-level tree."""
        from src.hierarchy.tree import TreeBuilder

        items = [
            {"hierarchy_id": "L1", "hierarchy_name": "Level 1", "parent_id": None},
            {"hierarchy_id": "L2", "hierarchy_name": "Level 2", "parent_id": "L1"},
            {"hierarchy_id": "L3", "hierarchy_name": "Level 3", "parent_id": "L2"},
            {"hierarchy_id": "L4", "hierarchy_name": "Level 4", "parent_id": "L3"},
        ]

        builder = TreeBuilder()
        roots = builder.build(items)

        assert len(roots) == 1
        assert roots[0].children[0].children[0].children[0].hierarchy_id == "L4"

    def test_build_multiple_roots(self):
        """Test building tree with multiple roots."""
        from src.hierarchy.tree import TreeBuilder

        items = [
            {"hierarchy_id": "ROOT1", "hierarchy_name": "Root 1", "parent_id": None},
            {"hierarchy_id": "ROOT2", "hierarchy_name": "Root 2", "parent_id": None},
            {"hierarchy_id": "CHILD1", "hierarchy_name": "Child 1", "parent_id": "ROOT1"},
        ]

        builder = TreeBuilder()
        roots = builder.build(items)

        assert len(roots) == 2

    def test_depth_calculation(self):
        """Test depth is calculated correctly."""
        from src.hierarchy.tree import TreeBuilder

        items = [
            {"hierarchy_id": "A", "hierarchy_name": "A", "parent_id": None},
            {"hierarchy_id": "B", "hierarchy_name": "B", "parent_id": "A"},
            {"hierarchy_id": "C", "hierarchy_name": "C", "parent_id": "B"},
        ]

        builder = TreeBuilder()
        roots = builder.build(items)

        assert roots[0].depth == 0
        assert roots[0].children[0].depth == 1
        assert roots[0].children[0].children[0].depth == 2

    def test_path_calculation(self):
        """Test path is calculated correctly."""
        from src.hierarchy.tree import TreeBuilder

        items = [
            {"hierarchy_id": "A", "hierarchy_name": "A", "parent_id": None},
            {"hierarchy_id": "B", "hierarchy_name": "B", "parent_id": "A"},
            {"hierarchy_id": "C", "hierarchy_name": "C", "parent_id": "B"},
        ]

        builder = TreeBuilder()
        roots = builder.build(items)

        leaf = roots[0].children[0].children[0]
        assert leaf.path == ["A", "B"]

    def test_sort_by_sort_order(self):
        """Test children are sorted by sort_order."""
        from src.hierarchy.tree import TreeBuilder

        items = [
            {"hierarchy_id": "ROOT", "hierarchy_name": "Root", "parent_id": None},
            {"hierarchy_id": "C1", "hierarchy_name": "Child 1", "parent_id": "ROOT", "sort_order": 3},
            {"hierarchy_id": "C2", "hierarchy_name": "Child 2", "parent_id": "ROOT", "sort_order": 1},
            {"hierarchy_id": "C3", "hierarchy_name": "Child 3", "parent_id": "ROOT", "sort_order": 2},
        ]

        builder = TreeBuilder()
        roots = builder.build(items)

        children = roots[0].children
        assert children[0].hierarchy_id == "C2"
        assert children[1].hierarchy_id == "C3"
        assert children[2].hierarchy_id == "C1"


class TestHierarchyNode:
    """Tests for HierarchyNode class."""

    def test_is_root(self):
        """Test is_root method."""
        from src.hierarchy.tree import HierarchyNode

        root = HierarchyNode(hierarchy_id="ROOT", hierarchy_name="Root", parent_id=None)
        child = HierarchyNode(hierarchy_id="CHILD", hierarchy_name="Child", parent_id="ROOT")

        assert root.is_root() is True
        assert child.is_root() is False

    def test_is_leaf(self):
        """Test is_leaf method."""
        from src.hierarchy.tree import HierarchyNode

        parent = HierarchyNode(hierarchy_id="P", hierarchy_name="Parent")
        child = HierarchyNode(hierarchy_id="C", hierarchy_name="Child", parent_id="P")
        parent.children.append(child)

        assert parent.is_leaf() is False
        assert child.is_leaf() is True

    def test_descendant_count(self):
        """Test descendant_count method."""
        from src.hierarchy.tree import HierarchyNode

        root = HierarchyNode(hierarchy_id="R", hierarchy_name="Root")
        c1 = HierarchyNode(hierarchy_id="C1", hierarchy_name="C1")
        c2 = HierarchyNode(hierarchy_id="C2", hierarchy_name="C2")
        c1_1 = HierarchyNode(hierarchy_id="C1_1", hierarchy_name="C1_1")

        root.children = [c1, c2]
        c1.children = [c1_1]

        assert root.descendant_count() == 3
        assert c1.descendant_count() == 1
        assert c2.descendant_count() == 0

    def test_to_dict(self):
        """Test to_dict method."""
        from src.hierarchy.tree import HierarchyNode

        node = HierarchyNode(
            hierarchy_id="TEST",
            hierarchy_name="Test Node",
            parent_id=None,
            data={"level_1": "Revenue"},
        )

        result = node.to_dict()

        assert result["hierarchy_id"] == "TEST"
        assert result["hierarchy_name"] == "Test Node"
        assert result["level_1"] == "Revenue"


class TestTreeNavigator:
    """Tests for TreeNavigator class."""

    @pytest.fixture
    def sample_tree(self):
        """Create a sample tree for testing."""
        from src.hierarchy.tree import TreeBuilder

        items = [
            {"hierarchy_id": "ROOT", "hierarchy_name": "Root", "parent_id": None},
            {"hierarchy_id": "A", "hierarchy_name": "A", "parent_id": "ROOT"},
            {"hierarchy_id": "B", "hierarchy_name": "B", "parent_id": "ROOT"},
            {"hierarchy_id": "A1", "hierarchy_name": "A1", "parent_id": "A"},
            {"hierarchy_id": "A2", "hierarchy_name": "A2", "parent_id": "A"},
            {"hierarchy_id": "B1", "hierarchy_name": "B1", "parent_id": "B"},
        ]

        builder = TreeBuilder()
        return builder.build(items)

    def test_get_node(self, sample_tree):
        """Test getting a node by ID."""
        from src.hierarchy.tree import TreeNavigator

        nav = TreeNavigator(sample_tree)

        node = nav.get_node("A1")
        assert node is not None
        assert node.hierarchy_id == "A1"

    def test_get_parent(self, sample_tree):
        """Test getting parent of a node."""
        from src.hierarchy.tree import TreeNavigator

        nav = TreeNavigator(sample_tree)

        parent = nav.get_parent("A1")
        assert parent.hierarchy_id == "A"

        root_parent = nav.get_parent("ROOT")
        assert root_parent is None

    def test_get_children(self, sample_tree):
        """Test getting children of a node."""
        from src.hierarchy.tree import TreeNavigator

        nav = TreeNavigator(sample_tree)

        children = nav.get_children("A")
        assert len(children) == 2
        child_ids = [c.hierarchy_id for c in children]
        assert "A1" in child_ids
        assert "A2" in child_ids

    def test_get_siblings(self, sample_tree):
        """Test getting siblings of a node."""
        from src.hierarchy.tree import TreeNavigator

        nav = TreeNavigator(sample_tree)

        siblings = nav.get_siblings("A")
        assert len(siblings) == 1
        assert siblings[0].hierarchy_id == "B"

    def test_get_ancestors(self, sample_tree):
        """Test getting ancestors of a node."""
        from src.hierarchy.tree import TreeNavigator

        nav = TreeNavigator(sample_tree)

        ancestors = nav.get_ancestors("A1")
        ancestor_ids = [a.hierarchy_id for a in ancestors]
        assert ancestor_ids == ["A", "ROOT"]

    def test_get_descendants(self, sample_tree):
        """Test getting descendants of a node."""
        from src.hierarchy.tree import TreeNavigator

        nav = TreeNavigator(sample_tree)

        descendants = nav.get_descendants("ROOT")
        assert len(descendants) == 5  # A, B, A1, A2, B1

    def test_get_leaves(self, sample_tree):
        """Test getting leaf nodes."""
        from src.hierarchy.tree import TreeNavigator

        nav = TreeNavigator(sample_tree)

        leaves = nav.get_leaves()
        leaf_ids = [l.hierarchy_id for l in leaves]
        assert "A1" in leaf_ids
        assert "A2" in leaf_ids
        assert "B1" in leaf_ids
        assert "ROOT" not in leaf_ids

    def test_get_max_depth(self, sample_tree):
        """Test getting maximum depth."""
        from src.hierarchy.tree import TreeNavigator

        nav = TreeNavigator(sample_tree)
        assert nav.get_max_depth() == 2

    def test_search(self, sample_tree):
        """Test searching for nodes."""
        from src.hierarchy.tree import TreeNavigator

        nav = TreeNavigator(sample_tree)

        results = nav.search("A")
        result_ids = [r.hierarchy_id for r in results]
        assert "A" in result_ids
        assert "A1" in result_ids
        assert "A2" in result_ids

    def test_traverse_breadth_first(self, sample_tree):
        """Test breadth-first traversal."""
        from src.hierarchy.tree import TreeNavigator

        nav = TreeNavigator(sample_tree)

        nodes = list(nav.traverse_breadth_first())
        node_ids = [n.hierarchy_id for n in nodes]

        # ROOT should be first
        assert node_ids[0] == "ROOT"
        # A and B should come before A1, A2, B1
        assert node_ids.index("A") < node_ids.index("A1")
        assert node_ids.index("B") < node_ids.index("B1")

    def test_traverse_depth_first(self, sample_tree):
        """Test depth-first traversal."""
        from src.hierarchy.tree import TreeNavigator

        nav = TreeNavigator(sample_tree)

        nodes = list(nav.traverse_depth_first())
        node_ids = [n.hierarchy_id for n in nodes]

        # In pre-order, ROOT should be first
        assert node_ids[0] == "ROOT"

    def test_validate_tree(self, sample_tree):
        """Test tree validation."""
        from src.hierarchy.tree import TreeNavigator

        nav = TreeNavigator(sample_tree)
        errors = nav.validate_tree()

        assert len(errors) == 0

    def test_to_flat_list(self, sample_tree):
        """Test converting to flat list."""
        from src.hierarchy.tree import TreeNavigator

        nav = TreeNavigator(sample_tree)
        flat = nav.to_flat_list()

        assert len(flat) == 6
        assert all("hierarchy_id" in item for item in flat)

    def test_to_nested_dict(self, sample_tree):
        """Test converting to nested dictionary."""
        from src.hierarchy.tree import TreeNavigator

        nav = TreeNavigator(sample_tree)
        nested = nav.to_nested_dict()

        assert len(nested) == 1
        assert nested[0]["hierarchy_id"] == "ROOT"
        assert "children" in nested[0]
