import pytest
import os
import networkx as nx
import chromadb
from book.models import Book, Node
from book.hierarchy import from_list, sort_nodes
from book.properties import (
    add_property,
    update_property,
    remove_property,
    propagate_to_children,
    propagate_to_parents,
    get_property,
)
from book.actions import (
    add_python_function,
    run_python_function,
    add_llm_prompt,
    run_llm_prompt,
)
from book.graph_db import book_to_graph, graph_to_book, save_graph, load_graph
from book.vector_db import (
    create_collection,
    add_nodes_to_collection,
    query_collection,
    get_node_text_representation,
)
from book.formulas import Formula
from book.formula_engine import execute_formulas

# Sample Data for hierarchy tests
sample_csv_data = [
    {"id": "1", "name": "A", "parent_id": None, "value": "10"},
    {"id": "2", "name": "B", "parent_id": "1", "value": "20"},
    {"id": "3", "name": "C", "parent_id": "1", "value": "30"},
    {"id": "4", "name": "D", "parent_id": "2", "value": "40"},
    {"id": "5", "name": "E", "parent_id": None, "value": "50"},
]

@pytest.fixture
def sample_book():
    """Fixture for a sample Book object."""
    # Create nodes
    node_d = Node(name="D", properties={"value": 40})
    node_b = Node(name="B", children=[node_d], properties={"value": 20})
    node_c = Node(name="C", properties={"value": 30})
    node_a = Node(name="A", children=[node_b, node_c], properties={"value": 10})
    node_e = Node(name="E", properties={"value": 50})
    return Book(name="Test Book", root_nodes=[node_a, node_e])

class TestModels:
    def test_node_creation(self):
        node = Node(name="Test Node")
        assert node.name == "Test Node"
        assert isinstance(node.id, str)
        assert not node.children
        assert not node.properties
        assert node.python_function is None
        assert node.llm_prompt is None
        assert not node.flags

    def test_book_creation(self):
        node = Node(name="Child Node")
        book = Book(name="My Book", root_nodes=[node], metadata={"author": "Me"})
        assert book.name == "My Book"
        assert book.root_nodes[0].name == "Child Node"
        assert book.metadata["author"] == "Me"
        assert not book.global_properties

class TestHierarchy:
    def test_from_list(self):
        root_nodes = from_list(sample_csv_data, "parent_id", "id", name_col="name")
        assert len(root_nodes) == 2
        assert root_nodes[0].name == "A"
        assert len(root_nodes[0].children) == 2
        assert root_nodes[0].children[0].name == "B"
        assert root_nodes[0].children[0].children[0].name == "D"
        assert root_nodes[1].name == "E"

    def test_sort_nodes(self):
        root_nodes = from_list(sample_csv_data, "parent_id", "id", name_col="name")
        
        # Find node 'A' to test its children sorting
        node_a = next((n for n in root_nodes if n.name == "A"), None)
        assert node_a is not None

        sort_nodes(node_a.children, sort_by="value")
        assert node_a.children[0].name == "B"
        assert node_a.children[1].name == "C"

        sort_nodes(node_a.children, sort_by="value", reverse=True)
        assert node_a.children[0].name == "C"
        assert node_a.children[1].name == "B"

class TestProperties:
    def test_add_update_remove_property(self, sample_book):
        node_a = sample_book.root_nodes[0]
        add_property(node_a, "new_key", "new_value")
        assert node_a.properties["new_key"] == "new_value"

        update_property(node_a, "new_key", "updated_value")
        assert node_a.properties["new_key"] == "updated_value"

        with pytest.raises(KeyError):
            update_property(node_a, "non_existent_key", "value")

        remove_property(node_a, "new_key")
        assert "new_key" not in node_a.properties
        with pytest.raises(KeyError):
            remove_property(node_a, "non_existent_key")

    def test_propagate_to_children(self, sample_book):
        node_a = sample_book.root_nodes[0]
        propagate_to_children(node_a, "shared_prop", True)
        assert node_a.properties["shared_prop"] is True
        assert node_a.children[0].properties["shared_prop"] is True # B
        assert node_a.children[1].properties["shared_prop"] is True # C
        assert node_a.children[0].children[0].properties["shared_prop"] is True # D

    def test_propagate_to_parents(self, sample_book):
        node_d = sample_book.root_nodes[0].children[0].children[0]
        propagate_to_parents(node_d, "ancestor_prop", "val", sample_book.root_nodes)
        assert node_d.properties["ancestor_prop"] == "val" # D itself
        assert sample_book.root_nodes[0].children[0].properties["ancestor_prop"] == "val" # B
        assert sample_book.root_nodes[0].properties["ancestor_prop"] == "val" # A
        assert "ancestor_prop" not in sample_book.root_nodes[1].properties # E should not have it

    def test_get_property_global_override(self):
        node_child = Node(name="Child", properties={"prop1": "node_value"})
        node_root = Node(name="Root", children=[node_child])
        book_with_global = Book(
            name="Global Book", 
            root_nodes=[node_root], 
            global_properties={"prop1": "global_value", "prop2": "global_only"}
        )

        assert get_property(node_child, book_with_global, "prop1") == "node_value"
        assert get_property(node_child, book_with_global, "prop2") == "global_only"
        
        with pytest.raises(KeyError):
            get_property(node_child, book_with_global, "non_existent")

class TestActions:
    def test_python_function(self, sample_book):
        node = sample_book.root_nodes[0] # Node A
        func_code = "result = node.properties.get('value', 1) * 2"
        add_python_function(node, func_code)
        result = run_python_function(node)
        assert result == 20

    def test_llm_prompt(self, sample_book):
        class MockLLMClient:
            def generate(self, prompt: str):
                return f"LLM responded to: {prompt}"

        mock_client = MockLLMClient()
        node = sample_book.root_nodes[0] # Node A
        prompt_text = "Analyze this node data: " + node.name
        add_llm_prompt(node, prompt_text)
        response = run_llm_prompt(node, mock_client)
        assert response == f"LLM responded to: {prompt_text}"

class TestGraphDB:
    def test_book_to_graph_and_back(self, sample_book, tmp_path):
        graph = book_to_graph(sample_book)
        assert isinstance(graph, nx.DiGraph)
        assert graph.number_of_nodes() == 5
        assert graph.number_of_edges() == 3

        # Save and load
        file_path = tmp_path / "test_book.gml"
        save_graph(graph, file_path)
        loaded_graph = load_graph(file_path)
        assert nx.is_isomorphic(graph, loaded_graph)

        # Convert back to book and check basic structure
        reconstructed_book = graph_to_book(loaded_graph, sample_book.name)
        assert reconstructed_book.name == sample_book.name
        assert len(reconstructed_book.root_nodes) == len(sample_book.root_nodes)
        
        # Deeper check would involve comparing node attributes, etc.
        # For simplicity, just check names and hierarchy levels
        assert reconstructed_book.root_nodes[0].name == "A"
        assert reconstructed_book.root_nodes[0].children[0].name == "B"
        assert reconstructed_book.root_nodes[0].children[0].children[0].name == "D"

class TestVectorDB:
    def test_node_text_representation(self, sample_book):
        node = sample_book.root_nodes[0] # Node A
        text = get_node_text_representation(node, sample_book)
        assert "Node: A" in text
        assert "value: 10" in text

    def test_add_and_query_collection(self, sample_book):
        # Use a temporary client for testing
        client = chromadb.Client()
        collection_name = "test_collection_123"
        
        # Ensure a clean slate for the test
        try:
            client.delete_collection(name=collection_name)
        except:
            pass

        collection = create_collection(collection_name)
        add_nodes_to_collection(collection_name, sample_book)

        results = query_collection(collection_name, "Node A with value 10", n_results=1)
        assert len(results["documents"][0]) == 1
        assert "Node: A" in results["documents"][0][0]
        
        # Clean up
        client.delete_collection(name=collection_name)

class TestFormulas:
    def test_execute_formulas(self, sample_book):
        node_a = sample_book.root_nodes[0]
        add_property(node_a, "revenue", 100)
        add_property(node_a, "cogs", 60)

        formula = Formula(
            name="gross_margin",
            expression="(revenue - cogs) / revenue",
            operands=["revenue", "cogs"],
        )
        node_a.formulas.append(formula)

        execute_formulas(node_a, sample_book)
        assert node_a.properties["gross_margin"] == 0.4
