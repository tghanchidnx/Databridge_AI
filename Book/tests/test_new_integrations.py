"""
Tests for new integration modules:
- dbt integration (project generation, manifest parsing)
- Great Expectations integration (book_to_dataframe)
- AI Agent enhancements (analyze_validation_results)
"""
import pytest
import os
import json
import shutil
import tempfile
from book.models import Book, Node
from book.dbt_integration.project import DbtProject, generate_dbt_project_from_book
from book.dbt_integration.manifest_parser import _parse_manifest_dict, create_book_from_dbt_manifest
from book.great_expectations_integration.generator import book_to_dataframe
from book.ai_agent import AIAgent


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def simple_book():
    """A simple 2-level Book for testing."""
    return Book(name="Test Hierarchy", root_nodes=[
        Node(name="Root A", properties={"amount": 100, "type": "revenue"}, children=[
            Node(name="Child A1", properties={"amount": 50, "type": "product"}),
            Node(name="Child A2", properties={"amount": 50, "type": "service"}),
        ]),
        Node(name="Root B", properties={"amount": 200, "type": "expense"}),
    ])


@pytest.fixture
def book_with_validation_results():
    """A Book with simulated validation results attached."""
    return Book(name="Validated Book", root_nodes=[
        Node(name="Good Record", properties={
            "id": 1,
            "name": "Laptop",
            "validation_results": {
                "success": True,
                "results": [],
            }
        }),
        Node(name="Bad Record", properties={
            "id": 2,
            "name": "Mouse",
            "validation_results": {
                "success": False,
                "results": [
                    {
                        "success": False,
                        "expectation_config": {
                            "expectation_type": "expect_column_values_to_be_positive",
                            "kwargs": {"column": "price"},
                        },
                    },
                    {
                        "success": True,
                        "expectation_config": {
                            "expectation_type": "expect_column_to_exist",
                            "kwargs": {"column": "id"},
                        },
                    },
                ],
            }
        }),
        Node(name="Null Record", properties={
            "id": 3,
            "validation_results": {
                "success": False,
                "results": [
                    {
                        "success": False,
                        "expectation_config": {
                            "expectation_type": "expect_column_values_to_not_be_null",
                            "kwargs": {"column": "product_name"},
                        },
                    },
                ],
            }
        }),
    ])


@pytest.fixture
def tmp_dir():
    """Create a temporary directory and clean up after test."""
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ============================================================
# Tests: book_to_dataframe (GE generator helper)
# ============================================================

class TestBookToDataframe:
    def test_basic_conversion(self, simple_book):
        df = book_to_dataframe(simple_book)
        assert len(df) == 4  # Root A, Child A1, Child A2, Root B
        assert "amount" in df.columns
        assert "type" in df.columns

    def test_preserves_property_values(self, simple_book):
        df = book_to_dataframe(simple_book)
        amounts = sorted(df["amount"].tolist())
        assert amounts == [50, 50, 100, 200]

    def test_empty_book(self):
        empty_book = Book(name="Empty")
        df = book_to_dataframe(empty_book)
        assert len(df) == 0

    def test_nested_hierarchy(self):
        deep_book = Book(name="Deep", root_nodes=[
            Node(name="L1", properties={"level": 1}, children=[
                Node(name="L2", properties={"level": 2}, children=[
                    Node(name="L3", properties={"level": 3}),
                ]),
            ]),
        ])
        df = book_to_dataframe(deep_book)
        assert len(df) == 3
        assert sorted(df["level"].tolist()) == [1, 2, 3]

    def test_mixed_properties(self):
        """Nodes with different property keys should produce columns with NaN for missing."""
        book = Book(name="Mixed", root_nodes=[
            Node(name="A", properties={"x": 1, "y": 2}),
            Node(name="B", properties={"y": 3, "z": 4}),
        ])
        df = book_to_dataframe(book)
        assert len(df) == 2
        assert set(df.columns) == {"x", "y", "z"}

    def test_node_with_no_properties(self):
        book = Book(name="NoProps", root_nodes=[
            Node(name="Empty Node"),
        ])
        df = book_to_dataframe(book)
        assert len(df) == 1  # Still includes the node even with empty properties


# ============================================================
# Tests: dbt Project Generation
# ============================================================

class TestDbtProjectGeneration:
    def test_create_project_structure(self, tmp_dir):
        project = DbtProject("test_project", tmp_dir)
        project.create_project_structure()

        # Check directory was created
        assert os.path.exists(project.project_dir)
        assert os.path.exists(project.models_dir)

        # Check dbt_project.yml was created
        yml_path = os.path.join(project.project_dir, "dbt_project.yml")
        assert os.path.exists(yml_path)

        with open(yml_path, "r") as f:
            content = f.read()
        assert "test_project" in content
        assert "model-paths" in content
        assert "config-version: 2" in content

    def test_generate_dbt_project_from_book(self, simple_book, tmp_dir):
        generate_dbt_project_from_book(simple_book, tmp_dir)

        project_dir = os.path.join(tmp_dir, simple_book.name)
        models_dir = os.path.join(project_dir, "models")
        assert os.path.exists(project_dir)
        assert os.path.exists(models_dir)

        # Check model SQL file was created
        model_file = os.path.join(models_dir, "test_hierarchy.sql")
        assert os.path.exists(model_file)

        with open(model_file, "r") as f:
            sql = f.read()
        assert "Root A" in sql
        assert "Child A1" in sql
        assert "Child A2" in sql
        assert "Root B" in sql
        assert "UNION ALL" in sql
        assert "hierarchy_paths" in sql

    def test_generate_dbt_sql_has_parent_ids(self, tmp_dir):
        book = Book(name="ParentTest", root_nodes=[
            Node(name="Parent", id="p1", children=[
                Node(name="Child", id="c1"),
            ]),
        ])
        generate_dbt_project_from_book(book, tmp_dir)

        model_file = os.path.join(tmp_dir, "ParentTest", "models", "parenttest.sql")
        with open(model_file, "r") as f:
            sql = f.read()
        # Parent node should have NULL parent_id
        assert "NULL" in sql
        # Child should reference parent
        assert "p1" in sql


# ============================================================
# Tests: dbt Manifest Parser
# ============================================================

class TestDbtManifestParser:
    def test_parse_simple_manifest(self):
        manifest = {
            "metadata": {"project_name": "my_project"},
            "nodes": {
                "source.my_project.src.table_a": {
                    "name": "table_a",
                    "resource_type": "source",
                    "package_name": "my_project",
                    "path": "models/src.yml",
                    "original_file_path": "models/src.yml",
                    "unique_id": "source.my_project.src.table_a",
                    "depends_on": {"nodes": []},
                },
                "model.my_project.dim_product": {
                    "name": "dim_product",
                    "resource_type": "model",
                    "package_name": "my_project",
                    "path": "models/dim_product.sql",
                    "original_file_path": "models/dim_product.sql",
                    "unique_id": "model.my_project.dim_product",
                    "depends_on": {"nodes": ["source.my_project.src.table_a"]},
                },
            },
        }
        parsed = _parse_manifest_dict(manifest)
        assert parsed["project_name"] == "my_project"
        assert len(parsed["nodes"]) == 2
        assert parsed["nodes"]["source.my_project.src.table_a"]["name"] == "table_a"
        assert parsed["nodes"]["model.my_project.dim_product"]["resource_type"] == "model"

    def test_parse_manifest_with_no_depends(self):
        manifest = {
            "metadata": {"project_name": "test"},
            "nodes": {
                "model.test.standalone": {
                    "name": "standalone",
                    "resource_type": "model",
                    "package_name": "test",
                    "path": "models/standalone.sql",
                    "original_file_path": "models/standalone.sql",
                    "unique_id": "model.test.standalone",
                },
            },
        }
        parsed = _parse_manifest_dict(manifest)
        assert len(parsed["nodes"]) == 1
        assert parsed["nodes"]["model.test.standalone"]["depends_on"]["nodes"] == []

    def test_parse_empty_manifest(self):
        manifest = {"metadata": {"project_name": "empty"}, "nodes": {}}
        parsed = _parse_manifest_dict(manifest)
        assert parsed["project_name"] == "empty"
        assert len(parsed["nodes"]) == 0

    def test_parse_missing_metadata(self):
        manifest = {"nodes": {}}
        parsed = _parse_manifest_dict(manifest)
        assert parsed["project_name"] == "unknown"

    def test_create_book_from_manifest_file(self, tmp_dir):
        manifest = {
            "metadata": {"project_name": "finance_project"},
            "nodes": {
                "source.finance.raw.gl": {
                    "name": "gl",
                    "resource_type": "source",
                    "package_name": "finance_project",
                    "path": "models/raw.yml",
                    "original_file_path": "models/raw.yml",
                    "unique_id": "source.finance.raw.gl",
                    "depends_on": {"nodes": []},
                },
                "model.finance.dim_account": {
                    "name": "dim_account",
                    "resource_type": "model",
                    "package_name": "finance_project",
                    "path": "models/dim_account.sql",
                    "original_file_path": "models/dim_account.sql",
                    "unique_id": "model.finance.dim_account",
                    "depends_on": {"nodes": ["source.finance.raw.gl"]},
                },
                "model.finance.fact_journal": {
                    "name": "fact_journal",
                    "resource_type": "model",
                    "package_name": "finance_project",
                    "path": "models/fact_journal.sql",
                    "original_file_path": "models/fact_journal.sql",
                    "unique_id": "model.finance.fact_journal",
                    "depends_on": {"nodes": ["source.finance.raw.gl", "model.finance.dim_account"]},
                },
            },
        }

        manifest_path = os.path.join(tmp_dir, "manifest.json")
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)

        book = create_book_from_dbt_manifest(manifest_path)
        assert book.name == "dbt Project: finance_project"

        # gl is root (no dependencies), dim_account depends on gl, fact_journal depends on both
        # Root nodes should be just 'gl'
        root_names = [n.name for n in book.root_nodes]
        assert "gl" in root_names
        assert len(book.root_nodes) == 1

        # gl should have dim_account and fact_journal as children
        gl_node = book.root_nodes[0]
        child_names = [c.name for c in gl_node.children]
        assert "dim_account" in child_names
        assert "fact_journal" in child_names

        # dim_account should also have fact_journal as a child
        dim_account = next(c for c in gl_node.children if c.name == "dim_account")
        dim_child_names = [c.name for c in dim_account.children]
        assert "fact_journal" in dim_child_names

    def test_properties_preserved_in_book(self, tmp_dir):
        manifest = {
            "metadata": {"project_name": "props_test"},
            "nodes": {
                "model.props_test.my_model": {
                    "name": "my_model",
                    "resource_type": "model",
                    "package_name": "props_test",
                    "path": "models/my_model.sql",
                    "original_file_path": "models/my_model.sql",
                    "unique_id": "model.props_test.my_model",
                    "depends_on": {"nodes": []},
                },
            },
        }

        manifest_path = os.path.join(tmp_dir, "manifest.json")
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)

        book = create_book_from_dbt_manifest(manifest_path)
        node = book.root_nodes[0]
        assert node.properties["resource_type"] == "model"
        assert node.properties["package_name"] == "props_test"
        assert node.properties["path"] == "models/my_model.sql"
        assert node.properties["unique_id"] == "model.props_test.my_model"


# ============================================================
# Tests: AI Agent - analyze_validation_results
# ============================================================

class TestAIAgentValidation:
    @pytest.fixture
    def agent(self, tmp_dir):
        """Create an AIAgent with a minimal project structure."""
        skills_dir = os.path.join(tmp_dir, "skills")
        os.makedirs(skills_dir, exist_ok=True)

        # Create a minimal skill
        skill = {
            "name": "test-skill",
            "description": "A test skill",
            "rules": [{"suggestion": "Check your data quality"}],
        }
        with open(os.path.join(skills_dir, "test-skill.json"), "w") as f:
            json.dump(skill, f)

        return AIAgent(databridge_project_path=tmp_dir)

    def test_analyze_finds_failures(self, agent, book_with_validation_results):
        """Test that analyze_validation_results finds failed expectations."""
        suggestions = agent.analyze_validation_results(book_with_validation_results)

        assert len(suggestions) == 2
        assert any("price" in s for s in suggestions)
        assert any("product_name" in s for s in suggestions)
        assert any("expect_column_values_to_be_positive" in s for s in suggestions)
        assert any("expect_column_values_to_not_be_null" in s for s in suggestions)

    def test_analyze_no_failures(self, agent):
        """Test with a book where all validations pass."""
        good_book = Book(name="Good Book", root_nodes=[
            Node(name="Record", properties={
                "validation_results": {"success": True, "results": []},
            }),
        ])
        suggestions = agent.analyze_validation_results(good_book)
        assert len(suggestions) == 0

    def test_analyze_no_validation_results(self, agent):
        """Test with a book that has no validation results."""
        plain_book = Book(name="Plain Book", root_nodes=[
            Node(name="Record", properties={"value": 42}),
        ])
        suggestions = agent.analyze_validation_results(plain_book)
        assert len(suggestions) == 0

    def test_agent_loads_skills(self, agent):
        """Test that the agent loads skills from the project directory."""
        assert "test-skill" in agent.skills
        assert agent.skills["test-skill"]["description"] == "A test skill"

    def test_agent_find_best_skill(self, agent):
        """Test skill finding (returns None if no embeddings DB)."""
        # With no embeddings DB, should return None
        if agent.skill_embeddings_db is None:
            result = agent.find_best_skill("test query")
            assert result is None

    def test_analyze_nested_validation_results(self, agent):
        """Test that analyze_validation_results traverses nested children."""
        nested_book = Book(name="Nested", root_nodes=[
            Node(name="Parent", properties={}, children=[
                Node(name="Child", properties={
                    "validation_results": {
                        "success": False,
                        "results": [
                            {
                                "success": False,
                                "expectation_config": {
                                    "expectation_type": "expect_column_values_to_be_unique",
                                    "kwargs": {"column": "id"},
                                },
                            },
                        ],
                    }
                }),
            ]),
        ])
        suggestions = agent.analyze_validation_results(nested_book)
        assert len(suggestions) == 1
        assert "id" in suggestions[0]
        assert "expect_column_values_to_be_unique" in suggestions[0]

    def test_suggest_enhancements_without_skill_match(self, agent):
        """Test suggest_enhancements when no skill matches."""
        book = Book(name="Test", root_nodes=[
            Node(name="A", properties={"value": 1}),
        ])
        suggestions = agent.suggest_enhancements(book, "random query")
        assert any("No relevant skills" in s for s in suggestions)


# ============================================================
# Tests: Edge Cases and Integration
# ============================================================

class TestEdgeCases:
    def test_dbt_project_with_special_characters_in_name(self, tmp_dir):
        book = Book(name="My Project (v2)", root_nodes=[
            Node(name="Node 1", id="n1"),
        ])
        generate_dbt_project_from_book(book, tmp_dir)
        project_dir = os.path.join(tmp_dir, "My Project (v2)")
        assert os.path.exists(project_dir)

    def test_dbt_manifest_single_node(self, tmp_dir):
        manifest = {
            "metadata": {"project_name": "single"},
            "nodes": {
                "model.single.only_one": {
                    "name": "only_one",
                    "resource_type": "model",
                    "package_name": "single",
                    "path": "models/only.sql",
                    "original_file_path": "models/only.sql",
                    "unique_id": "model.single.only_one",
                    "depends_on": {"nodes": []},
                },
            },
        }
        path = os.path.join(tmp_dir, "manifest.json")
        with open(path, "w") as f:
            json.dump(manifest, f)

        book = create_book_from_dbt_manifest(path)
        assert len(book.root_nodes) == 1
        assert book.root_nodes[0].name == "only_one"
        assert book.root_nodes[0].children == []

    def test_dataframe_with_none_values(self):
        book = Book(name="NullTest", root_nodes=[
            Node(name="A", properties={"x": 1, "y": None}),
            Node(name="B", properties={"x": None, "y": 2}),
        ])
        df = book_to_dataframe(book)
        assert len(df) == 2
        assert df.iloc[0]["x"] == 1
        assert df.iloc[1]["y"] == 2

    def test_book_to_dbt_round_trip(self, tmp_dir):
        """Test creating a dbt project from a Book, then reading manifest back."""
        original_book = Book(name="RoundTrip", root_nodes=[
            Node(name="Source", id="src", children=[
                Node(name="Model", id="mdl"),
            ]),
        ])

        # Generate dbt project
        generate_dbt_project_from_book(original_book, tmp_dir)

        # Create a manifest that represents the same structure
        manifest = {
            "metadata": {"project_name": "RoundTrip"},
            "nodes": {
                "source.RoundTrip.src": {
                    "name": "Source",
                    "resource_type": "source",
                    "package_name": "RoundTrip",
                    "path": "models/source.yml",
                    "original_file_path": "models/source.yml",
                    "unique_id": "source.RoundTrip.src",
                    "depends_on": {"nodes": []},
                },
                "model.RoundTrip.mdl": {
                    "name": "Model",
                    "resource_type": "model",
                    "package_name": "RoundTrip",
                    "path": "models/model.sql",
                    "original_file_path": "models/model.sql",
                    "unique_id": "model.RoundTrip.mdl",
                    "depends_on": {"nodes": ["source.RoundTrip.src"]},
                },
            },
        }
        manifest_path = os.path.join(tmp_dir, "manifest.json")
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)

        # Parse manifest back to Book
        restored_book = create_book_from_dbt_manifest(manifest_path)
        assert restored_book.name == "dbt Project: RoundTrip"
        assert len(restored_book.root_nodes) == 1
        assert restored_book.root_nodes[0].name == "Source"
        assert len(restored_book.root_nodes[0].children) == 1
        assert restored_book.root_nodes[0].children[0].name == "Model"

    def test_multiple_roots_manifest(self, tmp_dir):
        """Test manifest with multiple independent models (no dependencies)."""
        manifest = {
            "metadata": {"project_name": "multi_root"},
            "nodes": {
                "model.multi_root.a": {
                    "name": "model_a",
                    "resource_type": "model",
                    "package_name": "multi_root",
                    "path": "a.sql",
                    "original_file_path": "a.sql",
                    "unique_id": "model.multi_root.a",
                    "depends_on": {"nodes": []},
                },
                "model.multi_root.b": {
                    "name": "model_b",
                    "resource_type": "model",
                    "package_name": "multi_root",
                    "path": "b.sql",
                    "original_file_path": "b.sql",
                    "unique_id": "model.multi_root.b",
                    "depends_on": {"nodes": []},
                },
                "model.multi_root.c": {
                    "name": "model_c",
                    "resource_type": "model",
                    "package_name": "multi_root",
                    "path": "c.sql",
                    "original_file_path": "c.sql",
                    "unique_id": "model.multi_root.c",
                    "depends_on": {"nodes": []},
                },
            },
        }
        path = os.path.join(tmp_dir, "manifest.json")
        with open(path, "w") as f:
            json.dump(manifest, f)

        book = create_book_from_dbt_manifest(path)
        assert len(book.root_nodes) == 3
        root_names = sorted([n.name for n in book.root_nodes])
        assert root_names == ["model_a", "model_b", "model_c"]
