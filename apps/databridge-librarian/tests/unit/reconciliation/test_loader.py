"""Unit tests for the DataLoader class."""

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.reconciliation.loader import DataLoader, LoadResult


class TestDataLoader:
    """Tests for DataLoader."""

    @pytest.fixture
    def loader(self) -> DataLoader:
        """Create a DataLoader instance."""
        return DataLoader()

    @pytest.fixture
    def sample_csv_file(self, tmp_path: Path) -> Path:
        """Create a sample CSV file."""
        csv_content = """id,name,amount,date
1,Alice,100.50,2024-01-01
2,Bob,200.75,2024-01-02
3,Charlie,300.25,2024-01-03
"""
        file_path = tmp_path / "sample.csv"
        file_path.write_text(csv_content)
        return file_path

    @pytest.fixture
    def sample_tsv_file(self, tmp_path: Path) -> Path:
        """Create a sample TSV file."""
        tsv_content = """id\tname\tamount\tdate
1\tAlice\t100.50\t2024-01-01
2\tBob\t200.75\t2024-01-02
"""
        file_path = tmp_path / "sample.tsv"
        file_path.write_text(tsv_content)
        return file_path

    @pytest.fixture
    def sample_json_records_file(self, tmp_path: Path) -> Path:
        """Create a sample JSON records file."""
        data = [
            {"id": 1, "name": "Alice", "amount": 100.50},
            {"id": 2, "name": "Bob", "amount": 200.75},
            {"id": 3, "name": "Charlie", "amount": 300.25},
        ]
        file_path = tmp_path / "sample.json"
        file_path.write_text(json.dumps(data))
        return file_path

    @pytest.fixture
    def sample_json_lines_file(self, tmp_path: Path) -> Path:
        """Create a sample JSON lines file."""
        lines = [
            '{"id": 1, "name": "Alice", "amount": 100.50}',
            '{"id": 2, "name": "Bob", "amount": 200.75}',
        ]
        file_path = tmp_path / "sample.jsonl"
        file_path.write_text("\n".join(lines))
        return file_path

    # ==================== CSV Loading Tests ====================

    def test_load_csv_success(self, loader: DataLoader, sample_csv_file: Path):
        """Test successful CSV loading."""
        result = loader.load_csv(sample_csv_file)

        assert result.success is True
        assert result.rows_loaded == 3
        assert result.columns == ["id", "name", "amount", "date"]
        assert result.source_type == "csv"
        assert result.data is not None
        assert len(result.data) == 3

    def test_load_csv_file_not_found(self, loader: DataLoader, tmp_path: Path):
        """Test CSV loading with non-existent file."""
        result = loader.load_csv(tmp_path / "nonexistent.csv")

        assert result.success is False
        assert len(result.errors) > 0
        assert "not found" in result.errors[0].lower()

    def test_load_csv_with_delimiter(self, loader: DataLoader, sample_tsv_file: Path):
        """Test CSV loading with explicit delimiter."""
        result = loader.load_csv(sample_tsv_file, delimiter="\t")

        assert result.success is True
        assert result.rows_loaded == 2
        assert "name" in result.columns

    def test_load_csv_auto_detect_delimiter(self, loader: DataLoader, sample_tsv_file: Path):
        """Test CSV loading with auto-detected delimiter."""
        result = loader.load_csv(sample_tsv_file)

        assert result.success is True
        assert result.rows_loaded == 2
        # Should have warnings about auto-detected delimiter
        assert any("delimiter" in w.lower() for w in result.warnings)

    def test_load_csv_specific_columns(self, loader: DataLoader, sample_csv_file: Path):
        """Test CSV loading with specific columns."""
        result = loader.load_csv(sample_csv_file, columns=["id", "name"])

        assert result.success is True
        assert result.columns == ["id", "name"]
        assert len(result.data.columns) == 2

    def test_load_csv_max_rows(self, tmp_path: Path):
        """Test CSV loading with max rows limit."""
        csv_content = "id,name\n" + "\n".join([f"{i},Name{i}" for i in range(100)])
        file_path = tmp_path / "large.csv"
        file_path.write_text(csv_content)

        loader = DataLoader(max_rows=10)
        result = loader.load_csv(file_path)

        assert result.success is True
        assert result.rows_loaded == 10

    def test_load_csv_skip_rows(self, loader: DataLoader, tmp_path: Path):
        """Test CSV loading with skip rows."""
        csv_content = """# Comment line
# Another comment
id,name,amount
1,Alice,100
2,Bob,200
"""
        file_path = tmp_path / "with_comments.csv"
        file_path.write_text(csv_content)

        result = loader.load_csv(file_path, skip_rows=2)

        assert result.success is True
        assert result.rows_loaded == 2
        assert result.columns == ["id", "name", "amount"]

    def test_load_csv_schema_inference(self, loader: DataLoader, sample_csv_file: Path):
        """Test that schema is inferred correctly."""
        result = loader.load_csv(sample_csv_file)

        assert result.success is True
        assert "id" in result.schema
        assert result.schema["id"] == "integer"
        assert result.schema["name"] == "string"
        assert result.schema["amount"] == "decimal"

    # ==================== JSON Loading Tests ====================

    def test_load_json_records(self, loader: DataLoader, sample_json_records_file: Path):
        """Test JSON loading with records format."""
        result = loader.load_json(sample_json_records_file, json_format="records")

        assert result.success is True
        assert result.rows_loaded == 3
        assert "id" in result.columns

    def test_load_json_lines(self, loader: DataLoader, sample_json_lines_file: Path):
        """Test JSON loading with lines format."""
        result = loader.load_json(sample_json_lines_file, json_format="lines")

        assert result.success is True
        assert result.rows_loaded == 2

    def test_load_json_auto_detect(self, loader: DataLoader, sample_json_records_file: Path):
        """Test JSON loading with auto format detection."""
        result = loader.load_json(sample_json_records_file, json_format="auto")

        assert result.success is True
        assert result.rows_loaded == 3

    def test_load_json_nested(self, loader: DataLoader, tmp_path: Path):
        """Test JSON loading with nested format."""
        data = {
            "metadata": {"version": "1.0"},
            "data": {
                "items": [
                    {"id": 1, "name": "Item1"},
                    {"id": 2, "name": "Item2"},
                ]
            }
        }
        file_path = tmp_path / "nested.json"
        file_path.write_text(json.dumps(data))

        result = loader.load_json(file_path, json_format="nested", record_path="data.items")

        assert result.success is True
        assert result.rows_loaded == 2

    def test_load_json_file_not_found(self, loader: DataLoader, tmp_path: Path):
        """Test JSON loading with non-existent file."""
        result = loader.load_json(tmp_path / "nonexistent.json")

        assert result.success is False
        assert len(result.errors) > 0

    # ==================== Load from String Tests ====================

    def test_load_from_string_csv(self, loader: DataLoader):
        """Test loading from CSV string."""
        csv_content = "id,name\n1,Alice\n2,Bob"

        result = loader.load_from_string(csv_content, format_type="csv")

        assert result.success is True
        assert result.rows_loaded == 2
        assert result.columns == ["id", "name"]

    def test_load_from_string_json(self, loader: DataLoader):
        """Test loading from JSON string."""
        json_content = '[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]'

        result = loader.load_from_string(json_content, format_type="json")

        assert result.success is True
        assert result.rows_loaded == 2

    def test_load_from_string_unsupported_format(self, loader: DataLoader):
        """Test loading from string with unsupported format."""
        result = loader.load_from_string("data", format_type="xml")

        assert result.success is False
        assert "unsupported" in result.errors[0].lower()

    # ==================== Result Methods Tests ====================

    def test_load_result_to_dict(self, loader: DataLoader, sample_csv_file: Path):
        """Test LoadResult to_dict method."""
        result = loader.load_csv(sample_csv_file)
        result_dict = result.to_dict()

        assert "success" in result_dict
        assert "rows_loaded" in result_dict
        assert "columns" in result_dict
        assert "schema" in result_dict
        assert result_dict["success"] is True


class TestDataLoaderEdgeCases:
    """Edge case tests for DataLoader."""

    def test_empty_csv_file(self, tmp_path: Path):
        """Test loading an empty CSV file."""
        file_path = tmp_path / "empty.csv"
        file_path.write_text("")

        loader = DataLoader()
        result = loader.load_csv(file_path)

        # Empty files may result in an error or empty DataFrame
        if result.success:
            assert result.rows_loaded == 0

    def test_csv_with_special_characters(self, tmp_path: Path):
        """Test CSV with special characters in values."""
        csv_content = '''id,name,description
1,Alice,"Contains, comma"
2,Bob,"Contains ""quotes"""
'''
        file_path = tmp_path / "special.csv"
        file_path.write_text(csv_content)

        loader = DataLoader()
        result = loader.load_csv(file_path)

        assert result.success is True
        assert result.rows_loaded == 2

    def test_csv_with_unicode(self, tmp_path: Path):
        """Test CSV with unicode characters."""
        csv_content = """id,name
1,Héllo
2,日本語
3,Émoji 🎉
"""
        file_path = tmp_path / "unicode.csv"
        file_path.write_text(csv_content, encoding="utf-8")

        loader = DataLoader(encoding="utf-8")
        result = loader.load_csv(file_path)

        assert result.success is True
        assert result.rows_loaded == 3

    def test_csv_with_null_values(self, tmp_path: Path):
        """Test CSV with null/missing values."""
        csv_content = """id,name,value
1,Alice,100
2,,200
3,Charlie,
"""
        file_path = tmp_path / "nulls.csv"
        file_path.write_text(csv_content)

        loader = DataLoader()
        result = loader.load_csv(file_path)

        assert result.success is True
        assert result.rows_loaded == 3
        assert result.data is not None
        assert result.data["name"].isna().sum() == 1
