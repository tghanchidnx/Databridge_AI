"""
Unit tests for CSVHandler.
"""

import pytest
import tempfile
from pathlib import Path


class TestCSVFormatDetection:
    """Tests for CSV format detection."""

    def test_detect_hierarchy_format(self):
        """Test detecting hierarchy CSV format."""
        from src.hierarchy.csv_handler import CSVHandler

        handler = CSVHandler()

        csv_content = """HIERARCHY_ID,HIERARCHY_NAME,PARENT_ID,LEVEL_1
REV-001,Revenue,,Revenue"""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix="_HIERARCHY.csv", delete=False
        ) as f:
            f.write(csv_content)
            f.flush()

            format_type, is_legacy = handler.detect_format(f.name)

            assert format_type == "hierarchy"
            assert is_legacy is False

        Path(f.name).unlink()

    def test_detect_mapping_format(self):
        """Test detecting mapping CSV format."""
        from src.hierarchy.csv_handler import CSVHandler

        handler = CSVHandler()

        csv_content = """HIERARCHY_ID,SOURCE_DATABASE,SOURCE_SCHEMA,SOURCE_TABLE,SOURCE_COLUMN
REV-001,DB,SCHEMA,TABLE,COL"""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix="_MAPPING.csv", delete=False
        ) as f:
            f.write(csv_content)
            f.flush()

            format_type, is_legacy = handler.detect_format(f.name)

            assert format_type == "mapping"
            assert is_legacy is False

        Path(f.name).unlink()

    def test_detect_legacy_format(self):
        """Test detecting legacy CSV format."""
        from src.hierarchy.csv_handler import CSVHandler

        handler = CSVHandler()

        csv_content = """HIER_ID,NODE_NAME,PARENT_NODE_ID,LVL_1
REV-001,Revenue,,Revenue"""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write(csv_content)
            f.flush()

            format_type, is_legacy = handler.detect_format(f.name)

            assert is_legacy is True

        Path(f.name).unlink()


class TestHierarchyImport:
    """Tests for hierarchy CSV import."""

    def test_import_hierarchy_csv(self):
        """Test importing a hierarchy CSV."""
        from src.hierarchy.csv_handler import CSVHandler

        handler = CSVHandler()

        csv_content = """HIERARCHY_ID,HIERARCHY_NAME,PARENT_ID,LEVEL_1,LEVEL_2,INCLUDE_FLAG
REV-001,Total Revenue,,Revenue,,TRUE
REV-002,Product Sales,REV-001,Revenue,Product,TRUE
REV-003,Hardware,REV-002,Revenue,Product,TRUE"""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write(csv_content)
            f.flush()

            result = handler.import_hierarchy_csv(f.name)

            assert result.success is True
            assert result.rows_processed == 3
            assert result.rows_imported == 3
            assert len(result.data) == 3

            assert result.data[0]["hierarchy_id"] == "REV-001"
            assert result.data[0]["include_flag"] is True

        Path(f.name).unlink()

    def test_import_hierarchy_with_validation_errors(self):
        """Test importing with validation errors."""
        from src.hierarchy.csv_handler import CSVHandler

        handler = CSVHandler()

        csv_content = """HIERARCHY_ID,HIERARCHY_NAME,LEVEL_1_SORT
REV-001,Revenue,invalid_number"""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write(csv_content)
            f.flush()

            result = handler.import_hierarchy_csv(f.name, skip_errors=True)

            assert result.rows_imported == 1
            assert len(result.errors) > 0

        Path(f.name).unlink()

    def test_import_hierarchy_missing_required(self):
        """Test importing with missing required fields."""
        from src.hierarchy.csv_handler import CSVHandler

        handler = CSVHandler()

        csv_content = """HIERARCHY_ID,LEVEL_1
REV-001,Revenue"""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write(csv_content)
            f.flush()

            result = handler.import_hierarchy_csv(f.name, skip_errors=False)

            assert result.success is False
            assert any(e.column == "HIERARCHY_NAME" for e in result.errors)

        Path(f.name).unlink()

    def test_import_legacy_hierarchy(self):
        """Test importing legacy format hierarchy."""
        from src.hierarchy.csv_handler import CSVHandler

        handler = CSVHandler()

        csv_content = """HIER_ID,NODE_NAME,PARENT_NODE_ID,LVL_1
REV-001,Revenue,,Revenue"""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write(csv_content)
            f.flush()

            result = handler.import_hierarchy_csv(f.name)

            assert result.success is True
            assert len(result.warnings) > 0  # Should warn about legacy format
            assert result.data[0]["hierarchy_id"] == "REV-001"

        Path(f.name).unlink()


class TestMappingImport:
    """Tests for mapping CSV import."""

    def test_import_mapping_csv(self):
        """Test importing a mapping CSV."""
        from src.hierarchy.csv_handler import CSVHandler

        handler = CSVHandler()

        csv_content = """HIERARCHY_ID,MAPPING_INDEX,SOURCE_DATABASE,SOURCE_SCHEMA,SOURCE_TABLE,SOURCE_COLUMN,INCLUDE_FLAG
REV-001,0,ANALYTICS,PUBLIC,FACT_SALES,AMOUNT,TRUE
REV-001,1,ANALYTICS,PUBLIC,FACT_RETURNS,AMOUNT,FALSE"""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write(csv_content)
            f.flush()

            result = handler.import_mapping_csv(f.name)

            assert result.success is True
            assert result.rows_imported == 2
            assert result.data[0]["source_database"] == "ANALYTICS"
            assert result.data[0]["include_flag"] is True
            assert result.data[1]["include_flag"] is False

        Path(f.name).unlink()


class TestHierarchyExport:
    """Tests for hierarchy CSV export."""

    def test_export_hierarchy_csv(self):
        """Test exporting hierarchy to CSV."""
        from src.hierarchy.csv_handler import CSVHandler

        handler = CSVHandler()

        data = [
            {
                "hierarchy_id": "REV-001",
                "hierarchy_name": "Revenue",
                "parent_id": None,
                "level_1": "Revenue",
                "include_flag": True,
            },
            {
                "hierarchy_id": "REV-002",
                "hierarchy_name": "Product",
                "parent_id": "REV-001",
                "level_1": "Revenue",
                "level_2": "Product",
                "include_flag": True,
            },
        ]

        csv_content = handler.export_hierarchy_csv(data)

        assert "HIERARCHY_ID" in csv_content
        assert "REV-001" in csv_content
        assert "REV-002" in csv_content
        assert "TRUE" in csv_content

    def test_export_hierarchy_to_file(self):
        """Test exporting hierarchy to a file."""
        from src.hierarchy.csv_handler import CSVHandler

        handler = CSVHandler()

        data = [
            {
                "hierarchy_id": "TEST-001",
                "hierarchy_name": "Test",
                "parent_id": None,
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test_export.csv"
            handler.export_hierarchy_csv(data, file_path)

            assert file_path.exists()
            content = file_path.read_text()
            assert "TEST-001" in content


class TestMappingExport:
    """Tests for mapping CSV export."""

    def test_export_mapping_csv(self):
        """Test exporting mapping to CSV."""
        from src.hierarchy.csv_handler import CSVHandler

        handler = CSVHandler()

        data = [
            {
                "hierarchy_id": "REV-001",
                "mapping_index": 0,
                "source_database": "DB",
                "source_schema": "SCHEMA",
                "source_table": "TABLE",
                "source_column": "COL",
                "include_flag": True,
            },
        ]

        csv_content = handler.export_mapping_csv(data)

        assert "HIERARCHY_ID" in csv_content
        assert "SOURCE_DATABASE" in csv_content
        assert "REV-001" in csv_content


class TestImportFromString:
    """Tests for importing from string."""

    def test_import_hierarchy_from_string(self):
        """Test importing hierarchy from string."""
        from src.hierarchy.csv_handler import CSVHandler

        handler = CSVHandler()

        csv_content = """HIERARCHY_ID,HIERARCHY_NAME,PARENT_ID
REV-001,Revenue,"""

        result = handler.import_from_string(csv_content, "hierarchy")

        assert result.success is True
        assert len(result.data) == 1

    def test_import_mapping_from_string(self):
        """Test importing mapping from string."""
        from src.hierarchy.csv_handler import CSVHandler

        handler = CSVHandler()

        csv_content = """HIERARCHY_ID,SOURCE_DATABASE,SOURCE_SCHEMA,SOURCE_TABLE,SOURCE_COLUMN
REV-001,DB,SCHEMA,TABLE,COL"""

        result = handler.import_from_string(csv_content, "mapping")

        assert result.success is True
        assert len(result.data) == 1
