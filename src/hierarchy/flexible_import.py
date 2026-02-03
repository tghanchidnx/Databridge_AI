"""Flexible Hierarchy Import System.

Supports four tiers of input complexity:
- Tier 1: Ultra-simple 2-3 columns (source_value, group_name)
- Tier 2: Basic 5-7 columns (parent via names, sort order)
- Tier 3: Standard 10-12 columns (explicit IDs, full source info)
- Tier 4: Enterprise 28+ columns (LEVEL_1-10, all flags, formulas)

Auto-infers missing fields based on tier and project defaults.
"""

import json
import re
import io
import logging
from enum import Enum
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

from .types import (
    SmartHierarchy,
    HierarchyLevel,
    HierarchyFlags,
    SourceMapping,
    SourceMappingFlags,
)

logger = logging.getLogger("flexible_import")


class FormatTier(str, Enum):
    """Hierarchy format complexity tiers."""
    TIER_1 = "tier_1"  # Ultra-simple: 2-3 columns
    TIER_2 = "tier_2"  # Basic: 5-7 columns with parent names
    TIER_3 = "tier_3"  # Standard: 10-12 columns with IDs
    TIER_4 = "tier_4"  # Enterprise: 28+ columns with levels


class InputFormat(str, Enum):
    """Supported input formats."""
    CSV = "csv"
    EXCEL = "excel"
    JSON = "json"
    TEXT = "text"
    AUTO = "auto"


class ProjectDefaults:
    """Stores default source information for a project."""

    def __init__(
        self,
        source_database: str = "",
        source_schema: str = "",
        source_table: str = "",
        source_column: str = "",
    ):
        self.source_database = source_database
        self.source_schema = source_schema
        self.source_table = source_table
        self.source_column = source_column

    def to_dict(self) -> Dict[str, str]:
        return {
            "source_database": self.source_database,
            "source_schema": self.source_schema,
            "source_table": self.source_table,
            "source_column": self.source_column,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> "ProjectDefaults":
        return cls(
            source_database=data.get("source_database", ""),
            source_schema=data.get("source_schema", ""),
            source_table=data.get("source_table", ""),
            source_column=data.get("source_column", ""),
        )

    def is_complete(self) -> bool:
        """Check if all source defaults are set."""
        return all([
            self.source_database,
            self.source_schema,
            self.source_table,
            self.source_column,
        ])


class FormatDetector:
    """Detects input format and tier from content."""

    # Column name mappings for different tiers
    TIER_1_COLUMNS = {
        "source_value", "group_name", "value", "group", "category",
        "account", "account_code", "gl_code", "code"
    }

    TIER_2_COLUMNS = {
        "hierarchy_name", "parent_name", "parent", "sort_order",
        "name", "description", "order", "template_id"
    }

    TIER_3_COLUMNS = {
        "hierarchy_id", "parent_id", "source_database", "source_schema",
        "source_table", "source_column", "source_uid", "template_id"
    }

    TIER_4_COLUMNS = {
        "level_1", "level_2", "level_3", "level_4", "level_5",
        "level_1_sort", "level_2_sort", "include_flag", "exclude_flag",
        "transform_flag", "calculation_flag", "formula_group"
    }

    # Property column prefixes
    PROPERTY_PREFIXES = {
        "prop_": "custom",      # Generic property: PROP_*
        "dim_": "dimension",    # Dimension property: DIM_*
        "fact_": "fact",        # Fact property: FACT_*
        "filter_": "filter",    # Filter property: FILTER_*
        "display_": "display",  # Display property: DISPLAY_*
    }

    @classmethod
    def detect_property_columns(cls, columns: List[str]) -> Dict[str, str]:
        """
        Detect property columns from column names.

        Returns:
            Dict mapping column name to property category
        """
        property_cols = {}
        for col in columns:
            col_lower = col.lower().strip()
            for prefix, category in cls.PROPERTY_PREFIXES.items():
                if col_lower.startswith(prefix):
                    # Extract property name (everything after prefix)
                    prop_name = col_lower[len(prefix):]
                    property_cols[col] = {
                        "name": prop_name,
                        "category": category,
                        "original_column": col,
                    }
                    break
        return property_cols

    @classmethod
    def detect_format(cls, content: str, filename: str = "") -> InputFormat:
        """Detect the input format from content or filename."""
        # Check filename extension first
        if filename:
            ext = Path(filename).suffix.lower()
            if ext in [".xlsx", ".xls"]:
                return InputFormat.EXCEL
            if ext == ".csv":
                return InputFormat.CSV
            if ext == ".json":
                return InputFormat.JSON

        # Analyze content
        content_stripped = content.strip()

        # Check for JSON
        if content_stripped.startswith(("{", "[")):
            try:
                json.loads(content_stripped)
                return InputFormat.JSON
            except json.JSONDecodeError:
                pass

        # Check for CSV (has commas and newlines with consistent column counts)
        lines = content_stripped.split("\n")
        if len(lines) > 1:
            first_line_commas = lines[0].count(",")
            if first_line_commas > 0:
                # Verify it's consistent CSV
                second_line_commas = lines[1].count(",") if len(lines) > 1 else 0
                if first_line_commas == second_line_commas or abs(first_line_commas - second_line_commas) <= 1:
                    return InputFormat.CSV

        # Default to text (will be parsed as key-value pairs)
        return InputFormat.TEXT

    @classmethod
    def detect_tier(cls, columns: List[str]) -> FormatTier:
        """Detect tier based on available columns."""
        cols_lower = {c.lower().strip() for c in columns}

        # Check for Tier 4 indicators (LEVEL_X columns)
        tier_4_matches = cols_lower & {c.lower() for c in cls.TIER_4_COLUMNS}
        if len(tier_4_matches) >= 3:
            return FormatTier.TIER_4

        # Check for Tier 3 indicators (explicit IDs, source info)
        tier_3_matches = cols_lower & {c.lower() for c in cls.TIER_3_COLUMNS}
        if len(tier_3_matches) >= 3:
            return FormatTier.TIER_3

        # Check for Tier 2 indicators (parent names, named hierarchy)
        tier_2_matches = cols_lower & {c.lower() for c in cls.TIER_2_COLUMNS}
        if len(tier_2_matches) >= 2:
            return FormatTier.TIER_2

        # Default to Tier 1 (simple mapping)
        return FormatTier.TIER_1

    @classmethod
    def detect_parent_strategy(cls, columns: List[str]) -> str:
        """Detect how parent-child relationships are defined."""
        cols_lower = {c.lower().strip() for c in columns}

        if "parent_id" in cols_lower:
            return "id_reference"
        if "parent_name" in cols_lower or "parent" in cols_lower:
            return "name_reference"
        return "flat"  # No parent column = flat structure

    @classmethod
    def analyze(cls, content: str, filename: str = "") -> Dict[str, Any]:
        """Full analysis of input content."""
        format_type = cls.detect_format(content, filename)

        # Extract columns based on format
        columns = []
        sample_rows = []

        if format_type == InputFormat.CSV:
            lines = content.strip().split("\n")
            if lines:
                columns = [c.strip().strip('"') for c in lines[0].split(",")]
                for line in lines[1:6]:  # Sample first 5 rows
                    values = cls._parse_csv_line(line)
                    if values:
                        sample_rows.append(dict(zip(columns, values)))

        elif format_type == InputFormat.JSON:
            try:
                data = json.loads(content)
                if isinstance(data, list) and data:
                    columns = list(data[0].keys()) if isinstance(data[0], dict) else []
                    sample_rows = data[:5]
                elif isinstance(data, dict):
                    columns = list(data.keys())
            except json.JSONDecodeError:
                pass

        tier = cls.detect_tier(columns) if columns else FormatTier.TIER_1
        parent_strategy = cls.detect_parent_strategy(columns)
        property_columns = cls.detect_property_columns(columns) if columns else {}

        # Check for template_id column
        has_template = "template_id" in {c.lower() for c in columns}

        return {
            "format": format_type.value,
            "tier": tier.value,
            "columns_found": columns,
            "column_count": len(columns),
            "parent_strategy": parent_strategy,
            "sample_rows": sample_rows,
            "property_columns": property_columns,
            "property_column_count": len(property_columns),
            "has_template_column": has_template,
            "recommendations": cls._get_recommendations(tier, columns, parent_strategy, property_columns),
        }

    @classmethod
    def _get_recommendations(cls, tier: FormatTier, columns: List[str], parent_strategy: str, property_columns: Dict = None) -> List[str]:
        """Generate recommendations based on analysis."""
        recommendations = []
        cols_lower = {c.lower() for c in columns}
        property_columns = property_columns or {}

        if tier == FormatTier.TIER_1:
            if "source_value" not in cols_lower and "value" not in cols_lower:
                recommendations.append("Add a 'source_value' column for account codes/values to map")
            if "group_name" not in cols_lower and "group" not in cols_lower and "category" not in cols_lower:
                recommendations.append("Add a 'group_name' column for hierarchy categories")

        if tier in [FormatTier.TIER_1, FormatTier.TIER_2]:
            recommendations.append("Configure project defaults for source_database, source_schema, source_table, source_column")

        if parent_strategy == "flat":
            recommendations.append("Consider adding 'parent_name' column for hierarchical structure")

        # Property recommendations
        if property_columns:
            categories = set(p["category"] for p in property_columns.values())
            recommendations.append(f"Found {len(property_columns)} property columns: {', '.join(categories)}")
        else:
            recommendations.append("Add property columns (PROP_*, DIM_*, FACT_*, FILTER_*, DISPLAY_*) to set hierarchy properties")

        if "template_id" not in cols_lower and tier in [FormatTier.TIER_2, FormatTier.TIER_3]:
            recommendations.append("Add 'template_id' column to apply property templates automatically")

        return recommendations

    @staticmethod
    def _parse_csv_line(line: str) -> List[str]:
        """Parse a CSV line handling quoted values."""
        values = []
        current = ""
        in_quotes = False

        for char in line:
            if char == '"':
                in_quotes = not in_quotes
            elif char == "," and not in_quotes:
                values.append(current.strip().strip('"'))
                current = ""
            else:
                current += char

        values.append(current.strip().strip('"'))
        return values


class FlexibleImportService:
    """Service for importing hierarchies from various formats and tiers."""

    def __init__(self, hierarchy_service):
        """Initialize with a HierarchyService instance."""
        self.hierarchy_service = hierarchy_service
        self.project_defaults: Dict[str, ProjectDefaults] = {}
        self._defaults_file = hierarchy_service.data_dir / "project_defaults.json"
        self._load_defaults()

    def _load_defaults(self):
        """Load project defaults from storage."""
        if self._defaults_file.exists():
            try:
                with open(self._defaults_file) as f:
                    data = json.load(f)
                    for proj_id, defaults in data.items():
                        self.project_defaults[proj_id] = ProjectDefaults.from_dict(defaults)
            except Exception as e:
                logger.warning(f"Failed to load project defaults: {e}")

    def _save_defaults(self):
        """Save project defaults to storage."""
        try:
            data = {
                proj_id: defaults.to_dict()
                for proj_id, defaults in self.project_defaults.items()
            }
            with open(self._defaults_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save project defaults: {e}")

    def configure_defaults(
        self,
        project_id: str,
        source_database: str,
        source_schema: str,
        source_table: str,
        source_column: str,
    ) -> ProjectDefaults:
        """Configure default source information for a project."""
        defaults = ProjectDefaults(
            source_database=source_database,
            source_schema=source_schema,
            source_table=source_table,
            source_column=source_column,
        )
        self.project_defaults[project_id] = defaults
        self._save_defaults()
        return defaults

    def get_defaults(self, project_id: str) -> Optional[ProjectDefaults]:
        """Get project defaults."""
        return self.project_defaults.get(project_id)

    def _generate_hierarchy_id(self, name: str) -> str:
        """Generate hierarchy ID from name (SNAKE_CASE)."""
        slug = re.sub(r"[^A-Z0-9]+", "_", name.upper())
        slug = re.sub(r"^_+|_+$", "", slug)
        return slug[:50] if slug else "UNNAMED"

    def _parse_source_value(self, value: str) -> Tuple[str, str]:
        """Parse source_value to extract pattern type and value.

        Returns:
            Tuple of (source_uid, pattern_type)
            - "4100" -> ("4100", "exact")
            - "4%" -> ("4%", "like")
            - "41*" -> ("41%", "like")
        """
        value = str(value).strip()

        if value.endswith("%") or value.endswith("*"):
            # LIKE pattern
            pattern = value.rstrip("*").rstrip("%") + "%"
            return pattern, "like"
        else:
            # Exact match
            return value, "exact"

    def preview_import(
        self,
        content: str,
        format_type: str = "auto",
        source_defaults: Optional[Dict[str, str]] = None,
        limit: int = 10,
    ) -> Dict[str, Any]:
        """Preview import without creating hierarchies.

        Returns what would be created without persisting.
        """
        # Detect format and tier
        analysis = FormatDetector.analyze(content)
        detected_format = InputFormat(analysis["format"]) if format_type == "auto" else InputFormat(format_type)
        detected_tier = FormatTier(analysis["tier"])

        # Parse content based on format
        if detected_format == InputFormat.CSV:
            rows = self._parse_csv(content)
        elif detected_format == InputFormat.JSON:
            rows = self._parse_json(content)
        elif detected_format == InputFormat.EXCEL:
            rows = self._parse_excel(content)
        else:
            rows = self._parse_text(content)

        # Convert rows to hierarchy previews
        defaults = ProjectDefaults.from_dict(source_defaults) if source_defaults else ProjectDefaults()
        previews = []

        for i, row in enumerate(rows[:limit]):
            preview = self._row_to_hierarchy_preview(row, detected_tier, defaults, i + 1)
            previews.append(preview)

        return {
            "detected_format": detected_format.value,
            "detected_tier": detected_tier.value,
            "total_rows": len(rows),
            "preview_count": len(previews),
            "hierarchies_preview": previews,
            "columns": list(rows[0].keys()) if rows else [],
            "inferred_fields": self._get_inferred_fields(detected_tier),
            "source_defaults_used": defaults.to_dict(),
            "source_defaults_complete": defaults.is_complete(),
        }

    def import_flexible(
        self,
        project_id: str,
        content: str,
        format_type: str = "auto",
        source_defaults: Optional[Dict[str, str]] = None,
        tier_hint: str = "auto",
    ) -> Dict[str, Any]:
        """Import hierarchies from flexible format.

        Args:
            project_id: Target project ID
            content: Input content (CSV, JSON, etc.)
            format_type: Input format or "auto"
            source_defaults: Override defaults for this import
            tier_hint: Tier hint or "auto"

        Returns:
            Import result with created hierarchies and mappings
        """
        # Verify project exists
        project = self.hierarchy_service.get_project(project_id)
        if not project:
            return {"error": f"Project '{project_id}' not found"}

        # Detect format and tier
        analysis = FormatDetector.analyze(content)
        detected_format = InputFormat(analysis["format"]) if format_type == "auto" else InputFormat(format_type)
        detected_tier = FormatTier(analysis["tier"]) if tier_hint == "auto" else FormatTier(tier_hint)

        # Get defaults (passed in or from project)
        if source_defaults:
            defaults = ProjectDefaults.from_dict(source_defaults)
        else:
            defaults = self.get_defaults(project_id) or ProjectDefaults()

        # Parse content
        if detected_format == InputFormat.CSV:
            rows = self._parse_csv(content)
        elif detected_format == InputFormat.JSON:
            rows = self._parse_json(content)
        elif detected_format == InputFormat.EXCEL:
            rows = self._parse_excel(content)
        else:
            rows = self._parse_text(content)

        if not rows:
            return {"error": "No data rows found in content"}

        # Import based on tier
        if detected_tier == FormatTier.TIER_1:
            result = self._import_tier_1(project_id, rows, defaults)
        elif detected_tier == FormatTier.TIER_2:
            result = self._import_tier_2(project_id, rows, defaults)
        elif detected_tier == FormatTier.TIER_3:
            result = self._import_tier_3(project_id, rows, defaults)
        else:
            result = self._import_tier_4(project_id, rows, defaults)

        result["detected_format"] = detected_format.value
        result["detected_tier"] = detected_tier.value
        result["inferred_fields"] = self._get_inferred_fields(detected_tier)

        return result

    def _parse_csv(self, content: str) -> List[Dict[str, str]]:
        """Parse CSV content to list of dicts."""
        rows = []
        lines = content.strip().split("\n")
        if not lines:
            return rows

        headers = [h.strip().strip('"').lower() for h in lines[0].split(",")]

        for line in lines[1:]:
            if not line.strip():
                continue
            values = FormatDetector._parse_csv_line(line)
            if len(values) >= len(headers):
                row = dict(zip(headers, values[:len(headers)]))
                rows.append(row)
            elif values:
                # Pad with empty strings
                values.extend([""] * (len(headers) - len(values)))
                row = dict(zip(headers, values))
                rows.append(row)

        return rows

    def _parse_json(self, content: str) -> List[Dict[str, str]]:
        """Parse JSON content to list of dicts."""
        try:
            data = json.loads(content)
            if isinstance(data, list):
                return [{k.lower(): str(v) for k, v in row.items()} for row in data if isinstance(row, dict)]
            elif isinstance(data, dict):
                # Single object or nested structure
                if "hierarchies" in data:
                    return [{k.lower(): str(v) for k, v in row.items()} for row in data["hierarchies"]]
                return [{k.lower(): str(v) for k, v in data.items()}]
        except json.JSONDecodeError:
            return []
        return []

    def _parse_excel(self, content: str) -> List[Dict[str, str]]:
        """Parse Excel content (base64 or file path) to list of dicts."""
        if not HAS_PANDAS:
            logger.error("pandas not installed, Excel parsing unavailable")
            return []

        try:
            # If content is a file path
            if Path(content).exists():
                df = pd.read_excel(content)
            else:
                # Assume base64 encoded content
                import base64
                decoded = base64.b64decode(content)
                df = pd.read_excel(io.BytesIO(decoded))

            # Convert to list of dicts with lowercase keys
            df.columns = [str(c).lower().strip() for c in df.columns]
            return df.fillna("").astype(str).to_dict("records")
        except Exception as e:
            logger.error(f"Excel parsing failed: {e}")
            return []

    def _parse_text(self, content: str) -> List[Dict[str, str]]:
        """Parse plain text as simple two-column mapping."""
        rows = []
        for line in content.strip().split("\n"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # Try various delimiters
            for delim in [",", "\t", ":", "->", "=>"]:
                if delim in line:
                    parts = [p.strip() for p in line.split(delim, 1)]
                    if len(parts) == 2:
                        rows.append({
                            "source_value": parts[0],
                            "group_name": parts[1],
                        })
                        break

        return rows

    def _import_tier_1(
        self,
        project_id: str,
        rows: List[Dict[str, str]],
        defaults: ProjectDefaults,
    ) -> Dict[str, Any]:
        """Import Tier 1: Ultra-simple 2-3 column format.

        Expected columns: source_value, group_name (or similar)
        Auto-generates: hierarchy_id, parent relationships, flags
        """
        # Normalize column names
        col_map = self._normalize_tier_1_columns(rows[0].keys() if rows else [])

        # Group by hierarchy name
        groups: Dict[str, List[str]] = {}
        for row in rows:
            group_name = row.get(col_map.get("group_name", "group_name"), "").strip()
            source_value = row.get(col_map.get("source_value", "source_value"), "").strip()

            if group_name and source_value:
                if group_name not in groups:
                    groups[group_name] = []
                groups[group_name].append(source_value)

        # Create hierarchies and mappings
        created_hierarchies = []
        created_mappings = []
        errors = []

        for sort_idx, (group_name, values) in enumerate(groups.items(), start=1):
            try:
                # Create hierarchy
                hierarchy = self.hierarchy_service.create_hierarchy(
                    project_id=project_id,
                    hierarchy_name=group_name,
                    parent_id=None,  # Flat structure in Tier 1
                    description=f"Auto-imported from Tier 1 format ({len(values)} values)",
                    flags={
                        "include_flag": True,
                        "exclude_flag": False,
                        "active_flag": True,
                        "is_leaf_node": True,
                    },
                    sort_order=sort_idx,
                )

                created_hierarchies.append({
                    "hierarchy_id": hierarchy.hierarchy_id,
                    "hierarchy_name": hierarchy.hierarchy_name,
                    "id": hierarchy.id,
                })

                # Extract and apply properties from rows in this group
                # Find the first row that belongs to this group
                for row in rows:
                    row_group = row.get(col_map.get("group_name", "group_name"), "").strip()
                    if row_group == group_name:
                        columns = list(row.keys())
                        properties, template_id = self._extract_properties_from_row(row, columns)
                        if properties or template_id:
                            self._apply_properties_to_hierarchy(
                                project_id=project_id,
                                hierarchy_id=hierarchy.hierarchy_id,
                                properties=properties,
                                template_id=template_id,
                            )
                        break  # Only use first row for properties

                # Add mappings for each source value
                for value in values:
                    source_uid, pattern_type = self._parse_source_value(value)
                    result = self.hierarchy_service.add_source_mapping(
                        project_id=project_id,
                        hierarchy_id=hierarchy.hierarchy_id,
                        source_database=defaults.source_database or "DEFAULT",
                        source_schema=defaults.source_schema or "DEFAULT",
                        source_table=defaults.source_table or "SOURCE_TABLE",
                        source_column=defaults.source_column or "SOURCE_COLUMN",
                        source_uid=source_uid,
                        precedence_group="1",
                    )
                    if result:
                        created_mappings.append({
                            "hierarchy_id": hierarchy.hierarchy_id,
                            "source_uid": source_uid,
                            "pattern_type": pattern_type,
                        })

            except Exception as e:
                errors.append(f"Failed to create '{group_name}': {str(e)}")

        return {
            "status": "success" if not errors else "partial",
            "hierarchies_created": len(created_hierarchies),
            "mappings_created": len(created_mappings),
            "created_hierarchies": created_hierarchies,
            "created_mappings": created_mappings,
            "errors": errors,
        }

    def _import_tier_2(
        self,
        project_id: str,
        rows: List[Dict[str, str]],
        defaults: ProjectDefaults,
    ) -> Dict[str, Any]:
        """Import Tier 2: Basic format with parent names.

        Expected columns: hierarchy_name, parent_name, source_value, sort_order
        Auto-generates: hierarchy_id, parent_id (from name lookup)
        """
        col_map = self._normalize_tier_2_columns(rows[0].keys() if rows else [])

        # First pass: create all hierarchies
        name_to_id: Dict[str, str] = {}
        created_hierarchies = []
        errors = []

        # Sort by having parents first (roots), then children
        def sort_key(row):
            parent = row.get(col_map.get("parent_name", "parent_name"), "").strip()
            return (0 if not parent else 1, row.get(col_map.get("sort_order", "sort_order"), "0"))

        sorted_rows = sorted(rows, key=sort_key)

        for row in sorted_rows:
            hierarchy_name = row.get(col_map.get("hierarchy_name", "hierarchy_name"), "").strip()
            parent_name = row.get(col_map.get("parent_name", "parent_name"), "").strip()
            source_value = row.get(col_map.get("source_value", "source_value"), "").strip()
            sort_order_str = row.get(col_map.get("sort_order", "sort_order"), "").strip()
            description = row.get(col_map.get("description", "description"), "").strip()

            if not hierarchy_name:
                continue

            # Skip if already created
            if hierarchy_name in name_to_id:
                continue

            # Resolve parent ID from name
            parent_id = name_to_id.get(parent_name) if parent_name else None

            # Parse sort order
            sort_order = None
            if sort_order_str:
                try:
                    sort_order = int(sort_order_str)
                except ValueError:
                    pass

            try:
                hierarchy = self.hierarchy_service.create_hierarchy(
                    project_id=project_id,
                    hierarchy_name=hierarchy_name,
                    parent_id=parent_id,
                    description=description or f"Imported from Tier 2 format",
                    flags={
                        "include_flag": True,
                        "active_flag": True,
                        "is_leaf_node": not any(
                            r.get(col_map.get("parent_name", "parent_name"), "").strip() == hierarchy_name
                            for r in rows
                        ),
                    },
                    sort_order=sort_order,
                )

                name_to_id[hierarchy_name] = hierarchy.id
                created_hierarchies.append({
                    "hierarchy_id": hierarchy.hierarchy_id,
                    "hierarchy_name": hierarchy.hierarchy_name,
                    "id": hierarchy.id,
                    "parent_id": parent_id,
                })

                # Extract and apply properties from this row
                columns = list(row.keys())
                properties, template_id = self._extract_properties_from_row(row, columns)
                if properties or template_id:
                    self._apply_properties_to_hierarchy(
                        project_id=project_id,
                        hierarchy_id=hierarchy.hierarchy_id,
                        properties=properties,
                        template_id=template_id,
                    )

            except Exception as e:
                errors.append(f"Failed to create '{hierarchy_name}': {str(e)}")

        # Second pass: add mappings
        created_mappings = []
        for row in rows:
            hierarchy_name = row.get(col_map.get("hierarchy_name", "hierarchy_name"), "").strip()
            source_value = row.get(col_map.get("source_value", "source_value"), "").strip()

            if not hierarchy_name or not source_value:
                continue

            # Find the hierarchy
            hierarchy = None
            for h in created_hierarchies:
                if h["hierarchy_name"] == hierarchy_name:
                    hierarchy = h
                    break

            if not hierarchy:
                continue

            source_uid, pattern_type = self._parse_source_value(source_value)
            try:
                result = self.hierarchy_service.add_source_mapping(
                    project_id=project_id,
                    hierarchy_id=hierarchy["hierarchy_id"],
                    source_database=defaults.source_database or "DEFAULT",
                    source_schema=defaults.source_schema or "DEFAULT",
                    source_table=defaults.source_table or "SOURCE_TABLE",
                    source_column=defaults.source_column or "SOURCE_COLUMN",
                    source_uid=source_uid,
                    precedence_group="1",
                )
                if result:
                    created_mappings.append({
                        "hierarchy_id": hierarchy["hierarchy_id"],
                        "source_uid": source_uid,
                        "pattern_type": pattern_type,
                    })
            except Exception as e:
                errors.append(f"Failed to add mapping to '{hierarchy_name}': {str(e)}")

        return {
            "status": "success" if not errors else "partial",
            "hierarchies_created": len(created_hierarchies),
            "mappings_created": len(created_mappings),
            "created_hierarchies": created_hierarchies,
            "created_mappings": created_mappings,
            "errors": errors,
        }

    def _import_tier_3(
        self,
        project_id: str,
        rows: List[Dict[str, str]],
        defaults: ProjectDefaults,
    ) -> Dict[str, Any]:
        """Import Tier 3: Standard format with explicit IDs.

        Expected columns: hierarchy_id, hierarchy_name, parent_id, source_database, etc.
        """
        created_hierarchies = []
        created_mappings = []
        errors = []

        for row in rows:
            hierarchy_id = row.get("hierarchy_id", "").strip()
            hierarchy_name = row.get("hierarchy_name", "").strip()
            parent_id = row.get("parent_id", "").strip() or None
            description = row.get("description", "").strip()

            if not hierarchy_name:
                continue

            # Parse flags
            flags = {
                "include_flag": row.get("include_flag", "true").lower() == "true",
                "exclude_flag": row.get("exclude_flag", "false").lower() == "true",
                "transform_flag": row.get("transform_flag", "false").lower() == "true",
                "calculation_flag": row.get("calculation_flag", "false").lower() == "true",
                "active_flag": row.get("active_flag", "true").lower() == "true",
                "is_leaf_node": row.get("is_leaf_node", "false").lower() == "true",
            }

            # Parse sort order
            sort_order = None
            sort_str = row.get("sort_order", "").strip()
            if sort_str:
                try:
                    sort_order = int(sort_str)
                except ValueError:
                    pass

            try:
                hierarchy = self.hierarchy_service.create_hierarchy(
                    project_id=project_id,
                    hierarchy_name=hierarchy_name,
                    parent_id=parent_id,
                    description=description,
                    flags=flags,
                    sort_order=sort_order,
                )

                created_hierarchies.append({
                    "hierarchy_id": hierarchy.hierarchy_id,
                    "hierarchy_name": hierarchy.hierarchy_name,
                    "id": hierarchy.id,
                })

                # Extract and apply properties from this row
                columns = list(row.keys())
                properties, template_id = self._extract_properties_from_row(row, columns)
                if properties or template_id:
                    self._apply_properties_to_hierarchy(
                        project_id=project_id,
                        hierarchy_id=hierarchy.hierarchy_id,
                        properties=properties,
                        template_id=template_id,
                    )

                # Add mapping if source info present
                source_database = row.get("source_database", "").strip() or defaults.source_database
                source_schema = row.get("source_schema", "").strip() or defaults.source_schema
                source_table = row.get("source_table", "").strip() or defaults.source_table
                source_column = row.get("source_column", "").strip() or defaults.source_column
                source_uid = row.get("source_uid", "").strip()

                if source_uid and source_database:
                    result = self.hierarchy_service.add_source_mapping(
                        project_id=project_id,
                        hierarchy_id=hierarchy.hierarchy_id,
                        source_database=source_database,
                        source_schema=source_schema or "DEFAULT",
                        source_table=source_table or "SOURCE_TABLE",
                        source_column=source_column or "SOURCE_COLUMN",
                        source_uid=source_uid,
                        precedence_group=row.get("precedence_group", "1"),
                    )
                    if result:
                        created_mappings.append({
                            "hierarchy_id": hierarchy.hierarchy_id,
                            "source_uid": source_uid,
                        })

            except Exception as e:
                errors.append(f"Failed to create '{hierarchy_name}': {str(e)}")

        return {
            "status": "success" if not errors else "partial",
            "hierarchies_created": len(created_hierarchies),
            "mappings_created": len(created_mappings),
            "created_hierarchies": created_hierarchies,
            "created_mappings": created_mappings,
            "errors": errors,
        }

    def _import_tier_4(
        self,
        project_id: str,
        rows: List[Dict[str, str]],
        defaults: ProjectDefaults,
    ) -> Dict[str, Any]:
        """Import Tier 4: Enterprise format with LEVEL_X columns.

        This is essentially the existing import_hierarchy_csv format.
        """
        # Build CSV content from rows and use existing importer
        if not rows:
            return {"error": "No rows to import"}

        # Get all keys as headers
        headers = list(rows[0].keys())
        csv_lines = [",".join(h.upper() for h in headers)]

        for row in rows:
            values = [str(row.get(h, "")) for h in headers]
            # Escape values with commas
            escaped = []
            for v in values:
                if "," in v or '"' in v:
                    escaped.append(f'"{v.replace('"', '""')}"')
                else:
                    escaped.append(v)
            csv_lines.append(",".join(escaped))

        csv_content = "\n".join(csv_lines)

        # Use existing CSV importer
        result = self.hierarchy_service.import_hierarchy_csv(project_id, csv_content)

        return {
            "status": "success" if result.get("imported", 0) > 0 else "error",
            "hierarchies_created": result.get("imported", 0),
            "mappings_created": 0,  # Tier 4 doesn't include mappings in hierarchy CSV
            "skipped": result.get("skipped", 0),
            "errors": result.get("errors", []),
        }

    def _extract_properties_from_row(self, row: Dict[str, str], columns: List[str]) -> Tuple[List[Dict], Optional[str]]:
        """
        Extract properties and template_id from a row based on column prefixes.

        Property columns:
        - PROP_* → custom properties
        - DIM_* → dimension properties
        - FACT_* → fact properties
        - FILTER_* → filter properties
        - DISPLAY_* → display properties

        Returns:
            Tuple of (list of properties, template_id or None)
        """
        properties = []
        template_id = None

        property_cols = FormatDetector.detect_property_columns(columns)

        for col, prop_info in property_cols.items():
            value = row.get(col, "").strip()
            if value:
                # Try to parse as JSON for complex values
                try:
                    parsed_value = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    # Handle boolean strings
                    if value.lower() in ("true", "yes", "1"):
                        parsed_value = True
                    elif value.lower() in ("false", "no", "0"):
                        parsed_value = False
                    else:
                        parsed_value = value

                properties.append({
                    "name": prop_info["name"],
                    "value": parsed_value,
                    "category": prop_info["category"],
                    "inherit": True,
                    "override_allowed": True,
                })

        # Check for template_id column
        for col in columns:
            if col.lower() == "template_id":
                template_id = row.get(col, "").strip() or None
                break

        return properties, template_id

    def _apply_properties_to_hierarchy(
        self,
        project_id: str,
        hierarchy_id: str,
        properties: List[Dict],
        template_id: Optional[str] = None,
    ):
        """Apply extracted properties to a hierarchy."""
        # Apply template first if specified
        if template_id:
            try:
                self.hierarchy_service.apply_property_template(
                    project_id=project_id,
                    hierarchy_id=hierarchy_id,
                    template_id=template_id,
                    merge=True,
                )
            except Exception as e:
                logger.warning(f"Failed to apply template '{template_id}': {e}")

        # Apply individual properties
        for prop in properties:
            try:
                self.hierarchy_service.add_property(
                    project_id=project_id,
                    hierarchy_id=hierarchy_id,
                    name=prop["name"],
                    value=prop["value"],
                    category=prop.get("category", "custom"),
                    inherit=prop.get("inherit", True),
                    override_allowed=prop.get("override_allowed", True),
                )
            except Exception as e:
                logger.warning(f"Failed to add property '{prop['name']}': {e}")

    def _normalize_tier_1_columns(self, columns: List[str]) -> Dict[str, str]:
        """Map various column names to standard Tier 1 names."""
        col_map = {}
        cols_lower = {c.lower(): c for c in columns}

        # Map source_value
        for candidate in ["source_value", "value", "account", "account_code", "gl_code", "code"]:
            if candidate in cols_lower:
                col_map["source_value"] = cols_lower[candidate]
                break

        # Map group_name
        for candidate in ["group_name", "group", "category", "hierarchy_name", "name"]:
            if candidate in cols_lower:
                col_map["group_name"] = cols_lower[candidate]
                break

        return col_map

    def _normalize_tier_2_columns(self, columns: List[str]) -> Dict[str, str]:
        """Map various column names to standard Tier 2 names."""
        col_map = {}
        cols_lower = {c.lower(): c for c in columns}

        mappings = {
            "hierarchy_name": ["hierarchy_name", "name", "hierarchy"],
            "parent_name": ["parent_name", "parent", "parent_hierarchy"],
            "source_value": ["source_value", "value", "account_code", "code"],
            "sort_order": ["sort_order", "order", "sequence", "sort"],
            "description": ["description", "desc", "notes"],
            "template_id": ["template_id", "template"],
        }

        for standard, candidates in mappings.items():
            for candidate in candidates:
                if candidate in cols_lower:
                    col_map[standard] = cols_lower[candidate]
                    break

        return col_map

    def _row_to_hierarchy_preview(
        self,
        row: Dict[str, str],
        tier: FormatTier,
        defaults: ProjectDefaults,
        index: int,
    ) -> Dict[str, Any]:
        """Convert a row to a hierarchy preview dict."""
        if tier == FormatTier.TIER_1:
            col_map = self._normalize_tier_1_columns(row.keys())
            group_name = row.get(col_map.get("group_name", "group_name"), f"Group_{index}")
            source_value = row.get(col_map.get("source_value", "source_value"), "")

            return {
                "hierarchy_id": self._generate_hierarchy_id(group_name),
                "hierarchy_name": group_name,
                "parent_id": None,
                "source_mapping": {
                    "source_database": defaults.source_database or "[NEEDS_DEFAULT]",
                    "source_schema": defaults.source_schema or "[NEEDS_DEFAULT]",
                    "source_table": defaults.source_table or "[NEEDS_DEFAULT]",
                    "source_column": defaults.source_column or "[NEEDS_DEFAULT]",
                    "source_uid": source_value,
                },
                "inferred": ["hierarchy_id", "parent_id", "flags", "source_database", "source_schema", "source_table", "source_column"],
            }

        elif tier == FormatTier.TIER_2:
            col_map = self._normalize_tier_2_columns(row.keys())
            hierarchy_name = row.get(col_map.get("hierarchy_name", "hierarchy_name"), f"Hierarchy_{index}")
            parent_name = row.get(col_map.get("parent_name", "parent_name"), "")
            source_value = row.get(col_map.get("source_value", "source_value"), "")

            return {
                "hierarchy_id": self._generate_hierarchy_id(hierarchy_name),
                "hierarchy_name": hierarchy_name,
                "parent_name": parent_name,
                "parent_id": "[RESOLVED_FROM_NAME]" if parent_name else None,
                "source_mapping": {
                    "source_database": defaults.source_database or "[NEEDS_DEFAULT]",
                    "source_schema": defaults.source_schema or "[NEEDS_DEFAULT]",
                    "source_table": defaults.source_table or "[NEEDS_DEFAULT]",
                    "source_column": defaults.source_column or "[NEEDS_DEFAULT]",
                    "source_uid": source_value,
                } if source_value else None,
                "inferred": ["hierarchy_id", "parent_id"],
            }

        else:
            # Tier 3/4 - less inference needed
            return {
                "hierarchy_id": row.get("hierarchy_id", self._generate_hierarchy_id(row.get("hierarchy_name", f"H_{index}"))),
                "hierarchy_name": row.get("hierarchy_name", f"Hierarchy_{index}"),
                "parent_id": row.get("parent_id"),
                "source_uid": row.get("source_uid"),
                "inferred": [],
            }

    def _get_inferred_fields(self, tier: FormatTier) -> List[str]:
        """Get list of fields that will be auto-inferred for a tier."""
        if tier == FormatTier.TIER_1:
            return [
                "hierarchy_id (from group_name)",
                "parent_id (flat structure)",
                "is_leaf_node (true for all)",
                "include_flag (true)",
                "active_flag (true)",
                "source_database (from defaults)",
                "source_schema (from defaults)",
                "source_table (from defaults)",
                "source_column (from defaults)",
                "sort_order (row index)",
            ]
        elif tier == FormatTier.TIER_2:
            return [
                "hierarchy_id (from hierarchy_name)",
                "parent_id (resolved from parent_name)",
                "is_leaf_node (calculated from tree)",
                "include_flag (true)",
                "active_flag (true)",
                "source_database (from defaults)",
                "source_schema (from defaults)",
                "source_table (from defaults)",
                "source_column (from defaults)",
            ]
        elif tier == FormatTier.TIER_3:
            return [
                "source_* (from defaults if not provided)",
                "sort_order (row index if not provided)",
            ]
        else:
            return ["None - all fields expected"]

    def export_simplified(
        self,
        project_id: str,
        target_tier: str = "tier_2",
    ) -> Dict[str, Any]:
        """Export project in simplified format.

        Args:
            project_id: Project to export
            target_tier: Target tier format (tier_1, tier_2, tier_3)

        Returns:
            Dict with CSV content and metadata
        """
        hierarchies = self.hierarchy_service.list_hierarchies(project_id)
        if not hierarchies:
            return {"error": "No hierarchies found in project"}

        target = FormatTier(target_tier)

        if target == FormatTier.TIER_1:
            return self._export_tier_1(hierarchies)
        elif target == FormatTier.TIER_2:
            return self._export_tier_2(hierarchies)
        else:
            return self._export_tier_3(hierarchies)

    def _export_tier_1(self, hierarchies: List[Dict]) -> Dict[str, Any]:
        """Export as Tier 1 format (source_value, group_name)."""
        rows = []
        for h in hierarchies:
            hierarchy_name = h.get("hierarchy_name", "")
            for m in h.get("mapping", []):
                source_uid = m.get("source_uid", "")
                if source_uid:
                    rows.append(f"{source_uid},{hierarchy_name}")

        csv_content = "source_value,group_name\n" + "\n".join(rows)

        return {
            "format": "tier_1",
            "csv_content": csv_content,
            "row_count": len(rows),
            "note": "Simplified format - parent relationships and flags not included",
        }

    def _export_tier_2(self, hierarchies: List[Dict]) -> Dict[str, Any]:
        """Export as Tier 2 format (hierarchy_name, parent_name, source_value, sort_order)."""
        # Build name lookup for parent resolution
        id_to_name = {h.get("id"): h.get("hierarchy_name") for h in hierarchies}

        rows = ["hierarchy_name,parent_name,source_value,sort_order,description"]

        for h in hierarchies:
            hierarchy_name = h.get("hierarchy_name", "")
            parent_id = h.get("parent_id")
            parent_name = id_to_name.get(parent_id, "") if parent_id else ""
            sort_order = h.get("sort_order", 0)
            description = h.get("description", "").replace(",", ";")

            mappings = h.get("mapping", [])
            if mappings:
                for m in mappings:
                    source_uid = m.get("source_uid", "")
                    rows.append(f'"{hierarchy_name}","{parent_name}",{source_uid},{sort_order},"{description}"')
            else:
                rows.append(f'"{hierarchy_name}","{parent_name}",,{sort_order},"{description}"')

        return {
            "format": "tier_2",
            "csv_content": "\n".join(rows),
            "row_count": len(rows) - 1,
            "note": "Basic format with parent names - can be re-imported",
        }

    def _export_tier_3(self, hierarchies: List[Dict]) -> Dict[str, Any]:
        """Export as Tier 3 format (standard with IDs)."""
        rows = ["hierarchy_id,hierarchy_name,parent_id,description,sort_order,include_flag,exclude_flag,active_flag,is_leaf_node"]

        for h in hierarchies:
            flags = h.get("flags", {})
            row = [
                h.get("hierarchy_id", ""),
                f'"{h.get("hierarchy_name", "")}"',
                h.get("parent_id") or "",
                f'"{h.get("description", "")}"',
                str(h.get("sort_order", 0)),
                str(flags.get("include_flag", True)).lower(),
                str(flags.get("exclude_flag", False)).lower(),
                str(flags.get("active_flag", True)).lower(),
                str(flags.get("is_leaf_node", False)).lower(),
            ]
            rows.append(",".join(row))

        return {
            "format": "tier_3",
            "csv_content": "\n".join(rows),
            "row_count": len(rows) - 1,
            "note": "Standard format with explicit IDs - mappings exported separately",
        }
