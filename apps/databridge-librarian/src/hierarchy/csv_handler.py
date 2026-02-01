"""
CSV Handler for DataBridge AI V3.

Provides import and export of hierarchy and mapping CSV files.
Supports both current and legacy CSV formats.
"""

import csv
import io
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple, Union
from dataclasses import dataclass
from datetime import datetime


@dataclass
class CSVValidationError:
    """Represents a validation error in CSV data."""
    row: int
    column: str
    message: str
    value: Any = None


@dataclass
class CSVImportResult:
    """Result of a CSV import operation."""
    success: bool
    rows_processed: int
    rows_imported: int
    rows_skipped: int
    errors: List[CSVValidationError]
    warnings: List[str]
    data: List[Dict[str, Any]]


class CSVFormat:
    """CSV format definitions."""

    # Current format columns for hierarchy CSV
    HIERARCHY_COLUMNS = [
        "HIERARCHY_ID",
        "HIERARCHY_NAME",
        "PARENT_ID",
        "DESCRIPTION",
        "LEVEL_1", "LEVEL_2", "LEVEL_3", "LEVEL_4", "LEVEL_5",
        "LEVEL_6", "LEVEL_7", "LEVEL_8", "LEVEL_9", "LEVEL_10",
        "LEVEL_1_SORT", "LEVEL_2_SORT", "LEVEL_3_SORT", "LEVEL_4_SORT", "LEVEL_5_SORT",
        "LEVEL_6_SORT", "LEVEL_7_SORT", "LEVEL_8_SORT", "LEVEL_9_SORT", "LEVEL_10_SORT",
        "INCLUDE_FLAG", "EXCLUDE_FLAG", "TRANSFORM_FLAG", "CALCULATION_FLAG",
        "ACTIVE_FLAG", "IS_LEAF_NODE", "FORMULA_GROUP", "SORT_ORDER",
    ]

    # Required columns for hierarchy
    HIERARCHY_REQUIRED = ["HIERARCHY_ID", "HIERARCHY_NAME"]

    # Current format columns for mapping CSV
    MAPPING_COLUMNS = [
        "HIERARCHY_ID",
        "MAPPING_INDEX",
        "SOURCE_DATABASE",
        "SOURCE_SCHEMA",
        "SOURCE_TABLE",
        "SOURCE_COLUMN",
        "SOURCE_UID",
        "PRECEDENCE_GROUP",
        "INCLUDE_FLAG",
        "EXCLUDE_FLAG",
    ]

    # Required columns for mapping
    MAPPING_REQUIRED = [
        "HIERARCHY_ID",
        "SOURCE_DATABASE",
        "SOURCE_SCHEMA",
        "SOURCE_TABLE",
        "SOURCE_COLUMN",
    ]

    # Legacy format detection patterns
    LEGACY_HIERARCHY_PATTERNS = [
        "HIER_ID",  # Legacy used HIER_ID instead of HIERARCHY_ID
        "NODE_NAME",  # Legacy used NODE_NAME instead of HIERARCHY_NAME
    ]

    LEGACY_MAPPING_PATTERNS = [
        "HIER_ID",
        "DB_NAME",  # Legacy used DB_NAME instead of SOURCE_DATABASE
    ]


class CSVHandler:
    """
    Handles CSV import and export for hierarchies and mappings.

    Supports:
    - Current CSV format
    - Legacy CSV format with automatic detection
    - Validation and error reporting
    - Batch processing
    """

    def __init__(self, encoding: str = "utf-8"):
        """
        Initialize the CSV handler.

        Args:
            encoding: File encoding (default: utf-8).
        """
        self.encoding = encoding

    # =========================================================================
    # Format Detection
    # =========================================================================

    def detect_format(self, file_path: Union[str, Path]) -> Tuple[str, bool]:
        """
        Detect the format of a CSV file.

        Args:
            file_path: Path to the CSV file.

        Returns:
            Tuple of (format_type, is_legacy) where format_type is 'hierarchy' or 'mapping'.
        """
        path = Path(file_path)
        filename = path.name.upper()

        # Check filename patterns
        is_hierarchy = "HIERARCHY" in filename and "MAPPING" not in filename
        is_mapping = "MAPPING" in filename

        # Read headers
        with open(path, "r", encoding=self.encoding) as f:
            reader = csv.reader(f)
            headers = next(reader, [])
            headers_upper = [h.upper().strip() for h in headers]

        # Detect legacy format
        is_legacy = False
        for pattern in CSVFormat.LEGACY_HIERARCHY_PATTERNS + CSVFormat.LEGACY_MAPPING_PATTERNS:
            if pattern in headers_upper:
                is_legacy = True
                break

        # Determine type from headers if not from filename
        if not is_hierarchy and not is_mapping:
            if any(h in headers_upper for h in ["HIERARCHY_NAME", "NODE_NAME", "LEVEL_1"]):
                if any(h in headers_upper for h in ["SOURCE_DATABASE", "SOURCE_TABLE", "DB_NAME"]):
                    is_mapping = True
                else:
                    is_hierarchy = True
            elif any(h in headers_upper for h in ["SOURCE_DATABASE", "SOURCE_TABLE", "DB_NAME"]):
                is_mapping = True
            else:
                is_hierarchy = True  # Default to hierarchy

        format_type = "mapping" if is_mapping else "hierarchy"
        return format_type, is_legacy

    def _convert_legacy_headers(
        self,
        headers: List[str],
        format_type: str,
    ) -> Dict[str, str]:
        """
        Create a mapping from legacy headers to current headers.

        Args:
            headers: List of header names.
            format_type: 'hierarchy' or 'mapping'.

        Returns:
            Dictionary mapping old header to new header.
        """
        legacy_to_current = {}

        if format_type == "hierarchy":
            legacy_to_current = {
                "HIER_ID": "HIERARCHY_ID",
                "NODE_NAME": "HIERARCHY_NAME",
                "NODE_ID": "HIERARCHY_ID",
                "PARENT_NODE_ID": "PARENT_ID",
                "PARENT_NODE": "PARENT_ID",
                "LVL_1": "LEVEL_1",
                "LVL_2": "LEVEL_2",
                "LVL_3": "LEVEL_3",
                "LVL_4": "LEVEL_4",
                "LVL_5": "LEVEL_5",
                "INC_FLAG": "INCLUDE_FLAG",
                "EXC_FLAG": "EXCLUDE_FLAG",
                "TRANS_FLAG": "TRANSFORM_FLAG",
                "CALC_FLAG": "CALCULATION_FLAG",
            }
        else:  # mapping
            legacy_to_current = {
                "HIER_ID": "HIERARCHY_ID",
                "MAP_INDEX": "MAPPING_INDEX",
                "DB_NAME": "SOURCE_DATABASE",
                "SCHEMA_NAME": "SOURCE_SCHEMA",
                "TABLE_NAME": "SOURCE_TABLE",
                "COLUMN_NAME": "SOURCE_COLUMN",
                "UID": "SOURCE_UID",
                "PREC_GROUP": "PRECEDENCE_GROUP",
                "INC_FLAG": "INCLUDE_FLAG",
                "EXC_FLAG": "EXCLUDE_FLAG",
            }

        return legacy_to_current

    # =========================================================================
    # Import Operations
    # =========================================================================

    def import_hierarchy_csv(
        self,
        file_path: Union[str, Path],
        is_legacy: bool = False,
        validate: bool = True,
        skip_errors: bool = False,
    ) -> CSVImportResult:
        """
        Import a hierarchy CSV file.

        Args:
            file_path: Path to the CSV file.
            is_legacy: Force legacy format parsing.
            validate: Perform validation.
            skip_errors: Continue on validation errors.

        Returns:
            CSVImportResult with imported data and any errors.
        """
        path = Path(file_path)
        errors: List[CSVValidationError] = []
        warnings: List[str] = []
        data: List[Dict[str, Any]] = []

        # Auto-detect format if not specified
        if not is_legacy:
            _, is_legacy = self.detect_format(path)
            if is_legacy:
                warnings.append("Detected legacy CSV format, converting automatically")

        with open(path, "r", encoding=self.encoding) as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []

            # Convert legacy headers if needed
            header_map = {}
            if is_legacy:
                header_map = self._convert_legacy_headers(headers, "hierarchy")

            rows_processed = 0
            rows_imported = 0
            rows_skipped = 0

            for row_num, row in enumerate(reader, start=2):  # Start at 2 (after header)
                rows_processed += 1

                # Convert row keys if legacy
                converted_row = {}
                for key, value in row.items():
                    new_key = header_map.get(key.upper().strip(), key.upper().strip())
                    converted_row[new_key] = value.strip() if value else None

                # Validate required fields
                row_errors = []
                if validate:
                    row_errors = self._validate_hierarchy_row(row_num, converted_row)

                if row_errors and not skip_errors:
                    errors.extend(row_errors)
                    rows_skipped += 1
                    continue

                # Parse and normalize data
                parsed_row = self._parse_hierarchy_row(converted_row)
                data.append(parsed_row)
                rows_imported += 1

                if row_errors:
                    errors.extend(row_errors)

        success = len(errors) == 0 or skip_errors
        return CSVImportResult(
            success=success,
            rows_processed=rows_processed,
            rows_imported=rows_imported,
            rows_skipped=rows_skipped,
            errors=errors,
            warnings=warnings,
            data=data,
        )

    def import_mapping_csv(
        self,
        file_path: Union[str, Path],
        is_legacy: bool = False,
        validate: bool = True,
        skip_errors: bool = False,
    ) -> CSVImportResult:
        """
        Import a mapping CSV file.

        Args:
            file_path: Path to the CSV file.
            is_legacy: Force legacy format parsing.
            validate: Perform validation.
            skip_errors: Continue on validation errors.

        Returns:
            CSVImportResult with imported data and any errors.
        """
        path = Path(file_path)
        errors: List[CSVValidationError] = []
        warnings: List[str] = []
        data: List[Dict[str, Any]] = []

        # Auto-detect format if not specified
        if not is_legacy:
            _, is_legacy = self.detect_format(path)
            if is_legacy:
                warnings.append("Detected legacy CSV format, converting automatically")

        with open(path, "r", encoding=self.encoding) as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []

            # Convert legacy headers if needed
            header_map = {}
            if is_legacy:
                header_map = self._convert_legacy_headers(headers, "mapping")

            rows_processed = 0
            rows_imported = 0
            rows_skipped = 0

            for row_num, row in enumerate(reader, start=2):
                rows_processed += 1

                # Convert row keys if legacy
                converted_row = {}
                for key, value in row.items():
                    new_key = header_map.get(key.upper().strip(), key.upper().strip())
                    converted_row[new_key] = value.strip() if value else None

                # Validate required fields
                row_errors = []
                if validate:
                    row_errors = self._validate_mapping_row(row_num, converted_row)

                if row_errors and not skip_errors:
                    errors.extend(row_errors)
                    rows_skipped += 1
                    continue

                # Parse and normalize data
                parsed_row = self._parse_mapping_row(converted_row)
                data.append(parsed_row)
                rows_imported += 1

                if row_errors:
                    errors.extend(row_errors)

        success = len(errors) == 0 or skip_errors
        return CSVImportResult(
            success=success,
            rows_processed=rows_processed,
            rows_imported=rows_imported,
            rows_skipped=rows_skipped,
            errors=errors,
            warnings=warnings,
            data=data,
        )

    def import_from_string(
        self,
        csv_content: str,
        format_type: str,
        is_legacy: bool = False,
        validate: bool = True,
        skip_errors: bool = False,
    ) -> CSVImportResult:
        """
        Import CSV from a string.

        Args:
            csv_content: CSV content as string.
            format_type: 'hierarchy' or 'mapping'.
            is_legacy: Force legacy format parsing.
            validate: Perform validation.
            skip_errors: Continue on validation errors.

        Returns:
            CSVImportResult with imported data and any errors.
        """
        # Write to temp file and import
        import tempfile
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
            encoding=self.encoding,
        ) as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            if format_type == "hierarchy":
                return self.import_hierarchy_csv(temp_path, is_legacy, validate, skip_errors)
            else:
                return self.import_mapping_csv(temp_path, is_legacy, validate, skip_errors)
        finally:
            Path(temp_path).unlink(missing_ok=True)

    # =========================================================================
    # Validation
    # =========================================================================

    def _validate_hierarchy_row(
        self,
        row_num: int,
        row: Dict[str, Any],
    ) -> List[CSVValidationError]:
        """Validate a hierarchy row."""
        errors = []

        # Check required fields
        for field in CSVFormat.HIERARCHY_REQUIRED:
            if not row.get(field):
                errors.append(CSVValidationError(
                    row=row_num,
                    column=field,
                    message=f"Required field '{field}' is missing or empty",
                ))

        # Validate flags are boolean-like
        flag_fields = [
            "INCLUDE_FLAG", "EXCLUDE_FLAG", "TRANSFORM_FLAG",
            "CALCULATION_FLAG", "ACTIVE_FLAG", "IS_LEAF_NODE"
        ]
        for field in flag_fields:
            value = row.get(field)
            if value and value.upper() not in ("TRUE", "FALSE", "1", "0", "YES", "NO", "Y", "N"):
                errors.append(CSVValidationError(
                    row=row_num,
                    column=field,
                    message=f"Invalid boolean value for '{field}'",
                    value=value,
                ))

        # Validate sort order is numeric
        for i in range(1, 11):
            sort_field = f"LEVEL_{i}_SORT"
            value = row.get(sort_field)
            if value:
                try:
                    int(value)
                except ValueError:
                    errors.append(CSVValidationError(
                        row=row_num,
                        column=sort_field,
                        message=f"Invalid numeric value for '{sort_field}'",
                        value=value,
                    ))

        return errors

    def _validate_mapping_row(
        self,
        row_num: int,
        row: Dict[str, Any],
    ) -> List[CSVValidationError]:
        """Validate a mapping row."""
        errors = []

        # Check required fields
        for field in CSVFormat.MAPPING_REQUIRED:
            if not row.get(field):
                errors.append(CSVValidationError(
                    row=row_num,
                    column=field,
                    message=f"Required field '{field}' is missing or empty",
                ))

        # Validate mapping_index is numeric
        value = row.get("MAPPING_INDEX")
        if value:
            try:
                int(value)
            except ValueError:
                errors.append(CSVValidationError(
                    row=row_num,
                    column="MAPPING_INDEX",
                    message="Invalid numeric value for 'MAPPING_INDEX'",
                    value=value,
                ))

        return errors

    # =========================================================================
    # Parsing
    # =========================================================================

    def _parse_hierarchy_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Parse and normalize a hierarchy row."""
        result = {
            "hierarchy_id": row.get("HIERARCHY_ID"),
            "hierarchy_name": row.get("HIERARCHY_NAME"),
            "parent_id": row.get("PARENT_ID") or None,
            "description": row.get("DESCRIPTION"),
            "formula_group": row.get("FORMULA_GROUP"),
            "sort_order": self._parse_int(row.get("SORT_ORDER"), 0),
        }

        # Parse levels
        for i in range(1, 16):
            level_key = f"LEVEL_{i}"
            sort_key = f"LEVEL_{i}_SORT"
            result[f"level_{i}"] = row.get(level_key)
            result[f"level_{i}_sort"] = self._parse_int(row.get(sort_key))

        # Parse flags
        result["include_flag"] = self._parse_bool(row.get("INCLUDE_FLAG"), True)
        result["exclude_flag"] = self._parse_bool(row.get("EXCLUDE_FLAG"), False)
        result["transform_flag"] = self._parse_bool(row.get("TRANSFORM_FLAG"), False)
        result["calculation_flag"] = self._parse_bool(row.get("CALCULATION_FLAG"), False)
        result["active_flag"] = self._parse_bool(row.get("ACTIVE_FLAG"), True)
        result["is_leaf_node"] = self._parse_bool(row.get("IS_LEAF_NODE"), False)

        return result

    def _parse_mapping_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Parse and normalize a mapping row."""
        return {
            "hierarchy_id": row.get("HIERARCHY_ID"),
            "mapping_index": self._parse_int(row.get("MAPPING_INDEX"), 0),
            "source_database": row.get("SOURCE_DATABASE"),
            "source_schema": row.get("SOURCE_SCHEMA"),
            "source_table": row.get("SOURCE_TABLE"),
            "source_column": row.get("SOURCE_COLUMN"),
            "source_uid": row.get("SOURCE_UID"),
            "precedence_group": row.get("PRECEDENCE_GROUP") or "DEFAULT",
            "include_flag": self._parse_bool(row.get("INCLUDE_FLAG"), True),
            "exclude_flag": self._parse_bool(row.get("EXCLUDE_FLAG"), False),
        }

    def _parse_bool(self, value: Any, default: bool = False) -> bool:
        """Parse a boolean value from various formats."""
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.upper() in ("TRUE", "1", "YES", "Y")
        return bool(value)

    def _parse_int(self, value: Any, default: Optional[int] = None) -> Optional[int]:
        """Parse an integer value."""
        if value is None or value == "":
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    # =========================================================================
    # Export Operations
    # =========================================================================

    def export_hierarchy_csv(
        self,
        data: List[Dict[str, Any]],
        file_path: Optional[Union[str, Path]] = None,
        include_all_columns: bool = False,
    ) -> str:
        """
        Export hierarchy data to CSV.

        Args:
            data: List of hierarchy dictionaries.
            file_path: Optional file path to write to.
            include_all_columns: Include all columns even if empty.

        Returns:
            CSV content as string.
        """
        # Determine columns to include
        if include_all_columns:
            columns = CSVFormat.HIERARCHY_COLUMNS
        else:
            # Only include columns that have data
            used_columns = set()
            for row in data:
                for key, value in row.items():
                    if value is not None and value != "":
                        used_columns.add(key.upper())

            columns = [c for c in CSVFormat.HIERARCHY_COLUMNS if c in used_columns]
            # Always include required columns
            for req in CSVFormat.HIERARCHY_REQUIRED:
                if req not in columns:
                    columns.insert(0, req)

        # Write CSV
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()

        for row in data:
            # Convert to uppercase keys and format values
            csv_row = {}
            for col in columns:
                key = col.lower()
                value = row.get(key)

                # Format booleans
                if isinstance(value, bool):
                    value = "TRUE" if value else "FALSE"
                elif value is None:
                    value = ""

                csv_row[col] = value

            writer.writerow(csv_row)

        csv_content = output.getvalue()

        if file_path:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding=self.encoding, newline="") as f:
                f.write(csv_content)

        return csv_content

    def export_mapping_csv(
        self,
        data: List[Dict[str, Any]],
        file_path: Optional[Union[str, Path]] = None,
    ) -> str:
        """
        Export mapping data to CSV.

        Args:
            data: List of mapping dictionaries.
            file_path: Optional file path to write to.

        Returns:
            CSV content as string.
        """
        columns = CSVFormat.MAPPING_COLUMNS

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()

        for row in data:
            csv_row = {}
            for col in columns:
                key = col.lower()
                value = row.get(key)

                # Format booleans
                if isinstance(value, bool):
                    value = "TRUE" if value else "FALSE"
                elif value is None:
                    value = ""

                csv_row[col] = value

            writer.writerow(csv_row)

        csv_content = output.getvalue()

        if file_path:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding=self.encoding, newline="") as f:
                f.write(csv_content)

        return csv_content
