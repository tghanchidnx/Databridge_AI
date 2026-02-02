"""
Table parser module for extracting tabular data from text.

Detects and parses table structures from raw text,
including text extracted from PDFs and OCR.
"""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class ParsedTable:
    """A parsed table from text."""

    headers: List[str]
    rows: List[List[str]]
    row_count: int
    column_count: int
    delimiter_detected: str
    confidence: float  # 0.0 to 1.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "headers": self.headers,
            "rows": self.rows,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "delimiter_detected": self.delimiter_detected,
            "confidence": self.confidence,
        }

    def to_records(self) -> List[Dict[str, str]]:
        """Convert to list of dictionaries (records format)."""
        if not self.headers:
            return [{"col_" + str(i): cell for i, cell in enumerate(row)} for row in self.rows]
        return [dict(zip(self.headers, row)) for row in self.rows]


@dataclass
class TableParseResult:
    """Result of table parsing operation."""

    success: bool
    tables: List[ParsedTable]
    total_tables: int
    raw_text_length: int
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "success": self.success,
            "tables": [t.to_dict() for t in self.tables],
            "total_tables": self.total_tables,
            "raw_text_length": self.raw_text_length,
            "error": self.error,
        }


class TableParser:
    """
    Parse tabular data from raw text.

    Supports various table formats:
    - Tab-separated
    - Space-aligned (fixed-width)
    - Pipe-delimited (Markdown tables)
    - Custom delimiters

    Example:
        ```python
        parser = TableParser()

        # Auto-detect delimiter
        result = parser.parse(text)

        # Or specify delimiter
        result = parser.parse(text, delimiter="\\t")

        for table in result.tables:
            print(f"Found {table.row_count} rows, {table.column_count} columns")
            for record in table.to_records():
                print(record)
        ```
    """

    # Common delimiters to try in order of preference
    DELIMITERS = [
        ("\t", "tab"),
        ("|", "pipe"),
        (",", "comma"),
        (";", "semicolon"),
    ]

    def __init__(
        self,
        min_columns: int = 2,
        min_rows: int = 2,
        strip_cells: bool = True,
    ):
        """
        Initialize table parser.

        Args:
            min_columns: Minimum columns to consider as a table.
            min_rows: Minimum rows to consider as a table.
            strip_cells: Strip whitespace from cell values.
        """
        self.min_columns = min_columns
        self.min_rows = min_rows
        self.strip_cells = strip_cells

    def parse(
        self,
        text: str,
        delimiter: str = "auto",
        has_header: bool = True,
    ) -> TableParseResult:
        """
        Parse tables from text.

        Args:
            text: Raw text containing tabular data.
            delimiter: Column delimiter:
                - "auto": Auto-detect delimiter
                - "tab": Tab character
                - "space": Multiple spaces (fixed-width)
                - "pipe": Pipe character |
                - Or any custom string
            has_header: Treat first row as header.

        Returns:
            TableParseResult with parsed tables.
        """
        if not text or not text.strip():
            return TableParseResult(
                success=False,
                tables=[],
                total_tables=0,
                raw_text_length=0,
                error="Empty text provided",
            )

        try:
            # Detect or resolve delimiter
            if delimiter == "auto":
                delimiter, delimiter_name = self._detect_delimiter(text)
            else:
                delimiter, delimiter_name = self._resolve_delimiter(delimiter)

            # Parse tables using the delimiter
            tables = self._parse_with_delimiter(text, delimiter, delimiter_name, has_header)

            # If no tables found with explicit delimiter, try space alignment
            if not tables and delimiter != "space":
                space_tables = self._parse_space_aligned(text, has_header)
                tables.extend(space_tables)

            return TableParseResult(
                success=len(tables) > 0,
                tables=tables,
                total_tables=len(tables),
                raw_text_length=len(text),
            )

        except Exception as e:
            return TableParseResult(
                success=False,
                tables=[],
                total_tables=0,
                raw_text_length=len(text),
                error=f"Parse error: {e}",
            )

    def _detect_delimiter(self, text: str) -> Tuple[str, str]:
        """
        Auto-detect the most likely delimiter in text.

        Returns:
            Tuple of (delimiter_char, delimiter_name)
        """
        lines = text.strip().split("\n")
        if not lines:
            return "\t", "tab"

        # Score each delimiter based on consistency
        scores: Dict[str, float] = {}

        for delim, name in self.DELIMITERS:
            # Count occurrences per line
            counts = [line.count(delim) for line in lines[:10]]  # Sample first 10 lines

            if not counts or max(counts) < 1:
                continue

            # Check consistency (same count per line = higher score)
            non_zero = [c for c in counts if c > 0]
            if non_zero:
                # Score based on consistency and frequency
                consistency = 1.0 - (
                    (max(non_zero) - min(non_zero)) / max(non_zero) if max(non_zero) > 0 else 0
                )
                frequency = sum(non_zero) / len(counts)
                scores[name] = consistency * frequency

        # Return best delimiter
        if scores:
            best = max(scores, key=scores.get)
            for delim, name in self.DELIMITERS:
                if name == best:
                    return delim, name

        # Default to tab
        return "\t", "tab"

    def _resolve_delimiter(self, delimiter: str) -> Tuple[str, str]:
        """
        Resolve delimiter name to actual character.

        Args:
            delimiter: Delimiter name or character.

        Returns:
            Tuple of (delimiter_char, delimiter_name)
        """
        mapping = {
            "tab": ("\t", "tab"),
            "pipe": ("|", "pipe"),
            "comma": (",", "comma"),
            "semicolon": (";", "semicolon"),
            "space": ("  ", "space"),  # Multiple spaces
        }

        if delimiter.lower() in mapping:
            return mapping[delimiter.lower()]

        return delimiter, "custom"

    def _parse_with_delimiter(
        self,
        text: str,
        delimiter: str,
        delimiter_name: str,
        has_header: bool,
    ) -> List[ParsedTable]:
        """
        Parse tables using a specific delimiter.
        """
        tables = []
        lines = text.strip().split("\n")

        current_table_lines: List[str] = []
        expected_columns = 0

        for line in lines:
            line = line.strip()
            if not line:
                # Empty line - potential table boundary
                if len(current_table_lines) >= self.min_rows:
                    table = self._build_table(
                        current_table_lines, delimiter, delimiter_name, has_header
                    )
                    if table:
                        tables.append(table)
                current_table_lines = []
                expected_columns = 0
                continue

            # Skip markdown table dividers
            if re.match(r"^[\|\-\+\: ]+$", line):
                continue

            # Split line by delimiter
            cells = line.split(delimiter)

            # For pipe delimiter, handle leading/trailing pipes
            if delimiter == "|":
                cells = [c.strip() for c in cells if c.strip()]

            if len(cells) >= self.min_columns:
                if expected_columns == 0:
                    expected_columns = len(cells)
                    current_table_lines.append(line)
                elif len(cells) == expected_columns:
                    current_table_lines.append(line)
                else:
                    # Column count mismatch - end current table
                    if len(current_table_lines) >= self.min_rows:
                        table = self._build_table(
                            current_table_lines, delimiter, delimiter_name, has_header
                        )
                        if table:
                            tables.append(table)
                    current_table_lines = [line]
                    expected_columns = len(cells)
            else:
                # Not enough columns - end current table
                if len(current_table_lines) >= self.min_rows:
                    table = self._build_table(
                        current_table_lines, delimiter, delimiter_name, has_header
                    )
                    if table:
                        tables.append(table)
                current_table_lines = []
                expected_columns = 0

        # Handle last table
        if len(current_table_lines) >= self.min_rows:
            table = self._build_table(
                current_table_lines, delimiter, delimiter_name, has_header
            )
            if table:
                tables.append(table)

        return tables

    def _parse_space_aligned(
        self,
        text: str,
        has_header: bool,
    ) -> List[ParsedTable]:
        """
        Parse space-aligned (fixed-width) tables.
        """
        tables = []
        lines = text.strip().split("\n")

        # Find column boundaries by looking for consistent spaces
        column_positions: List[Tuple[int, int]] = []
        current_table_lines: List[str] = []

        for i, line in enumerate(lines):
            if not line.strip():
                if len(current_table_lines) >= self.min_rows and column_positions:
                    table = self._build_fixed_width_table(
                        current_table_lines, column_positions, has_header
                    )
                    if table:
                        tables.append(table)
                current_table_lines = []
                column_positions = []
                continue

            # Detect columns from first non-empty line
            if not column_positions and line.strip():
                column_positions = self._detect_column_positions(line, lines[i : i + 5])

            if column_positions:
                current_table_lines.append(line)

        # Handle last table
        if len(current_table_lines) >= self.min_rows and column_positions:
            table = self._build_fixed_width_table(
                current_table_lines, column_positions, has_header
            )
            if table:
                tables.append(table)

        return tables

    def _detect_column_positions(
        self,
        first_line: str,
        sample_lines: List[str],
    ) -> List[Tuple[int, int]]:
        """
        Detect column positions from space-aligned text.

        Returns list of (start, end) positions for each column.
        """
        if len(sample_lines) < 2:
            return []

        # Find positions where all sample lines have spaces
        line_length = max(len(line) for line in sample_lines)

        # Track where text exists across all lines
        text_positions = [False] * line_length
        for line in sample_lines:
            for i, char in enumerate(line):
                if char != " ":
                    text_positions[i] = True

        # Find gaps (consecutive False values = column separator)
        columns = []
        in_text = False
        start = 0

        for i, has_text in enumerate(text_positions):
            if has_text and not in_text:
                start = i
                in_text = True
            elif not has_text and in_text:
                columns.append((start, i))
                in_text = False

        if in_text:
            columns.append((start, line_length))

        if len(columns) >= self.min_columns:
            return columns
        return []

    def _build_table(
        self,
        lines: List[str],
        delimiter: str,
        delimiter_name: str,
        has_header: bool,
    ) -> Optional[ParsedTable]:
        """
        Build a ParsedTable from lines.
        """
        if not lines:
            return None

        rows = []
        for line in lines:
            cells = line.split(delimiter)
            if delimiter == "|":
                cells = [c.strip() for c in cells if c.strip() or len(cells) > 2]

            if self.strip_cells:
                cells = [c.strip() for c in cells]
            rows.append(cells)

        if not rows or len(rows[0]) < self.min_columns:
            return None

        headers = []
        data_rows = rows

        if has_header and rows:
            headers = rows[0]
            data_rows = rows[1:]

        if len(data_rows) < self.min_rows - (1 if has_header else 0):
            return None

        # Calculate confidence based on column consistency
        col_counts = [len(row) for row in data_rows]
        if col_counts:
            consistency = col_counts.count(col_counts[0]) / len(col_counts)
        else:
            consistency = 0.5

        return ParsedTable(
            headers=headers,
            rows=data_rows,
            row_count=len(data_rows),
            column_count=len(headers) if headers else (len(data_rows[0]) if data_rows else 0),
            delimiter_detected=delimiter_name,
            confidence=round(consistency, 2),
        )

    def _build_fixed_width_table(
        self,
        lines: List[str],
        column_positions: List[Tuple[int, int]],
        has_header: bool,
    ) -> Optional[ParsedTable]:
        """
        Build a ParsedTable from fixed-width lines.
        """
        rows = []
        for line in lines:
            cells = []
            for start, end in column_positions:
                cell = line[start:end] if len(line) > start else ""
                if self.strip_cells:
                    cell = cell.strip()
                cells.append(cell)
            rows.append(cells)

        if not rows:
            return None

        headers = []
        data_rows = rows

        if has_header and rows:
            headers = rows[0]
            data_rows = rows[1:]

        return ParsedTable(
            headers=headers,
            rows=data_rows,
            row_count=len(data_rows),
            column_count=len(column_positions),
            delimiter_detected="space",
            confidence=0.7,  # Fixed-width parsing is less reliable
        )

    def parse_markdown_table(self, text: str) -> Optional[ParsedTable]:
        """
        Parse a Markdown-formatted table.

        Args:
            text: Text containing a Markdown table.

        Returns:
            ParsedTable or None if no valid table found.
        """
        lines = [line.strip() for line in text.strip().split("\n") if line.strip()]

        if len(lines) < 2:
            return None

        # Find header row and separator
        header_idx = -1
        for i, line in enumerate(lines):
            if "|" in line and i + 1 < len(lines):
                next_line = lines[i + 1]
                if re.match(r"^[\|\-\:\s]+$", next_line):
                    header_idx = i
                    break

        if header_idx < 0:
            return None

        # Parse header
        header_line = lines[header_idx]
        headers = [h.strip() for h in header_line.split("|") if h.strip()]

        # Parse data rows (skip separator line)
        data_rows = []
        for line in lines[header_idx + 2 :]:
            if "|" in line:
                cells = [c.strip() for c in line.split("|") if c.strip()]
                if len(cells) == len(headers):
                    data_rows.append(cells)

        if not data_rows:
            return None

        return ParsedTable(
            headers=headers,
            rows=data_rows,
            row_count=len(data_rows),
            column_count=len(headers),
            delimiter_detected="pipe",
            confidence=0.95,  # Markdown tables are well-structured
        )
