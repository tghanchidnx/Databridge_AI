"""
Data Loader for DataBridge AI Librarian.

Provides unified loading from CSV, JSON, Excel, and SQL sources.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Union

import pandas as pd
from sqlalchemy import create_engine, text


@dataclass
class LoadResult:
    """Result of a data load operation."""

    success: bool
    data: Optional[pd.DataFrame] = None
    rows_loaded: int = 0
    columns: List[str] = field(default_factory=list)
    source_type: str = ""
    source_path: str = ""
    load_time_ms: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    schema: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "rows_loaded": self.rows_loaded,
            "columns": self.columns,
            "source_type": self.source_type,
            "source_path": self.source_path,
            "load_time_ms": self.load_time_ms,
            "errors": self.errors,
            "warnings": self.warnings,
            "schema": self.schema,
        }


class DataLoader:
    """
    Unified data loader for multiple source types.

    Supports:
    - CSV files (with delimiter detection)
    - JSON files (records, lines, or nested)
    - Excel files (.xlsx, .xls)
    - SQL queries (via SQLAlchemy)
    """

    def __init__(
        self,
        encoding: str = "utf-8",
        max_rows: Optional[int] = None,
        sample_size: int = 1000,
    ):
        """
        Initialize the data loader.

        Args:
            encoding: Default encoding for text files.
            max_rows: Maximum rows to load (None for unlimited).
            sample_size: Number of rows to sample for schema detection.
        """
        self.encoding = encoding
        self.max_rows = max_rows
        self.sample_size = sample_size

    def load_csv(
        self,
        path: Union[str, Path],
        delimiter: Optional[str] = None,
        has_header: bool = True,
        skip_rows: int = 0,
        columns: Optional[List[str]] = None,
        dtype: Optional[Dict[str, str]] = None,
    ) -> LoadResult:
        """
        Load data from a CSV file.

        Args:
            path: Path to CSV file.
            delimiter: Column delimiter (auto-detected if None).
            has_header: Whether file has a header row.
            skip_rows: Number of rows to skip at start.
            columns: Specific columns to load (None for all).
            dtype: Column data types to enforce.

        Returns:
            LoadResult with loaded DataFrame.
        """
        start_time = datetime.now()
        path = Path(path)
        errors = []
        warnings = []

        if not path.exists():
            return LoadResult(
                success=False,
                errors=[f"File not found: {path}"],
                source_type="csv",
                source_path=str(path),
            )

        try:
            # Auto-detect delimiter if not provided
            if delimiter is None:
                delimiter = self._detect_delimiter(path)
                if delimiter != ",":
                    warnings.append(f"Auto-detected delimiter: {repr(delimiter)}")

            # Load CSV
            df = pd.read_csv(
                path,
                delimiter=delimiter,
                header=0 if has_header else None,
                skiprows=skip_rows,
                usecols=columns,
                dtype=dtype,
                nrows=self.max_rows,
                encoding=self.encoding,
                on_bad_lines="warn",
            )

            # Generate schema
            schema = self._infer_schema(df)

            load_time = int((datetime.now() - start_time).total_seconds() * 1000)

            return LoadResult(
                success=True,
                data=df,
                rows_loaded=len(df),
                columns=list(df.columns),
                source_type="csv",
                source_path=str(path),
                load_time_ms=load_time,
                warnings=warnings,
                schema=schema,
            )

        except Exception as e:
            return LoadResult(
                success=False,
                errors=[str(e)],
                source_type="csv",
                source_path=str(path),
            )

    def load_json(
        self,
        path: Union[str, Path],
        json_format: str = "auto",
        record_path: Optional[str] = None,
    ) -> LoadResult:
        """
        Load data from a JSON file.

        Args:
            path: Path to JSON file.
            json_format: Format type ('records', 'lines', 'nested', or 'auto').
            record_path: JSON path to records for nested format (e.g., 'data.items').

        Returns:
            LoadResult with loaded DataFrame.
        """
        start_time = datetime.now()
        path = Path(path)
        warnings = []

        if not path.exists():
            return LoadResult(
                success=False,
                errors=[f"File not found: {path}"],
                source_type="json",
                source_path=str(path),
            )

        try:
            # Read raw content
            with open(path, "r", encoding=self.encoding) as f:
                content = f.read()

            # Auto-detect format
            if json_format == "auto":
                json_format = self._detect_json_format(content)
                warnings.append(f"Auto-detected JSON format: {json_format}")

            # Parse based on format
            if json_format == "lines":
                df = pd.read_json(path, lines=True, nrows=self.max_rows)
            elif json_format == "records":
                data = json.loads(content)
                if isinstance(data, list):
                    df = pd.DataFrame(data)
                else:
                    df = pd.DataFrame([data])
            elif json_format == "nested" and record_path:
                data = json.loads(content)
                # Navigate to the record path
                for key in record_path.split("."):
                    data = data[key]
                df = pd.DataFrame(data)
            else:
                df = pd.read_json(path)

            # Apply row limit
            if self.max_rows and len(df) > self.max_rows:
                df = df.head(self.max_rows)
                warnings.append(f"Truncated to {self.max_rows} rows")

            schema = self._infer_schema(df)
            load_time = int((datetime.now() - start_time).total_seconds() * 1000)

            return LoadResult(
                success=True,
                data=df,
                rows_loaded=len(df),
                columns=list(df.columns),
                source_type="json",
                source_path=str(path),
                load_time_ms=load_time,
                warnings=warnings,
                schema=schema,
            )

        except Exception as e:
            return LoadResult(
                success=False,
                errors=[str(e)],
                source_type="json",
                source_path=str(path),
            )

    def load_excel(
        self,
        path: Union[str, Path],
        sheet_name: Union[str, int] = 0,
        has_header: bool = True,
        skip_rows: int = 0,
        columns: Optional[List[str]] = None,
    ) -> LoadResult:
        """
        Load data from an Excel file.

        Args:
            path: Path to Excel file (.xlsx or .xls).
            sheet_name: Sheet name or index (0-based).
            has_header: Whether sheet has a header row.
            skip_rows: Number of rows to skip at start.
            columns: Specific columns to load.

        Returns:
            LoadResult with loaded DataFrame.
        """
        start_time = datetime.now()
        path = Path(path)

        if not path.exists():
            return LoadResult(
                success=False,
                errors=[f"File not found: {path}"],
                source_type="excel",
                source_path=str(path),
            )

        try:
            df = pd.read_excel(
                path,
                sheet_name=sheet_name,
                header=0 if has_header else None,
                skiprows=skip_rows,
                usecols=columns,
                nrows=self.max_rows,
            )

            schema = self._infer_schema(df)
            load_time = int((datetime.now() - start_time).total_seconds() * 1000)

            return LoadResult(
                success=True,
                data=df,
                rows_loaded=len(df),
                columns=list(df.columns),
                source_type="excel",
                source_path=str(path),
                load_time_ms=load_time,
                schema=schema,
            )

        except Exception as e:
            return LoadResult(
                success=False,
                errors=[str(e)],
                source_type="excel",
                source_path=str(path),
            )

    def load_sql(
        self,
        query: str,
        connection_string: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> LoadResult:
        """
        Load data from a SQL query.

        Args:
            query: SQL query to execute.
            connection_string: SQLAlchemy connection string.
            params: Query parameters for parameterized queries.

        Returns:
            LoadResult with loaded DataFrame.
        """
        start_time = datetime.now()

        try:
            engine = create_engine(connection_string)

            # Apply row limit if set
            if self.max_rows:
                # Try to add LIMIT clause if not present
                query_upper = query.upper().strip()
                if "LIMIT" not in query_upper and not query_upper.endswith(";"):
                    query = f"{query} LIMIT {self.max_rows}"

            with engine.connect() as conn:
                if params:
                    df = pd.read_sql(text(query), conn, params=params)
                else:
                    df = pd.read_sql(text(query), conn)

            schema = self._infer_schema(df)
            load_time = int((datetime.now() - start_time).total_seconds() * 1000)

            return LoadResult(
                success=True,
                data=df,
                rows_loaded=len(df),
                columns=list(df.columns),
                source_type="sql",
                source_path=connection_string.split("@")[-1] if "@" in connection_string else "database",
                load_time_ms=load_time,
                schema=schema,
            )

        except Exception as e:
            return LoadResult(
                success=False,
                errors=[str(e)],
                source_type="sql",
                source_path="database",
            )

    def load_from_string(
        self,
        content: str,
        format_type: str = "csv",
        delimiter: str = ",",
    ) -> LoadResult:
        """
        Load data from a string.

        Args:
            content: String content to load.
            format_type: Format type ('csv' or 'json').
            delimiter: Delimiter for CSV format.

        Returns:
            LoadResult with loaded DataFrame.
        """
        start_time = datetime.now()

        try:
            if format_type == "csv":
                from io import StringIO
                df = pd.read_csv(
                    StringIO(content),
                    delimiter=delimiter,
                    nrows=self.max_rows,
                )
            elif format_type == "json":
                data = json.loads(content)
                if isinstance(data, list):
                    df = pd.DataFrame(data)
                else:
                    df = pd.DataFrame([data])
            else:
                return LoadResult(
                    success=False,
                    errors=[f"Unsupported format: {format_type}"],
                    source_type=format_type,
                    source_path="string",
                )

            schema = self._infer_schema(df)
            load_time = int((datetime.now() - start_time).total_seconds() * 1000)

            return LoadResult(
                success=True,
                data=df,
                rows_loaded=len(df),
                columns=list(df.columns),
                source_type=format_type,
                source_path="string",
                load_time_ms=load_time,
                schema=schema,
            )

        except Exception as e:
            return LoadResult(
                success=False,
                errors=[str(e)],
                source_type=format_type,
                source_path="string",
            )

    def _detect_delimiter(self, path: Path) -> str:
        """Detect the delimiter in a CSV file."""
        import csv

        with open(path, "r", encoding=self.encoding) as f:
            sample = f.read(8192)

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
            return dialect.delimiter
        except csv.Error:
            return ","

    def _detect_json_format(self, content: str) -> str:
        """Detect the format of JSON content."""
        content = content.strip()

        # Check if it's JSON lines (multiple JSON objects, one per line)
        if "\n" in content:
            first_line = content.split("\n")[0].strip()
            if first_line.startswith("{") and first_line.endswith("}"):
                return "lines"

        # Check if it's an array of records
        if content.startswith("["):
            return "records"

        # Otherwise assume nested
        return "nested"

    def _infer_schema(self, df: pd.DataFrame) -> Dict[str, str]:
        """Infer schema from DataFrame."""
        schema = {}
        for col in df.columns:
            dtype = str(df[col].dtype)
            if dtype.startswith("int"):
                schema[col] = "integer"
            elif dtype.startswith("float"):
                schema[col] = "decimal"
            elif dtype == "bool":
                schema[col] = "boolean"
            elif dtype.startswith("datetime"):
                schema[col] = "datetime"
            else:
                schema[col] = "string"
        return schema
