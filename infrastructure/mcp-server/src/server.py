"""DataBridge AI V2 - MCP Server Implementation.

A headless, MCP-native data reconciliation engine that bridges messy sources
(OCR/PDF/SQL) with a structured Python-based comparison pipeline.

V2 Version - Runs independently on port 3002 backend.
"""
from fastmcp import FastMCP
import pandas as pd
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional
import re

# Conditional imports for optional dependencies
try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False

try:
    from sqlalchemy import create_engine, text
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

try:
    import pytesseract
    from PIL import Image
    TESSERACT_AVAILABLE = True
except ImportError:
    TESSERACT_AVAILABLE = False

try:
    from pypdf import PdfReader
    PYPDF_AVAILABLE = True
except ImportError:
    PYPDF_AVAILABLE = False

# Handle imports for both module and direct execution
try:
    from src.config import settings
except ImportError:
    from config import settings

# Initialize the V2 Server
mcp = FastMCP(
    "DataBridgeAI-V2",
    instructions="""Professional Data Reconciliation & Hierarchy Management Agent (V2).

I help you with two major capabilities:

**Data Reconciliation:**
- Compare and validate data from CSV, SQL, PDF, and JSON sources
- Find orphans, conflicts, and fuzzy matches between datasets
- Profile data quality and detect schema drift

**Hierarchy Builder:**
- Create and manage multi-level hierarchy projects (up to 15 levels)
- Define source mappings linking database columns to hierarchy nodes
- Build calculation formulas (SUM, SUBTRACT, MULTIPLY, DIVIDE)
- Export hierarchies to CSV/JSON and generate deployment scripts
- Deploy hierarchies to Snowflake and other databases

Use me for data quality checks, building financial hierarchies, and managing complex data structures."""
)

# Ensure data directory exists
Path(settings.data_dir).mkdir(parents=True, exist_ok=True)

# Initialize audit log with headers if it doesn't exist
if not Path(settings.audit_log).exists():
    with open(settings.audit_log, "w") as f:
        f.write("timestamp,user,action,impact\n")

# Initialize workflow file if it doesn't exist
if not Path(settings.workflow_file).exists():
    with open(settings.workflow_file, "w") as f:
        json.dump({"version": "1.0", "steps": []}, f)


# =============================================================================
# Internal Helpers
# =============================================================================

def log_action(user: str, action: str, impact: str) -> None:
    """Record an action to the audit trail (no PII)."""
    timestamp = datetime.now().isoformat()
    # Sanitize to prevent CSV injection
    action = action.replace(",", ";").replace("\n", " ")[:100]
    impact = impact.replace(",", ";").replace("\n", " ")[:200]
    log_entry = f"{timestamp},{user},{action},{impact}\n"
    with open(settings.audit_log, "a") as f:
        f.write(log_entry)


def compute_row_hash(row: pd.Series, columns: list) -> str:
    """Compute a deterministic SHA-256 hash for a row (truncated to 16 chars)."""
    values = "|".join(str(row[col]) for col in columns)
    return hashlib.sha256(values.encode()).hexdigest()[:16]


def truncate_dataframe(df: pd.DataFrame, max_rows: int = None) -> pd.DataFrame:
    """Truncate DataFrame to respect context sensitivity rules."""
    max_rows = max_rows or settings.max_rows_display
    return df.head(max_rows)


# =============================================================================
# Phase 1: Data Loading Tools
# =============================================================================

@mcp.tool()
def load_csv(file_path: str, preview_rows: int = 5) -> str:
    """
    Load a CSV file and return a preview with schema information.

    Args:
        file_path: Path to the CSV file.
        preview_rows: Number of rows to preview (max 10).

    Returns:
        JSON with schema info and sample data.
    """
    try:
        df = pd.read_csv(file_path)
        preview_rows = min(preview_rows, settings.max_rows_display)

        result = {
            "file": file_path,
            "rows": len(df),
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "preview": df.head(preview_rows).to_dict(orient="records"),
            "null_counts": df.isnull().sum().to_dict()
        }

        log_action("AI_AGENT", "load_csv", f"Loaded {len(df)} rows from {file_path}")
        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def load_json(file_path: str, preview_rows: int = 5) -> str:
    """
    Load a JSON file (array or object) and return a preview.

    Args:
        file_path: Path to the JSON file.
        preview_rows: Number of rows to preview (max 10).

    Returns:
        JSON with schema info and sample data.
    """
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        # Handle both array and object formats
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            # Check if it's a records-style dict or nested
            if all(isinstance(v, list) for v in data.values()):
                df = pd.DataFrame(data)
            else:
                df = pd.DataFrame([data])
        else:
            return json.dumps({"error": "Unsupported JSON structure"})

        preview_rows = min(preview_rows, settings.max_rows_display)

        result = {
            "file": file_path,
            "rows": len(df),
            "columns": list(df.columns),
            "preview": df.head(preview_rows).to_dict(orient="records")
        }

        log_action("AI_AGENT", "load_json", f"Loaded {len(df)} records from {file_path}")
        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def query_database(connection_string: str, query: str, preview_rows: int = 10) -> str:
    """
    Execute a SQL query and return results.

    Args:
        connection_string: SQLAlchemy connection string (e.g., 'sqlite:///data.db').
        query: SQL SELECT query to execute.
        preview_rows: Maximum rows to return (max 10).

    Returns:
        JSON with query results and metadata.
    """
    if not SQLALCHEMY_AVAILABLE:
        return json.dumps({"error": "SQLAlchemy not installed. Run: pip install sqlalchemy"})

    # Security: Only allow SELECT queries
    if not query.strip().upper().startswith("SELECT"):
        return json.dumps({"error": "Only SELECT queries are allowed"})

    try:
        engine = create_engine(connection_string)
        df = pd.read_sql(query, engine)
        preview_rows = min(preview_rows, settings.max_rows_display)

        result = {
            "query": query,
            "rows_returned": len(df),
            "columns": list(df.columns),
            "preview": df.head(preview_rows).to_dict(orient="records"),
            "truncated": len(df) > preview_rows
        }

        log_action("AI_AGENT", "query_database", f"Query returned {len(df)} rows")
        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        return json.dumps({"error": str(e)})


# =============================================================================
# Phase 2: Data Profiling Tools
# =============================================================================

@mcp.tool()
def profile_data(source_path: str) -> str:
    """
    Analyze data structure and quality. Identifies table type and anomalies.

    Args:
        source_path: Path to CSV file to profile.

    Returns:
        JSON with profiling statistics including structure type, cardinality, and data quality metrics.
    """
    try:
        df = pd.read_csv(source_path)

        cardinality = df.nunique() / len(df)

        # Determine structure type
        has_date = any(col.lower() in ["date", "datetime", "timestamp", "created_at", "updated_at"]
                      for col in df.columns)
        is_fact = "Transactional/Fact" if len(df) > 1000 and has_date else "Dimension/Reference"

        # Detect potential key columns
        potential_keys = list(cardinality[cardinality > 0.99].index)

        # Data quality checks
        null_pct = (df.isnull().sum() / len(df) * 100).round(2).to_dict()
        duplicate_rows = df.duplicated().sum()

        summary = {
            "file": source_path,
            "rows": len(df),
            "columns": len(df.columns),
            "structure_type": is_fact,
            "column_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "potential_key_columns": potential_keys,
            "high_cardinality_cols": list(cardinality[cardinality > 0.9].index),
            "low_cardinality_cols": list(cardinality[cardinality < 0.1].index),
            "data_quality": {
                "null_percentage": null_pct,
                "duplicate_rows": duplicate_rows,
                "duplicate_percentage": round(duplicate_rows / len(df) * 100, 2)
            },
            "statistics": json.loads(df.describe(include="all").to_json())
        }

        log_action("AI_AGENT", "profile_data", f"Profiled {source_path}")
        return json.dumps(summary, indent=2, default=str)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def detect_schema_drift(source_a_path: str, source_b_path: str) -> str:
    """
    Compare schemas between two CSV files to detect drift.

    Args:
        source_a_path: Path to first CSV (baseline).
        source_b_path: Path to second CSV (target).

    Returns:
        JSON with schema differences including added, removed, and type-changed columns.
    """
    try:
        df_a = pd.read_csv(source_a_path)
        df_b = pd.read_csv(source_b_path)

        cols_a = set(df_a.columns)
        cols_b = set(df_b.columns)

        types_a = {col: str(dtype) for col, dtype in df_a.dtypes.items()}
        types_b = {col: str(dtype) for col, dtype in df_b.dtypes.items()}

        # Find type changes in common columns
        common_cols = cols_a & cols_b
        type_changes = {
            col: {"from": types_a[col], "to": types_b[col]}
            for col in common_cols
            if types_a[col] != types_b[col]
        }

        result = {
            "source_a": source_a_path,
            "source_b": source_b_path,
            "columns_added": list(cols_b - cols_a),
            "columns_removed": list(cols_a - cols_b),
            "columns_common": list(common_cols),
            "type_changes": type_changes,
            "has_drift": bool((cols_b - cols_a) or (cols_a - cols_b) or type_changes)
        }

        log_action("AI_AGENT", "detect_schema_drift", f"Compared schemas")
        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


# =============================================================================
# Phase 3: Hashing & Comparison Engine
# =============================================================================

@mcp.tool()
def compare_hashes(
    source_a_path: str,
    source_b_path: str,
    key_columns: str,
    compare_columns: str = ""
) -> str:
    """
    Compare two CSV sources by hashing rows to identify orphans and conflicts.

    Args:
        source_a_path: Path to the first CSV file (source of truth).
        source_b_path: Path to the second CSV file (target).
        key_columns: Comma-separated column names that uniquely identify a row.
        compare_columns: Optional comma-separated columns to check for conflicts. Defaults to all non-key columns.

    Returns:
        JSON statistical summary with orphan and conflict counts (no raw data).
    """
    try:
        df_a = pd.read_csv(source_a_path)
        df_b = pd.read_csv(source_b_path)

        keys = [k.strip() for k in key_columns.split(",")]

        if compare_columns:
            compare_cols = [c.strip() for c in compare_columns.split(",")]
        else:
            compare_cols = [c for c in df_a.columns if c not in keys]

        # Validate columns
        for col in keys + compare_cols:
            if col not in df_a.columns:
                return json.dumps({"error": f"Column '{col}' not found in source A"})
            if col not in df_b.columns:
                return json.dumps({"error": f"Column '{col}' not found in source B"})

        # Create composite keys
        df_a["_composite_key"] = df_a[keys].astype(str).agg("|".join, axis=1)
        df_b["_composite_key"] = df_b[keys].astype(str).agg("|".join, axis=1)

        # Compute value hashes
        df_a["_value_hash"] = df_a.apply(lambda row: compute_row_hash(row, compare_cols), axis=1)
        df_b["_value_hash"] = df_b.apply(lambda row: compute_row_hash(row, compare_cols), axis=1)

        keys_a = set(df_a["_composite_key"])
        keys_b = set(df_b["_composite_key"])

        orphans_in_a = keys_a - keys_b
        orphans_in_b = keys_b - keys_a
        common_keys = keys_a & keys_b

        hash_map_a = df_a.set_index("_composite_key")["_value_hash"].to_dict()
        hash_map_b = df_b.set_index("_composite_key")["_value_hash"].to_dict()

        conflicts = [k for k in common_keys if hash_map_a[k] != hash_map_b[k]]
        matches = [k for k in common_keys if hash_map_a[k] == hash_map_b[k]]

        summary = {
            "source_a": {"path": source_a_path, "total_rows": len(df_a)},
            "source_b": {"path": source_b_path, "total_rows": len(df_b)},
            "key_columns": keys,
            "compare_columns": compare_cols,
            "statistics": {
                "orphans_only_in_source_a": len(orphans_in_a),
                "orphans_only_in_source_b": len(orphans_in_b),
                "total_orphans": len(orphans_in_a) + len(orphans_in_b),
                "conflicts": len(conflicts),
                "exact_matches": len(matches),
                "match_rate_percent": round(len(matches) / max(len(common_keys), 1) * 100, 2)
            }
        }

        log_action("AI_AGENT", "compare_hashes", f"Compared {len(df_a)} vs {len(df_b)} rows")
        return json.dumps(summary, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_orphan_details(
    source_a_path: str,
    source_b_path: str,
    key_columns: str,
    orphan_source: str = "both",
    limit: int = 10
) -> str:
    """
    Retrieve details of orphan records (records in one source but not the other).

    Args:
        source_a_path: Path to the first CSV file.
        source_b_path: Path to the second CSV file.
        key_columns: Comma-separated column names that uniquely identify a row.
        orphan_source: Which orphans to return: 'a', 'b', or 'both'.
        limit: Maximum orphans to return per source (max 10).

    Returns:
        JSON with orphan record details (limited to context sensitivity rules).
    """
    try:
        df_a = pd.read_csv(source_a_path)
        df_b = pd.read_csv(source_b_path)
        keys = [k.strip() for k in key_columns.split(",")]
        limit = min(limit, settings.max_rows_display)

        df_a["_composite_key"] = df_a[keys].astype(str).agg("|".join, axis=1)
        df_b["_composite_key"] = df_b[keys].astype(str).agg("|".join, axis=1)

        keys_a = set(df_a["_composite_key"])
        keys_b = set(df_b["_composite_key"])

        result = {"orphan_source": orphan_source}

        if orphan_source in ["a", "both"]:
            orphans_a = df_a[df_a["_composite_key"].isin(keys_a - keys_b)]
            orphans_a = orphans_a.drop(columns=["_composite_key"])
            result["orphans_in_a"] = {
                "total": len(orphans_a),
                "sample": orphans_a.head(limit).to_dict(orient="records")
            }

        if orphan_source in ["b", "both"]:
            orphans_b = df_b[df_b["_composite_key"].isin(keys_b - keys_a)]
            orphans_b = orphans_b.drop(columns=["_composite_key"])
            result["orphans_in_b"] = {
                "total": len(orphans_b),
                "sample": orphans_b.head(limit).to_dict(orient="records")
            }

        log_action("AI_AGENT", "get_orphan_details", f"Retrieved orphan details")
        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_conflict_details(
    source_a_path: str,
    source_b_path: str,
    key_columns: str,
    compare_columns: str = "",
    limit: int = 10
) -> str:
    """
    Retrieve details of conflicting records (same key, different values).

    Args:
        source_a_path: Path to the first CSV file.
        source_b_path: Path to the second CSV file.
        key_columns: Comma-separated column names that uniquely identify a row.
        compare_columns: Optional columns to compare. Defaults to all non-key columns.
        limit: Maximum conflicts to return (max 10).

    Returns:
        JSON with conflict details showing both versions side-by-side.
    """
    try:
        df_a = pd.read_csv(source_a_path)
        df_b = pd.read_csv(source_b_path)
        keys = [k.strip() for k in key_columns.split(",")]
        limit = min(limit, settings.max_rows_display)

        if compare_columns:
            compare_cols = [c.strip() for c in compare_columns.split(",")]
        else:
            compare_cols = [c for c in df_a.columns if c not in keys]

        df_a["_composite_key"] = df_a[keys].astype(str).agg("|".join, axis=1)
        df_b["_composite_key"] = df_b[keys].astype(str).agg("|".join, axis=1)

        df_a["_value_hash"] = df_a.apply(lambda row: compute_row_hash(row, compare_cols), axis=1)
        df_b["_value_hash"] = df_b.apply(lambda row: compute_row_hash(row, compare_cols), axis=1)

        hash_map_a = df_a.set_index("_composite_key")["_value_hash"].to_dict()
        hash_map_b = df_b.set_index("_composite_key")["_value_hash"].to_dict()

        common_keys = set(df_a["_composite_key"]) & set(df_b["_composite_key"])
        conflict_keys = [k for k in common_keys if hash_map_a.get(k) != hash_map_b.get(k)]

        conflicts = []
        for key in list(conflict_keys)[:limit]:
            row_a = df_a[df_a["_composite_key"] == key].iloc[0]
            row_b = df_b[df_b["_composite_key"] == key].iloc[0]

            diff_cols = []
            for col in compare_cols:
                if str(row_a[col]) != str(row_b[col]):
                    diff_cols.append({
                        "column": col,
                        "value_a": row_a[col],
                        "value_b": row_b[col]
                    })

            conflicts.append({
                "key": {k: row_a[k] for k in keys},
                "differences": diff_cols
            })

        result = {
            "total_conflicts": len(conflict_keys),
            "showing": len(conflicts),
            "conflicts": conflicts
        }

        log_action("AI_AGENT", "get_conflict_details", f"Retrieved {len(conflicts)} conflict details")
        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        return json.dumps({"error": str(e)})


# =============================================================================
# Phase 4: Fuzzy Matching Tools
# =============================================================================

@mcp.tool()
def fuzzy_match_columns(
    source_a_path: str,
    source_b_path: str,
    column_a: str,
    column_b: str,
    threshold: int = 80,
    limit: int = 10
) -> str:
    """
    Find fuzzy matches between two columns using RapidFuzz.

    Args:
        source_a_path: Path to the first CSV file.
        source_b_path: Path to the second CSV file.
        column_a: Column name in source A to match.
        column_b: Column name in source B to match against.
        threshold: Minimum similarity score (0-100). Default 80.
        limit: Maximum matches to return (max 10).

    Returns:
        JSON with fuzzy match results including similarity scores.
    """
    if not RAPIDFUZZ_AVAILABLE:
        return json.dumps({"error": "RapidFuzz not installed. Run: pip install rapidfuzz"})

    try:
        df_a = pd.read_csv(source_a_path)
        df_b = pd.read_csv(source_b_path)
        limit = min(limit, settings.max_rows_display)

        values_a = df_a[column_a].astype(str).unique().tolist()
        values_b = df_b[column_b].astype(str).unique().tolist()

        matches = []
        for val_a in values_a[:50]:  # Limit source values to prevent timeout
            result = process.extractOne(val_a, values_b, scorer=fuzz.ratio)
            if result and result[1] >= threshold:
                matches.append({
                    "value_a": val_a,
                    "value_b": result[0],
                    "similarity": result[1]
                })

        # Sort by similarity descending
        matches.sort(key=lambda x: x["similarity"], reverse=True)

        result = {
            "column_a": column_a,
            "column_b": column_b,
            "threshold": threshold,
            "total_matches": len(matches),
            "top_matches": matches[:limit]
        }

        log_action("AI_AGENT", "fuzzy_match_columns", f"Found {len(matches)} fuzzy matches")
        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def fuzzy_deduplicate(
    source_path: str,
    column: str,
    threshold: int = 90,
    limit: int = 10
) -> str:
    """
    Find potential duplicate values within a single column using fuzzy matching.

    Args:
        source_path: Path to the CSV file.
        column: Column name to check for duplicates.
        threshold: Minimum similarity score (0-100). Default 90.
        limit: Maximum duplicate groups to return (max 10).

    Returns:
        JSON with potential duplicate groups.
    """
    if not RAPIDFUZZ_AVAILABLE:
        return json.dumps({"error": "RapidFuzz not installed. Run: pip install rapidfuzz"})

    try:
        df = pd.read_csv(source_path)
        limit = min(limit, settings.max_rows_display)

        values = df[column].astype(str).unique().tolist()
        processed = set()
        duplicate_groups = []

        for i, val in enumerate(values):
            if val in processed:
                continue

            # Find similar values
            similar = []
            for other_val in values[i+1:]:
                if other_val in processed:
                    continue
                score = fuzz.ratio(val, other_val)
                if score >= threshold:
                    similar.append({"value": other_val, "similarity": score})
                    processed.add(other_val)

            if similar:
                duplicate_groups.append({
                    "primary": val,
                    "similar_values": similar
                })
                processed.add(val)

        result = {
            "column": column,
            "threshold": threshold,
            "total_groups": len(duplicate_groups),
            "duplicate_groups": duplicate_groups[:limit]
        }

        log_action("AI_AGENT", "fuzzy_deduplicate", f"Found {len(duplicate_groups)} duplicate groups")
        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


# =============================================================================
# Phase 5: PDF/OCR Tools
# =============================================================================

@mcp.tool()
def extract_text_from_pdf(file_path: str, pages: str = "all") -> str:
    """
    Extract text content from a PDF file.

    Args:
        file_path: Path to the PDF file.
        pages: Page numbers to extract ('all', or '1,2,3', or '1-5').

    Returns:
        JSON with extracted text per page.
    """
    if not PYPDF_AVAILABLE:
        return json.dumps({"error": "pypdf not installed. Run: pip install pypdf"})

    try:
        reader = PdfReader(file_path)
        total_pages = len(reader.pages)

        # Parse page selection
        if pages == "all":
            page_nums = list(range(total_pages))
        elif "-" in pages:
            start, end = map(int, pages.split("-"))
            page_nums = list(range(start - 1, min(end, total_pages)))
        else:
            page_nums = [int(p.strip()) - 1 for p in pages.split(",")]

        extracted = []
        for page_num in page_nums:
            if 0 <= page_num < total_pages:
                text = reader.pages[page_num].extract_text() or ""
                extracted.append({
                    "page": page_num + 1,
                    "text": text[:2000]  # Limit text per page
                })

        result = {
            "file": file_path,
            "total_pages": total_pages,
            "pages_extracted": len(extracted),
            "content": extracted
        }

        log_action("AI_AGENT", "extract_text_from_pdf", f"Extracted {len(extracted)} pages")
        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def ocr_image(file_path: str, language: str = "eng") -> str:
    """
    Extract text from an image using OCR (Tesseract).

    Args:
        file_path: Path to the image file (PNG, JPG, etc.).
        language: Tesseract language code (default 'eng').

    Returns:
        JSON with extracted text.
    """
    if not TESSERACT_AVAILABLE:
        return json.dumps({"error": "pytesseract/Pillow not installed. Run: pip install pytesseract Pillow"})

    try:
        # Configure tesseract path if set
        if settings.tesseract_path:
            pytesseract.pytesseract.tesseract_cmd = settings.tesseract_path

        image = Image.open(file_path)
        text = pytesseract.image_to_string(image, lang=language)

        result = {
            "file": file_path,
            "language": language,
            "text": text[:5000],  # Limit output
            "character_count": len(text)
        }

        log_action("AI_AGENT", "ocr_image", f"OCR extracted {len(text)} chars")
        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def parse_table_from_text(text: str, delimiter: str = "auto") -> str:
    """
    Attempt to parse tabular data from extracted text.

    Args:
        text: Raw text containing tabular data.
        delimiter: Column delimiter ('auto', 'tab', 'space', 'pipe', or custom).

    Returns:
        JSON with parsed table data.
    """
    try:
        lines = [line.strip() for line in text.strip().split("\n") if line.strip()]

        if not lines:
            return json.dumps({"error": "No text content to parse"})

        # Auto-detect delimiter
        if delimiter == "auto":
            first_line = lines[0]
            if "\t" in first_line:
                delimiter = "\t"
            elif "|" in first_line:
                delimiter = "|"
            elif "  " in first_line:
                delimiter = r"\s{2,}"  # Multiple spaces
            else:
                delimiter = r"\s+"
        elif delimiter == "tab":
            delimiter = "\t"
        elif delimiter == "space":
            delimiter = r"\s+"
        elif delimiter == "pipe":
            delimiter = "|"

        # Parse rows
        rows = []
        for line in lines:
            if delimiter in ["\t", "|"]:
                cells = [c.strip() for c in line.split(delimiter)]
            else:
                cells = [c.strip() for c in re.split(delimiter, line)]
            rows.append(cells)

        # Assume first row is header
        if len(rows) > 1:
            headers = rows[0]
            data = rows[1:]

            # Convert to records
            records = []
            for row in data[:settings.max_rows_display]:
                record = {}
                for i, val in enumerate(row):
                    col_name = headers[i] if i < len(headers) else f"col_{i}"
                    record[col_name] = val
                records.append(record)

            result = {
                "columns": headers,
                "row_count": len(data),
                "preview": records
            }
        else:
            result = {"raw_row": rows[0]}

        log_action("AI_AGENT", "parse_table_from_text", f"Parsed {len(rows)} rows")
        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


# =============================================================================
# Phase 6: Workflow Management
# =============================================================================

@mcp.tool()
def save_workflow_step(step_name: str, step_type: str, parameters: str) -> str:
    """
    Save a reconciliation step to the workflow recipe.

    Args:
        step_name: Descriptive name for this step.
        step_type: Type of operation (e.g., 'compare_hashes', 'fuzzy_match', 'transform').
        parameters: JSON string of parameters used for this step.

    Returns:
        Confirmation with updated workflow summary.
    """
    try:
        with open(settings.workflow_file, "r") as f:
            workflow = json.load(f)

        step = {
            "id": len(workflow["steps"]) + 1,
            "name": step_name,
            "type": step_type,
            "parameters": json.loads(parameters) if isinstance(parameters, str) else parameters,
            "created_at": datetime.now().isoformat()
        }

        workflow["steps"].append(step)

        with open(settings.workflow_file, "w") as f:
            json.dump(workflow, f, indent=2)

        log_action("AI_AGENT", "save_workflow_step", f"Added step: {step_name}")

        return json.dumps({
            "status": "success",
            "step_id": step["id"],
            "total_steps": len(workflow["steps"])
        })

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_workflow() -> str:
    """
    Retrieve the current workflow recipe.

    Returns:
        JSON with all workflow steps.
    """
    try:
        with open(settings.workflow_file, "r") as f:
            workflow = json.load(f)

        return json.dumps(workflow, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def clear_workflow() -> str:
    """
    Clear all steps from the current workflow.

    Returns:
        Confirmation message.
    """
    try:
        workflow = {"version": "1.0", "steps": [], "cleared_at": datetime.now().isoformat()}

        with open(settings.workflow_file, "w") as f:
            json.dump(workflow, f, indent=2)

        log_action("AI_AGENT", "clear_workflow", "Workflow cleared")

        return json.dumps({"status": "success", "message": "Workflow cleared"})

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_audit_log(limit: int = 10) -> str:
    """
    Retrieve recent entries from the audit trail.

    Args:
        limit: Maximum entries to return (max 10).

    Returns:
        JSON with recent audit entries.
    """
    try:
        limit = min(limit, settings.max_rows_display)
        df = pd.read_csv(settings.audit_log)

        recent = df.tail(limit).to_dict(orient="records")

        return json.dumps({
            "total_entries": len(df),
            "showing": len(recent),
            "entries": recent
        }, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


# =============================================================================
# Phase 7: Data Transformation Tools
# =============================================================================

@mcp.tool()
def transform_column(
    source_path: str,
    column: str,
    operation: str,
    output_path: str = ""
) -> str:
    """
    Apply a transformation to a column and optionally save the result.

    Args:
        source_path: Path to the CSV file.
        column: Column name to transform.
        operation: Transformation operation ('upper', 'lower', 'strip', 'trim_spaces', 'remove_special').
        output_path: Optional path to save transformed file. If empty, returns preview only.

    Returns:
        JSON with transformation preview and status.
    """
    try:
        df = pd.read_csv(source_path)

        if column not in df.columns:
            return json.dumps({"error": f"Column '{column}' not found"})

        original_sample = df[column].head(5).tolist()

        if operation == "upper":
            df[column] = df[column].astype(str).str.upper()
        elif operation == "lower":
            df[column] = df[column].astype(str).str.lower()
        elif operation == "strip":
            df[column] = df[column].astype(str).str.strip()
        elif operation == "trim_spaces":
            df[column] = df[column].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
        elif operation == "remove_special":
            df[column] = df[column].astype(str).str.replace(r"[^a-zA-Z0-9\s]", "", regex=True)
        else:
            return json.dumps({"error": f"Unknown operation: {operation}"})

        transformed_sample = df[column].head(5).tolist()

        result = {
            "column": column,
            "operation": operation,
            "preview": {
                "before": original_sample,
                "after": transformed_sample
            }
        }

        if output_path:
            df.to_csv(output_path, index=False)
            result["saved_to"] = output_path
            log_action("AI_AGENT", "transform_column", f"Transformed {column} and saved to {output_path}")
        else:
            result["note"] = "Preview only. Provide output_path to save."

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def merge_sources(
    source_a_path: str,
    source_b_path: str,
    key_columns: str,
    merge_type: str = "inner",
    output_path: str = ""
) -> str:
    """
    Merge two CSV sources on key columns.

    Args:
        source_a_path: Path to the first CSV file.
        source_b_path: Path to the second CSV file.
        key_columns: Comma-separated column names to join on.
        merge_type: Type of merge ('inner', 'left', 'right', 'outer').
        output_path: Optional path to save merged file.

    Returns:
        JSON with merge statistics and preview.
    """
    try:
        df_a = pd.read_csv(source_a_path)
        df_b = pd.read_csv(source_b_path)
        keys = [k.strip() for k in key_columns.split(",")]

        merged = pd.merge(df_a, df_b, on=keys, how=merge_type, suffixes=("_a", "_b"))

        result = {
            "source_a_rows": len(df_a),
            "source_b_rows": len(df_b),
            "merged_rows": len(merged),
            "merge_type": merge_type,
            "columns": list(merged.columns),
            "preview": merged.head(settings.max_rows_display).to_dict(orient="records")
        }

        if output_path:
            merged.to_csv(output_path, index=False)
            result["saved_to"] = output_path
            log_action("AI_AGENT", "merge_sources", f"Merged to {output_path}")

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        return json.dumps({"error": str(e)})


# =============================================================================
# Phase 8: Living Documentation
# =============================================================================

@mcp.tool()
def update_manifest() -> str:
    """
    Regenerate the MANIFEST.md documentation from tool docstrings.

    Returns:
        Confirmation message with tool count.
    """
    try:
        tools = mcp._tool_manager._tools

        manifest = f"""# DataBridge AI - Tool Manifest

> Auto-generated documentation for all MCP tools.
> Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

---

## Overview

DataBridge AI provides **{len(tools)} tools** for data reconciliation:

| Category | Tools |
|----------|-------|
| Data Loading | load_csv, load_json, query_database |
| Profiling | profile_data, detect_schema_drift |
| Comparison | compare_hashes, get_orphan_details, get_conflict_details |
| Fuzzy Matching | fuzzy_match_columns, fuzzy_deduplicate |
| PDF/OCR | extract_text_from_pdf, ocr_image, parse_table_from_text |
| Workflow | save_workflow_step, get_workflow, clear_workflow, get_audit_log |
| Transform | transform_column, merge_sources |
| Documentation | update_manifest |

---

## Tool Reference

"""
        for name, tool in tools.items():
            doc = tool.fn.__doc__ or "No description available."
            manifest += f"### `{name}`\n\n{doc.strip()}\n\n---\n\n"

        manifest_path = Path("docs/MANIFEST.md")
        manifest_path.parent.mkdir(parents=True, exist_ok=True)

        with open(manifest_path, "w") as f:
            f.write(manifest)

        log_action("AI_AGENT", "update_manifest", f"Updated with {len(tools)} tools")

        return json.dumps({
            "status": "success",
            "tools_documented": len(tools),
            "manifest_path": str(manifest_path)
        })

    except Exception as e:
        return json.dumps({"error": str(e)})


# =============================================================================
# Phase 9: Hierarchy Builder Integration
# =============================================================================

# Register hierarchy management tools
try:
    try:
        from src.hierarchy.mcp_tools import register_hierarchy_tools
    except ImportError:
        from hierarchy.mcp_tools import register_hierarchy_tools
    hierarchy_service = register_hierarchy_tools(mcp, str(settings.data_dir))
    log_action("SYSTEM", "hierarchy_init", "Hierarchy Builder tools registered")
except ImportError as e:
    print(f"Warning: Hierarchy module not loaded: {e}")


# =============================================================================
# Phase 10: Connections Module Integration
# =============================================================================

# Register connection management tools
try:
    try:
        from src.connections.mcp_tools import register_connection_tools
    except ImportError:
        from connections.mcp_tools import register_connection_tools
    register_connection_tools(mcp, settings.nestjs_backend_url, settings.nestjs_api_key)
    log_action("SYSTEM", "connections_init", "Connection tools registered")
except ImportError as e:
    print(f"Warning: Connections module not loaded: {e}")


# =============================================================================
# Phase 11: Schema Matcher Module Integration
# =============================================================================

# Register schema matcher tools
try:
    try:
        from src.schema_matcher.mcp_tools import register_schema_matcher_tools
    except ImportError:
        from schema_matcher.mcp_tools import register_schema_matcher_tools
    register_schema_matcher_tools(mcp, settings.nestjs_backend_url, settings.nestjs_api_key)
    log_action("SYSTEM", "schema_matcher_init", "Schema Matcher tools registered")
except ImportError as e:
    print(f"Warning: Schema Matcher module not loaded: {e}")


# =============================================================================
# Phase 12: Data Matcher Module Integration
# =============================================================================

# Register data matcher tools
try:
    try:
        from src.data_matcher.mcp_tools import register_data_matcher_tools
    except ImportError:
        from data_matcher.mcp_tools import register_data_matcher_tools
    register_data_matcher_tools(mcp, settings.nestjs_backend_url, settings.nestjs_api_key)
    log_action("SYSTEM", "data_matcher_init", "Data Matcher tools registered")
except ImportError as e:
    print(f"Warning: Data Matcher module not loaded: {e}")


# =============================================================================
# Phase 13: Templates, Skills & Knowledge Base Integration
# =============================================================================

# Register template, skill, and knowledge base tools
try:
    try:
        from src.templates.mcp_tools import register_template_tools
    except ImportError:
        from templates.mcp_tools import register_template_tools

    # Pass hierarchy_service to enable project creation from templates
    _hierarchy_service = hierarchy_service if 'hierarchy_service' in dir() else None
    register_template_tools(
        mcp,
        templates_dir="templates",
        skills_dir="skills",
        kb_dir="knowledge_base",
        hierarchy_service=_hierarchy_service
    )
    log_action("SYSTEM", "templates_init", "Templates & Knowledge Base tools registered")
except ImportError as e:
    print(f"Warning: Templates module not loaded: {e}")


# =============================================================================
# Phase 14: Slash Commands Integration
# =============================================================================

# Register slash command tools for frontend integration
try:
    try:
        from src.commands.slash_commands import register_slash_command_tools
    except ImportError:
        from commands.slash_commands import register_slash_command_tools
    register_slash_command_tools(mcp)
    log_action("SYSTEM", "slash_commands_init", "Slash command tools registered")
except ImportError as e:
    print(f"Warning: Slash commands module not loaded: {e}")


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    mcp.run()
