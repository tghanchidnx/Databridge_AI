"""DataBridge AI Community Edition - MCP Server Implementation.

A headless, MCP-native data reconciliation engine that bridges messy sources
(OCR/PDF/SQL) with a structured Python-based comparison pipeline.

Community Edition includes:
- Data Reconciliation (load, compare, profile)
- Fuzzy Matching
- Basic Hierarchy Management
- Data Quality Tools
- dbt Integration (basic)
- PDF/OCR Extraction
- Diff Utilities

For Pro features (Cortex AI, Wright Pipeline, GraphRAG, etc.),
install databridge-ai-pro with a valid license key.
"""
import os
import sys
import importlib.util
from pathlib import Path
from typing import Optional, Dict, Any, List
from fastmcp import FastMCP

# Handle imports for both module and direct execution
try:
    from src.config import settings
except ImportError:
    try:
        from .config import settings
    except ImportError:
        from config import settings

# Version info
__version__ = "0.40.0"
__edition__ = "Community"


def get_tool_count(mcp_instance: FastMCP) -> int:
    """Get the number of registered tools."""
    try:
        return len(mcp_instance.list_tools())
    except Exception:
        return 0


# Initialize the Server
mcp = FastMCP(
    "DataBridge AI Community Edition",
    instructions="""DataBridge AI Community Edition - Open Source Data Reconciliation

I help you with data reconciliation and quality management:

**Data Reconciliation:**
- Compare and validate data from CSV, SQL, PDF, and JSON sources
- Find orphans, conflicts, and fuzzy matches between datasets
- Profile data quality and detect schema drift

**Hierarchy Builder:**
- Create and manage multi-level hierarchy projects
- Export hierarchies to CSV/JSON formats

**Data Quality:**
- Generate expectation suites
- Run data validation

**Additional Tools:**
- dbt model generation
- PDF text extraction
- Data transformation utilities

For advanced features like Cortex AI, Wright Pipeline, GraphRAG, and more,
upgrade to DataBridge AI Pro: https://databridge.ai/pro
"""
)


# =============================================================================
# Plugin Loading System
# =============================================================================

def load_plugins(mcp_instance: FastMCP, plugins_dir: Path, tier: str = "CE") -> Dict[str, bool]:
    """Dynamically discovers and loads tools from a plugin directory.

    Args:
        mcp_instance: The FastMCP instance to register tools with
        plugins_dir: Directory containing plugin subdirectories
        tier: The tier of plugins being loaded (CE, PRO, ENTERPRISE)

    Returns:
        Dict mapping plugin names to load success status
    """
    results = {}

    if not plugins_dir.is_dir():
        return results

    for plugin_name in os.listdir(plugins_dir):
        plugin_path = plugins_dir / plugin_name
        if plugin_path.is_dir() and not plugin_name.startswith('_'):
            mcp_tools_file = plugin_path / "mcp_tools.py"
            if mcp_tools_file.is_file():
                try:
                    # Dynamically import the mcp_tools.py module
                    spec = importlib.util.spec_from_file_location(
                        f"databridge_plugins.{tier.lower()}.{plugin_name}.mcp_tools",
                        mcp_tools_file
                    )
                    if spec and spec.loader:
                        module = importlib.util.module_from_spec(spec)
                        sys.modules[spec.name] = module
                        spec.loader.exec_module(module)

                        # Call the registration function if it exists
                        if hasattr(module, "register_tools"):
                            print(f"[{tier}] Loading plugin: {plugin_name}")
                            module.register_tools(mcp_instance)
                            results[plugin_name] = True
                        else:
                            print(f"[{tier}] Skipping {plugin_name}: no register_tools function")
                            results[plugin_name] = False
                except Exception as e:
                    print(f"[{tier}] Error loading plugin '{plugin_name}': {e}")
                    results[plugin_name] = False

    return results


def load_pro_plugins(mcp_instance: FastMCP) -> bool:
    """Attempt to load Pro plugins if available and licensed.

    Returns:
        True if Pro plugins were loaded successfully
    """
    try:
        # Try to import the Pro package
        from databridge_ai_pro import register_pro_tools, validate_license

        # Validate license
        if not validate_license():
            print("[PRO] License validation failed - Pro features disabled")
            return False

        # Register Pro tools
        print("[PRO] Loading Pro features...")
        register_pro_tools(mcp_instance)
        return True

    except ImportError:
        # Pro package not installed - this is fine for CE users
        return False
    except Exception as e:
        print(f"[PRO] Error loading Pro features: {e}")
        return False


# =============================================================================
# Core CE Tools - Inline for simplicity and distribution
# =============================================================================

import pandas as pd
import hashlib
import json
from datetime import datetime
from typing import Optional

# Conditional imports
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


# Ensure data directory exists
Path(settings.data_dir).mkdir(parents=True, exist_ok=True)


# =============================================================================
# Internal Helpers
# =============================================================================

def compute_row_hash(row: pd.Series, columns: list) -> str:
    """Compute a deterministic SHA-256 hash for a row."""
    values = "|".join(str(row[col]) for col in columns)
    return hashlib.sha256(values.encode()).hexdigest()[:16]


def truncate_dataframe(df: pd.DataFrame, max_rows: int = None) -> pd.DataFrame:
    """Truncate DataFrame to respect context sensitivity rules."""
    max_rows = max_rows or settings.max_rows_display
    return df.head(max_rows)


# =============================================================================
# Phase 0: File Discovery Tools
# =============================================================================

def get_common_search_paths() -> list:
    """Get common directories where files might be located."""
    home = Path.home()
    cwd = Path.cwd()

    paths = [
        cwd,
        cwd / "data",
        cwd / "uploads",
        home,
        home / "Downloads",
        home / "Documents",
        home / "Desktop",
    ]

    return [p for p in paths if p.exists()]


@mcp.tool()
def find_files(
    pattern: str = "*.csv",
    search_name: str = "",
    max_results: int = 20
) -> str:
    """Search for files across common directories.

    Args:
        pattern: Glob pattern to match (default "*.csv")
        search_name: Optional filename substring to filter results
        max_results: Maximum number of results to return

    Returns:
        JSON with found files and their details
    """
    search_paths = get_common_search_paths()
    found_files = []
    seen_paths = set()

    for search_dir in search_paths:
        try:
            for file_path in search_dir.rglob(pattern):
                if len(found_files) >= max_results:
                    break

                abs_path = str(file_path.resolve())
                if abs_path in seen_paths:
                    continue
                seen_paths.add(abs_path)

                if search_name and search_name.lower() not in file_path.name.lower():
                    continue

                if len(file_path.parts) > 15:
                    continue

                try:
                    stat = file_path.stat()
                    found_files.append({
                        "name": file_path.name,
                        "path": abs_path,
                        "size_kb": round(stat.st_size / 1024, 2),
                        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    })
                except (OSError, PermissionError):
                    continue
        except (OSError, PermissionError):
            continue

    return json.dumps({
        "count": len(found_files),
        "files": found_files,
        "searched_directories": [str(p) for p in search_paths[:5]],
    }, indent=2)


@mcp.tool()
def get_working_directory() -> str:
    """Get the current working directory and data directory paths.

    Returns:
        JSON with directory information
    """
    return json.dumps({
        "working_directory": str(Path.cwd()),
        "data_directory": str(settings.data_dir),
        "home_directory": str(Path.home()),
    }, indent=2)


# =============================================================================
# Phase 1: Data Loading Tools
# =============================================================================

@mcp.tool()
def load_csv(
    file_path: str,
    preview_rows: int = 5,
    encoding: str = "utf-8"
) -> str:
    """Load a CSV file and return preview with statistics.

    Args:
        file_path: Path to the CSV file
        preview_rows: Number of rows to preview
        encoding: File encoding

    Returns:
        JSON with data preview and statistics
    """
    try:
        df = pd.read_csv(file_path, encoding=encoding)
        preview = truncate_dataframe(df, preview_rows)

        return json.dumps({
            "success": True,
            "file": file_path,
            "rows": len(df),
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "preview": preview.to_dict(orient="records"),
            "null_counts": df.isnull().sum().to_dict(),
        }, indent=2, default=str)

    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


@mcp.tool()
def load_json(
    file_path: str,
    preview_items: int = 5
) -> str:
    """Load a JSON file and return preview with statistics.

    Args:
        file_path: Path to the JSON file
        preview_items: Number of items to preview

    Returns:
        JSON with data preview and statistics
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Handle both list and dict structures
        if isinstance(data, list):
            preview = data[:preview_items]
            count = len(data)
        else:
            preview = data
            count = len(data) if isinstance(data, dict) else 1

        return json.dumps({
            "success": True,
            "file": file_path,
            "type": type(data).__name__,
            "count": count,
            "preview": preview,
        }, indent=2, default=str)

    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


@mcp.tool()
def query_database(
    connection_string: str,
    query: str,
    max_rows: int = 100
) -> str:
    """Execute a SQL query and return results.

    Args:
        connection_string: SQLAlchemy connection string
        query: SQL query to execute
        max_rows: Maximum rows to return

    Returns:
        JSON with query results
    """
    if not SQLALCHEMY_AVAILABLE:
        return json.dumps({"error": "SQLAlchemy not installed. Run: pip install sqlalchemy"})

    try:
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchmany(max_rows)
            columns = list(result.keys())

        return json.dumps({
            "success": True,
            "columns": columns,
            "row_count": len(rows),
            "rows": [dict(zip(columns, row)) for row in rows],
        }, indent=2, default=str)

    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


# =============================================================================
# Phase 2: Data Profiling Tools
# =============================================================================

@mcp.tool()
def profile_data(
    file_path: str,
    sample_size: int = 1000
) -> str:
    """Profile a CSV file and return comprehensive statistics.

    Args:
        file_path: Path to the CSV file
        sample_size: Number of rows to sample for profiling

    Returns:
        JSON with profiling statistics
    """
    try:
        df = pd.read_csv(file_path, nrows=sample_size)

        profile = {
            "file": file_path,
            "rows_sampled": len(df),
            "columns": len(df.columns),
            "memory_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
            "column_profiles": {}
        }

        for col in df.columns:
            col_data = df[col]
            col_profile = {
                "dtype": str(col_data.dtype),
                "null_count": int(col_data.isnull().sum()),
                "null_pct": round(col_data.isnull().sum() / len(df) * 100, 2),
                "unique_count": int(col_data.nunique()),
            }

            if pd.api.types.is_numeric_dtype(col_data):
                col_profile.update({
                    "min": float(col_data.min()) if not pd.isna(col_data.min()) else None,
                    "max": float(col_data.max()) if not pd.isna(col_data.max()) else None,
                    "mean": float(col_data.mean()) if not pd.isna(col_data.mean()) else None,
                    "std": float(col_data.std()) if not pd.isna(col_data.std()) else None,
                })

            profile["column_profiles"][col] = col_profile

        return json.dumps({"success": True, "profile": profile}, indent=2)

    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


# =============================================================================
# Phase 3: Comparison Tools
# =============================================================================

@mcp.tool()
def compare_hashes(
    source_file: str,
    target_file: str,
    key_columns: str,
    compare_columns: str = ""
) -> str:
    """Compare two CSV files using hash-based comparison.

    Args:
        source_file: Path to source CSV
        target_file: Path to target CSV
        key_columns: Comma-separated column names for key
        compare_columns: Comma-separated columns to compare (empty = all)

    Returns:
        JSON with comparison results
    """
    try:
        source_df = pd.read_csv(source_file)
        target_df = pd.read_csv(target_file)

        keys = [k.strip() for k in key_columns.split(",")]
        compare_cols = [c.strip() for c in compare_columns.split(",")] if compare_columns else list(source_df.columns)

        # Compute hashes
        source_df["_hash"] = source_df.apply(lambda r: compute_row_hash(r, compare_cols), axis=1)
        target_df["_hash"] = target_df.apply(lambda r: compute_row_hash(r, compare_cols), axis=1)

        # Create key-based lookup
        source_keys = set(source_df[keys].apply(tuple, axis=1))
        target_keys = set(target_df[keys].apply(tuple, axis=1))

        orphans_source = source_keys - target_keys
        orphans_target = target_keys - source_keys
        common_keys = source_keys & target_keys

        # Find conflicts (same key, different hash)
        conflicts = 0
        if common_keys:
            source_lookup = source_df.set_index(keys)["_hash"].to_dict()
            target_lookup = target_df.set_index(keys)["_hash"].to_dict()
            for key in common_keys:
                if source_lookup.get(key) != target_lookup.get(key):
                    conflicts += 1

        return json.dumps({
            "success": True,
            "source_rows": len(source_df),
            "target_rows": len(target_df),
            "orphans_in_source": len(orphans_source),
            "orphans_in_target": len(orphans_target),
            "conflicts": conflicts,
            "matches": len(common_keys) - conflicts,
        }, indent=2)

    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


# =============================================================================
# Phase 4: Fuzzy Matching Tools
# =============================================================================

@mcp.tool()
def fuzzy_match_columns(
    source_file: str,
    target_file: str,
    source_column: str,
    target_column: str,
    threshold: int = 80,
    max_results: int = 100
) -> str:
    """Find fuzzy matches between columns in two files.

    Args:
        source_file: Path to source CSV
        target_file: Path to target CSV
        source_column: Column name in source
        target_column: Column name in target
        threshold: Minimum similarity score (0-100)
        max_results: Maximum matches to return

    Returns:
        JSON with fuzzy match results
    """
    if not RAPIDFUZZ_AVAILABLE:
        return json.dumps({"error": "RapidFuzz not installed. Run: pip install rapidfuzz"})

    try:
        source_df = pd.read_csv(source_file)
        target_df = pd.read_csv(target_file)

        source_values = source_df[source_column].dropna().unique()
        target_values = target_df[target_column].dropna().unique()

        matches = []
        for source_val in source_values[:max_results]:
            result = process.extractOne(
                str(source_val),
                [str(t) for t in target_values],
                scorer=fuzz.ratio
            )
            if result and result[1] >= threshold:
                matches.append({
                    "source": str(source_val),
                    "target": result[0],
                    "score": result[1],
                })

        return json.dumps({
            "success": True,
            "matches_found": len(matches),
            "threshold": threshold,
            "matches": matches[:max_results],
        }, indent=2)

    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


# =============================================================================
# Phase 5: PDF/OCR Tools
# =============================================================================

@mcp.tool()
def extract_text_from_pdf(
    file_path: str,
    max_pages: int = 10
) -> str:
    """Extract text from a PDF file.

    Args:
        file_path: Path to PDF file
        max_pages: Maximum pages to extract

    Returns:
        JSON with extracted text
    """
    if not PYPDF_AVAILABLE:
        return json.dumps({"error": "pypdf not installed. Run: pip install pypdf"})

    try:
        reader = PdfReader(file_path)
        pages_text = []

        for i, page in enumerate(reader.pages[:max_pages]):
            text = page.extract_text() or ""
            pages_text.append({
                "page": i + 1,
                "text": text[:5000],  # Limit per page
                "char_count": len(text),
            })

        return json.dumps({
            "success": True,
            "file": file_path,
            "total_pages": len(reader.pages),
            "extracted_pages": len(pages_text),
            "pages": pages_text,
        }, indent=2)

    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


# =============================================================================
# Diff Utilities
# =============================================================================

@mcp.tool()
def diff_text(
    text1: str,
    text2: str,
    context_lines: int = 3
) -> str:
    """Compare two text strings and show differences.

    Args:
        text1: First text to compare
        text2: Second text to compare
        context_lines: Lines of context around changes

    Returns:
        JSON with diff results
    """
    import difflib

    lines1 = text1.splitlines(keepends=True)
    lines2 = text2.splitlines(keepends=True)

    diff = list(difflib.unified_diff(
        lines1, lines2,
        fromfile='text1',
        tofile='text2',
        n=context_lines
    ))

    return json.dumps({
        "success": True,
        "lines_in_text1": len(lines1),
        "lines_in_text2": len(lines2),
        "has_differences": len(diff) > 0,
        "diff": "".join(diff),
    }, indent=2)


# =============================================================================
# License Status Tool
# =============================================================================

@mcp.tool()
def get_license_status() -> str:
    """Get the current license status and available features.

    Returns:
        JSON with license information and available modules
    """
    # Try to get license info from plugins module
    try:
        from src.plugins import get_license_manager
        mgr = get_license_manager()
        return json.dumps(mgr.get_status(), indent=2)
    except ImportError:
        pass

    # Default CE response
    return json.dumps({
        "edition": "Community",
        "tier": "CE",
        "tier_level": 0,
        "is_valid": True,
        "message": "Running DataBridge AI Community Edition",
        "available_modules": [
            "file_discovery",
            "data_loading",
            "data_profiling",
            "hashing",
            "fuzzy_matching",
            "pdf_ocr",
            "diff_utilities",
        ],
        "upgrade_info": "For Pro features, visit https://databridge.ai/pro",
    }, indent=2)


# =============================================================================
# Server Initialization
# =============================================================================

def initialize_server():
    """Initialize the server and load plugins."""
    print(f"\n{'='*60}")
    print(f"DataBridge AI Community Edition v{__version__}")
    print(f"{'='*60}")

    # Get the project root
    project_root = Path(__file__).parent.parent

    # Plugin directories
    plugins_dirs = [
        project_root / "plugins",  # CE plugins
        project_root.parent / "private_plugins",  # User's private plugins
    ]

    # Load CE plugins
    total_plugins = 0
    for plugins_dir in plugins_dirs:
        if plugins_dir.exists():
            results = load_plugins(mcp, plugins_dir, "CE")
            total_plugins += sum(1 for v in results.values() if v)

    print(f"\nLoaded {total_plugins} plugin(s)")

    # Try to load Pro plugins
    if load_pro_plugins(mcp):
        print("Pro features enabled")

    print(f"\nTotal tools registered: {get_tool_count(mcp)}")
    print(f"{'='*60}\n")


def main():
    """Main entry point."""
    initialize_server()
    mcp.run()


# Initialize on import
initialize_server()

if __name__ == "__main__":
    main()
