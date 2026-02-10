"""DataShield Snowflake Generator - Generate UDFs and shielded views DDL.

Produces a 2-layer Snowflake architecture:
1. Scrambling UDFs in a DATASHIELD schema
2. Shielded views wrapping raw tables with per-column scrambling
"""

import logging
from typing import List, Optional

from .types import ShieldProject, TableShieldConfig, ColumnRule, ScrambleStrategy

logger = logging.getLogger(__name__)


# =============================================================================
# UDF Templates
# =============================================================================

SCRAMBLE_STRING_UDF = '''CREATE OR REPLACE FUNCTION DATASHIELD.SCRAMBLE_STRING(val VARCHAR, key VARCHAR, col VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'scramble_string'
AS $$
import hmac, hashlib
def scramble_string(val, key, col):
    if val is None:
        return None
    h = hmac.new((key + col).encode(), str(val).encode(), hashlib.sha256).hexdigest()
    result = []
    for i, ch in enumerate(str(val)):
        hi = int(h[i % len(h)], 16)
        if ch.isdigit():
            result.append(str(hi % 10))
        elif ch.isupper():
            result.append(chr(ord('A') + hi % 26))
        elif ch.islower():
            result.append(chr(ord('a') + hi % 26))
        else:
            result.append(ch)
    return ''.join(result)
$$;'''

SCRAMBLE_NUMBER_UDF = '''CREATE OR REPLACE FUNCTION DATASHIELD.SCRAMBLE_NUMBER(val NUMBER(38,6), key VARCHAR, col VARCHAR)
RETURNS NUMBER(38,6)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'scramble_number'
AS $$
import hmac, hashlib
def scramble_number(val, key, col):
    if val is None:
        return None
    h = hmac.new((key + col).encode(), str(val).encode(), hashlib.sha256).digest()
    factor = 0.5 + (int.from_bytes(h[:4], 'big') / (2**32)) * 1.5
    return round(float(val) * factor, 6)
$$;'''

SCRAMBLE_DATE_UDF = '''CREATE OR REPLACE FUNCTION DATASHIELD.SCRAMBLE_DATE(val DATE, key VARCHAR, col VARCHAR)
RETURNS DATE
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'scramble_date'
AS $$
import hmac, hashlib
from datetime import timedelta
def scramble_date(val, key, col):
    if val is None:
        return None
    h = hmac.new((key + col).encode(), str(val).encode(), hashlib.sha256).digest()
    offset = 30 + int(int.from_bytes(h[:4], 'big') / (2**32) * 335)
    sign = 1 if h[0] % 2 == 0 else -1
    return val + timedelta(days=offset * sign)
$$;'''


def _get_udf_call(rule: ColumnRule, key_ref: str) -> str:
    """Get the UDF call expression for a column rule.

    Args:
        rule: Column scrambling rule
        key_ref: Key reference string for the UDF

    Returns:
        SQL expression string
    """
    col = rule.column_name

    if rule.strategy == ScrambleStrategy.PASSTHROUGH:
        return col

    if rule.strategy in (ScrambleStrategy.FORMAT_PRESERVING_HASH,
                         ScrambleStrategy.PATTERN_PRESERVING,
                         ScrambleStrategy.SYNTHETIC_SUBSTITUTION):
        return f"DATASHIELD.SCRAMBLE_STRING({col}, '{key_ref}', '{col}')"

    if rule.strategy == ScrambleStrategy.NUMERIC_SCALING:
        return f"DATASHIELD.SCRAMBLE_NUMBER({col}, '{key_ref}', '{col}')"

    if rule.strategy == ScrambleStrategy.DATE_SHIFT:
        return f"DATASHIELD.SCRAMBLE_DATE({col}, '{key_ref}', '{col}')"

    return col


def generate_udfs() -> str:
    """Generate all DataShield UDF DDL statements.

    Returns:
        SQL string with all UDF CREATE statements
    """
    lines = [
        "-- ==========================================================",
        "-- DataShield UDFs - Scrambling Functions",
        "-- ==========================================================",
        "",
        "CREATE SCHEMA IF NOT EXISTS DATASHIELD;",
        "",
        SCRAMBLE_STRING_UDF,
        "",
        SCRAMBLE_NUMBER_UDF,
        "",
        SCRAMBLE_DATE_UDF,
    ]
    return "\n".join(lines)


def generate_view_ddl(table_config: TableShieldConfig,
                       key_ref: str) -> str:
    """Generate a shielded view DDL for a single table.

    Args:
        table_config: Table shield configuration
        key_ref: Key reference string

    Returns:
        SQL CREATE VIEW statement
    """
    view_name = f"DATASHIELD.VW_SHIELDED_{table_config.table_name}"
    source_fqn = f"{table_config.database}.{table_config.schema_name}.{table_config.table_name}"

    # Build SELECT columns
    select_cols = []
    covered_cols = {r.column_name for r in table_config.column_rules}
    skip_set = set(table_config.skip_columns)

    for rule in table_config.column_rules:
        if rule.column_name in skip_set:
            select_cols.append(f"    {rule.column_name}")
            continue
        expr = _get_udf_call(rule, key_ref)
        if expr == rule.column_name:
            select_cols.append(f"    {rule.column_name}")
        else:
            select_cols.append(f"    {expr} AS {rule.column_name}")

    lines = [
        f"-- Shielded view for {source_fqn}",
        f"CREATE OR REPLACE VIEW {view_name} AS",
        "SELECT",
        ",\n".join(select_cols),
        f"FROM {source_fqn};",
    ]
    return "\n".join(lines)


def generate_full_ddl(project: ShieldProject,
                       key_ref: Optional[str] = None) -> str:
    """Generate complete DDL for a shield project (UDFs + all views).

    Args:
        project: Shield project
        key_ref: Key reference string (defaults to project key_alias)

    Returns:
        Complete SQL DDL string
    """
    if key_ref is None:
        key_ref = project.key_alias

    parts = [
        "-- ==========================================================",
        f"-- DataShield DDL for project: {project.name}",
        f"-- Project ID: {project.id}",
        "-- ==========================================================",
        "",
        generate_udfs(),
        "",
    ]

    for table in project.tables:
        parts.append("")
        parts.append(generate_view_ddl(table, key_ref))

    return "\n".join(parts)
