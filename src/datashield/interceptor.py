"""DataShield Interceptor - DataFrame interceptor for CSV/local data.

Applies column-level scrambling rules to pandas DataFrames, enabling
DataShield protection for local files without requiring Snowflake.
"""

import logging
from typing import Optional

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

from .types import TableShieldConfig
from .engine import ScrambleEngine

logger = logging.getLogger(__name__)


class DataShieldInterceptor:
    """Applies DataShield scrambling to pandas DataFrames."""

    def __init__(self, engine: ScrambleEngine):
        """Initialize interceptor with a scrambling engine.

        Args:
            engine: ScrambleEngine instance with project key loaded
        """
        self._engine = engine

    def shield_dataframe(self, df: "pd.DataFrame",
                          table_config: TableShieldConfig) -> "pd.DataFrame":
        """Apply column rules to a pandas DataFrame.

        Args:
            df: Input DataFrame to scramble
            table_config: Table shield configuration with column rules

        Returns:
            New DataFrame with scrambled values
        """
        if not PANDAS_AVAILABLE:
            raise RuntimeError("pandas is required for DataFrame interception")

        result = df.copy()
        skip_set = set(table_config.skip_columns)

        for rule in table_config.column_rules:
            if rule.column_name not in result.columns:
                continue
            if rule.column_name in skip_set:
                continue

            result[rule.column_name] = result[rule.column_name].apply(
                lambda v: self._engine.scramble(v, rule)
            )

        return result

    def shield_csv(self, input_path: str, output_path: str,
                    table_config: TableShieldConfig) -> dict:
        """Read a CSV, apply scrambling, write shielded copy.

        Args:
            input_path: Path to source CSV
            output_path: Path to write shielded CSV
            table_config: Table shield configuration

        Returns:
            Summary dict with row count and columns processed
        """
        if not PANDAS_AVAILABLE:
            raise RuntimeError("pandas is required")

        df = pd.read_csv(input_path)
        shielded = self.shield_dataframe(df, table_config)
        shielded.to_csv(output_path, index=False)

        return {
            "input": input_path,
            "output": output_path,
            "rows": len(df),
            "columns_shielded": [
                r.column_name for r in table_config.column_rules
                if r.column_name in df.columns
                and r.column_name not in table_config.skip_columns
            ],
        }

    def shield_json(self, input_path: str, output_path: str,
                     table_config: TableShieldConfig) -> dict:
        """Read a JSON file, apply scrambling, write shielded copy.

        Args:
            input_path: Path to source JSON
            output_path: Path to write shielded JSON
            table_config: Table shield configuration

        Returns:
            Summary dict
        """
        if not PANDAS_AVAILABLE:
            raise RuntimeError("pandas is required")

        df = pd.read_json(input_path)
        shielded = self.shield_dataframe(df, table_config)
        shielded.to_json(output_path, orient="records", indent=2)

        return {
            "input": input_path,
            "output": output_path,
            "rows": len(df),
            "columns_shielded": [
                r.column_name for r in table_config.column_rules
                if r.column_name in df.columns
                and r.column_name not in table_config.skip_columns
            ],
        }
