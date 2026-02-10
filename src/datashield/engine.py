"""DataShield Scrambling Engine - Six deterministic, key-dependent strategies.

Each strategy uses HMAC-SHA256(project_key + column_name, value) to ensure:
- Same value in same column → same scrambled output (joins work)
- Same value in different columns → different output (no cross-column leakage)
- Different key → completely different output
"""

import hmac
import hashlib
import struct
from datetime import datetime, timedelta
from typing import Any, Optional

from .types import ColumnRule, ScrambleStrategy
from .constants import SYNTHETIC_POOLS


class ScrambleEngine:
    """Core scrambling engine with six deterministic strategies."""

    def __init__(self, project_key: bytes):
        """Initialize engine with a project-specific key.

        Args:
            project_key: 256-bit key for this shield project
        """
        self._key = project_key

    def _hmac(self, column_name: str, value: str) -> bytes:
        """Compute HMAC-SHA256 for a column+value pair."""
        context = self._key + column_name.encode("utf-8")
        return hmac.new(context, value.encode("utf-8"), hashlib.sha256).digest()

    def _hmac_int(self, column_name: str, value: str) -> int:
        """Get a deterministic integer from HMAC."""
        h = self._hmac(column_name, value)
        return int.from_bytes(h[:8], "big")

    def _hmac_float(self, column_name: str, value: str) -> float:
        """Get a deterministic float in [0, 1) from HMAC."""
        h = self._hmac(column_name, value)
        return int.from_bytes(h[:4], "big") / (2**32)

    def scramble(self, value: Any, rule: ColumnRule) -> Any:
        """Apply the appropriate scrambling strategy to a value.

        Args:
            value: The original value to scramble
            rule: Column rule specifying strategy and options

        Returns:
            Scrambled value preserving type and format
        """
        if value is None and rule.preserve_nulls:
            return None

        if rule.strategy == ScrambleStrategy.PASSTHROUGH:
            return value

        # Convert to string for HMAC computation
        str_val = str(value) if value is not None else ""

        if rule.strategy == ScrambleStrategy.FORMAT_PRESERVING_HASH:
            return self._format_preserving_hash(str_val, rule.column_name)
        elif rule.strategy == ScrambleStrategy.NUMERIC_SCALING:
            return self._numeric_scaling(value, rule.column_name)
        elif rule.strategy == ScrambleStrategy.SYNTHETIC_SUBSTITUTION:
            return self._synthetic_substitution(str_val, rule.column_name, rule.synthetic_pool)
        elif rule.strategy == ScrambleStrategy.DATE_SHIFT:
            return self._date_shift(value, rule.column_name)
        elif rule.strategy == ScrambleStrategy.PATTERN_PRESERVING:
            return self._pattern_preserving(str_val, rule.column_name)
        else:
            return value

    def _format_preserving_hash(self, value: str, column_name: str) -> str:
        """Scramble string preserving format (digits stay digits, letters stay letters).

        Input:  "INV-2024-00847"
        Output: "INV-7391-04218"
        """
        if not value:
            return value

        h = self._hmac(column_name, value).hex()
        result = []
        h_idx = 0
        for ch in value:
            hi = int(h[h_idx % len(h)], 16)
            h_idx += 1
            if ch.isdigit():
                result.append(str(hi % 10))
            elif ch.isupper():
                result.append(chr(ord("A") + hi % 26))
            elif ch.islower():
                result.append(chr(ord("a") + hi % 26))
            else:
                result.append(ch)
        return "".join(result)

    def _numeric_scaling(self, value: Any, column_name: str) -> Any:
        """Scale numeric value by a key-derived factor (0.5–2.0).

        Preserves sign, approximate magnitude, and numeric type.
        """
        try:
            num = float(value)
        except (TypeError, ValueError):
            return value

        if num == 0:
            return type(value)(0) if isinstance(value, (int, float)) else 0

        # Derive a scaling factor between 0.5 and 2.0
        factor = 0.5 + self._hmac_float(column_name, str(value)) * 1.5

        scaled = num * factor

        # Preserve type
        if isinstance(value, int):
            return int(round(scaled))
        elif isinstance(value, float):
            # Preserve decimal places
            str_val = str(value)
            if "." in str_val:
                decimals = len(str_val.split(".")[1])
                return round(scaled, decimals)
            return scaled
        return scaled

    def _synthetic_substitution(self, value: str, column_name: str,
                                 pool_name: Optional[str] = None) -> str:
        """Map value to a synthetic lookup from a named pool.

        Input:  "Acme Corp" (pool: company_names)
        Output: "Vertex Industries"
        """
        if not value:
            return value

        pool = SYNTHETIC_POOLS.get(pool_name or "company_names", [])
        if not pool:
            # Fallback to format_preserving_hash if no pool
            return self._format_preserving_hash(value, column_name)

        idx = self._hmac_int(column_name, value) % len(pool)
        return pool[idx]

    def _date_shift(self, value: Any, column_name: str) -> Any:
        """Shift date by a key-derived offset (±30-365 days).

        Preserves the type (string, datetime, date).
        """
        # Parse date from various formats
        dt = None
        original_format = None

        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, str):
            for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y",
                        "%d/%m/%Y", "%Y%m%d", "%m-%d-%Y"):
                try:
                    dt = datetime.strptime(value, fmt)
                    original_format = fmt
                    break
                except ValueError:
                    continue

        if dt is None:
            return value

        # Derive offset: ±30 to ±365 days
        h = self._hmac_float(column_name, str(value))
        offset_days = int(30 + h * 335)  # 30-365 range
        # Determine sign from another byte
        sign = 1 if self._hmac(column_name, str(value))[0] % 2 == 0 else -1
        offset_days *= sign

        shifted = dt + timedelta(days=offset_days)

        # Return in same format
        if isinstance(value, datetime):
            return shifted
        elif original_format:
            return shifted.strftime(original_format)
        return shifted.isoformat()

    def _pattern_preserving(self, value: str, column_name: str) -> str:
        """Preserve regex structure while scrambling content characters.

        Input:  "555-123-4567"
        Output: "555-847-2913"
        """
        if not value:
            return value

        h = self._hmac(column_name, value).hex()
        result = []
        h_idx = 0
        for ch in value:
            hi = int(h[h_idx % len(h)], 16)
            h_idx += 1
            if ch.isdigit():
                result.append(str(hi % 10))
            elif ch.isupper():
                result.append(chr(ord("A") + hi % 26))
            elif ch.islower():
                result.append(chr(ord("a") + hi % 26))
            else:
                result.append(ch)
        return "".join(result)
