"""
DataBridge Core CLI Module.

Provides Rich console utilities and formatters for CLI applications.
"""

from databridge_core.cli.console import console, get_console
from databridge_core.cli.formatters import (
    format_table,
    format_dict,
    format_list,
    format_error,
    format_success,
    format_warning,
    format_info,
)

__all__ = [
    "console",
    "get_console",
    "format_table",
    "format_dict",
    "format_list",
    "format_error",
    "format_success",
    "format_warning",
    "format_info",
]
