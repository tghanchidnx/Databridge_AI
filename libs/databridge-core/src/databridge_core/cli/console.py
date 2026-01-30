"""
Rich console utilities for DataBridge AI platform.

Provides a configured Rich console for consistent output formatting.
"""

from typing import Optional
from rich.console import Console
from rich.theme import Theme


# Custom theme for DataBridge
DATABRIDGE_THEME = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "bold red",
    "success": "bold green",
    "highlight": "magenta",
    "dim": "dim white",
    "header": "bold blue",
    "key": "cyan",
    "value": "white",
})


# Singleton console instance
_console: Optional[Console] = None


def get_console(force_terminal: Optional[bool] = None) -> Console:
    """
    Get or create the configured Rich console.

    Args:
        force_terminal: Force terminal output (useful for testing).

    Returns:
        Console: Configured Rich console instance.
    """
    global _console
    if _console is None:
        _console = Console(
            theme=DATABRIDGE_THEME,
            force_terminal=force_terminal,
            highlight=True,
        )
    return _console


# Default console instance for convenience
console = get_console()


def reset_console() -> Console:
    """Reset and recreate the console (useful for testing)."""
    global _console
    _console = None
    return get_console()
