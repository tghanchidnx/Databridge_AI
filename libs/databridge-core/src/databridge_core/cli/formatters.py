"""
Output formatters for DataBridge AI CLI.

Provides consistent formatting for tables, dictionaries, and messages.
"""

from typing import Any, Dict, List, Optional, Sequence
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box

from databridge_core.cli.console import console


def format_table(
    data: Sequence[Dict[str, Any]],
    title: Optional[str] = None,
    columns: Optional[List[str]] = None,
    max_rows: int = 50,
    show_row_numbers: bool = False,
) -> Table:
    """
    Create a Rich table from a sequence of dictionaries.

    Args:
        data: Sequence of dictionaries to display.
        title: Optional table title.
        columns: Optional list of columns to include (default: all).
        max_rows: Maximum rows to display.
        show_row_numbers: Show row numbers in first column.

    Returns:
        Table: Rich table object.
    """
    if not data:
        table = Table(title=title or "No Data", box=box.ROUNDED)
        table.add_column("(empty)")
        return table

    # Determine columns
    if columns is None:
        columns = list(data[0].keys())

    # Create table
    table = Table(title=title, box=box.ROUNDED, show_header=True, header_style="bold blue")

    # Add row numbers column if requested
    if show_row_numbers:
        table.add_column("#", style="dim", justify="right")

    # Add columns
    for col in columns:
        table.add_column(col, overflow="fold")

    # Add rows (up to max_rows)
    for i, row in enumerate(data[:max_rows]):
        values = []
        if show_row_numbers:
            values.append(str(i + 1))
        for col in columns:
            val = row.get(col, "")
            values.append(str(val) if val is not None else "")
        table.add_row(*values)

    # Show truncation warning if needed
    if len(data) > max_rows:
        table.caption = f"Showing {max_rows} of {len(data)} rows"

    return table


def format_dict(
    data: Dict[str, Any],
    title: Optional[str] = None,
    max_value_length: int = 100,
) -> Table:
    """
    Create a Rich table from a dictionary.

    Args:
        data: Dictionary to display.
        title: Optional table title.
        max_value_length: Maximum length for values before truncation.

    Returns:
        Table: Rich table object.
    """
    table = Table(title=title, box=box.ROUNDED, show_header=True, header_style="bold blue")
    table.add_column("Key", style="key")
    table.add_column("Value", style="value")

    for key, value in data.items():
        str_value = str(value)
        if len(str_value) > max_value_length:
            str_value = str_value[:max_value_length] + "..."
        table.add_row(str(key), str_value)

    return table


def format_list(
    items: List[Any],
    title: Optional[str] = None,
    numbered: bool = True,
) -> Table:
    """
    Create a Rich table from a list.

    Args:
        items: List of items to display.
        title: Optional table title.
        numbered: Show item numbers.

    Returns:
        Table: Rich table object.
    """
    table = Table(title=title, box=box.ROUNDED, show_header=False)

    if numbered:
        table.add_column("#", style="dim", justify="right", width=4)

    table.add_column("Item")

    for i, item in enumerate(items, 1):
        if numbered:
            table.add_row(str(i), str(item))
        else:
            table.add_row(str(item))

    return table


def format_error(message: str, title: str = "Error") -> Panel:
    """
    Create an error panel.

    Args:
        message: Error message.
        title: Panel title.

    Returns:
        Panel: Rich panel object.
    """
    return Panel(
        Text(message, style="error"),
        title=title,
        border_style="red",
        box=box.ROUNDED,
    )


def format_success(message: str, title: str = "Success") -> Panel:
    """
    Create a success panel.

    Args:
        message: Success message.
        title: Panel title.

    Returns:
        Panel: Rich panel object.
    """
    return Panel(
        Text(message, style="success"),
        title=title,
        border_style="green",
        box=box.ROUNDED,
    )


def format_warning(message: str, title: str = "Warning") -> Panel:
    """
    Create a warning panel.

    Args:
        message: Warning message.
        title: Panel title.

    Returns:
        Panel: Rich panel object.
    """
    return Panel(
        Text(message, style="warning"),
        title=title,
        border_style="yellow",
        box=box.ROUNDED,
    )


def format_info(message: str, title: str = "Info") -> Panel:
    """
    Create an info panel.

    Args:
        message: Info message.
        title: Panel title.

    Returns:
        Panel: Rich panel object.
    """
    return Panel(
        Text(message, style="info"),
        title=title,
        border_style="cyan",
        box=box.ROUNDED,
    )


def print_table(
    data: Sequence[Dict[str, Any]],
    title: Optional[str] = None,
    columns: Optional[List[str]] = None,
    max_rows: int = 50,
) -> None:
    """Print a formatted table to the console."""
    table = format_table(data, title, columns, max_rows)
    console.print(table)


def print_dict(data: Dict[str, Any], title: Optional[str] = None) -> None:
    """Print a formatted dictionary to the console."""
    table = format_dict(data, title)
    console.print(table)


def print_error(message: str) -> None:
    """Print an error message to the console."""
    console.print(format_error(message))


def print_success(message: str) -> None:
    """Print a success message to the console."""
    console.print(format_success(message))


def print_warning(message: str) -> None:
    """Print a warning message to the console."""
    console.print(format_warning(message))


def print_info(message: str) -> None:
    """Print an info message to the console."""
    console.print(format_info(message))
