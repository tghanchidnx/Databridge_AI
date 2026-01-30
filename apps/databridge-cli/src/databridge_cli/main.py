"""
DataBridge AI Unified CLI entry point.

Mounts V3 (Hierarchy Builder) and V4 (Analytics Engine) as subcommands
under a single `databridge` command.

Usage:
    databridge --help                    # Show all available commands
    databridge hierarchy --help          # V3 Hierarchy Builder commands
    databridge analytics --help          # V4 Analytics Engine commands
    databridge version                   # Show version info
"""

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

# Create main Typer app
main_app = typer.Typer(
    name="databridge",
    help="DataBridge AI - Unified Data Reconciliation and Analytics Platform",
    no_args_is_help=True,
)

console = Console()


def get_version_info() -> dict:
    """Get version information for all components."""
    versions = {
        "databridge-cli": "1.0.0",
    }

    try:
        from databridge_core import __version__ as core_version
        versions["databridge-core"] = core_version
    except ImportError:
        versions["databridge-core"] = "not installed"

    try:
        from databridge_models import __version__ as models_version
        versions["databridge-models"] = models_version
    except ImportError:
        versions["databridge-models"] = "not installed"

    # Try to get V3 version
    try:
        import databridge_v3
        versions["databridge-v3 (hierarchy)"] = getattr(databridge_v3, "__version__", "3.0.0")
    except ImportError:
        versions["databridge-v3 (hierarchy)"] = "not installed"

    # Try to get V4 version
    try:
        import databridge_v4
        versions["databridge-v4 (analytics)"] = getattr(databridge_v4, "__version__", "4.0.0")
    except ImportError:
        versions["databridge-v4 (analytics)"] = "not installed"

    return versions


@main_app.command()
def version():
    """Show version information for all DataBridge components."""
    versions = get_version_info()

    table = Table(
        title="DataBridge AI Version Info",
        box=box.ROUNDED,
        show_header=True,
        header_style="bold blue",
    )
    table.add_column("Component", style="cyan")
    table.add_column("Version", style="green")

    for component, ver in versions.items():
        style = "dim" if ver == "not installed" else None
        table.add_row(component, ver, style=style)

    console.print(table)


@main_app.command()
def info():
    """Show information about DataBridge AI platform."""
    info_text = """
[bold blue]DataBridge AI[/bold blue] - Unified Data Reconciliation and Analytics Platform

[bold]Available Modules:[/bold]
  • [cyan]hierarchy[/cyan] - V3 Hierarchy Builder
    - Create and manage hierarchical data structures
    - Source mappings and formula definitions
    - SQL generation and deployment

  • [cyan]analytics[/cyan] - V4 Analytics Engine
    - Multi-warehouse connectivity
    - Dynamic table generation
    - NLP-to-SQL queries
    - FP&A workflows

[bold]Quick Start:[/bold]
  databridge hierarchy project list     # List hierarchy projects
  databridge analytics connect list     # List data connections

[bold]Documentation:[/bold]
  https://github.com/databridge-ai/documentation
"""
    console.print(Panel(info_text, title="DataBridge AI", border_style="blue"))


# Try to mount V3 (Hierarchy Builder) commands
try:
    # V3 may have its own Typer app we can mount
    from databridge_v3.cli import app as v3_app
    main_app.add_typer(v3_app, name="hierarchy", help="V3 Hierarchy Builder commands")
except ImportError:
    # Create placeholder if V3 not installed
    v3_app = typer.Typer(help="V3 Hierarchy Builder (not installed)")

    @v3_app.command()
    def not_installed():
        """V3 Hierarchy Builder is not installed."""
        console.print("[yellow]V3 Hierarchy Builder is not installed.[/yellow]")
        console.print("Install with: pip install databridge-v3")

    main_app.add_typer(v3_app, name="hierarchy", help="V3 Hierarchy Builder (not installed)")


# Try to mount V4 (Analytics Engine) commands
try:
    # V4 may have its own Typer app we can mount
    from databridge_v4.cli import app as v4_app
    main_app.add_typer(v4_app, name="analytics", help="V4 Analytics Engine commands")
except ImportError:
    # Create placeholder if V4 not installed
    v4_app = typer.Typer(help="V4 Analytics Engine (not installed)")

    @v4_app.command()
    def not_installed():
        """V4 Analytics Engine is not installed."""
        console.print("[yellow]V4 Analytics Engine is not installed.[/yellow]")
        console.print("Install with: pip install databridge-v4")

    main_app.add_typer(v4_app, name="analytics", help="V4 Analytics Engine (not installed)")


def main():
    """Main entry point."""
    main_app()


if __name__ == "__main__":
    main()
