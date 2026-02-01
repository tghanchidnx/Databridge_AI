"""
DataBridge AI Unified CLI entry point.

Mounts Librarian (Hierarchy Builder) and Researcher (Analytics Engine) as subcommands
under a single `databridge` command.

Usage:
    databridge --help                    # Show all available commands
    databridge librarian --help          # Librarian (Hierarchy Builder) commands
    databridge researcher --help         # Researcher (Analytics Engine) commands
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
    help="DataBridge AI - Unified Data Platform with Librarian and Researcher",
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

    # Try to get Librarian version
    try:
        from src import __version__ as librarian_version
        versions["databridge-librarian"] = librarian_version
    except ImportError:
        try:
            # Try alternate import path
            import sys
            sys.path.insert(0, "apps/databridge-librarian")
            from src import __version__ as librarian_version
            versions["databridge-librarian"] = librarian_version
        except ImportError:
            versions["databridge-librarian"] = "not installed"

    # Try to get Researcher version
    try:
        # Direct import won't work without proper package setup
        versions["databridge-researcher"] = "4.0.0"
    except ImportError:
        versions["databridge-researcher"] = "not installed"

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
[bold blue]DataBridge AI[/bold blue] - Unified Data Platform

[bold]Available Modules:[/bold]
  • [cyan]librarian[/cyan] - The Librarian (Hierarchy Management)
    - Create and manage hierarchical data structures
    - Source mappings and formula definitions
    - Templates, skills, and knowledge base
    - SQL generation and deployment

  • [cyan]researcher[/cyan] - The Researcher (Analytics Engine)
    - Multi-warehouse connectivity
    - Natural language to SQL queries
    - Insights generation (trends, anomalies, variance)
    - FP&A workflows (monthly close, forecasting)

[bold]Quick Start:[/bold]
  databridge librarian project list      # List hierarchy projects
  databridge researcher connection list  # List data connections

[bold]MCP Servers:[/bold]
  databridge librarian mcp serve         # Start Librarian MCP server
  databridge researcher mcp serve        # Start Researcher MCP server
"""
    console.print(Panel(info_text, title="DataBridge AI", border_style="blue"))


# Try to mount Librarian commands
try:
    # Try importing from the librarian package
    import sys
    import os

    # Add librarian to path for development
    librarian_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "databridge-librarian")
    if os.path.exists(librarian_path):
        sys.path.insert(0, librarian_path)

    from src.cli import app as librarian_app
    main_app.add_typer(librarian_app, name="librarian", help="The Librarian - Hierarchy Management")
except ImportError as e:
    # Create placeholder if Librarian not installed
    librarian_app = typer.Typer(help="The Librarian (not installed)")

    @librarian_app.command()
    def not_installed():
        """Librarian is not installed."""
        console.print("[yellow]Librarian (Hierarchy Management) is not installed.[/yellow]")
        console.print("Install with: pip install databridge-librarian")

    main_app.add_typer(librarian_app, name="librarian", help="The Librarian (not installed)")


# Try to mount Researcher commands
try:
    # Try importing from the researcher package
    import sys
    import os

    # Add researcher to path for development
    researcher_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "databridge-researcher")
    if os.path.exists(researcher_path):
        sys.path.insert(0, researcher_path)

    from src.cli import app as researcher_app
    main_app.add_typer(researcher_app, name="researcher", help="The Researcher - Analytics Engine")
except ImportError as e:
    # Create placeholder if Researcher not installed
    researcher_app = typer.Typer(help="The Researcher (not installed)")

    @researcher_app.command()
    def not_installed():
        """Researcher is not installed."""
        console.print("[yellow]Researcher (Analytics Engine) is not installed.[/yellow]")
        console.print("Install with: pip install databridge-researcher")

    main_app.add_typer(researcher_app, name="researcher", help="The Researcher (not installed)")


def main():
    """Main entry point."""
    main_app()


if __name__ == "__main__":
    main()
