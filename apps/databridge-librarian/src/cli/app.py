"""
DataBridge AI V3 CLI Application.

A pure Python CLI for building and managing financial hierarchies.
"""

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src import __version__, __app_name__
from src.core.config import get_settings
from src.core.database import init_database

# Initialize console for rich output
console = Console()

# Create main application
app = typer.Typer(
    name="databridge",
    help=f"{__app_name__} - Financial Hierarchy Builder CLI",
    add_completion=True,
    no_args_is_help=True,
    rich_markup_mode="rich",
)


# =============================================================================
# CALLBACKS
# =============================================================================


def version_callback(value: bool):
    """Show version and exit."""
    if value:
        console.print(f"[bold blue]{__app_name__}[/bold blue] version [green]{__version__}[/green]")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        None,
        "--version",
        "-v",
        callback=version_callback,
        is_eager=True,
        help="Show version and exit.",
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        "-d",
        help="Enable debug mode.",
    ),
):
    """
    DataBridge AI V3 - Headless Financial Hierarchy Builder.

    Build and manage hierarchical data structures for financial reporting.
    Includes 92 MCP tools for AI integration.
    """
    if debug:
        import os
        os.environ["DATABRIDGE_DEBUG"] = "true"


# =============================================================================
# SUBCOMMAND GROUPS (stubs - to be implemented in separate files)
# =============================================================================

# Project commands
project_app = typer.Typer(
    name="project",
    help="Manage hierarchy projects.",
    no_args_is_help=True,
)
app.add_typer(project_app, name="project")


@project_app.command("list")
def project_list():
    """List all hierarchy projects."""
    from src.core.database import session_scope, Project

    init_database()

    with session_scope() as session:
        projects = session.query(Project).all()

        if not projects:
            console.print("[yellow]No projects found.[/yellow]")
            return

        table = Table(title="Hierarchy Projects")
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Name", style="green")
        table.add_column("Industry", style="blue")
        table.add_column("Created", style="magenta")

        for p in projects:
            table.add_row(
                p.id[:8] + "...",
                p.name,
                p.industry or "-",
                p.created_at.strftime("%Y-%m-%d") if p.created_at else "-",
            )

        console.print(table)


@project_app.command("create")
def project_create(
    name: str = typer.Argument(..., help="Project name"),
    description: str = typer.Option("", "--description", "-d", help="Project description"),
    industry: str = typer.Option(None, "--industry", "-i", help="Industry category"),
):
    """Create a new hierarchy project."""
    from src.core.database import session_scope, Project
    from src.core.audit import log_action

    init_database()

    with session_scope() as session:
        # Check for duplicate
        existing = session.query(Project).filter(Project.name == name).first()
        if existing:
            console.print(f"[red]Error: Project '{name}' already exists.[/red]")
            raise typer.Exit(1)

        project = Project(
            name=name,
            description=description,
            industry=industry,
        )
        session.add(project)
        session.flush()

        log_action(
            action="create_project",
            entity_type="project",
            entity_id=project.id,
            details={"name": name, "industry": industry},
        )

        console.print(f"[green]Created project:[/green] {project.id}")
        console.print(f"  Name: {name}")
        if description:
            console.print(f"  Description: {description}")
        if industry:
            console.print(f"  Industry: {industry}")


# Hierarchy commands
hierarchy_app = typer.Typer(
    name="hierarchy",
    help="Manage hierarchies within a project.",
    no_args_is_help=True,
)
app.add_typer(hierarchy_app, name="hierarchy")


@hierarchy_app.command("list")
def hierarchy_list(
    project_id: str = typer.Argument(..., help="Project ID"),
):
    """List hierarchies in a project."""
    from src.core.database import session_scope, Hierarchy, Project

    init_database()

    with session_scope() as session:
        project = session.query(Project).filter(Project.id.like(f"{project_id}%")).first()
        if not project:
            console.print(f"[red]Project not found: {project_id}[/red]")
            raise typer.Exit(1)

        hierarchies = (
            session.query(Hierarchy)
            .filter(Hierarchy.project_id == project.id)
            .filter(Hierarchy.is_current == True)
            .order_by(Hierarchy.sort_order)
            .all()
        )

        if not hierarchies:
            console.print(f"[yellow]No hierarchies in project '{project.name}'.[/yellow]")
            return

        table = Table(title=f"Hierarchies in '{project.name}'")
        table.add_column("ID", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Parent", style="blue")
        table.add_column("Level", style="magenta")

        for h in hierarchies:
            table.add_row(
                h.hierarchy_id,
                h.hierarchy_name,
                h.parent_id or "(root)",
                str(h.get_depth()),
            )

        console.print(table)


# CSV commands
csv_app = typer.Typer(
    name="csv",
    help="Import and export CSV files.",
    no_args_is_help=True,
)
app.add_typer(csv_app, name="csv")


@csv_app.command("import")
def csv_import(
    type: str = typer.Argument(..., help="Type: 'hierarchy' or 'mapping'"),
    project_id: str = typer.Argument(..., help="Project ID"),
    file_path: str = typer.Argument(..., help="Path to CSV file"),
    legacy: bool = typer.Option(False, "--legacy", help="Use legacy CSV format"),
):
    """Import hierarchy or mapping from CSV."""
    console.print(f"[yellow]Import {type} from {file_path} (legacy={legacy})[/yellow]")
    console.print("[dim]Implementation pending in Phase 2...[/dim]")


@csv_app.command("export")
def csv_export(
    type: str = typer.Argument(..., help="Type: 'hierarchy' or 'mapping'"),
    project_id: str = typer.Argument(..., help="Project ID"),
    output_path: str = typer.Argument(..., help="Output file path"),
):
    """Export hierarchy or mapping to CSV."""
    console.print(f"[yellow]Export {type} to {output_path}[/yellow]")
    console.print("[dim]Implementation pending in Phase 2...[/dim]")


# Connection commands
connection_app = typer.Typer(
    name="connection",
    help="Manage database connections.",
    no_args_is_help=True,
)
app.add_typer(connection_app, name="connection")


@connection_app.command("list")
def connection_list():
    """List all database connections."""
    from src.core.database import session_scope, Connection

    init_database()

    with session_scope() as session:
        connections = session.query(Connection).all()

        if not connections:
            console.print("[yellow]No connections configured.[/yellow]")
            return

        table = Table(title="Database Connections")
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Name", style="green")
        table.add_column("Type", style="blue")
        table.add_column("Host", style="magenta")
        table.add_column("Active", style="yellow")

        for c in connections:
            table.add_row(
                c.id[:8] + "...",
                c.name,
                c.connection_type,
                c.host or "-",
                "Yes" if c.is_active else "No",
            )

        console.print(table)


# Template commands
template_app = typer.Typer(
    name="template",
    help="Manage hierarchy templates.",
    no_args_is_help=True,
)
app.add_typer(template_app, name="template")


@template_app.command("list")
def template_list(
    domain: str = typer.Option(None, "--domain", "-d", help="Filter by domain"),
    industry: str = typer.Option(None, "--industry", "-i", help="Filter by industry"),
):
    """List available hierarchy templates."""
    console.print("[dim]Implementation pending in Phase 4...[/dim]")


# Skill commands
skill_app = typer.Typer(
    name="skill",
    help="Manage AI skills.",
    no_args_is_help=True,
)
app.add_typer(skill_app, name="skill")


@skill_app.command("list")
def skill_list():
    """List available AI skills."""
    import json
    from pathlib import Path

    skills_index = Path("skills/index.json")
    if not skills_index.exists():
        console.print("[yellow]Skills index not found.[/yellow]")
        return

    with open(skills_index) as f:
        data = json.load(f)

    table = Table(title="Available Skills")
    table.add_column("ID", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Domain", style="blue")

    for skill in data.get("skills", []):
        table.add_row(
            skill.get("id", ""),
            skill.get("name", ""),
            skill.get("domain", ""),
        )

    console.print(table)


# MCP commands
mcp_app = typer.Typer(
    name="mcp",
    help="MCP server commands.",
    no_args_is_help=True,
)
app.add_typer(mcp_app, name="mcp")


@mcp_app.command("serve")
def mcp_serve():
    """Start the MCP server."""
    console.print("[bold blue]Starting MCP Server...[/bold blue]")
    console.print(f"[dim]Server: {__app_name__} v{__version__}[/dim]")
    console.print("[dim]Implementation pending in Phase 8...[/dim]")


# =============================================================================
# STANDALONE COMMANDS
# =============================================================================


@app.command("init")
def init_command():
    """Initialize the DataBridge database and directories."""
    settings = get_settings()
    settings.ensure_directories()
    init_database()

    console.print(Panel(
        f"""[green]DataBridge AI V3 initialized successfully![/green]

Database: {settings.database.path}
Data directory: {settings.data.dir}
Vector store: {settings.vector.db_path}

Run [bold]databridge --help[/bold] to see available commands.""",
        title="Initialization Complete",
    ))


@app.command("info")
def info_command():
    """Show configuration and system information."""
    settings = get_settings()

    console.print(Panel(
        f"""[bold]{__app_name__}[/bold] v{__version__}

[cyan]Configuration:[/cyan]
  Database: {settings.database.path}
  Data directory: {settings.data.dir}
  Debug mode: {settings.debug}

[cyan]Vector Store:[/cyan]
  Provider: {settings.vector.provider}
  Model: {settings.vector.model}
  Path: {settings.vector.db_path}

[cyan]Backend Sync:[/cyan]
  Enabled: {settings.backend_sync.enabled}
  URL: {settings.backend_sync.url}

[cyan]Security:[/cyan]
  Master key configured: {settings.security.master_key is not None}
  2FA enabled: {settings.security.two_factor_enabled}""",
        title="System Information",
    ))


@app.command("shell")
def shell_command():
    """Start interactive REPL shell."""
    console.print("[bold blue]DataBridge Interactive Shell[/bold blue]")
    console.print("[dim]Implementation pending...[/dim]")
    console.print("Type 'exit' to quit.")


# =============================================================================
# ENTRY POINT
# =============================================================================


def main_cli():
    """Main entry point for the CLI."""
    app()


if __name__ == "__main__":
    main_cli()
