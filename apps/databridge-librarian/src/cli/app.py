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
    category: str = typer.Option(None, "--category", "-c", help="Filter by category"),
):
    """List available hierarchy templates."""
    from src.templates import TemplateService

    service = TemplateService()
    templates = service.list_templates(domain=domain, industry=industry, category=category)

    if not templates:
        console.print("[yellow]No templates found matching the filters.[/yellow]")
        return

    table = Table(title="Hierarchy Templates")
    table.add_column("ID", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Domain", style="blue")
    table.add_column("Industry", style="magenta")
    table.add_column("Hierarchies", style="yellow", justify="right")

    for t in templates:
        table.add_row(
            t.id,
            t.name,
            t.domain,
            t.industry,
            str(t.hierarchy_count),
        )

    console.print(table)


@template_app.command("show")
def template_show(
    template_id: str = typer.Argument(..., help="Template ID to show"),
):
    """Show details of a specific template."""
    from src.templates import TemplateService

    service = TemplateService()
    template = service.get_template(template_id)

    if not template:
        console.print(f"[red]Template not found: {template_id}[/red]")
        raise typer.Exit(1)

    console.print(Panel(
        f"""[bold]{template.name}[/bold]

[cyan]Domain:[/cyan] {template.domain}
[cyan]Category:[/cyan] {template.category}
[cyan]Industry:[/cyan] {template.industry}
[cyan]Version:[/cyan] {template.version}

[cyan]Description:[/cyan]
{template.description}

[cyan]Tags:[/cyan] {', '.join(template.tags) if template.tags else '-'}
[cyan]Hierarchies:[/cyan] {len(template.hierarchies)} nodes""",
        title=f"Template: {template_id}",
        border_style="blue",
    ))

    # Show hierarchy structure
    if template.hierarchies:
        tree_table = Table(title="Hierarchy Structure")
        tree_table.add_column("ID", style="cyan")
        tree_table.add_column("Name", style="green")
        tree_table.add_column("Parent", style="blue")
        tree_table.add_column("Level", style="magenta", justify="right")
        tree_table.add_column("Type", style="yellow")

        for h in sorted(template.hierarchies, key=lambda x: x.sort_order):
            tree_table.add_row(
                h.hierarchy_id,
                h.hierarchy_name,
                h.parent_id or "(root)",
                str(h.level),
                h.node_type,
            )

        console.print(tree_table)


@template_app.command("recommend")
def template_recommend(
    industry: str = typer.Option(None, "--industry", "-i", help="Target industry"),
    statement_type: str = typer.Option(None, "--type", "-t", help="Statement type (pl, balance_sheet, etc.)"),
):
    """Get template recommendations based on industry and needs."""
    from src.templates import TemplateService

    service = TemplateService()
    recommendations = service.recommend_templates(industry=industry, statement_type=statement_type)

    if not recommendations:
        console.print("[yellow]No matching templates found.[/yellow]")
        return

    console.print(Panel(
        f"Based on: industry='{industry or 'any'}', type='{statement_type or 'any'}'",
        title="Template Recommendations",
        border_style="green",
    ))

    table = Table()
    table.add_column("#", style="dim", width=3)
    table.add_column("Template", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Description", style="white")

    for i, t in enumerate(recommendations, 1):
        table.add_row(
            str(i),
            t.id,
            t.name,
            t.description[:60] + "..." if len(t.description) > 60 else t.description,
        )

    console.print(table)


# Skill commands
skill_app = typer.Typer(
    name="skill",
    help="Manage AI skills/personas.",
    no_args_is_help=True,
)
app.add_typer(skill_app, name="skill")


@skill_app.command("list")
def skill_list(
    domain: str = typer.Option(None, "--domain", "-d", help="Filter by domain"),
    industry: str = typer.Option(None, "--industry", "-i", help="Filter by industry"),
):
    """List available AI skills."""
    from src.templates import SkillManager

    manager = SkillManager()
    skills = manager.list_skills(domain=domain, industry=industry)

    if not skills:
        console.print("[yellow]No skills found matching the filters.[/yellow]")
        return

    table = Table(title="Available AI Skills")
    table.add_column("ID", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Domain", style="blue")
    table.add_column("Industries", style="magenta")

    for s in skills:
        table.add_row(
            s.id,
            s.name,
            s.domain,
            ", ".join(s.industries[:3]) + ("..." if len(s.industries) > 3 else ""),
        )

    console.print(table)


@skill_app.command("show")
def skill_show(
    skill_id: str = typer.Argument(..., help="Skill ID to show"),
):
    """Show details of a specific skill."""
    from src.templates import SkillManager

    manager = SkillManager()
    skill = manager.get_skill(skill_id, load_prompt=False)

    if not skill:
        console.print(f"[red]Skill not found: {skill_id}[/red]")
        raise typer.Exit(1)

    console.print(Panel(
        f"""[bold]{skill.name}[/bold]

[cyan]Domain:[/cyan] {skill.domain}
[cyan]Industries:[/cyan] {', '.join(skill.industries)}
[cyan]Communication Style:[/cyan] {skill.communication_style}

[cyan]Description:[/cyan]
{skill.description}

[cyan]Capabilities:[/cyan]
{chr(10).join('  • ' + c for c in skill.capabilities)}

[cyan]Frequently Used Tools:[/cyan]
{chr(10).join('  • ' + t for t in skill.tools_frequently_used) if skill.tools_frequently_used else '  (none specified)'}

[cyan]Prompt File:[/cyan] {skill.prompt_file}
[cyan]Documentation:[/cyan] {skill.documentation_file}""",
        title=f"Skill: {skill_id}",
        border_style="blue",
    ))


@skill_app.command("prompt")
def skill_prompt(
    skill_id: str = typer.Argument(..., help="Skill ID"),
    output: str = typer.Option(None, "--output", "-o", help="Save to file"),
):
    """Get the system prompt for a skill."""
    from src.templates import SkillManager

    manager = SkillManager()
    prompt = manager.get_prompt(skill_id)

    if not prompt:
        console.print(f"[red]Skill or prompt not found: {skill_id}[/red]")
        raise typer.Exit(1)

    if output:
        from pathlib import Path
        Path(output).write_text(prompt, encoding="utf-8")
        console.print(f"[green]Prompt saved to: {output}[/green]")
    else:
        console.print(Panel(
            prompt,
            title=f"System Prompt: {skill_id}",
            border_style="green",
        ))


@skill_app.command("recommend")
def skill_recommend(
    industry: str = typer.Option(None, "--industry", "-i", help="Target industry"),
    task_type: str = typer.Option(None, "--task", "-t", help="Task type (analysis, reconciliation, etc.)"),
):
    """Get skill recommendation based on context."""
    from src.templates import SkillManager

    manager = SkillManager()
    skill = manager.recommend_skill(industry=industry, task_type=task_type)

    if not skill:
        console.print("[yellow]No matching skill found.[/yellow]")
        return

    console.print(Panel(
        f"""[bold]Recommended Skill:[/bold] {skill.id}

[cyan]Name:[/cyan] {skill.name}
[cyan]Domain:[/cyan] {skill.domain}
[cyan]Industries:[/cyan] {', '.join(skill.industries)}

[cyan]Capabilities:[/cyan]
{chr(10).join('  • ' + c for c in skill.capabilities)}

[dim]Use 'librarian skill prompt {skill.id}' to get the system prompt.[/dim]""",
        title="Skill Recommendation",
        border_style="green",
    ))


@skill_app.command("domains")
def skill_domains():
    """List available skill domains."""
    from src.templates import SkillManager

    manager = SkillManager()
    domains = manager.get_domains()

    if not domains:
        console.print("[yellow]No domains found.[/yellow]")
        return

    console.print("[bold]Available Skill Domains:[/bold]")
    for domain in domains:
        console.print(f"  • {domain}")


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
