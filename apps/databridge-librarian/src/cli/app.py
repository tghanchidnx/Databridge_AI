"""
DataBridge AI Librarian CLI Application.

A pure Python CLI for building and managing financial hierarchies.
"""

from typing import List, Optional

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
    DataBridge AI Librarian - Headless Financial Hierarchy Builder.

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


# Source commands (Phase 1.3 - Interactive Refinement)
source_app = typer.Typer(
    name="source",
    help="Manage source data models and mappings.",
    no_args_is_help=True,
)
app.add_typer(source_app, name="source")


@source_app.command("list")
def source_list():
    """List all source models."""
    from src.source import SourceModelStore

    store = SourceModelStore()
    models = store.list_models()

    if not models:
        console.print("[yellow]No source models found.[/yellow]")
        console.print("[dim]Use 'databridge source analyze' to create one.[/dim]")
        return

    table = Table(title="Source Models")
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Name", style="green")
    table.add_column("Status", style="blue")
    table.add_column("Tables", style="magenta", justify="right")
    table.add_column("Entities", style="yellow", justify="right")
    table.add_column("Updated", style="dim")

    for m in models:
        status_style = {
            "draft": "yellow",
            "reviewed": "blue",
            "approved": "green",
        }.get(m.status, "white")

        table.add_row(
            m.id[:8],
            m.name,
            f"[{status_style}]{m.status}[/{status_style}]",
            str(len(m.tables)),
            str(len(m.entities)),
            m.updated_at.strftime("%Y-%m-%d %H:%M"),
        )

    console.print(table)


@source_app.command("analyze")
def source_analyze(
    connection_name: str = typer.Argument(..., help="Connection name to analyze"),
    database: str = typer.Option(None, "--database", "-d", help="Database to analyze"),
    schema: str = typer.Option(None, "--schema", "-s", help="Schema to analyze"),
    name: str = typer.Option(None, "--name", "-n", help="Model name"),
):
    """Analyze a database connection and create a source model."""
    from src.core.database import session_scope, Connection, init_database
    from src.source import SourceModelStore, SourceAnalyzer

    init_database()

    # Find connection
    with session_scope() as session:
        conn = session.query(Connection).filter(
            Connection.name.ilike(f"%{connection_name}%")
        ).first()

        if not conn:
            console.print(f"[red]Connection not found: {connection_name}[/red]")
            raise typer.Exit(1)

        if conn.connection_type != "snowflake":
            console.print(f"[yellow]Note: Only Snowflake is fully supported. Found: {conn.connection_type}[/yellow]")

        console.print(f"[dim]Connecting to {conn.name}...[/dim]")

        # Create adapter based on connection type
        if conn.connection_type == "snowflake":
            from src.connections.adapters import SnowflakeAdapter

            adapter = SnowflakeAdapter(
                host=conn.host,
                database=database or conn.database,
                username=conn.username,
                password=conn.password_encrypted,  # TODO: decrypt
                extra_config=conn.extra_config or {},
            )
        else:
            console.print(f"[red]Unsupported connection type: {conn.connection_type}[/red]")
            raise typer.Exit(1)

        # Run analysis
        console.print(f"[dim]Analyzing schema...[/dim]")

        try:
            with adapter:
                analyzer = SourceAnalyzer(adapter)
                model = analyzer.analyze_schema(
                    database=database or conn.database,
                    schema=schema,
                    name=name or f"Analysis of {conn.name}",
                )

                # Save the model
                store = SourceModelStore()
                store.save_model(model)

                console.print(Panel(
                    f"""[green]Analysis complete![/green]

Model ID: [cyan]{model.id}[/cyan]
Tables: {len(model.tables)}
Entities: {len(model.entities)}
Relationships: {len(model.relationships)}

Use [bold]databridge source review {model.id[:8]}[/bold] to review the model.""",
                    title="Source Analysis",
                    border_style="green",
                ))

        except Exception as e:
            console.print(f"[red]Analysis failed: {e}[/red]")
            raise typer.Exit(1)


@source_app.command("discover")
def source_discover(
    connection_type: str = typer.Option("snowflake", "--type", "-t", help="Connection type (snowflake)"),
    account: str = typer.Option(..., "--account", "-a", help="Account/host identifier"),
    username: str = typer.Option(..., "--username", "-u", help="Database username"),
    password: str = typer.Option(None, "--password", "-p", help="Password (prompts if not provided)"),
    database: str = typer.Option(..., "--database", "-d", help="Database to scan"),
    schema: str = typer.Option(None, "--schema", "-s", help="Schema to scan (optional)"),
    warehouse: str = typer.Option(None, "--warehouse", "-w", help="Snowflake warehouse"),
    name: str = typer.Option(None, "--name", "-n", help="Model name"),
    include_views: bool = typer.Option(True, "--include-views/--no-views", help="Include views"),
):
    """
    Run full source discovery on a database.

    Scans the schema, analyzes tables, infers entities and relationships,
    and creates a canonical source model ready for review.

    Example:
        databridge source discover -a myorg.snowflakecomputing.com -u user -d ANALYTICS -s RAW
    """
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
    from src.source import SourceDiscoveryService, DiscoveryConfig, DiscoveryProgress

    # Prompt for password if not provided
    if not password:
        import getpass
        password = getpass.getpass(f"Password for {username}: ")

    if not password:
        console.print("[red]Password is required[/red]")
        raise typer.Exit(1)

    # Create adapter
    if connection_type.lower() == "snowflake":
        try:
            from src.connections.adapters import SnowflakeAdapter

            adapter = SnowflakeAdapter(
                account=account,
                username=username,
                password=password,
                warehouse=warehouse or "",
                database=database,
                schema=schema or "",
            )
        except ImportError:
            console.print("[red]Snowflake adapter not available. Install snowflake-connector-python.[/red]")
            raise typer.Exit(1)
    else:
        console.print(f"[red]Unsupported connection type: {connection_type}[/red]")
        raise typer.Exit(1)

    # Configure discovery
    config = DiscoveryConfig(include_views=include_views)
    service = SourceDiscoveryService(config=config)

    # Progress display
    console.print(f"\n[bold]Source Discovery[/bold]")
    console.print(f"Target: {account} / {database}" + (f" / {schema}" if schema else ""))
    console.print()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        console=console,
    ) as progress:
        task = progress.add_task("Initializing...", total=100)

        def on_progress(p: DiscoveryProgress):
            progress.update(
                task,
                completed=int(p.overall_progress * 100),
                description=f"{p.phase.value.replace('_', ' ').title()}...",
            )

        service.set_progress_callback(on_progress)

        result = service.discover(
            adapter=adapter,
            database=database,
            schema=schema,
            model_name=name or f"Discovery of {database}" + (f".{schema}" if schema else ""),
        )

    # Show results
    if result.status == "completed":
        console.print(Panel(
            f"""[green]Discovery complete![/green]

Model ID: [cyan]{result.model_id}[/cyan]
Duration: {result.duration_seconds:.1f}s

[bold]Summary:[/bold]
  Tables discovered: {result.tables_discovered}
  Columns discovered: {result.columns_discovered}
  Entities inferred: {result.entities_inferred}
  Relationships inferred: {result.relationships_inferred}

[bold]Quality:[/bold]
  High confidence entities: {result.high_confidence_entities}
  Low confidence entities: {result.low_confidence_entities}
  Tables needing review: {len(result.tables_needing_review)}

Use [bold]databridge source review {result.model_id[:8]}[/bold] to review the model.""",
            title="Source Discovery",
            border_style="green",
        ))

        if result.warnings:
            console.print("\n[yellow]Warnings:[/yellow]")
            for w in result.warnings:
                console.print(f"  • {w}")
    else:
        console.print(Panel(
            f"""[red]Discovery failed![/red]

{chr(10).join(result.errors)}""",
            title="Source Discovery",
            border_style="red",
        ))
        raise typer.Exit(1)


@source_app.command("review")
def source_review(
    model_id: str = typer.Argument(..., help="Model ID to review"),
):
    """Review an analyzed source model."""
    from src.source import SourceModelStore

    store = SourceModelStore()

    # Find model by partial ID
    models = store.list_models()
    model = next((m for m in models if m.id.startswith(model_id)), None)

    if not model:
        console.print(f"[red]Model not found: {model_id}[/red]")
        raise typer.Exit(1)

    # Show model summary
    status_colors = {"draft": "yellow", "reviewed": "blue", "approved": "green"}
    status_color = status_colors.get(model.status, "white")

    console.print(Panel(
        f"""[bold]{model.name}[/bold]

Status: [{status_color}]{model.status}[/{status_color}]
Connection: {model.connection_name or "(none)"}
Created: {model.created_at.strftime("%Y-%m-%d %H:%M")}
Updated: {model.updated_at.strftime("%Y-%m-%d %H:%M")}""",
        title=f"Source Model: {model.id[:8]}",
        border_style="blue",
    ))

    # Show approval progress
    progress = model.approval_progress
    console.print(f"\n[bold]Approval Progress:[/bold] {progress['overall_progress']:.0%}")
    console.print(f"  Entities: {progress['entities']['approved']}/{progress['entities']['total']} approved")
    console.print(f"  Relationships: {progress['relationships']['approved']}/{progress['relationships']['total']} approved")

    # Show tables
    if model.tables:
        console.print(f"\n[bold]Tables ({len(model.tables)}):[/bold]")
        table = Table()
        table.add_column("Table", style="cyan")
        table.add_column("Type", style="blue")
        table.add_column("Entity", style="green")
        table.add_column("Columns", justify="right")
        table.add_column("Confidence", justify="right")

        for t in model.tables[:10]:  # Show first 10
            table.add_row(
                t.name,
                t.table_type,
                t.effective_entity_type.value if t.effective_entity_type else "-",
                str(len(t.columns)),
                f"{t.confidence:.0%}" if t.confidence else "-",
            )

        if len(model.tables) > 10:
            table.add_row("[dim]...[/dim]", "", "", "", "")

        console.print(table)

    # Show entities
    if model.entities:
        console.print(f"\n[bold]Entities ({len(model.entities)}):[/bold]")
        for e in model.entities:
            status = "[green]✓[/green]" if e.approved else "[red]✗[/red]" if e.rejected else "[yellow]?[/yellow]"
            console.print(f"  {status} {e.name} ({e.entity_type.value}) - {len(e.source_tables)} table(s)")

    # Show relationships
    if model.relationships:
        console.print(f"\n[bold]Relationships ({len(model.relationships)}):[/bold]")
        for r in model.relationships[:5]:
            status = "[green]✓[/green]" if r.approved else "[red]✗[/red]" if r.rejected else "[yellow]?[/yellow]"
            console.print(f"  {status} {r.source_entity} → {r.target_entity} ({r.relationship_type.value})")

        if len(model.relationships) > 5:
            console.print(f"  [dim]... and {len(model.relationships) - 5} more[/dim]")

    console.print(f"\n[dim]Use 'databridge source link' to define joins, 'databridge source merge' to consolidate columns.[/dim]")


@source_app.command("link")
def source_link(
    model_id: str = typer.Argument(..., help="Model ID"),
    source: str = typer.Option(..., "--source", "-s", help="Source table/entity"),
    target: str = typer.Option(..., "--target", "-t", help="Target table/entity"),
    source_col: str = typer.Option(..., "--source-col", "-sc", help="Source column"),
    target_col: str = typer.Option(..., "--target-col", "-tc", help="Target column"),
    relationship_type: str = typer.Option("many_to_one", "--type", help="Relationship type"),
):
    """Define a join between two tables/entities."""
    from src.source import SourceModelStore
    from src.source.models import RelationshipType

    store = SourceModelStore()

    # Find model
    models = store.list_models()
    model = next((m for m in models if m.id.startswith(model_id)), None)

    if not model:
        console.print(f"[red]Model not found: {model_id}[/red]")
        raise typer.Exit(1)

    # Parse relationship type
    try:
        rel_type = RelationshipType(relationship_type)
    except ValueError:
        console.print(f"[red]Invalid relationship type: {relationship_type}[/red]")
        console.print(f"[dim]Valid types: {', '.join(t.value for t in RelationshipType)}[/dim]")
        raise typer.Exit(1)

    # Add relationship
    rel = model.add_relationship(
        source=source,
        target=target,
        source_columns=[source_col],
        target_columns=[target_col],
        relationship_type=rel_type,
        confidence=1.0,
        inferred_by="user",
    )

    store.save_model(model)

    console.print(f"[green]Created relationship:[/green] {rel.name}")
    console.print(f"  {source}.{source_col} → {target}.{target_col}")
    console.print(f"  Type: {rel_type.value}")


@source_app.command("merge")
def source_merge(
    model_id: str = typer.Argument(..., help="Model ID"),
    canonical_name: str = typer.Option(..., "--name", "-n", help="Canonical column name"),
    columns: List[str] = typer.Option(..., "--column", "-c", help="Source columns to merge (can specify multiple)"),
    description: str = typer.Option("", "--description", "-d", help="Description"),
):
    """Merge multiple source columns into one canonical column."""
    from src.source import SourceModelStore

    store = SourceModelStore()

    # Find model
    models = store.list_models()
    model = next((m for m in models if m.id.startswith(model_id)), None)

    if not model:
        console.print(f"[red]Model not found: {model_id}[/red]")
        raise typer.Exit(1)

    # Add column merge
    merge = model.add_column_merge(
        canonical_name=canonical_name,
        source_columns=list(columns),
        description=description,
    )

    store.save_model(model)

    console.print(f"[green]Created column merge:[/green] {canonical_name}")
    for col in columns:
        console.print(f"  ← {col}")


@source_app.command("approve")
def source_approve(
    model_id: str = typer.Argument(..., help="Model ID"),
    entity: str = typer.Option(None, "--entity", "-e", help="Entity name to approve"),
    relationship: str = typer.Option(None, "--relationship", "-r", help="Relationship ID to approve"),
    all_flag: bool = typer.Option(False, "--all", "-a", help="Approve all pending items"),
):
    """Approve entities or relationships in a model."""
    from src.source import SourceModelStore

    store = SourceModelStore()

    # Find model
    models = store.list_models()
    model = next((m for m in models if m.id.startswith(model_id)), None)

    if not model:
        console.print(f"[red]Model not found: {model_id}[/red]")
        raise typer.Exit(1)

    approved_count = 0

    if all_flag:
        for e in model.entities:
            if not e.approved and not e.rejected:
                e.approved = True
                approved_count += 1
        for r in model.relationships:
            if not r.approved and not r.rejected:
                r.approved = True
                approved_count += 1
        console.print(f"[green]Approved {approved_count} items[/green]")

    elif entity:
        if model.approve_entity(entity):
            console.print(f"[green]Approved entity: {entity}[/green]")
        else:
            console.print(f"[red]Entity not found: {entity}[/red]")
            raise typer.Exit(1)

    elif relationship:
        if model.approve_relationship(relationship):
            console.print(f"[green]Approved relationship: {relationship}[/green]")
        else:
            console.print(f"[red]Relationship not found: {relationship}[/red]")
            raise typer.Exit(1)

    else:
        console.print("[yellow]Specify --entity, --relationship, or --all[/yellow]")
        raise typer.Exit(1)

    store.save_model(model)


@source_app.command("rename")
def source_rename(
    model_id: str = typer.Argument(..., help="Model ID"),
    old_name: str = typer.Option(..., "--from", help="Current entity name"),
    new_name: str = typer.Option(..., "--to", help="New entity name"),
):
    """Rename an entity in a source model."""
    from src.source import SourceModelStore

    store = SourceModelStore()

    # Find model
    models = store.list_models()
    model = next((m for m in models if m.id.startswith(model_id)), None)

    if not model:
        console.print(f"[red]Model not found: {model_id}[/red]")
        raise typer.Exit(1)

    if model.rename_entity(old_name, new_name):
        store.save_model(model)
        console.print(f"[green]Renamed entity: {old_name} → {new_name}[/green]")
    else:
        console.print(f"[red]Entity not found: {old_name}[/red]")
        raise typer.Exit(1)


@source_app.command("delete")
def source_delete(
    model_id: str = typer.Argument(..., help="Model ID to delete"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
):
    """Delete a source model."""
    from src.source import SourceModelStore

    store = SourceModelStore()

    # Find model
    models = store.list_models()
    model = next((m for m in models if m.id.startswith(model_id)), None)

    if not model:
        console.print(f"[red]Model not found: {model_id}[/red]")
        raise typer.Exit(1)

    if not force:
        confirm = typer.confirm(f"Delete model '{model.name}'?")
        if not confirm:
            console.print("[yellow]Cancelled[/yellow]")
            raise typer.Exit(0)

    if store.delete_model(model.id):
        console.print(f"[green]Deleted model: {model.name}[/green]")
    else:
        console.print(f"[red]Failed to delete model[/red]")
        raise typer.Exit(1)


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
        f"""[green]DataBridge AI Librarian initialized successfully![/green]

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
