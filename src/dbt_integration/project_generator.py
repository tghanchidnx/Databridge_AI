"""
dbt Project Generator.

Scaffolds dbt projects with:
- dbt_project.yml configuration
- profiles.yml template
- Directory structure (models/, seeds/, tests/, etc.)
- README and gitignore
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml

from .types import (
    DbtProject,
    DbtProjectConfig,
    DbtMaterialization,
    CiCdConfig,
)

logger = logging.getLogger(__name__)


class DbtProjectGenerator:
    """Generates dbt project scaffolding."""

    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir) if output_dir else Path("data/dbt_projects")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._projects: Dict[str, DbtProject] = {}
        self._load()

    def create_project(
        self,
        name: str,
        profile: str,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        hierarchy_project_id: Optional[str] = None,
        include_cicd: bool = False,
    ) -> DbtProject:
        """
        Create a new dbt project.

        Args:
            name: Project name (will be slugified)
            profile: dbt profile name for connections
            target_database: Target database name
            target_schema: Target schema name
            hierarchy_project_id: Link to DataBridge hierarchy project
            include_cicd: Whether to include CI/CD configuration

        Returns:
            DbtProject instance
        """
        # Slugify name
        slug = self._slugify(name)

        if slug in self._projects:
            raise ValueError(f"Project '{slug}' already exists")

        config = DbtProjectConfig(
            name=slug,
            profile=profile,
            target_database=target_database,
            target_schema=target_schema,
            hierarchy_project_id=hierarchy_project_id,
        )

        project = DbtProject(
            config=config,
            cicd_config=CiCdConfig() if include_cicd else None,
        )

        self._projects[slug] = project
        self._save()

        logger.info(f"Created dbt project: {slug}")
        return project

    def get_project(self, name: str) -> Optional[DbtProject]:
        """Get a project by name."""
        slug = self._slugify(name)
        return self._projects.get(slug)

    def list_projects(self) -> List[Dict[str, Any]]:
        """List all projects."""
        return [
            {
                "id": p.id,
                "name": p.config.name,
                "profile": p.config.profile,
                "hierarchy_project_id": p.config.hierarchy_project_id,
                "models_count": len(p.models),
                "sources_count": len(p.sources),
                "created_at": p.created_at.isoformat(),
            }
            for p in self._projects.values()
        ]

    def delete_project(self, name: str) -> bool:
        """Delete a project."""
        slug = self._slugify(name)
        if slug in self._projects:
            del self._projects[slug]
            self._save()
            return True
        return False

    def generate_project_yml(self, project: DbtProject) -> str:
        """Generate dbt_project.yml content."""
        config = project.config

        project_dict = {
            "name": config.name,
            "version": config.version,
            "config-version": 2,
            "profile": config.profile,
            "model-paths": config.model_paths,
            "seed-paths": config.seed_paths,
            "test-paths": config.test_paths,
            "analysis-paths": config.analysis_paths,
            "macro-paths": config.macro_paths,
            "clean-targets": ["target", "dbt_packages"],
        }

        # Add vars if present
        if config.vars:
            project_dict["vars"] = config.vars

        # Add model configurations
        project_dict["models"] = {
            config.name: {
                "+materialized": config.default_materialization.value,
                "staging": {
                    "+materialized": "view",
                    "+schema": "staging",
                },
                "intermediate": {
                    "+materialized": "view",
                    "+schema": "intermediate",
                },
                "marts": {
                    "+materialized": "table",
                    "+schema": "marts",
                },
            }
        }

        return yaml.dump(project_dict, default_flow_style=False, sort_keys=False)

    def generate_profiles_yml(self, project: DbtProject) -> str:
        """Generate profiles.yml template content."""
        config = project.config

        profiles_dict = {
            config.profile: {
                "target": "dev",
                "outputs": {
                    "dev": {
                        "type": "snowflake",
                        "account": "{{ env_var('SNOWFLAKE_ACCOUNT') }}",
                        "user": "{{ env_var('SNOWFLAKE_USER') }}",
                        "password": "{{ env_var('SNOWFLAKE_PASSWORD') }}",
                        "role": "{{ env_var('SNOWFLAKE_ROLE') }}",
                        "warehouse": "{{ env_var('SNOWFLAKE_WAREHOUSE') }}",
                        "database": config.target_database or "{{ env_var('SNOWFLAKE_DATABASE') }}",
                        "schema": config.target_schema or "{{ env_var('SNOWFLAKE_SCHEMA') }}",
                        "threads": 4,
                    },
                    "prod": {
                        "type": "snowflake",
                        "account": "{{ env_var('SNOWFLAKE_ACCOUNT') }}",
                        "user": "{{ env_var('SNOWFLAKE_USER') }}",
                        "password": "{{ env_var('SNOWFLAKE_PASSWORD') }}",
                        "role": "{{ env_var('SNOWFLAKE_ROLE') }}",
                        "warehouse": "{{ env_var('SNOWFLAKE_WAREHOUSE') }}",
                        "database": config.target_database or "{{ env_var('SNOWFLAKE_DATABASE') }}",
                        "schema": config.target_schema or "{{ env_var('SNOWFLAKE_SCHEMA') }}",
                        "threads": 8,
                    },
                },
            }
        }

        return yaml.dump(profiles_dict, default_flow_style=False, sort_keys=False)

    def generate_gitignore(self) -> str:
        """Generate .gitignore content."""
        return """# dbt
target/
dbt_packages/
logs/
.user.yml

# Python
__pycache__/
*.py[cod]
.venv/
venv/

# IDE
.idea/
.vscode/
*.swp

# OS
.DS_Store
Thumbs.db

# Credentials
profiles.yml
.env
*.pem
"""

    def generate_readme(self, project: DbtProject) -> str:
        """Generate README.md content."""
        config = project.config

        return f"""# {config.name}

A dbt project generated by DataBridge AI.

## Setup

1. Install dbt:
   ```bash
   pip install dbt-snowflake
   ```

2. Configure your profile in `~/.dbt/profiles.yml` or set environment variables:
   ```bash
   export SNOWFLAKE_ACCOUNT=your_account
   export SNOWFLAKE_USER=your_user
   export SNOWFLAKE_PASSWORD=your_password
   export SNOWFLAKE_ROLE=your_role
   export SNOWFLAKE_WAREHOUSE=your_warehouse
   export SNOWFLAKE_DATABASE={config.target_database or 'your_database'}
   export SNOWFLAKE_SCHEMA={config.target_schema or 'your_schema'}
   ```

3. Install dependencies:
   ```bash
   dbt deps
   ```

## Usage

### Run all models
```bash
dbt run
```

### Run specific model
```bash
dbt run --select model_name
```

### Run tests
```bash
dbt test
```

### Generate documentation
```bash
dbt docs generate
dbt docs serve
```

## Project Structure

```
{config.name}/
├── dbt_project.yml      # Project configuration
├── profiles.yml.template # Profile template
├── models/
│   ├── staging/         # Raw data transformations
│   ├── intermediate/    # Business logic
│   └── marts/           # Final tables (dims, facts)
├── seeds/               # Static data files
├── tests/               # Custom tests
├── macros/              # Reusable SQL macros
└── analyses/            # Ad-hoc analyses
```

## DataBridge Integration

This project was generated from DataBridge hierarchy project: `{config.hierarchy_project_id or 'N/A'}`

---
Generated by DataBridge AI on {datetime.now().strftime('%Y-%m-%d')}
"""

    def scaffold_project(
        self,
        project: DbtProject,
        output_path: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Generate all project files.

        Args:
            project: The dbt project to scaffold
            output_path: Optional output directory

        Returns:
            Dict of filename -> content
        """
        files = {}

        # Core configuration
        files["dbt_project.yml"] = self.generate_project_yml(project)
        files["profiles.yml.template"] = self.generate_profiles_yml(project)
        files[".gitignore"] = self.generate_gitignore()
        files["README.md"] = self.generate_readme(project)

        # Create directory placeholders
        dirs = [
            "models/staging/.gitkeep",
            "models/intermediate/.gitkeep",
            "models/marts/.gitkeep",
            "seeds/.gitkeep",
            "tests/.gitkeep",
            "macros/.gitkeep",
            "analyses/.gitkeep",
        ]
        for dir_path in dirs:
            files[dir_path] = ""

        # Add packages.yml
        files["packages.yml"] = yaml.dump({
            "packages": [
                {"package": "dbt-labs/dbt_utils", "version": "1.1.1"},
            ]
        }, default_flow_style=False)

        # Store generated files
        project.generated_files = files
        project.updated_at = datetime.now()
        self._save()

        # Write to disk if output path provided
        if output_path:
            self._write_files(output_path, files)

        return files

    def _write_files(self, base_path: str, files: Dict[str, str]) -> None:
        """Write files to disk."""
        base = Path(base_path)
        base.mkdir(parents=True, exist_ok=True)

        for filepath, content in files.items():
            full_path = base / filepath
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text(content)

        logger.info(f"Wrote {len(files)} files to {base_path}")

    def _slugify(self, name: str) -> str:
        """Convert name to slug."""
        return name.lower().replace(" ", "_").replace("-", "_")

    def _save(self) -> None:
        """Persist projects to disk."""
        data_file = self.output_dir / "projects.json"

        data = {}
        for name, project in self._projects.items():
            data[name] = project.model_dump(mode="json")

        data_file.write_text(json.dumps(data, indent=2, default=str))

    def _load(self) -> None:
        """Load projects from disk."""
        data_file = self.output_dir / "projects.json"

        if data_file.exists():
            try:
                data = json.loads(data_file.read_text())
                for name, proj_data in data.items():
                    self._projects[name] = DbtProject(**proj_data)
            except Exception as e:
                logger.error(f"Failed to load projects: {e}")
