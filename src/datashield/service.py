"""DataShield Service - CRUD operations for shield projects and table configs.

Manages the lifecycle of shield projects: create, list, get, delete projects,
and add/remove table shield configurations.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

from .types import ShieldProject, TableShieldConfig, ColumnRule
from .key_manager import KeyManager
from .engine import ScrambleEngine

logger = logging.getLogger(__name__)


class ShieldService:
    """Manages DataShield projects and their configurations."""

    def __init__(self, data_dir: str):
        """Initialize the shield service.

        Args:
            data_dir: Directory for storing project configs and keystore
        """
        self._data_dir = Path(data_dir)
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._projects_file = self._data_dir / "datashield_projects.json"
        self._keystore_path = str(self._data_dir / "datashield_keystore.enc")
        self._key_manager = KeyManager(self._keystore_path)
        self._projects: Dict[str, ShieldProject] = {}
        self._engines: Dict[str, ScrambleEngine] = {}
        self._load_projects()

    def _load_projects(self):
        """Load projects from disk."""
        if self._projects_file.exists():
            try:
                data = json.loads(self._projects_file.read_text(encoding="utf-8"))
                for pid, pdata in data.get("projects", {}).items():
                    self._projects[pid] = ShieldProject(**pdata)
            except Exception as e:
                logger.error("Failed to load shield projects: %s", e)

    def _save_projects(self):
        """Save projects to disk."""
        data = {
            "projects": {
                pid: proj.model_dump() for pid, proj in self._projects.items()
            }
        }
        self._projects_file.write_text(
            json.dumps(data, indent=2, default=str), encoding="utf-8"
        )

    def create_project(self, name: str, passphrase: str,
                       description: Optional[str] = None) -> ShieldProject:
        """Create a new shield project with a dedicated encryption key.

        Args:
            name: Project name
            passphrase: Passphrase to protect the keystore
            description: Optional description

        Returns:
            The created ShieldProject
        """
        # Ensure keystore exists
        if not self._key_manager.keystore_exists:
            self._key_manager.create_keystore(passphrase)
        elif not self._key_manager.is_unlocked:
            self._key_manager.unlock(passphrase)

        project = ShieldProject(
            name=name,
            description=description,
            key_alias=f"shield_{name.lower().replace(' ', '_')}",
        )

        # Generate project key
        self._key_manager.generate_project_key(project.key_alias)

        # Store project
        self._projects[project.id] = project
        self._save_projects()

        logger.info("Created shield project: %s (%s)", project.name, project.id)
        return project

    def list_projects(self) -> List[Dict[str, Any]]:
        """List all shield projects (summary view)."""
        return [
            {
                "id": p.id,
                "name": p.name,
                "description": p.description,
                "active": p.active,
                "table_count": len(p.tables),
                "created_at": p.created_at,
            }
            for p in self._projects.values()
        ]

    def get_project(self, project_id: str) -> Optional[ShieldProject]:
        """Get a project by ID."""
        return self._projects.get(project_id)

    def delete_project(self, project_id: str) -> bool:
        """Delete a shield project and its key.

        Args:
            project_id: Project ID to delete

        Returns:
            True if deleted
        """
        project = self._projects.get(project_id)
        if not project:
            return False

        # Delete key if keystore is unlocked
        if self._key_manager.is_unlocked:
            try:
                self._key_manager.delete_project_key(project.key_alias)
            except KeyError:
                pass

        # Remove engine cache
        self._engines.pop(project_id, None)

        # Remove project
        del self._projects[project_id]
        self._save_projects()

        logger.info("Deleted shield project: %s", project_id)
        return True

    def add_table_shield(self, project_id: str,
                         config: TableShieldConfig) -> bool:
        """Add or update a table shield config in a project.

        Args:
            project_id: Target project ID
            config: Table shield configuration

        Returns:
            True if added/updated
        """
        project = self._projects.get(project_id)
        if not project:
            raise ValueError(f"Project not found: {project_id}")

        # Replace if table already exists
        fqn = f"{config.database}.{config.schema_name}.{config.table_name}"
        project.tables = [
            t for t in project.tables
            if f"{t.database}.{t.schema_name}.{t.table_name}" != fqn
        ]
        project.tables.append(config)
        project.updated_at = datetime.now().isoformat()

        self._save_projects()
        logger.info("Added table shield: %s to project %s", fqn, project_id)
        return True

    def remove_table_shield(self, project_id: str, database: str,
                            schema_name: str, table_name: str) -> bool:
        """Remove a table from a shield project.

        Returns:
            True if removed
        """
        project = self._projects.get(project_id)
        if not project:
            raise ValueError(f"Project not found: {project_id}")

        fqn = f"{database}.{schema_name}.{table_name}"
        original_count = len(project.tables)
        project.tables = [
            t for t in project.tables
            if f"{t.database}.{t.schema_name}.{t.table_name}" != fqn
        ]

        if len(project.tables) < original_count:
            project.updated_at = datetime.now().isoformat()
            self._save_projects()
            logger.info("Removed table shield: %s from project %s", fqn, project_id)
            return True
        return False

    def get_engine(self, project_id: str, passphrase: str) -> ScrambleEngine:
        """Get or create a ScrambleEngine for a project.

        Args:
            project_id: Project ID
            passphrase: Passphrase to unlock keystore

        Returns:
            ScrambleEngine instance
        """
        if project_id in self._engines:
            return self._engines[project_id]

        project = self._projects.get(project_id)
        if not project:
            raise ValueError(f"Project not found: {project_id}")

        # Unlock keystore if needed
        if not self._key_manager.is_unlocked:
            self._key_manager.unlock(passphrase)

        # Get project key
        raw_key = self._key_manager.get_project_key(project.key_alias)

        engine = ScrambleEngine(raw_key)
        self._engines[project_id] = engine
        return engine

    def get_status(self, project_id: Optional[str] = None) -> Dict[str, Any]:
        """Get shield status overview.

        Args:
            project_id: Optional specific project ID

        Returns:
            Status dictionary
        """
        status = {
            "keystore": self._key_manager.get_status(),
            "total_projects": len(self._projects),
            "active_projects": sum(1 for p in self._projects.values() if p.active),
        }

        if project_id:
            project = self._projects.get(project_id)
            if project:
                status["project"] = {
                    "id": project.id,
                    "name": project.name,
                    "active": project.active,
                    "table_count": len(project.tables),
                    "tables": [
                        {
                            "fqn": f"{t.database}.{t.schema_name}.{t.table_name}",
                            "type": t.table_type,
                            "rules": len(t.column_rules),
                            "key_columns": t.key_columns,
                        }
                        for t in project.tables
                    ],
                }

        return status
