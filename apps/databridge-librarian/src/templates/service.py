"""
Template service for managing hierarchy templates.

Provides functionality to:
- List available templates by domain/industry
- Get template details and hierarchies
- Create projects from templates
- Save projects as templates
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class TemplateInfo:
    """Summary information about a template."""
    id: str
    name: str
    domain: str
    category: str
    industry: str
    description: str
    hierarchy_count: int
    file: str


@dataclass
class TemplateHierarchy:
    """A hierarchy node within a template."""
    hierarchy_id: str
    hierarchy_name: str
    parent_id: Optional[str]
    level: int
    sort_order: int
    description: str = ""
    is_calculated: bool = False
    node_type: str = "detail"
    formula_hint: str = ""
    mapping_hints: list = field(default_factory=list)
    flags: dict = field(default_factory=dict)


@dataclass
class Template:
    """Full template with all hierarchies."""
    id: str
    name: str
    domain: str
    category: str
    industry: str
    description: str
    version: str
    tags: list
    hierarchies: list[TemplateHierarchy]


class TemplateService:
    """Service for managing hierarchy templates."""

    def __init__(self, templates_dir: Optional[Path] = None):
        """
        Initialize the template service.

        Args:
            templates_dir: Path to templates directory. Defaults to project root templates/.
        """
        if templates_dir is None:
            # Look for templates in multiple locations
            # __file__ = apps/databridge-librarian/src/templates/service.py
            # parent x5 = Databridge_AI (root)
            possible_paths = [
                Path(__file__).parent.parent.parent.parent.parent / "templates",  # Root templates/
                Path(__file__).parent.parent.parent / "templates",  # App-local templates/
                Path.cwd() / "templates",  # Current directory
            ]
            for path in possible_paths:
                if path.exists() and (path / "index.json").exists():
                    templates_dir = path
                    break
            else:
                templates_dir = possible_paths[0]  # Default to root

        self.templates_dir = Path(templates_dir)
        self._index: Optional[dict] = None
        self._templates_cache: dict[str, Template] = {}

    def _load_index(self) -> dict:
        """Load the templates index."""
        if self._index is None:
            index_path = self.templates_dir / "index.json"
            if index_path.exists():
                with open(index_path, "r", encoding="utf-8") as f:
                    self._index = json.load(f)
            else:
                self._index = {"templates": [], "domains": {}, "industries": {}}
        return self._index

    def list_templates(
        self,
        domain: Optional[str] = None,
        industry: Optional[str] = None,
        category: Optional[str] = None,
    ) -> list[TemplateInfo]:
        """
        List available templates with optional filtering.

        Args:
            domain: Filter by domain (accounting, finance, operations)
            industry: Filter by industry (general, oil_gas, manufacturing, etc.)
            category: Filter by category (income_statement, balance_sheet, etc.)

        Returns:
            List of template summaries.
        """
        index = self._load_index()
        templates = []

        for t in index.get("templates", []):
            # Apply filters
            if domain and t.get("domain") != domain:
                continue
            if industry and t.get("industry") != industry:
                continue
            if category and t.get("category") != category:
                continue

            templates.append(TemplateInfo(
                id=t["id"],
                name=t["name"],
                domain=t.get("domain", ""),
                category=t.get("category", ""),
                industry=t.get("industry", "general"),
                description=t.get("description", ""),
                hierarchy_count=t.get("hierarchy_count", 0),
                file=t.get("file", ""),
            ))

        return templates

    def get_template(self, template_id: str) -> Optional[Template]:
        """
        Get full template details including all hierarchies.

        Args:
            template_id: The template ID.

        Returns:
            Template with all hierarchy nodes, or None if not found.
        """
        # Check cache first
        if template_id in self._templates_cache:
            return self._templates_cache[template_id]

        index = self._load_index()

        # Find template in index
        template_info = None
        for t in index.get("templates", []):
            if t["id"] == template_id:
                template_info = t
                break

        if not template_info:
            return None

        # Load the full template file
        template_path = self.templates_dir / template_info["file"]
        if not template_path.exists():
            return None

        with open(template_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Parse hierarchies
        hierarchies = []
        for h in data.get("hierarchies", []):
            hierarchies.append(TemplateHierarchy(
                hierarchy_id=h["hierarchy_id"],
                hierarchy_name=h["hierarchy_name"],
                parent_id=h.get("parent_id"),
                level=h.get("level", 1),
                sort_order=h.get("sort_order", 1),
                description=h.get("description", ""),
                is_calculated=h.get("is_calculated", False),
                node_type=h.get("node_type", "detail"),
                formula_hint=h.get("formula_hint", ""),
                mapping_hints=h.get("mapping_hints", []),
                flags=h.get("flags", {}),
            ))

        template = Template(
            id=data["id"],
            name=data["name"],
            domain=data.get("domain", ""),
            category=data.get("category", ""),
            industry=data.get("industry", "general"),
            description=data.get("description", ""),
            version=data.get("version", "1.0"),
            tags=data.get("tags", []),
            hierarchies=hierarchies,
        )

        # Cache it
        self._templates_cache[template_id] = template

        return template

    def get_domains(self) -> dict:
        """Get available domains with their descriptions."""
        index = self._load_index()
        return index.get("domains", {})

    def get_industries(self) -> dict:
        """Get available industries with their descriptions."""
        index = self._load_index()
        return index.get("industries", {})

    def recommend_templates(
        self,
        industry: Optional[str] = None,
        statement_type: Optional[str] = None,
    ) -> list[TemplateInfo]:
        """
        Get template recommendations based on industry and needs.

        Args:
            industry: Target industry.
            statement_type: Type of statement (pl, balance_sheet, etc.)

        Returns:
            Ranked list of recommended templates.
        """
        templates = self.list_templates()
        scored = []

        for t in templates:
            score = 0

            # Industry match
            if industry:
                if t.industry == industry:
                    score += 10
                elif t.industry == "general":
                    score += 3
                elif industry.startswith(t.industry.replace("_", "")) or t.industry.startswith(industry.replace("_", "")):
                    score += 5

            # Statement type match
            if statement_type:
                st_lower = statement_type.lower()
                if st_lower in ["pl", "p&l", "pnl", "income"]:
                    if "income_statement" in t.category or "pl" in t.id:
                        score += 10
                elif st_lower in ["bs", "balance"]:
                    if "balance_sheet" in t.category or "bs" in t.id:
                        score += 10
                elif st_lower in t.category:
                    score += 8

            if score > 0:
                scored.append((score, t))

        # Sort by score descending
        scored.sort(key=lambda x: x[0], reverse=True)

        return [t for _, t in scored[:5]]

    def create_project_from_template(
        self,
        template_id: str,
        project_name: str,
        hierarchy_service=None,
    ) -> dict:
        """
        Create a new project pre-populated from a template.

        Args:
            template_id: The template to use.
            project_name: Name for the new project.
            hierarchy_service: The hierarchy service to use for creation.

        Returns:
            Dict with project details and created hierarchy count.
        """
        template = self.get_template(template_id)
        if not template:
            return {"error": f"Template not found: {template_id}"}

        if hierarchy_service is None:
            return {
                "template": template_id,
                "project_name": project_name,
                "hierarchies_to_create": len(template.hierarchies),
                "note": "Hierarchy service not provided - dry run only",
            }

        # Create project
        project = hierarchy_service.create_project(
            name=project_name,
            description=f"Created from template: {template.name}",
            industry=template.industry,
        )

        # Create hierarchies
        created_count = 0
        for h in template.hierarchies:
            try:
                hierarchy_service.create_hierarchy(
                    project_id=project.id,
                    hierarchy_id=h.hierarchy_id,
                    hierarchy_name=h.hierarchy_name,
                    parent_id=h.parent_id,
                    description=h.description,
                    sort_order=h.sort_order,
                    flags=h.flags,
                )
                created_count += 1
            except Exception as e:
                # Log but continue
                print(f"Warning: Failed to create hierarchy {h.hierarchy_id}: {e}")

        return {
            "project_id": project.id,
            "project_name": project_name,
            "template_id": template_id,
            "template_name": template.name,
            "hierarchies_created": created_count,
            "hierarchies_total": len(template.hierarchies),
        }
