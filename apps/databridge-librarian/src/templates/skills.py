"""
Skills service for managing AI expertise personas.

Provides functionality to:
- List available skills by domain/industry
- Get skill details and prompts
- Load skill system prompts for AI configuration
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class SkillInfo:
    """Summary information about a skill."""
    id: str
    name: str
    description: str
    domain: str
    industries: list[str]
    capabilities: list[str]
    communication_style: str


@dataclass
class Skill:
    """Full skill with prompt content."""
    id: str
    name: str
    description: str
    domain: str
    industries: list[str]
    capabilities: list[str]
    tools_frequently_used: list[str]
    communication_style: str
    prompt_file: str
    documentation_file: str
    prompt_content: Optional[str] = None
    documentation_content: Optional[str] = None


class SkillManager:
    """Service for managing AI skills/personas."""

    def __init__(self, skills_dir: Optional[Path] = None):
        """
        Initialize the skill manager.

        Args:
            skills_dir: Path to skills directory. Defaults to app's skills/.
        """
        if skills_dir is None:
            # Look for skills in multiple locations
            possible_paths = [
                Path(__file__).parent.parent.parent / "skills",  # App skills/
                Path(__file__).parent.parent.parent.parent.parent.parent / "skills",  # Root skills/
                Path.cwd() / "skills",  # Current directory
            ]
            for path in possible_paths:
                if path.exists() and (path / "index.json").exists():
                    skills_dir = path
                    break
            else:
                skills_dir = possible_paths[0]  # Default to app skills

        self.skills_dir = Path(skills_dir)
        self._index: Optional[dict] = None
        self._skills_cache: dict[str, Skill] = {}

    def _load_index(self) -> dict:
        """Load the skills index."""
        if self._index is None:
            index_path = self.skills_dir / "index.json"
            if index_path.exists():
                with open(index_path, "r", encoding="utf-8") as f:
                    self._index = json.load(f)
            else:
                self._index = {"skills": [], "metadata": {}}
        return self._index

    def list_skills(
        self,
        domain: Optional[str] = None,
        industry: Optional[str] = None,
    ) -> list[SkillInfo]:
        """
        List available skills with optional filtering.

        Args:
            domain: Filter by domain (finance, accounting, operations, etc.)
            industry: Filter by industry (general, oil_gas, manufacturing, etc.)

        Returns:
            List of skill summaries.
        """
        index = self._load_index()
        skills = []

        for s in index.get("skills", []):
            # Apply filters
            if domain and s.get("domain") != domain:
                continue
            if industry and industry not in s.get("industries", []):
                continue

            skills.append(SkillInfo(
                id=s["id"],
                name=s["name"],
                description=s.get("description", ""),
                domain=s.get("domain", ""),
                industries=s.get("industries", []),
                capabilities=s.get("capabilities", []),
                communication_style=s.get("communication_style", ""),
            ))

        return skills

    def get_skill(self, skill_id: str, load_prompt: bool = True) -> Optional[Skill]:
        """
        Get full skill details.

        Args:
            skill_id: The skill ID.
            load_prompt: Whether to load the prompt content.

        Returns:
            Skill with details and optionally prompt content.
        """
        # Check cache first
        if skill_id in self._skills_cache:
            skill = self._skills_cache[skill_id]
            if load_prompt and skill.prompt_content is None:
                skill.prompt_content = self._load_prompt_file(skill.prompt_file)
            return skill

        index = self._load_index()

        # Find skill in index
        skill_info = None
        for s in index.get("skills", []):
            if s["id"] == skill_id:
                skill_info = s
                break

        if not skill_info:
            return None

        # Build skill object
        skill = Skill(
            id=skill_info["id"],
            name=skill_info["name"],
            description=skill_info.get("description", ""),
            domain=skill_info.get("domain", ""),
            industries=skill_info.get("industries", []),
            capabilities=skill_info.get("capabilities", []),
            tools_frequently_used=skill_info.get("tools_frequently_used", []),
            communication_style=skill_info.get("communication_style", ""),
            prompt_file=skill_info.get("prompt_file", ""),
            documentation_file=skill_info.get("documentation_file", ""),
        )

        # Load prompt if requested
        if load_prompt and skill.prompt_file:
            skill.prompt_content = self._load_prompt_file(skill.prompt_file)

        # Cache it
        self._skills_cache[skill_id] = skill

        return skill

    def _load_prompt_file(self, filename: str) -> Optional[str]:
        """Load a prompt file content."""
        prompt_path = self.skills_dir / filename
        if prompt_path.exists():
            with open(prompt_path, "r", encoding="utf-8") as f:
                return f.read()
        return None

    def get_prompt(self, skill_id: str) -> Optional[str]:
        """
        Get the system prompt for a skill.

        Args:
            skill_id: The skill ID.

        Returns:
            The prompt content, or None if not found.
        """
        skill = self.get_skill(skill_id, load_prompt=True)
        if skill:
            return skill.prompt_content
        return None

    def get_domains(self) -> list[str]:
        """Get available domains."""
        index = self._load_index()
        domains = set()
        for s in index.get("skills", []):
            if s.get("domain"):
                domains.add(s["domain"])
        return sorted(domains)

    def get_industries(self) -> list[str]:
        """Get all industries covered by skills."""
        index = self._load_index()
        industries = set()
        for s in index.get("skills", []):
            for ind in s.get("industries", []):
                industries.add(ind)
        return sorted(industries)

    def recommend_skill(
        self,
        industry: Optional[str] = None,
        task_type: Optional[str] = None,
    ) -> Optional[SkillInfo]:
        """
        Recommend the best skill for a given context.

        Args:
            industry: Target industry.
            task_type: Type of task (analysis, reconciliation, reporting, etc.)

        Returns:
            Best matching skill, or None.
        """
        skills = self.list_skills()
        if not skills:
            return None

        scored = []
        for s in skills:
            score = 0

            # Industry match
            if industry:
                if industry in s.industries:
                    score += 10
                elif "general" in s.industries:
                    score += 3

            # Task type hints
            if task_type:
                task_lower = task_type.lower()
                if "reconcil" in task_lower and s.domain == "accounting":
                    score += 8
                elif "budget" in task_lower or "forecast" in task_lower:
                    if s.domain == "finance":
                        score += 8
                elif "report" in task_lower:
                    if s.domain in ["analytics", "executive"]:
                        score += 6
                elif "sql" in task_lower or "database" in task_lower:
                    if s.domain == "technical":
                        score += 10

            if score > 0:
                scored.append((score, s))

        if scored:
            scored.sort(key=lambda x: x[0], reverse=True)
            return scored[0][1]

        # Default to first skill
        return skills[0] if skills else None
