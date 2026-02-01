"""Templates, skills, and knowledge base management module."""

from .service import TemplateService, TemplateInfo, Template, TemplateHierarchy
from .skills import SkillManager, SkillInfo, Skill

__all__ = [
    # Template classes
    "TemplateService",
    "TemplateInfo",
    "Template",
    "TemplateHierarchy",
    # Skill classes
    "SkillManager",
    "SkillInfo",
    "Skill",
]
