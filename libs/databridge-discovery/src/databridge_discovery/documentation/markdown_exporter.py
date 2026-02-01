"""
Markdown Exporter for generating documentation exports.

This module provides unified export capabilities:
- Project documentation
- Data dictionary exports
- Lineage documentation
- Combined reports
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from databridge_discovery.hierarchy.case_to_hierarchy import ConvertedHierarchy


@dataclass
class ExportConfig:
    """Configuration for markdown exports."""

    title: str = "DataBridge Discovery Report"
    include_toc: bool = True
    include_metadata: bool = True
    include_diagrams: bool = True
    include_statistics: bool = True
    output_format: str = "markdown"  # markdown, html, pdf
    theme: str = "default"
    date_format: str = "%Y-%m-%d %H:%M"


@dataclass
class ExportResult:
    """Result of an export operation."""

    content: str
    format: str
    file_path: str | None = None
    sections: list[str] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)


class MarkdownExporter:
    """
    Exports discovery results to markdown documentation.

    Generates comprehensive documentation including:
    - Executive summary
    - Hierarchy details
    - Data dictionaries
    - Lineage diagrams
    - Statistics and metrics

    Example:
        exporter = MarkdownExporter()

        # Export hierarchies
        result = exporter.export_hierarchies(
            hierarchies=[hier1, hier2],
            config=ExportConfig(title="GL Hierarchies")
        )

        # Write to file
        exporter.write(result, "docs/hierarchies.md")
    """

    def __init__(self):
        """Initialize the exporter."""
        pass

    def export_hierarchies(
        self,
        hierarchies: list[ConvertedHierarchy],
        config: ExportConfig | None = None,
    ) -> ExportResult:
        """
        Export hierarchies to markdown.

        Args:
            hierarchies: List of hierarchies to export
            config: Export configuration

        Returns:
            ExportResult with markdown content
        """
        config = config or ExportConfig()
        sections = []
        content_parts = []

        # Header
        content_parts.append(f"# {config.title}")
        content_parts.append("")
        sections.append("header")

        # Metadata
        if config.include_metadata:
            content_parts.append(f"**Generated:** {datetime.now().strftime(config.date_format)}")
            content_parts.append(f"**Hierarchies:** {len(hierarchies)}")
            content_parts.append("")
            sections.append("metadata")

        # Table of contents
        if config.include_toc:
            content_parts.append("## Table of Contents")
            content_parts.append("")
            content_parts.append("1. [Summary](#summary)")
            content_parts.append("2. [Hierarchies](#hierarchies)")
            for i, hier in enumerate(hierarchies, 3):
                anchor = self._to_anchor(hier.name)
                content_parts.append(f"{i}. [{hier.name}](#{anchor})")
            content_parts.append("")
            sections.append("toc")

        # Summary section
        content_parts.append("## Summary")
        content_parts.append("")
        content_parts.extend(self._generate_summary(hierarchies))
        content_parts.append("")
        sections.append("summary")

        # Hierarchies overview
        content_parts.append("## Hierarchies")
        content_parts.append("")
        content_parts.append("| Name | Entity Type | Levels | Nodes |")
        content_parts.append("|------|-------------|--------|-------|")
        for hier in hierarchies:
            content_parts.append(
                f"| {hier.name} | {hier.entity_type} | {hier.level_count} | {hier.total_nodes} |"
            )
        content_parts.append("")
        sections.append("hierarchies_table")

        # Detailed sections for each hierarchy
        for hier in hierarchies:
            content_parts.extend(self._generate_hierarchy_section(hier, config))
            sections.append(f"hierarchy_{hier.id}")

        return ExportResult(
            content="\n".join(content_parts),
            format="markdown",
            sections=sections,
            metadata={
                "hierarchy_count": len(hierarchies),
                "config": {
                    "title": config.title,
                    "include_toc": config.include_toc,
                },
            },
        )

    def export_project(
        self,
        project_name: str,
        hierarchies: list[ConvertedHierarchy],
        data_dictionary: dict[str, Any] | None = None,
        lineage_diagram: str | None = None,
        config: ExportConfig | None = None,
    ) -> ExportResult:
        """
        Export a complete project report.

        Args:
            project_name: Project name
            hierarchies: List of hierarchies
            data_dictionary: Optional data dictionary
            lineage_diagram: Optional lineage diagram (Mermaid)
            config: Export configuration

        Returns:
            ExportResult with complete report
        """
        config = config or ExportConfig(title=f"{project_name} Documentation")
        sections = []
        content_parts = []

        # Header
        content_parts.append(f"# {config.title}")
        content_parts.append("")
        content_parts.append(f"Project: **{project_name}**")
        content_parts.append(f"Generated: {datetime.now().strftime(config.date_format)}")
        content_parts.append("")
        content_parts.append("---")
        content_parts.append("")
        sections.append("header")

        # Executive Summary
        content_parts.append("## Executive Summary")
        content_parts.append("")
        content_parts.append(f"This document describes the **{project_name}** project containing "
                           f"{len(hierarchies)} hierarchy(s) for data warehouse modeling.")
        content_parts.append("")

        # Key metrics
        total_nodes = sum(h.total_nodes for h in hierarchies)
        total_levels = sum(h.level_count for h in hierarchies)
        entity_types = list(set(h.entity_type for h in hierarchies))

        content_parts.append("### Key Metrics")
        content_parts.append("")
        content_parts.append(f"- **Hierarchies:** {len(hierarchies)}")
        content_parts.append(f"- **Total Nodes:** {total_nodes}")
        content_parts.append(f"- **Total Levels:** {total_levels}")
        content_parts.append(f"- **Entity Types:** {', '.join(entity_types)}")
        content_parts.append("")
        sections.append("executive_summary")

        # Hierarchies section
        content_parts.append("## Hierarchies")
        content_parts.append("")
        for hier in hierarchies:
            content_parts.extend(self._generate_hierarchy_section(hier, config))
        sections.append("hierarchies")

        # Data Dictionary section
        if data_dictionary:
            content_parts.append("## Data Dictionary")
            content_parts.append("")
            content_parts.extend(self._format_data_dictionary(data_dictionary))
            content_parts.append("")
            sections.append("data_dictionary")

        # Lineage section
        if lineage_diagram and config.include_diagrams:
            content_parts.append("## Data Lineage")
            content_parts.append("")
            content_parts.append(lineage_diagram)
            content_parts.append("")
            sections.append("lineage")

        # Statistics
        if config.include_statistics:
            content_parts.append("## Statistics")
            content_parts.append("")
            content_parts.extend(self._generate_statistics(hierarchies))
            sections.append("statistics")

        return ExportResult(
            content="\n".join(content_parts),
            format="markdown",
            sections=sections,
            metadata={
                "project_name": project_name,
                "hierarchy_count": len(hierarchies),
                "has_dictionary": data_dictionary is not None,
                "has_lineage": lineage_diagram is not None,
            },
        )

    def write(
        self,
        result: ExportResult,
        file_path: str,
    ) -> str:
        """
        Write export result to file.

        Args:
            result: Export result
            file_path: Output file path

        Returns:
            Absolute path of written file
        """
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            f.write(result.content)

        result.file_path = str(path.absolute())
        return result.file_path

    def _generate_summary(self, hierarchies: list[ConvertedHierarchy]) -> list[str]:
        """Generate summary section."""
        lines = []

        if not hierarchies:
            lines.append("No hierarchies to document.")
            return lines

        # Entity type breakdown
        entity_counts: dict[str, int] = {}
        for hier in hierarchies:
            entity_counts[hier.entity_type] = entity_counts.get(hier.entity_type, 0) + 1

        lines.append("### Entity Type Breakdown")
        lines.append("")
        lines.append("| Entity Type | Count |")
        lines.append("|-------------|-------|")
        for entity, count in sorted(entity_counts.items()):
            lines.append(f"| {entity} | {count} |")
        lines.append("")

        # Level distribution
        level_counts: dict[int, int] = {}
        for hier in hierarchies:
            level_counts[hier.level_count] = level_counts.get(hier.level_count, 0) + 1

        lines.append("### Level Distribution")
        lines.append("")
        lines.append("| Levels | Count |")
        lines.append("|--------|-------|")
        for levels, count in sorted(level_counts.items()):
            lines.append(f"| {levels} | {count} |")

        return lines

    def _generate_hierarchy_section(
        self,
        hierarchy: ConvertedHierarchy,
        config: ExportConfig,
    ) -> list[str]:
        """Generate section for a single hierarchy."""
        lines = []

        lines.append(f"### {hierarchy.name}")
        lines.append("")
        lines.append(f"**Entity Type:** {hierarchy.entity_type}")
        lines.append(f"**Levels:** {hierarchy.level_count}")
        lines.append(f"**Total Nodes:** {hierarchy.total_nodes}")

        if hierarchy.source_column:
            lines.append(f"**Source Column:** {hierarchy.source_column}")

        lines.append("")

        # Level breakdown
        if hierarchy.level_count > 0:
            lines.append("#### Levels")
            lines.append("")
            lines.append("| Level | Description |")
            lines.append("|-------|-------------|")
            for i in range(1, hierarchy.level_count + 1):
                lines.append(f"| Level {i} | Hierarchy level {i} |")
            lines.append("")

        # Sample nodes
        if hierarchy.nodes and len(hierarchy.nodes) > 0:
            lines.append("#### Sample Nodes")
            lines.append("")
            lines.append("| ID | Name | Level | Parent |")
            lines.append("|----|------|-------|--------|")

            sample_nodes = list(hierarchy.nodes.items())[:5]
            for node_id, node in sample_nodes:
                parent = node.parent_id or "-"
                lines.append(f"| {node_id} | {node.name} | {node.level} | {parent} |")

            if len(hierarchy.nodes) > 5:
                lines.append(f"| ... | *{len(hierarchy.nodes) - 5} more nodes* | ... | ... |")

            lines.append("")

        return lines

    def _format_data_dictionary(self, dictionary: dict[str, Any]) -> list[str]:
        """Format data dictionary for markdown."""
        lines = []

        tables = dictionary.get("tables", [])
        for table in tables:
            lines.append(f"### {table.get('name', 'Unknown')}")
            lines.append("")
            lines.append(table.get("description", ""))
            lines.append("")

            columns = table.get("columns", [])
            if columns:
                lines.append("| Column | Type | Nullable | Description |")
                lines.append("|--------|------|----------|-------------|")
                for col in columns:
                    nullable = "Yes" if col.get("nullable", True) else "No"
                    lines.append(
                        f"| {col.get('name', '')} | {col.get('data_type', '')} | "
                        f"{nullable} | {col.get('description', '')} |"
                    )
                lines.append("")

        return lines

    def _generate_statistics(self, hierarchies: list[ConvertedHierarchy]) -> list[str]:
        """Generate statistics section."""
        lines = []

        total_nodes = sum(h.total_nodes for h in hierarchies)
        avg_levels = sum(h.level_count for h in hierarchies) / len(hierarchies) if hierarchies else 0
        max_levels = max(h.level_count for h in hierarchies) if hierarchies else 0
        min_levels = min(h.level_count for h in hierarchies) if hierarchies else 0

        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        lines.append(f"| Total Hierarchies | {len(hierarchies)} |")
        lines.append(f"| Total Nodes | {total_nodes} |")
        lines.append(f"| Average Levels | {avg_levels:.1f} |")
        lines.append(f"| Max Levels | {max_levels} |")
        lines.append(f"| Min Levels | {min_levels} |")
        lines.append("")

        return lines

    def _to_anchor(self, text: str) -> str:
        """Convert text to markdown anchor."""
        import re
        anchor = text.lower()
        anchor = re.sub(r'[^a-z0-9\s-]', '', anchor)
        anchor = re.sub(r'\s+', '-', anchor)
        return anchor
