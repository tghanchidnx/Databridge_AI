"""
Lineage Extractor - Parse SQL and dbt artifacts to extract lineage.

This module provides automatic lineage extraction from:
1. SQL statements (CREATE VIEW, SELECT, dbt models)
2. dbt manifest.json files
3. DataBridge catalog assets

The extracted lineage integrates with the existing lineage module.
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class SQLLineageExtractor:
    """Extract lineage information from SQL statements."""

    # SQL patterns for source table extraction
    TABLE_PATTERNS = [
        # FROM clause
        r'FROM\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2})',
        # JOIN clauses
        r'JOIN\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2})',
        # dbt ref() calls
        r"\{\{\s*ref\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}",
        # dbt source() calls
        r"\{\{\s*source\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}",
    ]

    # Patterns to detect transformations
    TRANSFORMATION_PATTERNS = {
        "AGGREGATION": [
            r'\bSUM\s*\(', r'\bCOUNT\s*\(', r'\bAVG\s*\(',
            r'\bMIN\s*\(', r'\bMAX\s*\(', r'\bLIST_AGG\s*\(',
        ],
        "CASE": [r'\bCASE\s+WHEN\b', r'\bCASE\s+\w+\s+WHEN\b'],
        "JOIN": [r'\bJOIN\b', r'\bLEFT\s+JOIN\b', r'\bRIGHT\s+JOIN\b', r'\bINNER\s+JOIN\b'],
        "UNION": [r'\bUNION\b', r'\bUNION\s+ALL\b'],
        "FILTER": [r'\bWHERE\b', r'\bHAVING\b'],
        "WINDOW": [r'\bOVER\s*\(', r'\bPARTITION\s+BY\b', r'\bROW_NUMBER\s*\('],
        "CALCULATION": [r'\bCOALESCE\s*\(', r'\bNULLIF\s*\(', r'\bIIF\s*\('],
    }

    def __init__(self):
        """Initialize the SQL lineage extractor."""
        pass

    def extract_from_sql(
        self,
        sql: str,
        target_name: Optional[str] = None,
        target_type: str = "VIEW",
    ) -> Dict[str, Any]:
        """
        Extract lineage information from a SQL statement.

        Args:
            sql: SQL statement (CREATE VIEW, SELECT, etc.)
            target_name: Name of the target object (extracted from SQL if None)
            target_type: Type of target (VIEW, TABLE, MODEL)

        Returns:
            Lineage info with sources, target, transformations, columns
        """
        result = {
            "target": {
                "name": target_name,
                "type": target_type,
            },
            "sources": [],
            "transformations": [],
            "column_lineage": [],
            "raw_sql": sql[:500] if len(sql) > 500 else sql,
        }

        # Extract target from CREATE statement if not provided
        if not target_name:
            target_name = self._extract_target_name(sql)
            result["target"]["name"] = target_name

        # Extract source tables
        sources = self._extract_source_tables(sql)
        result["sources"] = [
            {"name": s, "type": "TABLE"} for s in sources
        ]

        # Detect transformations
        result["transformations"] = self._detect_transformations(sql)

        # Extract column lineage
        result["column_lineage"] = self._extract_column_lineage(sql, sources)

        return result

    def _extract_target_name(self, sql: str) -> Optional[str]:
        """Extract target name from CREATE statement."""
        patterns = [
            r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:SECURE\s+)?VIEW\s+([A-Za-z_][A-Za-z0-9_\.]*)',
            r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([A-Za-z_][A-Za-z0-9_\.]*)',
            r'CREATE\s+(?:OR\s+REPLACE\s+)?DYNAMIC\s+TABLE\s+([A-Za-z_][A-Za-z0-9_\.]*)',
            r'CREATE\s+(?:OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\s+([A-Za-z_][A-Za-z0-9_\.]*)',
        ]

        for pattern in patterns:
            match = re.search(pattern, sql, re.IGNORECASE)
            if match:
                return match.group(1)

        return None

    def _extract_source_tables(self, sql: str) -> List[str]:
        """Extract source table names from SQL."""
        sources: Set[str] = set()

        # Remove comments and strings to avoid false matches
        cleaned_sql = self._clean_sql(sql)

        # Extract FROM clause tables
        from_pattern = r'FROM\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2})'
        for match in re.finditer(from_pattern, cleaned_sql, re.IGNORECASE):
            sources.add(match.group(1))

        # Extract JOIN clause tables
        join_pattern = r'(?:LEFT|RIGHT|INNER|OUTER|CROSS|FULL)?\s*JOIN\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2})'
        for match in re.finditer(join_pattern, cleaned_sql, re.IGNORECASE):
            sources.add(match.group(1))

        # Extract dbt ref() calls
        ref_pattern = r"\{\{\s*ref\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}"
        for match in re.finditer(ref_pattern, sql):
            sources.add(match.group(1))

        # Extract dbt source() calls
        source_pattern = r"\{\{\s*source\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}"
        for match in re.finditer(source_pattern, sql):
            sources.add(f"{match.group(1)}.{match.group(2)}")

        # Filter out keywords that might be captured
        keywords = {'SELECT', 'FROM', 'WHERE', 'JOIN', 'ON', 'AND', 'OR', 'AS', 'IN'}
        sources = {s for s in sources if s.upper() not in keywords}

        return list(sources)

    def _detect_transformations(self, sql: str) -> List[Dict[str, Any]]:
        """Detect transformation types in SQL."""
        transformations = []

        for trans_type, patterns in self.TRANSFORMATION_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, sql, re.IGNORECASE):
                    transformations.append({
                        "type": trans_type,
                        "pattern": pattern,
                    })
                    break  # Only count each type once

        return transformations

    def _extract_column_lineage(
        self,
        sql: str,
        sources: List[str],
    ) -> List[Dict[str, Any]]:
        """Extract column-level lineage from SQL."""
        column_lineage = []

        # Extract SELECT clause
        select_match = re.search(
            r'SELECT\s+(.*?)(?=\s+FROM\s)',
            sql,
            re.IGNORECASE | re.DOTALL
        )

        if not select_match:
            return column_lineage

        select_clause = select_match.group(1)

        # Parse column expressions
        # Split by comma, but handle nested parentheses
        columns = self._split_columns(select_clause)

        for col in columns:
            col = col.strip()
            if not col or col == '*':
                continue

            lineage_entry = self._parse_column_expression(col, sources)
            if lineage_entry:
                column_lineage.append(lineage_entry)

        return column_lineage

    def _split_columns(self, select_clause: str) -> List[str]:
        """Split SELECT clause into individual column expressions."""
        columns = []
        current = []
        paren_depth = 0

        for char in select_clause + ',':
            if char == '(':
                paren_depth += 1
                current.append(char)
            elif char == ')':
                paren_depth -= 1
                current.append(char)
            elif char == ',' and paren_depth == 0:
                columns.append(''.join(current))
                current = []
            else:
                current.append(char)

        return columns

    def _parse_column_expression(
        self,
        col_expr: str,
        sources: List[str],
    ) -> Optional[Dict[str, Any]]:
        """Parse a single column expression."""
        # Find alias
        alias_match = re.search(r'\s+AS\s+["\']?(\w+)["\']?\s*$', col_expr, re.IGNORECASE)
        target_column = alias_match.group(1) if alias_match else None

        if not target_column:
            # Try to extract column name from simple expression
            simple_match = re.match(r'^["\']?(\w+)["\']?\s*$', col_expr.strip())
            if simple_match:
                target_column = simple_match.group(1)
            else:
                # Use a generated name for complex expressions
                target_column = f"expr_{hash(col_expr) % 10000}"

        # Find source columns
        source_columns = []
        for source in sources:
            # Look for table.column patterns
            pattern = rf'{re.escape(source)}\.(\w+)'
            for match in re.finditer(pattern, col_expr, re.IGNORECASE):
                source_columns.append({
                    "source": source,
                    "column": match.group(1),
                })

        # Also look for unqualified column names
        unqualified = re.findall(r'(?<!\.)\b([A-Za-z_][A-Za-z0-9_]*)\b', col_expr)
        keywords = {'SELECT', 'AS', 'FROM', 'WHERE', 'CASE', 'WHEN', 'THEN',
                   'ELSE', 'END', 'AND', 'OR', 'NOT', 'NULL', 'SUM', 'COUNT',
                   'AVG', 'MIN', 'MAX', 'COALESCE', 'OVER', 'PARTITION', 'BY'}
        for col in unqualified:
            if col.upper() not in keywords and col != target_column:
                source_columns.append({
                    "source": None,  # Unknown source
                    "column": col,
                })

        # Detect transformation type
        trans_type = "DIRECT"
        for t_type, patterns in self.TRANSFORMATION_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, col_expr, re.IGNORECASE):
                    trans_type = t_type
                    break

        if source_columns or trans_type != "DIRECT":
            return {
                "target_column": target_column,
                "source_columns": source_columns,
                "transformation_type": trans_type,
                "expression": col_expr[:200] if len(col_expr) > 200 else col_expr,
            }

        return None

    def _clean_sql(self, sql: str) -> str:
        """Remove comments and string literals from SQL."""
        # Remove single-line comments
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        # Remove multi-line comments
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        # Replace string literals with placeholder
        sql = re.sub(r"'[^']*'", "'X'", sql)
        return sql


class DbtLineageExtractor:
    """Extract lineage from dbt manifest.json files."""

    def __init__(self):
        """Initialize the dbt lineage extractor."""
        self.sql_extractor = SQLLineageExtractor()

    def extract_from_manifest(
        self,
        manifest_path: str,
    ) -> Dict[str, Any]:
        """
        Extract lineage from a dbt manifest.json file.

        Args:
            manifest_path: Path to the manifest.json file

        Returns:
            Full lineage graph with all models and sources
        """
        manifest_file = Path(manifest_path)
        if not manifest_file.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest_path}")

        with open(manifest_file, 'r') as f:
            manifest = json.load(f)

        result = {
            "project_name": manifest.get("metadata", {}).get("project_name"),
            "dbt_version": manifest.get("metadata", {}).get("dbt_version"),
            "nodes": [],
            "edges": [],
            "sources": [],
        }

        # Extract sources
        for source_key, source in manifest.get("sources", {}).items():
            result["sources"].append({
                "unique_id": source.get("unique_id"),
                "name": source.get("name"),
                "schema": source.get("schema"),
                "database": source.get("database"),
                "source_name": source.get("source_name"),
                "relation_name": source.get("relation_name"),
            })

        # Extract nodes (models, seeds, snapshots)
        for node_key, node in manifest.get("nodes", {}).items():
            if node.get("resource_type") not in ("model", "seed", "snapshot"):
                continue

            node_info = {
                "unique_id": node.get("unique_id"),
                "name": node.get("name"),
                "schema": node.get("schema"),
                "database": node.get("database"),
                "resource_type": node.get("resource_type"),
                "materialized": node.get("config", {}).get("materialized"),
                "depends_on": node.get("depends_on", {}).get("nodes", []),
                "columns": [],
            }

            # Extract columns
            for col_name, col_info in node.get("columns", {}).items():
                node_info["columns"].append({
                    "name": col_name,
                    "description": col_info.get("description"),
                    "data_type": col_info.get("data_type"),
                })

            result["nodes"].append(node_info)

            # Create edges for dependencies
            for dep in node_info["depends_on"]:
                result["edges"].append({
                    "source": dep,
                    "target": node_info["unique_id"],
                    "type": "depends_on",
                })

        return result

    def extract_column_lineage_from_model(
        self,
        manifest_path: str,
        model_name: str,
    ) -> Dict[str, Any]:
        """
        Extract column-level lineage for a specific model.

        Args:
            manifest_path: Path to the manifest.json file
            model_name: Name of the model

        Returns:
            Column lineage information
        """
        manifest_file = Path(manifest_path)
        if not manifest_file.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest_path}")

        with open(manifest_file, 'r') as f:
            manifest = json.load(f)

        # Find the model
        model_key = None
        for key in manifest.get("nodes", {}):
            if key.endswith(f".{model_name}"):
                model_key = key
                break

        if not model_key:
            return {"error": f"Model '{model_name}' not found"}

        node = manifest["nodes"][model_key]

        # Get compiled SQL
        compiled_sql = node.get("compiled_code") or node.get("compiled_sql")

        if not compiled_sql:
            return {"error": "No compiled SQL found for model"}

        # Extract lineage from SQL
        return self.sql_extractor.extract_from_sql(
            compiled_sql,
            target_name=model_name,
            target_type="MODEL",
        )


class LineageGraphBuilder:
    """Build lineage graph compatible with the lineage module."""

    def __init__(self):
        """Initialize the graph builder."""
        pass

    def build_from_extraction(
        self,
        extraction: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Convert extraction result to lineage graph format.

        Args:
            extraction: Result from SQLLineageExtractor or DbtLineageExtractor

        Returns:
            Lineage graph compatible with the lineage module
        """
        nodes = []
        edges = []
        column_lineage = []

        # Add target node
        if extraction.get("target"):
            target = extraction["target"]
            nodes.append({
                "name": target.get("name"),
                "node_type": self._map_type(target.get("type", "VIEW")),
            })

        # Add source nodes
        for source in extraction.get("sources", []):
            nodes.append({
                "name": source.get("name"),
                "node_type": self._map_type(source.get("type", "TABLE")),
            })

            # Add edge
            if extraction.get("target"):
                trans_type = "DIRECT"
                if extraction.get("transformations"):
                    trans_type = extraction["transformations"][0].get("type", "DIRECT")

                edges.append({
                    "source": source.get("name"),
                    "target": extraction["target"].get("name"),
                    "transformation_type": trans_type,
                })

        # Add column lineage
        for col_lin in extraction.get("column_lineage", []):
            for src_col in col_lin.get("source_columns", []):
                column_lineage.append({
                    "source_node": src_col.get("source"),
                    "source_columns": [src_col.get("column")],
                    "target_node": extraction.get("target", {}).get("name"),
                    "target_column": col_lin.get("target_column"),
                    "transformation_type": col_lin.get("transformation_type", "DIRECT"),
                })

        return {
            "nodes": nodes,
            "edges": edges,
            "column_lineage": column_lineage,
            "metadata": {
                "extracted_at": datetime.now().isoformat(),
                "transformation_count": len(extraction.get("transformations", [])),
            },
        }

    def _map_type(self, type_str: str) -> str:
        """Map extraction type to lineage NodeType."""
        type_map = {
            "TABLE": "TABLE",
            "VIEW": "VIEW",
            "MODEL": "DBT_MODEL",
            "DYNAMIC_TABLE": "DYNAMIC_TABLE",
            "MATERIALIZED": "VIEW",
            "HIERARCHY": "HIERARCHY",
        }
        return type_map.get(type_str.upper(), "TABLE")

    def generate_mermaid_diagram(
        self,
        graph: Dict[str, Any],
        direction: str = "TD",
    ) -> str:
        """
        Generate a Mermaid diagram from the lineage graph.

        Args:
            graph: Lineage graph data
            direction: Diagram direction (TD=top-down, LR=left-right)

        Returns:
            Mermaid diagram code
        """
        lines = [f"graph {direction}"]

        # Node styling
        node_styles = {
            "TABLE": ":::table",
            "VIEW": ":::view",
            "DYNAMIC_TABLE": ":::dynamic",
            "DBT_MODEL": ":::dbt",
            "HIERARCHY": ":::hierarchy",
            "DATA_MART": ":::mart",
        }

        # Add nodes
        for node in graph.get("nodes", []):
            name = node.get("name", "")
            node_type = node.get("node_type", "TABLE")
            safe_name = name.replace(".", "_").replace("-", "_")
            style = node_styles.get(node_type, "")
            lines.append(f"    {safe_name}[{name}]{style}")

        # Add edges
        for edge in graph.get("edges", []):
            source = edge.get("source", "").replace(".", "_").replace("-", "_")
            target = edge.get("target", "").replace(".", "_").replace("-", "_")
            trans_type = edge.get("transformation_type", "")

            if trans_type and trans_type != "DIRECT":
                lines.append(f"    {source} -->|{trans_type}| {target}")
            else:
                lines.append(f"    {source} --> {target}")

        # Add style definitions
        lines.extend([
            "",
            "    classDef table fill:#E3F2FD,stroke:#1976D2",
            "    classDef view fill:#E8F5E9,stroke:#388E3C",
            "    classDef dynamic fill:#FFF3E0,stroke:#F57C00",
            "    classDef dbt fill:#E0F7FA,stroke:#0097A7",
            "    classDef hierarchy fill:#FCE4EC,stroke:#C2185B",
            "    classDef mart fill:#F3E5F5,stroke:#7B1FA2",
        ])

        return "\n".join(lines)
