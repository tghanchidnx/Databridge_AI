"""
View Generator Service for SQL Generator.

Generates VW_1 tier views from:
- Hierarchy definitions and mappings
- Detected patterns
- Manual specifications
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
import os

from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy.orm import Session

from .models import (
    GeneratedView,
    SQLDialect,
    PatternType,
    TransformationType,
    ObjectType,
)
from .pattern_detector import PatternDetectorService, get_pattern_detector
from .lineage_tracker import LineageTrackerService, get_lineage_tracker


class ViewGeneratorService:
    """
    Service for generating SQL views (VW_1 tier).

    Provides:
    - Generate views from hierarchies
    - Generate views from detected patterns
    - Preview SQL without saving
    - Multi-dialect support (Snowflake, PostgreSQL)
    """

    def __init__(
        self,
        session: Optional[Session] = None,
        pattern_detector: Optional[PatternDetectorService] = None,
        lineage_tracker: Optional[LineageTrackerService] = None,
    ):
        """
        Initialize the view generator service.

        Args:
            session: Optional SQLAlchemy session
            pattern_detector: Optional pattern detector service
            lineage_tracker: Optional lineage tracker service
        """
        self._session = session
        self._pattern_detector = pattern_detector or get_pattern_detector()
        self._lineage_tracker = lineage_tracker

        # Set up Jinja2 template environment
        template_dir = Path(__file__).parent / "templates"
        self._env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=select_autoescape(default=False),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def _get_session(self, session: Optional[Session] = None) -> Session:
        """Get session from parameter or instance."""
        if session:
            return session
        if self._session:
            return self._session
        raise ValueError("No database session available")

    def _get_template(self, dialect: SQLDialect, template_name: str):
        """Get Jinja2 template for dialect."""
        dialect_dir = dialect.value.lower()
        return self._env.get_template(f"{dialect_dir}/{template_name}")

    def generate_view_sql(
        self,
        view_name: str,
        source_table: str,
        columns: List[Dict[str, Any]],
        dialect: SQLDialect = SQLDialect.SNOWFLAKE,
        source_database: Optional[str] = None,
        source_schema: Optional[str] = None,
        source_alias: Optional[str] = None,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        joins: Optional[List[Dict[str, Any]]] = None,
        filters: Optional[List[Dict[str, Any]]] = None,
        group_by: Optional[List[str]] = None,
        order_by: Optional[List[Dict[str, Any]]] = None,
        create_or_replace: bool = True,
        comment: Optional[str] = None,
    ) -> str:
        """
        Generate SQL for a view using templates.

        Args:
            view_name: Name of the view to create
            source_table: Name of the source table
            columns: List of column definitions
            dialect: SQL dialect
            source_database: Optional source database
            source_schema: Optional source schema
            source_alias: Optional alias for source table
            target_database: Optional target database
            target_schema: Optional target schema
            joins: Optional list of joins
            filters: Optional list of filters
            group_by: Optional list of group by columns
            order_by: Optional list of order by definitions
            create_or_replace: Whether to use CREATE OR REPLACE
            comment: Optional comment for the view

        Returns:
            Generated SQL string
        """
        template = self._get_template(dialect, "view.j2")

        sql = template.render(
            view_name=view_name,
            source_table=source_table,
            source_database=source_database,
            source_schema=source_schema,
            source_alias=source_alias,
            target_database=target_database,
            target_schema=target_schema,
            columns=columns,
            joins=joins or [],
            filters=filters or [],
            group_by=group_by or [],
            order_by=order_by or [],
            create_or_replace=create_or_replace,
            comment=comment,
        )

        return sql.strip()

    def generate_view_from_hierarchy(
        self,
        project_id: str,
        hierarchy_id: str,
        view_name: str,
        dialect: SQLDialect = SQLDialect.SNOWFLAKE,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        include_measures: bool = True,
        include_dimensions: bool = True,
        session: Optional[Session] = None,
    ) -> GeneratedView:
        """
        Generate a view from a hierarchy definition and its mappings.

        Args:
            project_id: Project ID
            hierarchy_id: Hierarchy ID to generate view for
            view_name: Name for the generated view
            dialect: SQL dialect
            target_database: Target database name
            target_schema: Target schema name
            include_measures: Include measure columns
            include_dimensions: Include dimension columns
            session: Optional database session

        Returns:
            Created GeneratedView object
        """
        db = self._get_session(session)

        # Import here to avoid circular imports
        from core.database import Hierarchy, SourceMapping

        # Get hierarchy and its mappings
        hierarchy = db.query(Hierarchy).filter(
            Hierarchy.project_id == project_id,
            Hierarchy.hierarchy_id == hierarchy_id,
            Hierarchy.is_current == True,
        ).first()

        if not hierarchy:
            raise ValueError(f"Hierarchy not found: {hierarchy_id}")

        mappings = db.query(SourceMapping).filter(
            SourceMapping.hierarchy_id == hierarchy_id,
            SourceMapping.include_flag == True,
        ).order_by(SourceMapping.mapping_index).all()

        if not mappings:
            raise ValueError(f"No source mappings found for hierarchy: {hierarchy_id}")

        # Use first mapping's source as the base table
        primary_mapping = mappings[0]
        source_table = primary_mapping.source_table
        source_database = primary_mapping.source_database
        source_schema = primary_mapping.source_schema

        # Build column list from mappings
        columns = []
        filters = []

        for mapping in mappings:
            col_def = {
                "name": mapping.source_column,
                "alias": mapping.source_column,
            }

            # Add to columns if from same table
            if mapping.source_table == source_table:
                columns.append(col_def)

            # Add filter for source_uid if specified
            if mapping.source_uid:
                filters.append({
                    "column": mapping.source_column,
                    "operator": "LIKE" if "%" in mapping.source_uid else "=",
                    "value": mapping.source_uid,
                })

        # Add hierarchy level as dimension
        if include_dimensions and hierarchy.hierarchy_name:
            columns.insert(0, {
                "expression": f"'{hierarchy.hierarchy_name}'",
                "alias": "HIERARCHY_NAME",
            })

        # Generate SQL
        sql = self.generate_view_sql(
            view_name=view_name,
            source_table=source_table,
            columns=columns,
            dialect=dialect,
            source_database=source_database,
            source_schema=source_schema,
            target_database=target_database,
            target_schema=target_schema,
            filters=filters if filters else None,
            comment=f"Generated from hierarchy: {hierarchy.hierarchy_name}",
        )

        # Create GeneratedView record
        view = GeneratedView(
            project_id=project_id,
            view_name=view_name,
            display_name=hierarchy.hierarchy_name,
            description=f"Auto-generated view for hierarchy {hierarchy_id}",
            source_database=source_database,
            source_schema=source_schema,
            source_table=source_table,
            pattern_type=PatternType.FACT,  # Hierarchies typically map to facts
            select_columns=[{"name": c.get("name") or c.get("alias")} for c in columns],
            filters=filters,
            generated_sql=sql,
            dialect=dialect,
            target_database=target_database,
            target_schema=target_schema,
            hierarchy_id=hierarchy_id,
        )

        db.add(view)
        db.commit()
        db.refresh(view)

        # Track lineage
        if self._lineage_tracker:
            self._lineage_tracker.track_lineage(
                project_id=project_id,
                source_object=f"{source_database}.{source_schema}.{source_table}".strip("."),
                target_object=f"{target_database}.{target_schema}.{view_name}".strip("."),
                transformation_type=TransformationType.SELECT,
                source_object_type=ObjectType.TABLE,
                target_object_type=ObjectType.VIEW,
                target_object_id=view.id,
                description=f"View generated from hierarchy {hierarchy_id}",
                session=db,
            )

        return view

    def generate_view_from_pattern(
        self,
        project_id: str,
        view_name: str,
        source_table: str,
        columns: List[Dict[str, Any]],
        dialect: SQLDialect = SQLDialect.SNOWFLAKE,
        source_database: Optional[str] = None,
        source_schema: Optional[str] = None,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        pattern_type: Optional[PatternType] = None,
        measure_columns: Optional[List[str]] = None,
        dimension_columns: Optional[List[str]] = None,
        key_columns: Optional[List[str]] = None,
        session: Optional[Session] = None,
    ) -> GeneratedView:
        """
        Generate a view from detected pattern.

        Args:
            project_id: Project ID
            view_name: Name for the view
            source_table: Source table name
            columns: Column definitions with types
            dialect: SQL dialect
            source_database: Source database
            source_schema: Source schema
            target_database: Target database
            target_schema: Target schema
            pattern_type: Detected pattern type
            measure_columns: List of measure column names to include
            dimension_columns: List of dimension column names to include
            key_columns: List of key column names to include
            session: Database session

        Returns:
            Created GeneratedView object
        """
        db = self._get_session(session)

        # If pattern not provided, detect it
        if not pattern_type:
            pattern = self._pattern_detector.detect_table_pattern(
                source_table, columns
            )
            pattern_type = pattern.pattern_type

        # Build column selection based on pattern and parameters
        select_columns = []

        # Always include keys first
        if key_columns:
            for key in key_columns:
                col = next((c for c in columns if c["name"] == key), None)
                if col:
                    select_columns.append({"name": col["name"]})

        # Include dimensions
        if dimension_columns:
            for dim in dimension_columns:
                col = next((c for c in columns if c["name"] == dim), None)
                if col and col["name"] not in [c["name"] for c in select_columns]:
                    select_columns.append({"name": col["name"]})

        # Include measures
        if measure_columns:
            for meas in measure_columns:
                col = next((c for c in columns if c["name"] == meas), None)
                if col and col["name"] not in [c["name"] for c in select_columns]:
                    select_columns.append({"name": col["name"]})

        # If no specific columns requested, include all
        if not select_columns:
            select_columns = [{"name": c["name"]} for c in columns]

        # Generate SQL
        sql = self.generate_view_sql(
            view_name=view_name,
            source_table=source_table,
            columns=select_columns,
            dialect=dialect,
            source_database=source_database,
            source_schema=source_schema,
            target_database=target_database,
            target_schema=target_schema,
            comment=f"Generated from {pattern_type.value} pattern",
        )

        # Create GeneratedView record
        view = GeneratedView(
            project_id=project_id,
            view_name=view_name,
            description=f"Auto-generated {pattern_type.value} view from {source_table}",
            source_database=source_database,
            source_schema=source_schema,
            source_table=source_table,
            pattern_type=pattern_type,
            detected_columns=[
                {
                    "name": c["name"],
                    "type": c.get("type", "unknown"),
                    "data_type": c.get("data_type", ""),
                }
                for c in columns
            ],
            select_columns=select_columns,
            generated_sql=sql,
            dialect=dialect,
            target_database=target_database,
            target_schema=target_schema,
        )

        db.add(view)
        db.commit()
        db.refresh(view)

        # Track lineage
        if self._lineage_tracker:
            self._lineage_tracker.track_lineage(
                project_id=project_id,
                source_object=f"{source_database}.{source_schema}.{source_table}".strip("."),
                target_object=f"{target_database}.{target_schema}.{view_name}".strip("."),
                transformation_type=TransformationType.SELECT,
                source_object_type=ObjectType.TABLE,
                target_object_type=ObjectType.VIEW,
                target_object_id=view.id,
                description=f"View generated from {pattern_type.value} pattern",
                session=db,
            )

        return view

    def preview_view_sql(
        self,
        view_name: str,
        source_table: str,
        columns: List[Dict[str, Any]],
        dialect: SQLDialect = SQLDialect.SNOWFLAKE,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Preview SQL without saving to database.

        Args:
            view_name: Name of the view
            source_table: Source table name
            columns: Column definitions
            dialect: SQL dialect
            **kwargs: Additional arguments passed to generate_view_sql

        Returns:
            Dictionary with SQL preview and metadata
        """
        sql = self.generate_view_sql(
            view_name=view_name,
            source_table=source_table,
            columns=columns,
            dialect=dialect,
            **kwargs,
        )

        return {
            "view_name": view_name,
            "source_table": source_table,
            "dialect": dialect.value,
            "column_count": len(columns),
            "sql": sql,
            "estimated_length": len(sql),
        }

    def list_generated_views(
        self,
        project_id: str,
        pattern_type: Optional[PatternType] = None,
        is_active: bool = True,
        session: Optional[Session] = None,
    ) -> List[GeneratedView]:
        """
        List generated views for a project.

        Args:
            project_id: Project ID
            pattern_type: Optional filter by pattern type
            is_active: Filter by active status
            session: Database session

        Returns:
            List of GeneratedView objects
        """
        db = self._get_session(session)

        query = db.query(GeneratedView).filter(
            GeneratedView.project_id == project_id,
            GeneratedView.is_active == is_active,
        )

        if pattern_type:
            query = query.filter(GeneratedView.pattern_type == pattern_type)

        return query.order_by(GeneratedView.created_at.desc()).all()

    def get_generated_view(
        self,
        view_id: str,
        session: Optional[Session] = None,
    ) -> Optional[GeneratedView]:
        """
        Get a generated view by ID.

        Args:
            view_id: View ID
            session: Database session

        Returns:
            GeneratedView or None
        """
        db = self._get_session(session)
        return db.query(GeneratedView).filter(GeneratedView.id == view_id).first()

    def update_view_sql(
        self,
        view_id: str,
        columns: Optional[List[Dict[str, Any]]] = None,
        filters: Optional[List[Dict[str, Any]]] = None,
        joins: Optional[List[Dict[str, Any]]] = None,
        regenerate: bool = True,
        session: Optional[Session] = None,
    ) -> GeneratedView:
        """
        Update an existing generated view.

        Args:
            view_id: View ID to update
            columns: New column list (optional)
            filters: New filter list (optional)
            joins: New join list (optional)
            regenerate: Whether to regenerate SQL
            session: Database session

        Returns:
            Updated GeneratedView
        """
        db = self._get_session(session)

        view = db.query(GeneratedView).filter(GeneratedView.id == view_id).first()
        if not view:
            raise ValueError(f"View not found: {view_id}")

        if columns:
            view.select_columns = columns
        if filters is not None:
            view.filters = filters
        if joins is not None:
            view.joins = joins

        if regenerate:
            sql = self.generate_view_sql(
                view_name=view.view_name,
                source_table=view.source_table,
                columns=view.select_columns,
                dialect=view.dialect,
                source_database=view.source_database,
                source_schema=view.source_schema,
                target_database=view.target_database,
                target_schema=view.target_schema,
                filters=view.filters,
                joins=view.joins,
            )
            view.generated_sql = sql

        view.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(view)

        return view

    def delete_view(
        self,
        view_id: str,
        soft_delete: bool = True,
        session: Optional[Session] = None,
    ) -> bool:
        """
        Delete a generated view.

        Args:
            view_id: View ID to delete
            soft_delete: If True, mark as inactive; otherwise delete
            session: Database session

        Returns:
            True if successful
        """
        db = self._get_session(session)

        view = db.query(GeneratedView).filter(GeneratedView.id == view_id).first()
        if not view:
            return False

        if soft_delete:
            view.is_active = False
            view.updated_at = datetime.utcnow()
        else:
            db.delete(view)

        db.commit()
        return True


# Singleton instance
_view_generator = None


def get_view_generator(session: Optional[Session] = None) -> ViewGeneratorService:
    """Get or create the view generator singleton."""
    global _view_generator
    if _view_generator is None:
        _view_generator = ViewGeneratorService(session)
    return _view_generator
