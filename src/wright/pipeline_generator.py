"""
Mart Pipeline Generator.

Generates the 4-object DDL pipeline from configuration:
- VW_1: Translation View (CASE on ID_SOURCE)
- DT_2: Granularity Dynamic Table (UNPIVOT, exclusions)
- DT_3A: Pre-Aggregation Fact (UNION ALL branches, aggregation)
- DT_3: Data Mart (formula precedence, surrogate keys)
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .types import (
    MartConfig,
    PipelineObject,
    PipelineLayer,
    ObjectType,
    FormulaPrecedence,
)

logger = logging.getLogger(__name__)


class MartPipelineGenerator:
    """Generates 4-object DDL pipeline from configuration."""

    def __init__(self, output_dir: Optional[str] = None):
        """
        Initialize the pipeline generator.

        Args:
            output_dir: Directory for generated DDL files
        """
        self.output_dir = Path(output_dir) if output_dir else Path("data/mart_pipelines")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_vw1(self, config: MartConfig) -> PipelineObject:
        """
        Generate VW_1 Translation View.

        The Translation View reads from the hierarchy mapping table
        and translates abstract ID_SOURCE values to physical dimension
        column references using a CASE statement.

        Args:
            config: Mart configuration

        Returns:
            Generated PipelineObject
        """
        object_name = f"VW_1_{config.project_name.upper()}_TRANSLATED"

        # Build CASE statement for dynamic column mapping
        case_branches = []
        for mapping in config.dynamic_column_map:
            case_branches.append(
                f"        WHEN ID_SOURCE = '{mapping.id_source}' THEN {mapping.physical_column}"
            )

        case_statement = "CASE\n" + "\n".join(case_branches) + "\n        ELSE NULL\n    END"

        # Build dimension table joins
        dim_tables = set()
        for mapping in config.dynamic_column_map:
            if mapping.dimension_table:
                dim_tables.add(mapping.dimension_table)

        join_clauses = []
        for dim_table in dim_tables:
            alias = dim_table.split("_")[-1][:4].upper()
            join_clauses.append(
                f"LEFT JOIN {dim_table} {alias} ON MAP.ID = {alias}.ID"
            )

        joins_sql = "\n".join(join_clauses) if join_clauses else "-- No dimension joins defined"

        ddl = f"""-- VW_1: Translation View for {config.project_name}
-- Generated: {datetime.now().isoformat()}
-- Purpose: Parse hierarchy mapping metadata into join-ready columns

CREATE OR REPLACE VIEW {object_name} AS
SELECT
    MAP.FK_REPORT_KEY,
    MAP.ID,
    MAP.ID_NAME,
    MAP.ID_SOURCE,
    MAP.ID_TABLE,
    MAP.EXCLUSION_FLAG,
    MAP.FILTER_GROUP_1,
    MAP.FILTER_GROUP_2,
    MAP.FILTER_GROUP_3,
    MAP.FILTER_GROUP_4,
    {"MAP.GROUP_FILTER_PRECEDENCE," if config.has_group_filter_precedence else "-- GROUP_FILTER_PRECEDENCE not used"}

    -- Dynamic Column Mapping (ID_SOURCE to physical column)
    {case_statement} AS RESOLVED_VALUE,

    -- Hierarchy level columns (denormalized for performance)
    HIER.LEVEL_1,
    HIER.LEVEL_2,
    HIER.LEVEL_3,
    HIER.LEVEL_4,
    HIER.LEVEL_5,
    HIER.LEVEL_6,
    HIER.LEVEL_7,
    HIER.LEVEL_8,
    HIER.LEVEL_9,

    -- Formula metadata
    HIER.FORMULA_GROUP,
    HIER.FORMULA_PRECEDENCE,
    HIER.FORMULA_PARAM_REF,
    HIER.FORMULA_LOGIC,
    HIER.FORMULA_PARAM2_REF,

    -- Flags
    HIER.ACTIVE_FLAG,
    HIER.CALCULATION_FLAG,
    {"HIER.SIGN_CHANGE_FLAG," if config.has_sign_change else "-- SIGN_CHANGE_FLAG not used"}
    HIER.INCLUDE_FLAG,
    HIER.EXCLUDE_FLAG

FROM {config.mapping_table} MAP
INNER JOIN {config.hierarchy_table} HIER
    ON MAP.FK_REPORT_KEY = HIER.HIERARCHY_ID
{joins_sql}
WHERE HIER.ACTIVE_FLAG = TRUE
;
"""

        return PipelineObject(
            object_type=ObjectType.VIEW,
            object_name=object_name,
            layer=PipelineLayer.VW_1,
            layer_order=1,
            ddl=ddl,
            description="Translation View - CASE on ID_SOURCE to physical columns",
            dependencies=[config.mapping_table, config.hierarchy_table],
        )

    def generate_dt2(self, config: MartConfig) -> PipelineObject:
        """
        Generate DT_2 Granularity Dynamic Table.

        Converts translated mappings to the lowest grain for fact joins:
        - UNPIVOT FILTER_GROUP columns
        - Apply dynamic column mapping resolution
        - Handle exclusions via NOT IN subquery
        - Multi-round filtering if HAS_GROUP_FILTER_PRECEDENCE

        Args:
            config: Mart configuration

        Returns:
            Generated PipelineObject
        """
        object_name = f"DT_2_{config.project_name.upper()}_GRANULARITY"
        vw1_name = f"VW_1_{config.project_name.upper()}_TRANSLATED"

        # Build filter column list for each join pattern
        filter_columns = set()
        for pattern in config.join_patterns:
            for key in pattern.join_keys:
                filter_columns.add(key)

        filter_column_list = ", ".join(sorted(filter_columns)) if filter_columns else "RESOLVED_VALUE AS FILTER_VALUE"

        # Exclusion subquery
        exclusion_sql = ""
        if config.has_exclusions:
            exclusion_sql = f"""
    -- Exclusion filtering
    AND NOT EXISTS (
        SELECT 1 FROM VW1 EXCL
        WHERE EXCL.FK_REPORT_KEY = VW1.FK_REPORT_KEY
        AND EXCL.EXCLUSION_FLAG = TRUE
        AND VW1.RESOLVED_VALUE = EXCL.RESOLVED_VALUE
    )"""

        # Group filter precedence handling
        gfp_sql = ""
        if config.has_group_filter_precedence:
            gfp_sql = """
    -- Multi-round filtering by GROUP_FILTER_PRECEDENCE
    -- Precedence 1: Primary dimension join
    -- Precedence 2: Secondary filter dimension
    -- Precedence 3: Tertiary filter dimension
    GROUP_FILTER_PRECEDENCE,"""

        ddl = f"""-- DT_2: Granularity Dynamic Table for {config.project_name}
-- Generated: {datetime.now().isoformat()}
-- Purpose: Convert translated mappings to lowest grain for fact joins

CREATE OR REPLACE DYNAMIC TABLE {object_name}
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
AS
WITH VW1 AS (
    SELECT * FROM {vw1_name}
),

-- Unpivot filter groups to individual rows
UNPIVOTED AS (
    SELECT
        FK_REPORT_KEY,
        RESOLVED_VALUE,
        FILTER_GROUP_1,
        FILTER_GROUP_2,
        FILTER_GROUP_3,
        FILTER_GROUP_4,
        {gfp_sql}

        -- Hierarchy levels
        LEVEL_1, LEVEL_2, LEVEL_3, LEVEL_4, LEVEL_5,
        LEVEL_6, LEVEL_7, LEVEL_8, LEVEL_9,

        -- Formula metadata
        FORMULA_GROUP,
        FORMULA_PRECEDENCE,
        FORMULA_PARAM_REF,
        FORMULA_LOGIC,
        FORMULA_PARAM2_REF,

        -- Flags
        CALCULATION_FLAG,
        {"SIGN_CHANGE_FLAG," if config.has_sign_change else ""}
        INCLUDE_FLAG,
        EXCLUDE_FLAG

    FROM VW1
    WHERE CALCULATION_FLAG = FALSE  -- Only source rows, not calculated
    AND EXCLUSION_FLAG = FALSE
    {exclusion_sql}
),

-- Generate filter columns for each join pattern
FILTER_COLUMNS AS (
    SELECT
        FK_REPORT_KEY,

        -- Account filter (example - actual logic depends on ID_SOURCE)
        MAX(CASE WHEN FILTER_GROUP_1 IS NOT NULL
            THEN RESOLVED_VALUE END) AS LOS_ACCOUNT_ID_FILTER,

        -- Deduct filter
        MAX(CASE WHEN FILTER_GROUP_2 IS NOT NULL
            AND FILTER_GROUP_1 LIKE '%Deduct%'
            THEN RESOLVED_VALUE END) AS LOS_DEDUCT_CODE_FILTER,

        -- Product filter
        MAX(CASE WHEN FILTER_GROUP_3 IS NOT NULL
            OR FILTER_GROUP_1 LIKE '%Product%'
            THEN RESOLVED_VALUE END) AS LOS_PRODUCT_CODE_FILTER,

        -- Royalty filter (GROSS-specific)
        MAX(CASE WHEN FILTER_GROUP_1 LIKE '%Royalt%'
            THEN 'Y' END) AS ROYALTY_FILTER,

        -- Pass through hierarchy levels
        MAX(LEVEL_1) AS LEVEL_1,
        MAX(LEVEL_2) AS LEVEL_2,
        MAX(LEVEL_3) AS LEVEL_3,
        MAX(LEVEL_4) AS LEVEL_4,
        MAX(LEVEL_5) AS LEVEL_5,
        MAX(LEVEL_6) AS LEVEL_6,
        MAX(LEVEL_7) AS LEVEL_7,
        MAX(LEVEL_8) AS LEVEL_8,
        MAX(LEVEL_9) AS LEVEL_9,

        -- Formula metadata
        MAX(FORMULA_GROUP) AS FORMULA_GROUP,
        MAX(FORMULA_PRECEDENCE) AS FORMULA_PRECEDENCE,
        MAX(FORMULA_PARAM_REF) AS FORMULA_PARAM_REF,
        MAX(FORMULA_LOGIC) AS FORMULA_LOGIC,
        MAX(FORMULA_PARAM2_REF) AS FORMULA_PARAM2_REF,

        -- Flags
        MAX(CALCULATION_FLAG) AS CALCULATION_FLAG
        {"," if config.has_sign_change else ""}
        {"MAX(SIGN_CHANGE_FLAG) AS SIGN_CHANGE_FLAG" if config.has_sign_change else ""}

    FROM UNPIVOTED
    GROUP BY FK_REPORT_KEY
)

SELECT * FROM FILTER_COLUMNS
;
"""

        return PipelineObject(
            object_type=ObjectType.DYNAMIC_TABLE,
            object_name=object_name,
            layer=PipelineLayer.DT_2,
            layer_order=2,
            ddl=ddl,
            description="Granularity Table - UNPIVOT, exclusions, dynamic column mapping",
            dependencies=[vw1_name],
        )

    def generate_dt3a(self, config: MartConfig) -> PipelineObject:
        """
        Generate DT_3A Pre-Aggregation Fact Dynamic Table.

        Joins hierarchy metadata to fact table using UNION ALL branches:
        - One branch per join pattern
        - Apply ACCOUNT_SEGMENT filter
        - Handle SIGN_CHANGE_FLAG multiplication
        - Aggregate measures by hierarchy key

        Args:
            config: Mart configuration

        Returns:
            Generated PipelineObject
        """
        object_name = f"DT_3A_{config.project_name.upper()}"
        dt2_name = f"DT_2_{config.project_name.upper()}_GRANULARITY"
        fact_table = config.fact_table or "FACT_FINANCIAL_ACTUALS"

        # Build UNION ALL branches
        union_branches = []
        for i, pattern in enumerate(config.join_patterns):
            # Build join conditions
            join_conditions = []
            for j, (jk, fk) in enumerate(zip(pattern.join_keys, pattern.fact_keys)):
                join_conditions.append(f"DT2.{jk} = FACT.{fk}")

            join_sql = " AND ".join(join_conditions)

            # Build filter clause
            filter_sql = f"AND {pattern.filter}" if pattern.filter else ""

            # Sign change handling
            sign_multiplier = "CASE WHEN DT2.SIGN_CHANGE_FLAG = TRUE THEN -1 ELSE 1 END *" if config.has_sign_change else ""

            branch_sql = f"""
    -- Branch {i + 1}: {pattern.name}
    SELECT
        DT2.FK_REPORT_KEY,
        DT2.LEVEL_1, DT2.LEVEL_2, DT2.LEVEL_3, DT2.LEVEL_4, DT2.LEVEL_5,
        DT2.LEVEL_6, DT2.LEVEL_7, DT2.LEVEL_8, DT2.LEVEL_9,
        DT2.FORMULA_GROUP,
        DT2.FORMULA_PRECEDENCE,
        DT2.FORMULA_PARAM_REF,
        DT2.FORMULA_LOGIC,
        DT2.FORMULA_PARAM2_REF,

        -- Aggregated measures
        SUM({sign_multiplier} FACT.AMOUNT) AS {config.effective_measure_prefix}_AMOUNT,
        SUM({sign_multiplier} FACT.VOLUME) AS {config.effective_measure_prefix}_VOLUME,
        SUM({sign_multiplier} FACT.MCFE) AS {config.effective_measure_prefix}_MCFE,

        -- Fact keys for lineage
        '{pattern.name}' AS JOIN_BRANCH

    FROM {dt2_name} DT2
    INNER JOIN {fact_table} FACT
        ON {join_sql}
    WHERE FACT.ACCOUNT_SEGMENT = '{config.account_segment}'
    {filter_sql}
    GROUP BY
        DT2.FK_REPORT_KEY,
        DT2.LEVEL_1, DT2.LEVEL_2, DT2.LEVEL_3, DT2.LEVEL_4, DT2.LEVEL_5,
        DT2.LEVEL_6, DT2.LEVEL_7, DT2.LEVEL_8, DT2.LEVEL_9,
        DT2.FORMULA_GROUP,
        DT2.FORMULA_PRECEDENCE,
        DT2.FORMULA_PARAM_REF,
        DT2.FORMULA_LOGIC,
        DT2.FORMULA_PARAM2_REF"""

            union_branches.append(branch_sql)

        # Join branches with UNION ALL
        if union_branches:
            union_sql = "\n\n    UNION ALL\n".join(union_branches)
        else:
            # Fallback if no patterns defined
            union_sql = f"""
    -- No join patterns defined - simple passthrough
    SELECT
        DT2.FK_REPORT_KEY,
        DT2.LEVEL_1, DT2.LEVEL_2, DT2.LEVEL_3, DT2.LEVEL_4, DT2.LEVEL_5,
        DT2.LEVEL_6, DT2.LEVEL_7, DT2.LEVEL_8, DT2.LEVEL_9,
        DT2.FORMULA_GROUP,
        DT2.FORMULA_PRECEDENCE,
        DT2.FORMULA_PARAM_REF,
        DT2.FORMULA_LOGIC,
        DT2.FORMULA_PARAM2_REF,
        0 AS {config.effective_measure_prefix}_AMOUNT,
        0 AS {config.effective_measure_prefix}_VOLUME,
        0 AS {config.effective_measure_prefix}_MCFE,
        'none' AS JOIN_BRANCH
    FROM {dt2_name} DT2"""

        ddl = f"""-- DT_3A: Pre-Aggregation Fact for {config.project_name}
-- Generated: {datetime.now().isoformat()}
-- Purpose: Join hierarchy to fact, compute base measures with UNION ALL branches

CREATE OR REPLACE DYNAMIC TABLE {object_name}
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
AS
WITH AGGREGATED AS (
    {union_sql}
)

SELECT
    FK_REPORT_KEY,
    LEVEL_1, LEVEL_2, LEVEL_3, LEVEL_4, LEVEL_5,
    LEVEL_6, LEVEL_7, LEVEL_8, LEVEL_9,
    FORMULA_GROUP,
    FORMULA_PRECEDENCE,
    FORMULA_PARAM_REF,
    FORMULA_LOGIC,
    FORMULA_PARAM2_REF,

    -- Final aggregated measures (across all branches for same hierarchy key)
    SUM({config.effective_measure_prefix}_AMOUNT) AS {config.effective_measure_prefix}_AMOUNT,
    SUM({config.effective_measure_prefix}_VOLUME) AS {config.effective_measure_prefix}_VOLUME,
    SUM({config.effective_measure_prefix}_MCFE) AS {config.effective_measure_prefix}_MCFE,

    LISTAGG(DISTINCT JOIN_BRANCH, ',') AS JOIN_BRANCHES

FROM AGGREGATED
GROUP BY
    FK_REPORT_KEY,
    LEVEL_1, LEVEL_2, LEVEL_3, LEVEL_4, LEVEL_5,
    LEVEL_6, LEVEL_7, LEVEL_8, LEVEL_9,
    FORMULA_GROUP,
    FORMULA_PRECEDENCE,
    FORMULA_PARAM_REF,
    FORMULA_LOGIC,
    FORMULA_PARAM2_REF
;
"""

        return PipelineObject(
            object_type=ObjectType.DYNAMIC_TABLE,
            object_name=object_name,
            layer=PipelineLayer.DT_3A,
            layer_order=3,
            ddl=ddl,
            description="Pre-Aggregation Fact - UNION ALL branches, SUM aggregation",
            dependencies=[dt2_name, fact_table],
        )

    def generate_dt3(
        self,
        config: MartConfig,
        formulas: Optional[List[FormulaPrecedence]] = None,
    ) -> PipelineObject:
        """
        Generate DT_3 Data Mart Dynamic Table.

        Final output with:
        - 5-level formula precedence cascade
        - DENSE_RANK surrogate key generation
        - Hierarchy level backfill
        - Extension hierarchy join (if applicable)

        Args:
            config: Mart configuration
            formulas: Optional list of formula definitions

        Returns:
            Generated PipelineObject
        """
        object_name = f"DT_3_{config.project_name.upper()}"
        dt3a_name = f"DT_3A_{config.project_name.upper()}"
        prefix = config.effective_measure_prefix

        # Build formula precedence CTEs
        formula_ctes = self._build_formula_ctes(config, formulas or [])

        ddl = f"""-- DT_3: Data Mart for {config.project_name}
-- Generated: {datetime.now().isoformat()}
-- Purpose: Apply formula precedence, generate surrogate keys, backfill levels

CREATE OR REPLACE DYNAMIC TABLE {object_name}
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
AS
WITH
-- Base aggregated data from DT_3A
BASE_DATA AS (
    SELECT
        FK_REPORT_KEY,
        LEVEL_1, LEVEL_2, LEVEL_3, LEVEL_4, LEVEL_5,
        LEVEL_6, LEVEL_7, LEVEL_8, LEVEL_9,
        FORMULA_GROUP,
        FORMULA_PRECEDENCE,
        FORMULA_PARAM_REF,
        FORMULA_LOGIC,
        FORMULA_PARAM2_REF,
        {prefix}_AMOUNT,
        {prefix}_VOLUME,
        {prefix}_MCFE,
        0 AS IS_CALCULATED
    FROM {dt3a_name}
),

{formula_ctes}

-- Combine base data with calculated rows
COMBINED AS (
    SELECT * FROM BASE_DATA
    UNION ALL
    SELECT * FROM PRECEDENCE_5_CALC
),

-- Generate surrogate keys
WITH_KEYS AS (
    SELECT
        *,
        -- Surrogate keys using DENSE_RANK
        DENSE_RANK() OVER (ORDER BY LEVEL_1, LEVEL_2) AS LEVEL_2_KEY,
        DENSE_RANK() OVER (ORDER BY LEVEL_1, LEVEL_2, LEVEL_3) AS LEVEL_3_KEY,
        DENSE_RANK() OVER (ORDER BY LEVEL_1, LEVEL_2, LEVEL_3, LEVEL_4) AS LEVEL_4_KEY,
        DENSE_RANK() OVER (ORDER BY LEVEL_1, LEVEL_2, LEVEL_3, LEVEL_4, LEVEL_5) AS LEVEL_5_KEY

    FROM COMBINED
),

-- Backfill empty hierarchy levels
BACKFILLED AS (
    SELECT
        FK_REPORT_KEY,

        -- Backfill levels (use previous non-null level)
        LEVEL_1,
        COALESCE(LEVEL_2, LEVEL_1) AS LEVEL_2,
        COALESCE(LEVEL_3, LEVEL_2, LEVEL_1) AS LEVEL_3,
        COALESCE(LEVEL_4, LEVEL_3, LEVEL_2, LEVEL_1) AS LEVEL_4,
        COALESCE(LEVEL_5, LEVEL_4, LEVEL_3, LEVEL_2, LEVEL_1) AS LEVEL_5,
        COALESCE(LEVEL_6, LEVEL_5, LEVEL_4, LEVEL_3, LEVEL_2, LEVEL_1) AS LEVEL_6,
        COALESCE(LEVEL_7, LEVEL_6, LEVEL_5, LEVEL_4, LEVEL_3, LEVEL_2, LEVEL_1) AS LEVEL_7,
        COALESCE(LEVEL_8, LEVEL_7, LEVEL_6, LEVEL_5, LEVEL_4, LEVEL_3, LEVEL_2, LEVEL_1) AS LEVEL_8,
        COALESCE(LEVEL_9, LEVEL_8, LEVEL_7, LEVEL_6, LEVEL_5, LEVEL_4, LEVEL_3, LEVEL_2, LEVEL_1) AS LEVEL_9,

        -- Surrogate keys
        LEVEL_2_KEY,
        LEVEL_3_KEY,
        LEVEL_4_KEY,
        LEVEL_5_KEY,

        -- Measures
        {prefix}_AMOUNT,
        {prefix}_VOLUME,
        {prefix}_MCFE,

        -- Metadata
        FORMULA_GROUP,
        IS_CALCULATED

    FROM WITH_KEYS
)

SELECT * FROM BACKFILLED
;
"""

        return PipelineObject(
            object_type=ObjectType.DYNAMIC_TABLE,
            object_name=object_name,
            layer=PipelineLayer.DT_3,
            layer_order=4,
            ddl=ddl,
            description="Data Mart - formula precedence, surrogate keys, backfill",
            dependencies=[dt3a_name],
        )

    def _build_formula_ctes(
        self,
        config: MartConfig,
        formulas: List[FormulaPrecedence],
    ) -> str:
        """Build CTEs for 5-level formula precedence cascade."""
        prefix = config.effective_measure_prefix

        # Group formulas by precedence level
        by_level: Dict[int, List[FormulaPrecedence]] = {}
        for formula in formulas:
            level = formula.precedence_level
            if level not in by_level:
                by_level[level] = []
            by_level[level].append(formula)

        ctes = []

        # Generate CTE for each precedence level (1-5)
        for level in range(1, 6):
            source_cte = "BASE_DATA" if level == 1 else f"PRECEDENCE_{level-1}_CALC"
            formulas_at_level = by_level.get(level, [])

            if formulas_at_level:
                # Build calculated rows for this level
                calc_unions = []
                for formula in formulas_at_level:
                    calc_unions.append(self._build_formula_calc(formula, source_cte, prefix))

                calc_sql = "\n    UNION ALL\n".join(calc_unions)
                cte = f"""-- Precedence {level} calculations
PRECEDENCE_{level}_CALC AS (
    SELECT * FROM {source_cte}
    UNION ALL
    {calc_sql}
)"""
            else:
                # No formulas at this level - passthrough
                cte = f"""-- Precedence {level} (no formulas)
PRECEDENCE_{level}_CALC AS (
    SELECT * FROM {source_cte}
)"""

            ctes.append(cte)

        return ",\n\n".join(ctes)

    def _build_formula_calc(
        self,
        formula: FormulaPrecedence,
        source_cte: str,
        prefix: str,
    ) -> str:
        """Build a single formula calculation row."""
        logic = formula.logic

        if logic == "SUM":
            amount_expr = f"SUM({prefix}_AMOUNT)"
            volume_expr = f"SUM({prefix}_VOLUME)"
            mcfe_expr = f"SUM({prefix}_MCFE)"
        elif logic == "SUBTRACT":
            amount_expr = f"(SELECT SUM({prefix}_AMOUNT) FROM {source_cte} WHERE FORMULA_GROUP = '{formula.param_ref}') - (SELECT SUM({prefix}_AMOUNT) FROM {source_cte} WHERE FORMULA_GROUP = '{formula.param2_ref}')"
            volume_expr = f"(SELECT SUM({prefix}_VOLUME) FROM {source_cte} WHERE FORMULA_GROUP = '{formula.param_ref}') - (SELECT SUM({prefix}_VOLUME) FROM {source_cte} WHERE FORMULA_GROUP = '{formula.param2_ref}')"
            mcfe_expr = f"(SELECT SUM({prefix}_MCFE) FROM {source_cte} WHERE FORMULA_GROUP = '{formula.param_ref}') - (SELECT SUM({prefix}_MCFE) FROM {source_cte} WHERE FORMULA_GROUP = '{formula.param2_ref}')"
        else:
            # Default to SUM
            amount_expr = f"SUM({prefix}_AMOUNT)"
            volume_expr = f"SUM({prefix}_VOLUME)"
            mcfe_expr = f"SUM({prefix}_MCFE)"

        return f"""    SELECT
        '{formula.hierarchy_key or "CALC_" + formula.formula_group}' AS FK_REPORT_KEY,
        NULL AS LEVEL_1, NULL AS LEVEL_2, NULL AS LEVEL_3, NULL AS LEVEL_4, NULL AS LEVEL_5,
        NULL AS LEVEL_6, NULL AS LEVEL_7, NULL AS LEVEL_8, NULL AS LEVEL_9,
        '{formula.formula_group}' AS FORMULA_GROUP,
        {formula.precedence_level} AS FORMULA_PRECEDENCE,
        '{formula.param_ref}' AS FORMULA_PARAM_REF,
        '{formula.logic.value}' AS FORMULA_LOGIC,
        '{formula.param2_ref or ""}' AS FORMULA_PARAM2_REF,
        {amount_expr} AS {prefix}_AMOUNT,
        {volume_expr} AS {prefix}_VOLUME,
        {mcfe_expr} AS {prefix}_MCFE,
        1 AS IS_CALCULATED
    FROM {source_cte}
    WHERE FORMULA_GROUP = '{formula.param_ref}'"""

    def generate_full_pipeline(
        self,
        config: MartConfig,
        formulas: Optional[List[FormulaPrecedence]] = None,
    ) -> List[PipelineObject]:
        """
        Generate all 4 pipeline objects.

        Args:
            config: Mart configuration
            formulas: Optional formula definitions

        Returns:
            List of generated PipelineObjects
        """
        objects = [
            self.generate_vw1(config),
            self.generate_dt2(config),
            self.generate_dt3a(config),
            self.generate_dt3(config, formulas),
        ]

        logger.info(f"Generated full pipeline for {config.project_name}: {len(objects)} objects")
        return objects

    def export_pipeline(
        self,
        config: MartConfig,
        output_dir: Optional[str] = None,
        formulas: Optional[List[FormulaPrecedence]] = None,
    ) -> Dict[str, str]:
        """
        Export pipeline to SQL files.

        Args:
            config: Mart configuration
            output_dir: Output directory
            formulas: Optional formula definitions

        Returns:
            Dict mapping object names to file paths
        """
        objects = self.generate_full_pipeline(config, formulas)
        out_dir = Path(output_dir) if output_dir else self.output_dir / config.project_name
        out_dir.mkdir(parents=True, exist_ok=True)

        result = {}
        for obj in objects:
            file_name = f"{obj.layer_order:02d}_{obj.object_name}.sql"
            file_path = out_dir / file_name
            file_path.write_text(obj.ddl)
            result[obj.object_name] = str(file_path)

        logger.info(f"Exported pipeline to {out_dir}")
        return result

    def generate_dbt_models(
        self,
        config: MartConfig,
        output_dir: str,
        formulas: Optional[List[FormulaPrecedence]] = None,
    ) -> Dict[str, str]:
        """
        Generate dbt model files from pipeline.

        Args:
            config: Mart configuration
            output_dir: dbt models directory
            formulas: Optional formula definitions

        Returns:
            Dict mapping model names to file paths
        """
        objects = self.generate_full_pipeline(config, formulas)
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        result = {}
        for obj in objects:
            # Convert DDL to dbt model format
            model_sql = self._convert_to_dbt_model(obj)
            model_name = obj.object_name.lower()
            file_path = out_dir / f"{model_name}.sql"
            file_path.write_text(model_sql)
            result[model_name] = str(file_path)

        # Generate schema.yml
        schema_yml = self._generate_dbt_schema(config, objects)
        schema_path = out_dir / "schema.yml"
        schema_path.write_text(schema_yml)
        result["schema"] = str(schema_path)

        return result

    def _convert_to_dbt_model(self, obj: PipelineObject) -> str:
        """Convert DDL to dbt model format."""
        # Extract the SELECT portion from the DDL
        ddl = obj.ddl

        # Find the AS clause
        as_idx = ddl.upper().find(" AS\n")
        if as_idx == -1:
            as_idx = ddl.upper().find(" AS\r\n")
        if as_idx == -1:
            return ddl  # Return as-is if can't parse

        select_sql = ddl[as_idx + 4:].strip().rstrip(";")

        # Add dbt config block
        if obj.object_type == ObjectType.DYNAMIC_TABLE:
            config_block = """{{
  config(
    materialized='incremental',
    unique_key='FK_REPORT_KEY'
  )
}}

"""
        else:
            config_block = """{{
  config(
    materialized='view'
  )
}}

"""

        # Add model description
        header = f"-- {obj.description}\n\n"

        return header + config_block + select_sql

    def _generate_dbt_schema(
        self,
        config: MartConfig,
        objects: List[PipelineObject],
    ) -> str:
        """Generate dbt schema.yml for the pipeline."""
        models = []
        for obj in objects:
            models.append({
                "name": obj.object_name.lower(),
                "description": obj.description,
                "config": {
                    "tags": ["mart_factory", config.project_name],
                },
            })

        schema = {
            "version": 2,
            "models": models,
        }

        import yaml
        return yaml.dump(schema, default_flow_style=False, sort_keys=False)
