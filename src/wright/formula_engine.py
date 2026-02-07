"""
Formula Precedence Engine.

Manages 5-level formula precedence for data mart calculations:
- Extract formula definitions from hierarchy data
- Build precedence chains
- Generate calculation SQL
- Validate formula dependencies
"""

import logging
from typing import Any, Dict, List, Optional

from .types import (
    FormulaPrecedence,
    FormulaLogic,
)

logger = logging.getLogger(__name__)


class FormulaPrecedenceEngine:
    """Manages formula precedence for data mart calculations."""

    # Logic type mapping
    LOGIC_MAP = {
        "SUM": FormulaLogic.SUM,
        "ADD": FormulaLogic.SUM,
        "SUBTRACT": FormulaLogic.SUBTRACT,
        "SUB": FormulaLogic.SUBTRACT,
        "MINUS": FormulaLogic.SUBTRACT,
        "MULTIPLY": FormulaLogic.MULTIPLY,
        "MUL": FormulaLogic.MULTIPLY,
        "TIMES": FormulaLogic.MULTIPLY,
        "DIVIDE": FormulaLogic.DIVIDE,
        "DIV": FormulaLogic.DIVIDE,
        "AVERAGE": FormulaLogic.AVERAGE,
        "AVG": FormulaLogic.AVERAGE,
    }

    def __init__(self):
        """Initialize the formula engine."""
        self._formulas: Dict[str, List[FormulaPrecedence]] = {}

    def extract_formulas(
        self,
        hierarchy_data: List[Dict[str, Any]],
        project_name: str = "default",
    ) -> List[FormulaPrecedence]:
        """
        Extract formula definitions from hierarchy data.

        Parses hierarchy nodes that have FORMULA_GROUP and FORMULA_PRECEDENCE
        columns to extract calculation definitions.

        Args:
            hierarchy_data: List of hierarchy row dictionaries
            project_name: Project name for storing formulas

        Returns:
            List of extracted FormulaPrecedence objects
        """
        formulas = []

        for row in hierarchy_data:
            # Check if this is a calculation row
            formula_group = row.get("FORMULA_GROUP") or row.get("formula_group")
            if not formula_group:
                continue

            precedence = row.get("FORMULA_PRECEDENCE") or row.get("formula_precedence")
            if precedence is None:
                continue

            # Parse precedence level
            try:
                precedence_level = int(precedence)
            except (ValueError, TypeError):
                continue

            # Get formula parameters
            param_ref = row.get("FORMULA_PARAM_REF") or row.get("formula_param_ref") or ""
            param2_ref = row.get("FORMULA_PARAM2_REF") or row.get("formula_param2_ref")
            logic_str = row.get("FORMULA_LOGIC") or row.get("formula_logic") or "SUM"

            # Map logic string to enum
            logic = self.LOGIC_MAP.get(logic_str.upper(), FormulaLogic.SUM)

            # Get hierarchy key if available
            hierarchy_key = (
                row.get("FK_REPORT_KEY") or
                row.get("HIERARCHY_ID") or
                row.get("hierarchy_id")
            )

            # Parse additional parameters (comma-separated)
            additional_params = []
            if param2_ref and "," in str(param2_ref):
                parts = [p.strip() for p in str(param2_ref).split(",")]
                param2_ref = parts[0]
                additional_params = parts[1:]

            formula = FormulaPrecedence(
                precedence_level=precedence_level,
                formula_group=formula_group,
                hierarchy_key=str(hierarchy_key) if hierarchy_key else None,
                logic=logic,
                param_ref=param_ref,
                param2_ref=param2_ref,
                additional_params=additional_params,
            )

            formulas.append(formula)

        # Store formulas by project
        self._formulas[project_name] = formulas

        logger.info(f"Extracted {len(formulas)} formulas for project {project_name}")
        return formulas

    def build_precedence_chain(
        self,
        formulas: List[FormulaPrecedence],
    ) -> Dict[int, List[FormulaPrecedence]]:
        """
        Group formulas by precedence level (1-5).

        Args:
            formulas: List of formula definitions

        Returns:
            Dict mapping precedence level to list of formulas
        """
        chain: Dict[int, List[FormulaPrecedence]] = {
            1: [],
            2: [],
            3: [],
            4: [],
            5: [],
        }

        for formula in formulas:
            level = formula.precedence_level
            if 1 <= level <= 5:
                chain[level].append(formula)
            else:
                logger.warning(f"Formula has invalid precedence level {level}: {formula.formula_group}")

        return chain

    def generate_calculation_sql(
        self,
        precedence_level: int,
        formulas: List[FormulaPrecedence],
        source_table: str,
        measure_prefix: str = "GROSS",
    ) -> str:
        """
        Generate SQL for a single precedence level.

        Args:
            precedence_level: Level (1-5)
            formulas: Formulas at this level
            source_table: Source CTE or table name
            measure_prefix: Column name prefix

        Returns:
            SQL for the calculation rows
        """
        if not formulas:
            return f"-- No formulas at precedence level {precedence_level}"

        union_parts = []

        for formula in formulas:
            calc_sql = self._generate_formula_sql(
                formula, source_table, measure_prefix
            )
            union_parts.append(calc_sql)

        return "\nUNION ALL\n".join(union_parts)

    def generate_cascade_cte(
        self,
        all_formulas: List[FormulaPrecedence],
        base_table: str = "DT_3A",
        measure_prefix: str = "GROSS",
    ) -> str:
        """
        Generate full 5-level cascade as CTEs.

        P1: Base aggregations from DT_3A (totals)
        P2-P5: Calculated rows injected via UNION ALL

        Args:
            all_formulas: All formula definitions
            base_table: Base table/CTE name
            measure_prefix: Column name prefix

        Returns:
            SQL with CTE definitions
        """
        chain = self.build_precedence_chain(all_formulas)

        ctes = []
        prev_cte = base_table

        for level in range(1, 6):
            formulas_at_level = chain[level]
            cte_name = f"PRECEDENCE_{level}"

            if formulas_at_level:
                calc_sql = self.generate_calculation_sql(
                    level, formulas_at_level, prev_cte, measure_prefix
                )

                cte = f"""{cte_name} AS (
    -- Precedence {level}: {len(formulas_at_level)} calculations
    SELECT * FROM {prev_cte}
    UNION ALL
    {calc_sql}
)"""
            else:
                cte = f"""{cte_name} AS (
    -- Precedence {level}: passthrough (no formulas)
    SELECT * FROM {prev_cte}
)"""

            ctes.append(cte)
            prev_cte = cte_name

        return ",\n\n".join(ctes)

    def validate_dependencies(
        self,
        formulas: List[FormulaPrecedence],
    ) -> Dict[str, Any]:
        """
        Validate formula dependencies are satisfiable.

        Checks that:
        - Referenced formula groups exist
        - Lower precedence formulas are computed before higher ones
        - No circular dependencies

        Args:
            formulas: List of formula definitions

        Returns:
            Validation result with errors and warnings
        """
        errors = []
        warnings = []

        # Build lookup of formula groups by precedence
        group_by_precedence: Dict[str, int] = {}
        for formula in formulas:
            group_by_precedence[formula.formula_group] = formula.precedence_level

        # Check each formula's dependencies
        for formula in formulas:
            # Check param_ref exists
            if formula.param_ref:
                if formula.param_ref not in group_by_precedence:
                    warnings.append(
                        f"Formula '{formula.formula_group}' references unknown group '{formula.param_ref}'"
                    )
                else:
                    ref_level = group_by_precedence[formula.param_ref]
                    if ref_level >= formula.precedence_level:
                        errors.append(
                            f"Formula '{formula.formula_group}' (P{formula.precedence_level}) "
                            f"references '{formula.param_ref}' (P{ref_level}) - "
                            "dependency must have lower precedence"
                        )

            # Check param2_ref exists
            if formula.param2_ref:
                if formula.param2_ref not in group_by_precedence:
                    warnings.append(
                        f"Formula '{formula.formula_group}' references unknown group '{formula.param2_ref}'"
                    )
                else:
                    ref_level = group_by_precedence[formula.param2_ref]
                    if ref_level >= formula.precedence_level:
                        errors.append(
                            f"Formula '{formula.formula_group}' (P{formula.precedence_level}) "
                            f"references '{formula.param2_ref}' (P{ref_level}) - "
                            "dependency must have lower precedence"
                        )

        # Check for duplicate formula groups
        seen_groups: Dict[str, int] = {}
        for formula in formulas:
            if formula.formula_group in seen_groups:
                warnings.append(
                    f"Duplicate formula group '{formula.formula_group}' at precedence levels "
                    f"{seen_groups[formula.formula_group]} and {formula.precedence_level}"
                )
            seen_groups[formula.formula_group] = formula.precedence_level

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "formula_count": len(formulas),
            "levels_used": sorted(set(f.precedence_level for f in formulas)),
        }

    def get_formulas(self, project_name: str = "default") -> List[FormulaPrecedence]:
        """Get stored formulas for a project."""
        return self._formulas.get(project_name, [])

    def _generate_formula_sql(
        self,
        formula: FormulaPrecedence,
        source_table: str,
        measure_prefix: str,
    ) -> str:
        """Generate SQL for a single formula calculation."""
        logic = formula.logic

        # Build measure expressions based on logic type
        if logic == FormulaLogic.SUM:
            amount_expr = f"SUM({measure_prefix}_AMOUNT)"
            volume_expr = f"SUM({measure_prefix}_VOLUME)"
            mcfe_expr = f"SUM({measure_prefix}_MCFE)"
            where_clause = f"WHERE FORMULA_GROUP IN ('{formula.param_ref}')"

        elif logic == FormulaLogic.SUBTRACT:
            # Subtract: param_ref - param2_ref
            amount_expr = f"""(
        SELECT SUM({measure_prefix}_AMOUNT) FROM {source_table}
        WHERE FORMULA_GROUP = '{formula.param_ref}'
    ) - (
        SELECT COALESCE(SUM({measure_prefix}_AMOUNT), 0) FROM {source_table}
        WHERE FORMULA_GROUP = '{formula.param2_ref}'
    )"""
            volume_expr = f"""(
        SELECT SUM({measure_prefix}_VOLUME) FROM {source_table}
        WHERE FORMULA_GROUP = '{formula.param_ref}'
    ) - (
        SELECT COALESCE(SUM({measure_prefix}_VOLUME), 0) FROM {source_table}
        WHERE FORMULA_GROUP = '{formula.param2_ref}'
    )"""
            mcfe_expr = f"""(
        SELECT SUM({measure_prefix}_MCFE) FROM {source_table}
        WHERE FORMULA_GROUP = '{formula.param_ref}'
    ) - (
        SELECT COALESCE(SUM({measure_prefix}_MCFE), 0) FROM {source_table}
        WHERE FORMULA_GROUP = '{formula.param2_ref}'
    )"""
            where_clause = ""

        elif logic == FormulaLogic.MULTIPLY:
            amount_expr = f"""(
        SELECT SUM({measure_prefix}_AMOUNT) FROM {source_table}
        WHERE FORMULA_GROUP = '{formula.param_ref}'
    ) * (
        SELECT COALESCE(SUM({measure_prefix}_AMOUNT), 1) FROM {source_table}
        WHERE FORMULA_GROUP = '{formula.param2_ref}'
    )"""
            volume_expr = f"0"
            mcfe_expr = f"0"
            where_clause = ""

        elif logic == FormulaLogic.DIVIDE:
            amount_expr = f"""CASE
        WHEN (SELECT SUM({measure_prefix}_AMOUNT) FROM {source_table}
              WHERE FORMULA_GROUP = '{formula.param2_ref}') = 0 THEN 0
        ELSE (
            SELECT SUM({measure_prefix}_AMOUNT) FROM {source_table}
            WHERE FORMULA_GROUP = '{formula.param_ref}'
        ) / NULLIF((
            SELECT SUM({measure_prefix}_AMOUNT) FROM {source_table}
            WHERE FORMULA_GROUP = '{formula.param2_ref}'
        ), 0)
    END"""
            volume_expr = f"0"
            mcfe_expr = f"0"
            where_clause = ""

        elif logic == FormulaLogic.AVERAGE:
            amount_expr = f"AVG({measure_prefix}_AMOUNT)"
            volume_expr = f"AVG({measure_prefix}_VOLUME)"
            mcfe_expr = f"AVG({measure_prefix}_MCFE)"
            where_clause = f"WHERE FORMULA_GROUP = '{formula.param_ref}'"

        else:
            # Default to SUM
            amount_expr = f"SUM({measure_prefix}_AMOUNT)"
            volume_expr = f"SUM({measure_prefix}_VOLUME)"
            mcfe_expr = f"SUM({measure_prefix}_MCFE)"
            where_clause = f"WHERE FORMULA_GROUP = '{formula.param_ref}'"

        return f"""SELECT
    '{formula.hierarchy_key or "CALC_" + formula.formula_group}' AS FK_REPORT_KEY,
    NULL AS LEVEL_1, NULL AS LEVEL_2, NULL AS LEVEL_3, NULL AS LEVEL_4, NULL AS LEVEL_5,
    NULL AS LEVEL_6, NULL AS LEVEL_7, NULL AS LEVEL_8, NULL AS LEVEL_9,
    '{formula.formula_group}' AS FORMULA_GROUP,
    {formula.precedence_level} AS FORMULA_PRECEDENCE,
    '{formula.param_ref}' AS FORMULA_PARAM_REF,
    '{formula.logic.value}' AS FORMULA_LOGIC,
    '{formula.param2_ref or ""}' AS FORMULA_PARAM2_REF,
    {amount_expr} AS {measure_prefix}_AMOUNT,
    {volume_expr} AS {measure_prefix}_VOLUME,
    {mcfe_expr} AS {measure_prefix}_MCFE,
    1 AS IS_CALCULATED
FROM {source_table}
{where_clause}"""


def create_standard_los_formulas(
    report_type: str = "GROSS",
) -> List[FormulaPrecedence]:
    """
    Create standard LOS (Lease Operating Statement) formulas.

    Standard 5-level cascade for oil & gas LOS:
    P1: Total Revenue, Total Taxes, Total Deducts, Total Royalties, Total OpEx, Total CapEx
    P2: Total Taxes and Deducts
    P3: Gross Profit
    P4: Operating Income
    P5: Cash Flow

    Args:
        report_type: GROSS or NET

    Returns:
        List of standard LOS formulas
    """
    is_gross = report_type.upper() == "GROSS"

    formulas = [
        # Precedence 1: Base totals (computed in DT_3A, but listed for completeness)
        FormulaPrecedence(
            precedence_level=1,
            formula_group="Total Revenue",
            logic=FormulaLogic.SUM,
            param_ref="Revenue",
        ),
        FormulaPrecedence(
            precedence_level=1,
            formula_group="Total Taxes",
            logic=FormulaLogic.SUM,
            param_ref="Taxes",
        ),
        FormulaPrecedence(
            precedence_level=1,
            formula_group="Total Deducts",
            logic=FormulaLogic.SUM,
            param_ref="Deducts",
        ),
        FormulaPrecedence(
            precedence_level=1,
            formula_group="Total OpEx",
            logic=FormulaLogic.SUM,
            param_ref="Operating Expense",
        ),
        FormulaPrecedence(
            precedence_level=1,
            formula_group="Total CapEx",
            logic=FormulaLogic.SUM,
            param_ref="Capital Spend",
        ),

        # Precedence 2: Combined totals
        FormulaPrecedence(
            precedence_level=2,
            formula_group="Total Taxes and Deducts",
            logic=FormulaLogic.SUM,
            param_ref="Total Taxes",
            param2_ref="Total Deducts",
        ),

        # Precedence 3: Gross Profit
        FormulaPrecedence(
            precedence_level=3,
            formula_group="Gross Profit",
            logic=FormulaLogic.SUBTRACT,
            param_ref="Total Revenue",
            param2_ref="Total Taxes and Deducts" + (", Total Royalties" if is_gross else ""),
        ),

        # Precedence 4: Operating Income
        FormulaPrecedence(
            precedence_level=4,
            formula_group="Operating Income",
            logic=FormulaLogic.SUBTRACT,
            param_ref="Gross Profit",
            param2_ref="Total OpEx",
        ),

        # Precedence 5: Cash Flow
        FormulaPrecedence(
            precedence_level=5,
            formula_group="Cash Flow",
            logic=FormulaLogic.SUBTRACT,
            param_ref="Operating Income",
            param2_ref="Total CapEx",
        ),
    ]

    # Add GROSS-specific formulas
    if is_gross:
        formulas.insert(5, FormulaPrecedence(
            precedence_level=1,
            formula_group="Total Royalties",
            logic=FormulaLogic.SUM,
            param_ref="Royalties",
        ))

    return formulas
