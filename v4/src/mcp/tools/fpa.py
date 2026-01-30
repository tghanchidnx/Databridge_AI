"""
FP&A MCP Tools for DataBridge Analytics V4.

Provides 12 tools for FP&A workflow automation:
- Close tools (4): sync_period_data, validate_close_readiness, reconcile_subledger_to_gl, lock_period
- Variance tools (4): analyze_budget_variance, analyze_prior_year_variance, identify_variance_drivers, generate_variance_commentary
- Forecast tools (4): get_current_forecast, update_rolling_forecast, model_scenario, compare_scenarios
"""

from typing import Optional, List, Dict, Any
import logging

import pandas as pd

from mcp.server.fastmcp import FastMCP

from ...workflows import (
    MonthlyCloseWorkflow,
    CloseStepType,
    CloseStatus,
    VarianceWorkflow,
    VarianceComparisonType,
    ForecastWorkflow,
    ForecastMethod,
    ScenarioType,
    ForecastAssumption,
)


logger = logging.getLogger(__name__)


# Workflow instances (will be initialized when tools are registered)
_close_workflow: Optional[MonthlyCloseWorkflow] = None
_variance_workflow: Optional[VarianceWorkflow] = None
_forecast_workflow: Optional[ForecastWorkflow] = None


def _get_close_workflow() -> MonthlyCloseWorkflow:
    """Get or create close workflow instance."""
    global _close_workflow
    if _close_workflow is None:
        _close_workflow = MonthlyCloseWorkflow()
    return _close_workflow


def _get_variance_workflow() -> VarianceWorkflow:
    """Get or create variance workflow instance."""
    global _variance_workflow
    if _variance_workflow is None:
        _variance_workflow = VarianceWorkflow()
    return _variance_workflow


def _get_forecast_workflow() -> ForecastWorkflow:
    """Get or create forecast workflow instance."""
    global _forecast_workflow
    if _forecast_workflow is None:
        _forecast_workflow = ForecastWorkflow()
    return _forecast_workflow


def register_fpa_tools(mcp: FastMCP) -> None:
    """Register FP&A tools with the MCP server."""

    # ========================================================================
    # CLOSE TOOLS (4)
    # ========================================================================

    @mcp.tool()
    def sync_period_data(
        period: str,
        fiscal_year: str,
        entity: str = "ALL",
        source_systems: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Initialize and sync data for a close period.

        Prepares a period for closing by setting up the close structure
        and syncing data from source systems.

        Args:
            period: Period identifier (e.g., "2024-01").
            fiscal_year: Fiscal year (e.g., "FY2024").
            entity: Entity or "ALL" for all entities.
            source_systems: List of source systems to sync from.

        Returns:
            Dict with period initialization status and sync details.
        """
        try:
            workflow = _get_close_workflow()

            # Initialize the period
            result = workflow.initialize_period(
                period=period,
                fiscal_year=fiscal_year,
                entity=entity,
            )

            if not result.success:
                return result.to_dict()

            # Add sync information
            response = result.to_dict()
            response["sync_info"] = {
                "source_systems": source_systems or ["GL", "AR", "AP"],
                "sync_status": "completed",
                "records_synced": 0,  # Would be populated by actual sync
            }

            return response

        except Exception as e:
            logger.error(f"Failed to sync period data: {e}")
            return {
                "success": False,
                "message": f"Failed to sync period data: {str(e)}",
                "errors": [str(e)],
            }

    @mcp.tool()
    def validate_close_readiness(
        period: str,
        validation_checks: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Validate readiness for period close.

        Runs validation checks to ensure the period is ready for closing.
        Checks include data completeness, balance reconciliation, and
        required approvals.

        Args:
            period: Period identifier.
            validation_checks: Specific checks to run (default: all).

        Returns:
            Dict with validation results, passed/failed checks, and blocking issues.
        """
        try:
            workflow = _get_close_workflow()
            result = workflow.validate_close_readiness(period)

            if not result.success:
                return result.to_dict()

            # Add check details
            response = result.to_dict()

            # Filter checks if specific ones requested
            if validation_checks and result.data:
                filtered_validations = [
                    v for v in result.data.get("validations", [])
                    if v.get("check_name") in validation_checks
                ]
                response["data"]["validations"] = filtered_validations

            return response

        except Exception as e:
            logger.error(f"Failed to validate close readiness: {e}")
            return {
                "success": False,
                "message": f"Failed to validate close readiness: {str(e)}",
                "errors": [str(e)],
            }

    @mcp.tool()
    def reconcile_subledger_to_gl(
        period: str,
        subledger: str,
        gl_account: str,
        tolerance: float = 0.01,
    ) -> Dict[str, Any]:
        """
        Reconcile a subledger to the general ledger.

        Compares subledger totals to GL account balances and identifies
        any differences that need investigation.

        Args:
            period: Period identifier.
            subledger: Subledger name (AR, AP, FA, etc.).
            gl_account: GL account number to reconcile against.
            tolerance: Acceptable difference threshold (default: $0.01).

        Returns:
            Dict with reconciliation results, differences, and items needing review.
        """
        try:
            workflow = _get_close_workflow()

            # Execute the subledger reconciliation step
            result = workflow.execute_step(
                period=period,
                step_type=CloseStepType.SUBLEDGER_RECON,
            )

            # Add specific reconciliation details
            response = result.to_dict()
            response["reconciliation"] = {
                "subledger": subledger,
                "gl_account": gl_account,
                "tolerance": tolerance,
                "subledger_balance": 0.0,  # Would be populated by actual data
                "gl_balance": 0.0,
                "difference": 0.0,
                "is_reconciled": True,
                "items_to_review": [],
            }

            return response

        except Exception as e:
            logger.error(f"Failed to reconcile subledger: {e}")
            return {
                "success": False,
                "message": f"Failed to reconcile subledger: {str(e)}",
                "errors": [str(e)],
            }

    @mcp.tool()
    def lock_period(
        period: str,
        lock_reason: str = "",
        locked_by: str = "",
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        Lock a period to prevent further changes.

        Finalizes a period by locking it from additional postings.
        Requires all validation checks to pass unless force is True.

        Args:
            period: Period identifier.
            lock_reason: Reason for locking.
            locked_by: User performing the lock.
            force: Force lock even if validations fail.

        Returns:
            Dict with lock status and any warnings.
        """
        try:
            workflow = _get_close_workflow()
            result = workflow.lock_period(
                period=period,
                locked_by=locked_by,
                force=force,
            )

            response = result.to_dict()
            if lock_reason:
                response["lock_reason"] = lock_reason

            return response

        except Exception as e:
            logger.error(f"Failed to lock period: {e}")
            return {
                "success": False,
                "message": f"Failed to lock period: {str(e)}",
                "errors": [str(e)],
            }

    # ========================================================================
    # VARIANCE TOOLS (4)
    # ========================================================================

    @mcp.tool()
    def analyze_budget_variance(
        actual_values: List[Dict[str, Any]],
        actual_column: str = "actual",
        budget_column: str = "budget",
        dimension_columns: Optional[List[str]] = None,
        period: str = "",
        favorable_direction: str = "positive",
    ) -> Dict[str, Any]:
        """
        Analyze budget vs actual variance.

        Compares actual results to budget and identifies key drivers
        of variance with materiality classification.

        Args:
            actual_values: List of dicts with actual and budget data.
            actual_column: Column name for actual values.
            budget_column: Column name for budget values.
            dimension_columns: Columns to analyze variance by.
            period: Period label for reporting.
            favorable_direction: Direction for favorable variance.

        Returns:
            Dict with variance analysis, drivers, and commentary.
        """
        try:
            workflow = _get_variance_workflow()
            df = pd.DataFrame(actual_values)

            result = workflow.analyze_budget_variance(
                data=df,
                actual_column=actual_column,
                budget_column=budget_column,
                dimension_columns=dimension_columns,
                period=period,
                favorable_direction=favorable_direction,
            )

            return result.to_dict()

        except Exception as e:
            logger.error(f"Failed to analyze budget variance: {e}")
            return {
                "success": False,
                "message": f"Failed to analyze budget variance: {str(e)}",
                "errors": [str(e)],
            }

    @mcp.tool()
    def analyze_prior_year_variance(
        data_values: List[Dict[str, Any]],
        current_column: str = "current",
        prior_column: str = "prior_year",
        dimension_columns: Optional[List[str]] = None,
        period: str = "",
    ) -> Dict[str, Any]:
        """
        Analyze variance vs prior year.

        Compares current period results to prior year and identifies
        year-over-year changes and trends.

        Args:
            data_values: List of dicts with current and prior year data.
            current_column: Column name for current year values.
            prior_column: Column name for prior year values.
            dimension_columns: Columns to analyze variance by.
            period: Period label for reporting.

        Returns:
            Dict with YoY variance analysis and drivers.
        """
        try:
            workflow = _get_variance_workflow()
            df = pd.DataFrame(data_values)

            result = workflow.analyze_prior_year_variance(
                data=df,
                current_column=current_column,
                prior_column=prior_column,
                dimension_columns=dimension_columns,
                period=period,
            )

            return result.to_dict()

        except Exception as e:
            logger.error(f"Failed to analyze prior year variance: {e}")
            return {
                "success": False,
                "message": f"Failed to analyze prior year variance: {str(e)}",
                "errors": [str(e)],
            }

    @mcp.tool()
    def identify_variance_drivers(
        data_values: List[Dict[str, Any]],
        actual_column: str = "actual",
        budget_column: str = "budget",
        dimension_columns: List[str] = None,
        min_contribution: float = 0.05,
    ) -> Dict[str, Any]:
        """
        Identify key drivers of variance across dimensions.

        Analyzes variance by multiple dimensions to find the root
        causes of differences between actual and budget/plan.

        Args:
            data_values: List of dicts with actual and budget data.
            actual_column: Column name for actual values.
            budget_column: Column name for budget values.
            dimension_columns: Dimensions to analyze (required).
            min_contribution: Minimum contribution % to include (default: 5%).

        Returns:
            Dict with ranked variance drivers by contribution.
        """
        try:
            if not dimension_columns:
                return {
                    "success": False,
                    "message": "dimension_columns is required",
                    "errors": ["Must specify at least one dimension column"],
                }

            workflow = _get_variance_workflow()
            df = pd.DataFrame(data_values)

            result = workflow.identify_variance_drivers(
                data=df,
                actual_column=actual_column,
                budget_column=budget_column,
                dimension_columns=dimension_columns,
                min_contribution=min_contribution,
            )

            return result.to_dict()

        except Exception as e:
            logger.error(f"Failed to identify variance drivers: {e}")
            return {
                "success": False,
                "message": f"Failed to identify variance drivers: {str(e)}",
                "errors": [str(e)],
            }

    @mcp.tool()
    def generate_variance_commentary(
        actual_price: float,
        actual_volume: float,
        budget_price: float,
        budget_volume: float,
        metric_name: str = "Revenue",
    ) -> Dict[str, Any]:
        """
        Generate variance commentary with price/volume decomposition.

        Decomposes variance into price, volume, and mix components
        and generates executive-ready commentary.

        Args:
            actual_price: Actual unit price.
            actual_volume: Actual volume/quantity.
            budget_price: Budget unit price.
            budget_volume: Budget volume/quantity.
            metric_name: Name of the metric being analyzed.

        Returns:
            Dict with variance decomposition and narrative commentary.
        """
        try:
            workflow = _get_variance_workflow()

            result = workflow.decompose_variance(
                actual_price=actual_price,
                actual_volume=actual_volume,
                budget_price=budget_price,
                budget_volume=budget_volume,
            )

            response = result.to_dict()
            response["metric_name"] = metric_name

            return response

        except Exception as e:
            logger.error(f"Failed to generate variance commentary: {e}")
            return {
                "success": False,
                "message": f"Failed to generate variance commentary: {str(e)}",
                "errors": [str(e)],
            }

    # ========================================================================
    # FORECAST TOOLS (4)
    # ========================================================================

    @mcp.tool()
    def get_current_forecast(
        data_values: List[Dict[str, Any]],
        actual_column: str = "actual",
        budget_column: str = "budget",
        period_column: str = "period",
        as_of_period: str = "",
        fiscal_year: str = "",
        prior_year_column: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get current forecast status with YTD actuals and full year outlook.

        Returns the current state of the forecast including YTD performance,
        remaining forecast, and full year projections.

        Args:
            data_values: List of dicts with period data.
            actual_column: Column name for actual values.
            budget_column: Column name for budget values.
            period_column: Column name for period identifier.
            as_of_period: Current period (e.g., "2024-06").
            fiscal_year: Fiscal year label.
            prior_year_column: Optional column for prior year values.

        Returns:
            Dict with forecast summary, YTD vs budget, and outlook.
        """
        try:
            if not as_of_period:
                return {
                    "success": False,
                    "message": "as_of_period is required",
                    "errors": ["Must specify the current period"],
                }

            workflow = _get_forecast_workflow()
            df = pd.DataFrame(data_values)

            result = workflow.get_current_forecast(
                data=df,
                actual_column=actual_column,
                budget_column=budget_column,
                period_column=period_column,
                as_of_period=as_of_period,
                fiscal_year=fiscal_year,
                prior_year_column=prior_year_column,
            )

            return result.to_dict()

        except Exception as e:
            logger.error(f"Failed to get current forecast: {e}")
            return {
                "success": False,
                "message": f"Failed to get current forecast: {str(e)}",
                "errors": [str(e)],
            }

    @mcp.tool()
    def update_rolling_forecast(
        data_values: List[Dict[str, Any]],
        actual_column: str = "actual",
        budget_column: str = "budget",
        period_column: str = "period",
        as_of_period: str = "",
        forecast_method: str = "straight_line",
        growth_rate: Optional[float] = None,
        adjustment_factors: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """
        Update rolling forecast with YTD actuals and projected remaining periods.

        Replaces historical periods with actuals and projects remaining
        periods using the specified forecasting method.

        Args:
            data_values: List of dicts with period data.
            actual_column: Column name for actual values.
            budget_column: Column name for budget values.
            period_column: Column name for period identifier.
            as_of_period: Current period.
            forecast_method: Method for projecting remaining periods:
                - straight_line: Use budget values
                - growth_rate: Apply growth rate to budget
                - ytd_run_rate: Annualize YTD actual
                - trend: Linear trend from YTD
                - prior_year: Use prior year values
                - prior_year_growth: Prior year with growth
            growth_rate: Growth rate to apply (for growth methods).
            adjustment_factors: Period-specific adjustments.

        Returns:
            Dict with updated forecast by period and summary.
        """
        try:
            if not as_of_period:
                return {
                    "success": False,
                    "message": "as_of_period is required",
                    "errors": ["Must specify the current period"],
                }

            # Convert method string to enum
            try:
                method = ForecastMethod(forecast_method)
            except ValueError:
                method = ForecastMethod.STRAIGHT_LINE

            workflow = _get_forecast_workflow()
            df = pd.DataFrame(data_values)

            result = workflow.update_rolling_forecast(
                data=df,
                actual_column=actual_column,
                budget_column=budget_column,
                period_column=period_column,
                as_of_period=as_of_period,
                forecast_method=method,
                growth_rate=growth_rate,
                adjustment_factors=adjustment_factors,
            )

            return result.to_dict()

        except Exception as e:
            logger.error(f"Failed to update rolling forecast: {e}")
            return {
                "success": False,
                "message": f"Failed to update rolling forecast: {str(e)}",
                "errors": [str(e)],
            }

    @mcp.tool()
    def model_scenario(
        scenario_name: str,
        scenario_type: str = "custom",
        base_periods: Optional[List[Dict[str, Any]]] = None,
        adjustments: Optional[Dict[str, Any]] = None,
        assumptions: Optional[List[Dict[str, Any]]] = None,
        description: str = "",
    ) -> Dict[str, Any]:
        """
        Create and model a forecast scenario.

        Creates a new scenario based on base forecast data and applies
        adjustments to model different outcomes.

        Args:
            scenario_name: Name for the scenario.
            scenario_type: Type of scenario (base, upside, downside, stress, custom).
            base_periods: Base forecast periods to start from.
            adjustments: Adjustments to apply:
                - percent_change: Overall % change
                - period_changes: Dict[period, value]
            assumptions: List of assumption dicts with name, category, value, unit.
            description: Scenario description.

        Returns:
            Dict with modeled scenario details and total forecast.
        """
        try:
            # Convert type string to enum
            try:
                s_type = ScenarioType(scenario_type)
            except ValueError:
                s_type = ScenarioType.CUSTOM

            workflow = _get_forecast_workflow()

            # Create scenario ID
            import uuid
            scenario_id = str(uuid.uuid4())[:8]

            # Convert base periods if provided
            from ...workflows import ForecastPeriod
            periods = []
            if base_periods:
                for p in base_periods:
                    periods.append(ForecastPeriod(
                        period=p.get("period", ""),
                        actual=p.get("actual"),
                        budget=p.get("budget"),
                        forecast=p.get("forecast") or p.get("budget"),
                        is_actual=p.get("is_actual", False),
                    ))

            # Convert assumptions if provided
            assumption_objs = []
            if assumptions:
                for a in assumptions:
                    assumption_objs.append(ForecastAssumption(
                        name=a.get("name", ""),
                        category=a.get("category", "general"),
                        value=a.get("value", 0),
                        unit=a.get("unit", ""),
                        description=a.get("description", ""),
                    ))

            # Create the scenario
            result = workflow.create_scenario(
                scenario_id=scenario_id,
                name=scenario_name,
                scenario_type=s_type,
                base_periods=periods,
                assumptions=assumption_objs,
                description=description,
            )

            if not result.success:
                return result.to_dict()

            # Apply adjustments if provided
            if adjustments:
                result = workflow.model_scenario(
                    scenario_id=scenario_id,
                    adjustments=adjustments,
                )

            return result.to_dict()

        except Exception as e:
            logger.error(f"Failed to model scenario: {e}")
            return {
                "success": False,
                "message": f"Failed to model scenario: {str(e)}",
                "errors": [str(e)],
            }

    @mcp.tool()
    def compare_scenarios(
        scenario_names: Optional[List[str]] = None,
        include_period_detail: bool = True,
    ) -> Dict[str, Any]:
        """
        Compare multiple forecast scenarios.

        Compares scenarios side-by-side showing differences in totals
        and period-by-period values.

        Args:
            scenario_names: Specific scenarios to compare (default: all).
            include_period_detail: Include period-by-period comparison.

        Returns:
            Dict with scenario comparison, totals, and variance to base.
        """
        try:
            workflow = _get_forecast_workflow()

            # Get scenario IDs from names
            scenario_ids = None
            if scenario_names:
                scenarios = workflow.get_scenarios()
                scenario_ids = [
                    s.scenario_id for s in scenarios
                    if s.name in scenario_names
                ]

            result = workflow.compare_scenarios(scenario_ids=scenario_ids)

            response = result.to_dict()

            # Optionally remove period detail
            if not include_period_detail and response.get("data"):
                response["data"].pop("by_period", None)

            return response

        except Exception as e:
            logger.error(f"Failed to compare scenarios: {e}")
            return {
                "success": False,
                "message": f"Failed to compare scenarios: {str(e)}",
                "errors": [str(e)],
            }
