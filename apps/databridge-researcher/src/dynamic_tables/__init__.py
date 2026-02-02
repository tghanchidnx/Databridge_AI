"""
Dynamic Tables Module for DataBridge AI Researcher.

This module provides services for building the DT_2 and DT_3 tiers:
- DT_2: Dynamic tables built from VW_1 views
- DT_3A: Intermediate aggregations with precedence handling
- DT_3: Output tables with formula calculations

Architecture:
    [Librarian: TBL_0 -> VW_1] -> DT_2 -> DT_3A -> DT_3 (Output)
"""

from .models import (
    DynamicTable,
    IntermediateAggregation,
    OutputTable,
    DynamicTableColumn,
    JoinDefinition,
    FilterDefinition,
    AggregationDefinition,
    FormulaColumn,
    TableStatus,
    JoinType,
    AggregationType,
    FormulaType,
)
from .builder import DynamicTableBuilderService, get_dynamic_table_builder
from .aggregator import AggregationService, get_aggregation_service
from .formula_executor import FormulaExecutorService, get_formula_executor

__all__ = [
    # Models
    "DynamicTable",
    "IntermediateAggregation",
    "OutputTable",
    "DynamicTableColumn",
    "JoinDefinition",
    "FilterDefinition",
    "AggregationDefinition",
    "FormulaColumn",
    "TableStatus",
    "JoinType",
    "AggregationType",
    "FormulaType",
    # Services
    "DynamicTableBuilderService",
    "get_dynamic_table_builder",
    "AggregationService",
    "get_aggregation_service",
    "FormulaExecutorService",
    "get_formula_executor",
]
