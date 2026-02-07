"""
Data Quality Module.

Provides data quality validation capabilities inspired by Great Expectations:
- Expectation suites for column and table validations
- Data contracts with schema definitions and SLAs
- Validation runner with detailed results

Components:
- ExpectationSuiteGenerator: Create expectation suites from hierarchies
- DataContractGenerator: Create YAML data contracts
- ValidationRunner: Execute validations and generate reports
"""

from .types import (
    # Enums
    ExpectationType,
    SeverityLevel,
    ValidationStatus,
    # Expectations
    Expectation,
    ExpectationSuite,
    # Contracts
    ColumnSchema,
    QualityRules,
    SlaConfig,
    DataContract,
    # Results
    ExpectationResult,
    ValidationResult,
)

from .suite_generator import ExpectationSuiteGenerator
from .contract_generator import DataContractGenerator
from .validation_runner import ValidationRunner
from .mcp_tools import register_data_quality_tools

__all__ = [
    # Types - Enums
    "ExpectationType",
    "SeverityLevel",
    "ValidationStatus",
    # Types - Expectations
    "Expectation",
    "ExpectationSuite",
    # Types - Contracts
    "ColumnSchema",
    "QualityRules",
    "SlaConfig",
    "DataContract",
    # Types - Results
    "ExpectationResult",
    "ValidationResult",
    # Generators
    "ExpectationSuiteGenerator",
    "DataContractGenerator",
    "ValidationRunner",
    # MCP
    "register_data_quality_tools",
]
