"""
Configuration types for Data Quality / Expectations Integration.

Defines Pydantic models for:
- Expectation definitions
- Expectation suites
- Data contracts
- Validation results
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field
import uuid


class ExpectationType(str, Enum):
    """Types of expectations."""
    # Column value expectations
    NOT_NULL = "expect_column_values_to_not_be_null"
    UNIQUE = "expect_column_values_to_be_unique"
    IN_SET = "expect_column_values_to_be_in_set"
    NOT_IN_SET = "expect_column_values_to_not_be_in_set"
    MATCH_REGEX = "expect_column_values_to_match_regex"
    MATCH_LIKE = "expect_column_values_to_match_like_pattern"
    BETWEEN = "expect_column_values_to_be_between"
    IN_TYPE_LIST = "expect_column_values_to_be_in_type_list"

    # Column aggregate expectations
    MIN_TO_BE_BETWEEN = "expect_column_min_to_be_between"
    MAX_TO_BE_BETWEEN = "expect_column_max_to_be_between"
    MEAN_TO_BE_BETWEEN = "expect_column_mean_to_be_between"
    SUM_TO_BE_BETWEEN = "expect_column_sum_to_be_between"
    DISTINCT_COUNT = "expect_column_distinct_values_to_be_in_set"

    # Table expectations
    ROW_COUNT_BETWEEN = "expect_table_row_count_to_be_between"
    ROW_COUNT_EQUAL = "expect_table_row_count_to_equal"
    COLUMN_TO_EXIST = "expect_column_to_exist"
    TABLE_COLUMNS_TO_MATCH = "expect_table_columns_to_match_ordered_list"

    # Referential integrity
    COMPOUND_UNIQUE = "expect_compound_columns_to_be_unique"
    FOREIGN_KEY = "expect_column_pair_values_to_be_in_set"

    # Custom
    CUSTOM_SQL = "expect_custom_sql_to_return_rows"


class SeverityLevel(str, Enum):
    """Severity levels for expectation failures."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class ValidationStatus(str, Enum):
    """Status of a validation run."""
    SUCCESS = "success"
    FAILURE = "failure"
    WARNING = "warning"
    ERROR = "error"
    PENDING = "pending"


class Expectation(BaseModel):
    """A single expectation definition."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    expectation_type: ExpectationType
    kwargs: Dict[str, Any] = Field(default_factory=dict)

    # Metadata
    column: Optional[str] = None
    description: Optional[str] = None
    severity: SeverityLevel = SeverityLevel.MEDIUM

    # Result handling
    catch_exceptions: bool = True
    result_format: str = "BASIC"

    # Tags
    tags: List[str] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "allow"}

    def to_ge_format(self) -> Dict[str, Any]:
        """Convert to Great Expectations format."""
        ge_expectation = {
            "expectation_type": self.expectation_type.value,
            "kwargs": self.kwargs.copy(),
            "meta": {
                "id": self.id,
                "severity": self.severity.value,
                **({"description": self.description} if self.description else {}),
                **self.meta,
            },
        }
        if self.column:
            ge_expectation["kwargs"]["column"] = self.column
        return ge_expectation


class ExpectationSuite(BaseModel):
    """A collection of expectations for a dataset."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    description: Optional[str] = None

    # Target
    data_asset_name: Optional[str] = None
    database: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None

    # Expectations
    expectations: List[Expectation] = Field(default_factory=list)

    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    version: str = "1.0.0"

    # Source
    hierarchy_project_id: Optional[str] = None
    generated_from: Optional[str] = None

    # Tags
    tags: List[str] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "allow"}

    def add_expectation(self, expectation: Expectation) -> None:
        """Add an expectation to the suite."""
        self.expectations.append(expectation)
        self.updated_at = datetime.now()

    def to_ge_format(self) -> Dict[str, Any]:
        """Convert to Great Expectations suite format."""
        return {
            "expectation_suite_name": self.name,
            "ge_cloud_id": self.id,
            "data_asset_type": "Dataset",
            "expectations": [e.to_ge_format() for e in self.expectations],
            "meta": {
                "description": self.description,
                "version": self.version,
                "created_at": self.created_at.isoformat(),
                **self.meta,
            },
        }


class ColumnSchema(BaseModel):
    """Column definition for a data contract."""
    name: str
    data_type: str
    description: Optional[str] = None

    # Constraints
    not_null: bool = False
    unique: bool = False
    primary_key: bool = False

    # Validation rules
    pattern: Optional[str] = None
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    allowed_values: Optional[List[str]] = None

    # Foreign key
    foreign_key: Optional[Dict[str, str]] = None

    model_config = {"extra": "allow"}


class QualityRules(BaseModel):
    """Quality rules for a data contract."""
    # Freshness
    freshness_max_age_hours: Optional[int] = None

    # Completeness
    completeness_min_percent: Optional[float] = None

    # Uniqueness
    uniqueness_columns: List[str] = Field(default_factory=list)

    # Row count
    row_count_min: Optional[int] = None
    row_count_max: Optional[int] = None

    # Custom rules
    custom_rules: List[Dict[str, Any]] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class SlaConfig(BaseModel):
    """SLA configuration for a data contract."""
    validation_schedule: Optional[str] = None  # Cron expression
    alert_on_failure: bool = True
    alert_channels: List[str] = Field(default_factory=list)

    # Thresholds
    max_failures_before_alert: int = 1
    cool_down_minutes: int = 60

    model_config = {"extra": "allow"}


class DataContract(BaseModel):
    """A data contract defining quality expectations."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    version: str = "1.0.0"
    description: Optional[str] = None

    # Ownership
    owner: Optional[str] = None
    team: Optional[str] = None

    # Target
    database: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None

    # Schema
    columns: List[ColumnSchema] = Field(default_factory=list)

    # Quality rules
    quality: QualityRules = Field(default_factory=QualityRules)

    # SLA
    sla: SlaConfig = Field(default_factory=SlaConfig)

    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    tags: List[str] = Field(default_factory=list)

    # Source
    hierarchy_project_id: Optional[str] = None

    model_config = {"extra": "allow"}


class ExpectationResult(BaseModel):
    """Result of a single expectation validation."""
    expectation_id: str
    expectation_type: str
    success: bool

    # Details
    observed_value: Optional[Any] = None
    expected_value: Optional[Any] = None
    element_count: Optional[int] = None
    unexpected_count: Optional[int] = None
    unexpected_percent: Optional[float] = None

    # Samples
    unexpected_values: List[Any] = Field(default_factory=list)

    # Error
    exception_info: Optional[Dict[str, Any]] = None

    model_config = {"extra": "allow"}


class ValidationResult(BaseModel):
    """Result of a validation run."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    suite_name: str
    status: ValidationStatus

    # Timing
    run_at: datetime = Field(default_factory=datetime.now)
    duration_seconds: float = 0.0

    # Results
    success_count: int = 0
    failure_count: int = 0
    error_count: int = 0
    total_expectations: int = 0

    # Details
    results: List[ExpectationResult] = Field(default_factory=list)

    # Target
    data_asset_name: Optional[str] = None
    row_count: Optional[int] = None

    # Metadata
    meta: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "allow"}

    @property
    def success_percent(self) -> float:
        """Calculate success percentage."""
        if self.total_expectations == 0:
            return 100.0
        return (self.success_count / self.total_expectations) * 100
