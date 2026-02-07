# Phase 26: Hierarchy-Driven Data Mart Factory

## Overview

Implement the **Hierarchy-Driven Data Mart Factory** pattern based on the technical analysis in `databridge_workflow_analysis.md`. This phase extends DataBridge with automated data mart generation from hierarchy definitions using dbt, Cortex AI, and native Snowflake features.

**Key Deliverables:**
- 4-object pipeline generation (VW_1 → DT_2 → DT_3A → DT_3)
- AI-powered hierarchy discovery using Cortex COMPLETE()
- Dynamic YAML configuration generation
- Formula precedence engine with 5-level cascading calculations
- Validation and monitoring agents

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                 Data Mart Factory Module                        │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │         MartConfigGenerator (7 Config Variables)         │   │
│  │  - JOIN_PATTERNS[]      - Dynamic branch definitions     │   │
│  │  - DYNAMIC_COLUMN_MAP{} - ID_SOURCE → physical column    │   │
│  │  - ACCOUNT_SEGMENT      - GROSS/NET filter               │   │
│  │  - MEASURE_PREFIX       - Column name prefix             │   │
│  │  - HAS_SIGN_CHANGE      - Sign flip flag                 │   │
│  │  - HAS_EXCLUSIONS       - NOT IN subquery flag           │   │
│  │  - HAS_GROUP_FILTER     - Multi-round filter flag        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           ↓                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │         MartPipelineGenerator (4-Object DDL)             │   │
│  │  - VW_1: Translation View (CASE on ID_SOURCE)            │   │
│  │  - DT_2: Granularity Table (UNPIVOT, exclusions)         │   │
│  │  - DT_3A: Pre-Aggregation Fact (UNION ALL branches)      │   │
│  │  - DT_3: Data Mart (formula precedence, surrogates)      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           ↓                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │         CortexDiscoveryAgent (AI-Powered)                │   │
│  │  - Scan hierarchy tables for structure patterns          │   │
│  │  - Infer join patterns from mapping distribution         │   │
│  │  - Generate YAML config recommendations                  │   │
│  │  - Detect data quality issues (typos, orphans)           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           ↓                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │         FormulaPrecedenceEngine (5-Level Cascade)        │   │
│  │  - P1: DT_3A base aggregations (totals)                  │   │
│  │  - P2: DT_3 simple calculations                          │   │
│  │  - P3: DT_3 gross profit = revenue - taxes - deducts     │   │
│  │  - P4: DT_3 operating income                             │   │
│  │  - P5: DT_3 cash flow                                    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           ↓                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │         MartValidationAgent                              │   │
│  │  - Row count validation per pipeline stage               │   │
│  │  - Join cardinality checks                               │   │
│  │  - Formula result verification                           │   │
│  │  - Orphan/duplicate detection                            │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                           ↓
              Snowflake Data Cloud
```

---

## Deliverables (8 files)

| File | Action | Purpose |
|------|--------|---------|
| `src/mart_factory/types.py` | **NEW** | Pydantic models for mart config (~250 lines) |
| `src/mart_factory/config_generator.py` | **NEW** | MartConfigGenerator (~300 lines) |
| `src/mart_factory/pipeline_generator.py` | **NEW** | 4-object DDL generation (~500 lines) |
| `src/mart_factory/formula_engine.py` | **NEW** | FormulaPrecedenceEngine (~250 lines) |
| `src/mart_factory/cortex_discovery.py` | **NEW** | AI-powered hierarchy discovery (~350 lines) |
| `src/mart_factory/mcp_tools.py` | **NEW** | 10 MCP tools (~450 lines) |
| `src/mart_factory/__init__.py` | **NEW** | Module exports |
| `tests/test_mart_factory.py` | **NEW** | Unit tests (~400 lines) |

---

## File 1: `src/mart_factory/types.py`

```python
class JoinPattern(BaseModel):
    """A single UNION ALL branch definition."""
    name: str  # e.g., "account", "deduct_product"
    join_keys: List[str]  # DT_2 columns: [LOS_ACCOUNT_ID_FILTER]
    fact_keys: List[str]  # Fact columns: [FK_ACCOUNT_KEY]
    filter: Optional[str] = None  # Optional WHERE: "ROYALTY_FILTER = 'Y'"

class DynamicColumnMapping(BaseModel):
    """Maps ID_SOURCE to physical dimension column."""
    id_source: str  # "BILLING_CATEGORY_CODE"
    physical_column: str  # "ACCT.ACCOUNT_BILLING_CATEGORY_CODE"
    is_alias: bool = False  # True for typo corrections

class MartConfig(BaseModel):
    """Complete configuration for a data mart pipeline."""
    project_name: str
    report_type: str  # "GROSS" or "NET"
    hierarchy_table: str
    mapping_table: str
    account_segment: str
    measure_prefix: str
    has_sign_change: bool = False
    has_exclusions: bool = False
    has_group_filter_precedence: bool = False
    dynamic_column_map: List[DynamicColumnMapping]
    join_patterns: List[JoinPattern]

class PipelineObject(BaseModel):
    """A generated DDL object in the pipeline."""
    object_type: str  # "VIEW", "DYNAMIC_TABLE"
    object_name: str  # "VW_1_UPSTREAM_GROSS_REPORT_HIERARCHY_TRANSLATED"
    layer: int  # 1, 2, 3A, 3
    ddl: str  # Generated SQL
    estimated_rows: Optional[int] = None

class FormulaPrecedence(BaseModel):
    """A formula calculation definition."""
    precedence_level: int  # 1-5
    formula_group: str  # "GROSS_PROFIT"
    logic: str  # "SUBTRACT"
    param_ref: str  # "Total Revenue"
    param2_ref: Optional[str] = None  # "Total Taxes and Deducts"

class DiscoveryResult(BaseModel):
    """Result of AI-powered hierarchy discovery."""
    hierarchy_type: str  # Detected type
    level_count: int
    node_count: int
    mapping_count: int
    join_pattern_suggestion: List[JoinPattern]
    column_map_suggestion: List[DynamicColumnMapping]
    data_quality_issues: List[Dict[str, Any]]
    recommended_config: Optional[MartConfig] = None
```

---

## File 2: `src/mart_factory/config_generator.py`

```python
class MartConfigGenerator:
    """Generates mart configuration from hierarchy analysis."""

    def create_config(
        self,
        project_name: str,
        report_type: str,
        hierarchy_table: str,
        mapping_table: str,
        account_segment: str,
    ) -> MartConfig:
        """Create a new mart configuration."""

    def add_join_pattern(
        self,
        config_name: str,
        name: str,
        join_keys: List[str],
        fact_keys: List[str],
        filter: Optional[str] = None,
    ) -> JoinPattern:
        """Add a UNION ALL branch definition."""

    def add_column_mapping(
        self,
        config_name: str,
        id_source: str,
        physical_column: str,
        is_alias: bool = False,
    ) -> DynamicColumnMapping:
        """Add ID_SOURCE to physical column mapping."""

    def from_hierarchy_project(
        self,
        project_id: str,
        hierarchy_service,  # HierarchyKnowledgeBase
    ) -> MartConfig:
        """Generate config from existing DataBridge hierarchy project."""

    def validate_config(self, config_name: str) -> Dict[str, Any]:
        """Validate configuration completeness and consistency."""

    def export_yaml(self, config_name: str) -> str:
        """Export configuration to dbt YAML format."""

    def list_configs(self) -> List[Dict]:
        """List all configurations."""

    def get_config(self, name: str) -> Optional[MartConfig]:
        """Get a specific configuration."""
```

---

## File 3: `src/mart_factory/pipeline_generator.py`

```python
class MartPipelineGenerator:
    """Generates 4-object DDL pipeline from configuration."""

    def generate_vw1(self, config: MartConfig) -> PipelineObject:
        """
        Generate VW_1 Translation View.

        Core logic: CASE statement mapping ID_SOURCE to physical columns.
        Joins hierarchy table with dimension tables for ID resolution.
        """

    def generate_dt2(self, config: MartConfig) -> PipelineObject:
        """
        Generate DT_2 Granularity Dynamic Table.

        Operations:
        - UNPIVOT FILTER_GROUP columns
        - Apply dynamic column mapping
        - Handle exclusions via NOT IN subquery
        - Multi-round filtering if HAS_GROUP_FILTER_PRECEDENCE
        """

    def generate_dt3a(self, config: MartConfig) -> PipelineObject:
        """
        Generate DT_3A Pre-Aggregation Fact Dynamic Table.

        Operations:
        - Generate UNION ALL branches from join_patterns
        - Join DT_2 to fact table per branch
        - Apply ACCOUNT_SEGMENT filter
        - Handle SIGN_CHANGE_FLAG multiplication
        - Aggregate measures (SUM) by hierarchy key
        """

    def generate_dt3(
        self,
        config: MartConfig,
        formulas: List[FormulaPrecedence],
    ) -> PipelineObject:
        """
        Generate DT_3 Data Mart Dynamic Table.

        Operations:
        - 5-level formula precedence cascade
        - DENSE_RANK surrogate key generation
        - Hierarchy level backfill
        - Extension hierarchy join (if applicable)
        """

    def generate_full_pipeline(
        self,
        config: MartConfig,
    ) -> List[PipelineObject]:
        """Generate all 4 pipeline objects."""

    def generate_dbt_models(
        self,
        config: MartConfig,
        output_dir: str,
    ) -> Dict[str, str]:
        """Generate dbt model files from pipeline."""
```

---

## File 4: `src/mart_factory/formula_engine.py`

```python
class FormulaPrecedenceEngine:
    """Manages formula precedence for data mart calculations."""

    def extract_formulas(
        self,
        hierarchy_data: List[Dict],
    ) -> List[FormulaPrecedence]:
        """Extract formula definitions from hierarchy data."""

    def build_precedence_chain(
        self,
        formulas: List[FormulaPrecedence],
    ) -> Dict[int, List[FormulaPrecedence]]:
        """Group formulas by precedence level (1-5)."""

    def generate_calculation_sql(
        self,
        precedence_level: int,
        formulas: List[FormulaPrecedence],
    ) -> str:
        """Generate SQL for a single precedence level."""

    def generate_cascade_cte(
        self,
        all_formulas: List[FormulaPrecedence],
    ) -> str:
        """
        Generate full 5-level cascade as CTEs.

        P1: Base aggregations from DT_3A
        P2-P5: Calculated rows injected via UNION ALL
        """

    def validate_dependencies(
        self,
        formulas: List[FormulaPrecedence],
    ) -> Dict[str, Any]:
        """Validate formula dependencies are satisfiable."""
```

---

## File 5: `src/mart_factory/cortex_discovery.py`

```python
class CortexDiscoveryAgent:
    """AI-powered hierarchy discovery using Cortex COMPLETE()."""

    def __init__(self, connection_id: str, query_func: callable):
        self.connection_id = connection_id
        self.query_func = query_func

    def discover_hierarchy(
        self,
        hierarchy_table: str,
        mapping_table: str,
    ) -> DiscoveryResult:
        """
        Scan hierarchy and mapping tables to discover structure.

        Uses Cortex COMPLETE() to:
        - Infer hierarchy type (P&L, Balance Sheet, LOS, etc.)
        - Detect level patterns and naming conventions
        - Identify join pattern requirements
        - Find data quality issues
        """

    def analyze_id_source_distribution(
        self,
        mapping_table: str,
    ) -> Dict[str, int]:
        """Analyze ID_SOURCE value distribution for column mapping."""

    def detect_join_patterns(
        self,
        mapping_table: str,
    ) -> List[JoinPattern]:
        """
        Infer UNION ALL branch structure from mapping patterns.

        Analyzes which dimension combinations appear together
        to suggest optimal join branch definitions.
        """

    def detect_typos(
        self,
        mapping_table: str,
        known_values: List[str],
    ) -> List[Dict[str, Any]]:
        """Detect likely typos in ID_SOURCE values."""

    def generate_config_recommendation(
        self,
        discovery_result: DiscoveryResult,
    ) -> MartConfig:
        """Generate complete MartConfig from discovery results."""

    def explain_discovery(
        self,
        result: DiscoveryResult,
    ) -> str:
        """Generate human-readable explanation of discovery."""
```

---

## File 6: `src/mart_factory/mcp_tools.py`

### 10 MCP Tools

**Configuration Management (3):**
| Tool | Description |
|------|-------------|
| `create_mart_config` | Create data mart pipeline configuration |
| `add_mart_join_pattern` | Add UNION ALL branch to configuration |
| `export_mart_config` | Export configuration to dbt YAML |

**Pipeline Generation (3):**
| Tool | Description |
|------|-------------|
| `generate_mart_pipeline` | Generate all 4 DDL objects |
| `generate_mart_object` | Generate single pipeline object (VW_1, DT_2, etc.) |
| `generate_mart_dbt_project` | Generate complete dbt project from config |

**AI Discovery (2):**
| Tool | Description |
|------|-------------|
| `discover_hierarchy_pattern` | AI-powered hierarchy discovery |
| `suggest_mart_config` | Get AI-recommended configuration |

**Validation (2):**
| Tool | Description |
|------|-------------|
| `validate_mart_config` | Validate configuration completeness |
| `validate_mart_pipeline` | Test generated DDL against source data |

---

## Tool Specifications

### `create_mart_config`

```python
def create_mart_config(
    project_name: str,
    report_type: str,
    hierarchy_table: str,
    mapping_table: str,
    account_segment: str,
    measure_prefix: Optional[str] = None,
    has_sign_change: bool = False,
    has_exclusions: bool = False,
    has_group_filter_precedence: bool = False,
) -> Dict[str, Any]:
    """
    Create a new data mart pipeline configuration.

    The configuration defines the 7 variables that parameterize
    the pipeline generation for any hierarchy type.

    Args:
        project_name: Unique name for this mart config
        report_type: Type of report (GROSS, NET, etc.)
        hierarchy_table: Fully qualified hierarchy table name
        mapping_table: Fully qualified mapping table name
        account_segment: Filter value for ACCOUNT_SEGMENT
        measure_prefix: Prefix for measure columns (default: report_type)
        has_sign_change: Whether to apply sign change logic
        has_exclusions: Whether mapping has exclusion rows
        has_group_filter_precedence: Whether to use multi-round filtering

    Returns:
        Created configuration details

    Example:
        create_mart_config(
            project_name="upstream_gross",
            report_type="GROSS",
            hierarchy_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY_",
            mapping_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY_MAPPING",
            account_segment="GROSS",
            has_group_filter_precedence=True
        )
    """
```

### `discover_hierarchy_pattern`

```python
def discover_hierarchy_pattern(
    hierarchy_table: str,
    mapping_table: str,
    connection_id: str,
) -> Dict[str, Any]:
    """
    Use AI to discover hierarchy structure and suggest configuration.

    Scans the hierarchy and mapping tables to detect:
    - Hierarchy type (P&L, Balance Sheet, LOS, etc.)
    - Level structure and naming conventions
    - Optimal join patterns for UNION ALL branches
    - ID_SOURCE to physical column mappings
    - Data quality issues (typos, orphans, duplicates)

    Uses Snowflake Cortex COMPLETE() for intelligent pattern detection.

    Args:
        hierarchy_table: Fully qualified hierarchy table name
        mapping_table: Fully qualified mapping table name
        connection_id: Snowflake connection for queries

    Returns:
        Discovery result with suggested configuration

    Example:
        discover_hierarchy_pattern(
            hierarchy_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY_",
            mapping_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY_MAPPING",
            connection_id="snowflake-prod"
        )
    """
```

### `generate_mart_pipeline`

```python
def generate_mart_pipeline(
    config_name: str,
    output_format: str = "ddl",
) -> Dict[str, Any]:
    """
    Generate the complete 4-object data mart pipeline.

    Creates:
    - VW_1: Translation View (CASE on ID_SOURCE)
    - DT_2: Granularity Dynamic Table (UNPIVOT, exclusions)
    - DT_3A: Pre-Aggregation Fact (UNION ALL branches)
    - DT_3: Data Mart (formula precedence, surrogates)

    Args:
        config_name: Name of the configuration to use
        output_format: Output format - "ddl" or "dbt"

    Returns:
        Generated pipeline objects

    Example:
        generate_mart_pipeline(
            config_name="upstream_gross",
            output_format="ddl"
        )
    """
```

---

## Integration with Existing Modules

### Phase 24 (dbt Integration)
```python
# Generate dbt project from mart config
generate_mart_dbt_project(
    config_name="upstream_gross",
    dbt_project_name="upstream_gross_marts",
    output_dir="./dbt_projects/upstream_gross"
)
```

### Phase 19 (Cortex Agent)
```python
# Use Cortex for AI discovery
configure_cortex_agent(connection_id="snowflake-prod")
discover_hierarchy_pattern(
    hierarchy_table="...",
    mapping_table="...",
    connection_id="snowflake-prod"
)
```

### Phase 25 (Data Quality)
```python
# Generate expectations from mart config
generate_expectation_suite(
    name="upstream_gross_suite",
    hierarchy_project_id="upstream_gross"
)
```

---

## 7 Configuration Variables

| # | Variable | Type | Purpose |
|---|----------|------|---------|
| 1 | `JOIN_PATTERNS[]` | Array | UNION ALL branch definitions |
| 2 | `DYNAMIC_COLUMN_MAP{}` | Object | ID_SOURCE → physical column |
| 3 | `ACCOUNT_SEGMENT` | String | GROSS/NET filter value |
| 4 | `MEASURE_PREFIX` | String | Column name prefix |
| 5 | `HAS_SIGN_CHANGE` | Boolean | Sign flip flag |
| 6 | `HAS_EXCLUSIONS` | Boolean | NOT IN subquery flag |
| 7 | `HAS_GROUP_FILTER_PRECEDENCE` | Boolean | Multi-round filter flag |

---

## Example Workflow

```python
# 1. Discover hierarchy pattern using AI
result = discover_hierarchy_pattern(
    hierarchy_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY_",
    mapping_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY_MAPPING",
    connection_id="snowflake-prod"
)
# Returns: suggested config, data quality issues, join patterns

# 2. Create configuration (or use AI suggestion)
create_mart_config(
    project_name="upstream_gross",
    report_type="GROSS",
    hierarchy_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY_",
    mapping_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY_MAPPING",
    account_segment="GROSS",
    has_group_filter_precedence=True
)

# 3. Add join patterns
add_mart_join_pattern(
    config_name="upstream_gross",
    name="account",
    join_keys=["LOS_ACCOUNT_ID_FILTER"],
    fact_keys=["FK_ACCOUNT_KEY"]
)
add_mart_join_pattern(
    config_name="upstream_gross",
    name="deduct_product",
    join_keys=["LOS_DEDUCT_CODE_FILTER", "LOS_PRODUCT_CODE_FILTER"],
    fact_keys=["FK_DEDUCT_KEY", "FK_PRODUCT_KEY"]
)
add_mart_join_pattern(
    config_name="upstream_gross",
    name="royalty",
    join_keys=["LOS_PRODUCT_CODE_FILTER"],
    fact_keys=["FK_PRODUCT_KEY"],
    filter="ROYALTY_FILTER = 'Y'"
)

# 4. Generate pipeline
generate_mart_pipeline(
    config_name="upstream_gross",
    output_format="ddl"
)

# 5. Or generate as dbt project
generate_mart_dbt_project(
    config_name="upstream_gross",
    dbt_project_name="upstream_gross_marts",
    output_dir="./dbt_projects"
)

# 6. Validate against source data
validate_mart_pipeline(
    config_name="upstream_gross",
    connection_id="snowflake-prod"
)
```

---

## Dependencies

- Phase 19 (Cortex Agent) - For AI-powered discovery
- Phase 24 (dbt Integration) - For dbt project generation
- Phase 25 (Data Quality) - For validation expectations
- Existing Hierarchy Module - For hierarchy project integration

---

## Tool Count Summary

| Phase | New Tools |
|-------|-----------|
| Phase 25 (Data Quality) | 7 |
| Phase 26 (Mart Factory) | 10 |
| **Total After Phase 26** | **234** |

---

## Verification

```bash
# Unit tests
python -m pytest tests/test_mart_factory.py -v

# Full regression
python -m pytest tests/ -q

# Manual verification (requires Snowflake)
# 1. Discover pattern:
discover_hierarchy_pattern(...)

# 2. Generate pipeline:
generate_mart_pipeline(config_name="upstream_gross")

# 3. Compare with production DDL:
# Diff generated VW_1 vs 01_VW_1_UPSTREAM_GROSS_REPORT_HIERARCHY_TRANSLATED.sql
```

---

## Next Steps After Phase 26

- **Phase 27**: Monitoring Agent - Track data quality, alert on drift
- **Phase 28**: Documentation Agent - Auto-generate pipeline docs via Cortex
- **Phase 29**: CI/CD Deployment - Automated pipeline deployment workflows
