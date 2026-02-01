# DataBridge AI: Comprehensive Deployment & Enhancement Plan

## Executive Summary

DataBridge AI is a mature, MCP-native data platform with **77+ implemented tools** across two core applications:
- **V3 Hierarchy Builder**: 40+ tools for financial hierarchy management, reconciliation, and SQL generation
- **V4 Analytics Engine**: 37+ tools for query building, NL-to-SQL, insights, and FP&A workflows

This plan outlines the strategic roadmap to transform DataBridge from a powerful CLI/MCP tool into an enterprise-grade, automated data warehouse platform with both **Basic Mode** (direct SQL execution) and **Advanced Mode** (dbt + GitHub integration).

---

## Current State Assessment

### What's Built (Production Ready)

| Component | Status | Tools/Features |
|-----------|--------|----------------|
| **V3 Hierarchy Builder** | ✅ Complete | 40+ MCP tools, hierarchy CRUD, formula management |
| **V4 Analytics Engine** | ✅ Complete | 37+ MCP tools, NL-to-SQL, insights engines |
| **SQL Generator** | ✅ Complete | Snowflake/PostgreSQL templates, view generation |
| **Vector Store (RAG)** | ✅ Complete | ChromaDB integration, semantic search |
| **Reconciliation Engine** | ✅ Complete | Fuzzy matching, data profiling, comparison |
| **Query Builder** | ✅ Complete | 4 SQL dialects, fluent API |
| **Insights Engines** | ✅ Complete | Anomaly, trend, variance analysis |
| **FP&A Workflows** | ✅ Complete | Monthly close, variance, forecast workflows |
| **Templates** | ✅ Complete | 20 industry-specific templates |
| **Skills** | ✅ Complete | 7 AI expertise definitions |
| **Docker Infrastructure** | ✅ Complete | Dev/Prod compose files, health checks |
| **CI/CD Pipeline** | ✅ Complete | Multi-stage testing, security scans |
| **Shared Libraries** | ✅ Complete | databridge-core, databridge-models |

### What's Partially Built

| Component | Status | Gap |
|-----------|--------|-----|
| **Connector Adapters** | ⚠️ Partial | PostgreSQL complete; Snowflake, Databricks, SQL Server need implementation |
| **Deployment Module** | ⚠️ Partial | Script generation exists; orchestration needed |
| **CLI Tool** | ⚠️ Partial | Basic structure; needs command implementation |
| **V3-V4 Sync** | ⚠️ Partial | Client exists; event-driven sync needed |

### What's Not Built

| Component | Priority | Complexity |
|-----------|----------|------------|
| **Frontend UI** | Medium | High |
| **GitHub Integration** | High | Medium |
| **dbt Project Generation** | High | Medium |
| **Authentication Layer** | High | Medium |
| **Notification System** | Low | Low |
| **Real-time Streaming** | Low | High |

---

## Architecture Overview

### Current Monorepo Structure

```
DataBridge_AI/
├── apps/
│   ├── databridge-v3/          # Hierarchy Builder (Python 3.10+)
│   │   ├── src/
│   │   │   ├── hierarchy/      # Core hierarchy operations
│   │   │   ├── reconciliation/ # Data comparison engine
│   │   │   ├── sql_generator/  # DDL/DML generation
│   │   │   ├── vectors/        # RAG & embeddings
│   │   │   └── mcp/tools/      # 40+ MCP tools
│   │   └── tests/
│   │
│   ├── databridge-v4/          # Analytics Engine (Python 3.10+)
│   │   ├── src/
│   │   │   ├── query/          # SQL builder
│   │   │   ├── nlp/            # NL-to-SQL
│   │   │   ├── insights/       # Analytics engines
│   │   │   ├── workflows/      # FP&A workflows
│   │   │   ├── connectors/     # Warehouse connectors
│   │   │   └── mcp/tools/      # 37+ MCP tools
│   │   └── tests/
│   │
│   └── databridge-cli/         # Unified CLI (planned)
│
├── libs/
│   ├── databridge-core/        # Shared utilities
│   └── databridge-models/      # Shared models & enums
│
├── docker/                     # Container orchestration
├── templates/                  # 20 hierarchy templates
├── skills/                     # 7 AI skills
└── .github/workflows/          # CI/CD
```

### Target Architecture (Post-Enhancement)

```
                                    ┌─────────────────────────────────┐
                                    │         User Interface          │
                                    │   (CLI / MCP / Web UI)          │
                                    └─────────────┬───────────────────┘
                                                  │
                    ┌─────────────────────────────┼─────────────────────────────┐
                    │                             │                             │
          ┌─────────▼─────────┐         ┌────────▼────────┐         ┌─────────▼─────────┐
          │   V3 Hierarchy    │         │   V4 Analytics  │         │   Orchestration   │
          │     Builder       │◄───────►│      Engine     │◄───────►│      Engine       │
          │   (40+ tools)     │  Sync   │   (37+ tools)   │  Trigger│   (New Module)    │
          └─────────┬─────────┘         └────────┬────────┘         └─────────┬─────────┘
                    │                            │                            │
                    │                            │                            │
          ┌─────────▼─────────────────────────────▼────────────────────────────▼─────────┐
          │                              Connector Layer                                  │
          │   PostgreSQL │ Snowflake │ Databricks │ SQL Server │ MySQL │ BigQuery       │
          └─────────────────────────────────────┬─────────────────────────────────────────┘
                                                │
                    ┌───────────────────────────┼───────────────────────────┐
                    │                           │                           │
          ┌─────────▼─────────┐       ┌────────▼────────┐       ┌─────────▼─────────┐
          │   Basic Mode      │       │  Advanced Mode  │       │    Validation     │
          │ (Direct SQL Exec) │       │  (dbt + GitHub) │       │      Suite        │
          └───────────────────┘       └─────────────────┘       └───────────────────┘
```

---

## Phase 1: Connector Completion & Semantic Analysis (Weeks 1-4)

### Goal
Complete the warehouse connector framework and add intelligent source analysis capabilities.

### 1.1 Complete Warehouse Connectors

**Current State**: PostgreSQL connector implemented; others are stubs.

**Implementation Tasks**:

| Connector | Priority | Effort | Dependencies |
|-----------|----------|--------|--------------|
| Snowflake | P0 | 3 days | snowflake-connector-python |
| Databricks | P1 | 3 days | databricks-sql-connector |
| SQL Server | P1 | 2 days | pyodbc |
| MySQL | P2 | 2 days | pymysql |
| BigQuery | P2 | 3 days | google-cloud-bigquery |

**File Locations**:
- `apps/databridge-v4/src/connectors/snowflake.py`
- `apps/databridge-v4/src/connectors/databricks.py`
- `apps/databridge-v4/src/connectors/sqlserver.py`

**Base Interface** (already defined in `connectors/base.py`):
```python
class BaseConnector(ABC):
    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def execute(self, query: str) -> QueryResult: ...

    @abstractmethod
    def get_schema(self, database: str, schema: str) -> SchemaMetadata: ...

    @abstractmethod
    def get_tables(self, database: str, schema: str) -> List[TableMetadata]: ...
```

### 1.2 Semantic Analysis Module (New)

**Purpose**: Automatically analyze source data and infer dimensional model structure.

**New Module Location**: `apps/databridge-v3/src/semantic/`

**Components**:

```python
# apps/databridge-v3/src/semantic/__init__.py
from .analyzer import SemanticAnalyzer
from .entity_detector import EntityDetector
from .relationship_inferencer import RelationshipInferencer

# apps/databridge-v3/src/semantic/analyzer.py
class SemanticAnalyzer:
    """
    Analyzes source metadata to build a canonical data model.

    Detected Entities:
    - Employee, Customer, Vendor → "Business Associate" dimension
    - Cost Center, Department → "Organization" dimension
    - Product, Inventory → "Product" dimension
    - Location, Region → "Geography" dimension
    - Date, Period → "Time" dimension
    - Account, GL Code → "Chart of Accounts" dimension
    """

    def analyze_source(self, connector: BaseConnector, schema: str) -> CanonicalModel:
        """Full semantic analysis of a source schema."""
        ...

    def detect_scd_type(self, table_metadata: TableMetadata) -> SCDType:
        """Detect SCD type based on column patterns (start_date, end_date, is_current)."""
        ...

    def suggest_grain(self, table_metadata: TableMetadata) -> List[str]:
        """Suggest the grain (primary key columns) for a fact table."""
        ...
```

### 1.3 New MCP Tools for Phase 1

**Add to** `apps/databridge-v3/src/mcp/tools/semantic.py`:

```python
@mcp.tool()
def analyze_source_schema(
    connection_id: str,
    database: str,
    schema: str
) -> dict:
    """
    Analyze a source schema and return inferred entities, relationships, and data types.

    Returns:
        - entities: List of detected dimension/fact candidates
        - relationships: Suggested joins between tables
        - scd_types: Detected slowly changing dimension types
        - grain_suggestions: Recommended primary keys for facts
    """
    ...

@mcp.tool()
def review_canonical_model(project_id: str) -> dict:
    """Display the current canonical model for user review."""
    ...

@mcp.tool()
def approve_entity_mapping(
    project_id: str,
    source_table: str,
    target_entity: str,
    entity_type: Literal["dimension", "fact", "bridge"]
) -> dict:
    """Confirm or override the semantic analyzer's entity classification."""
    ...

@mcp.tool()
def merge_source_columns(
    project_id: str,
    columns: List[str],
    target_column: str,
    merge_strategy: Literal["coalesce", "concatenate", "first_non_null"]
) -> dict:
    """Merge multiple source columns into a single canonical column."""
    ...
```

### 1.4 Testing Strategy

**Unit Tests**:
```python
# apps/databridge-v4/tests/unit/connectors/test_snowflake.py
class TestSnowflakeConnector:
    def test_connect_with_valid_credentials(self): ...
    def test_get_schema_returns_metadata(self): ...
    def test_execute_query_returns_results(self): ...

# apps/databridge-v3/tests/unit/semantic/test_analyzer.py
class TestSemanticAnalyzer:
    def test_detects_customer_entity(self): ...
    def test_detects_scd_type_2_pattern(self): ...
    def test_suggests_correct_joins(self): ...
```

**Integration Tests**:
```python
# apps/databridge-v3/tests/integration/test_semantic_analysis.py
def test_full_semantic_analysis_workflow():
    """End-to-end test: connect → analyze → review → approve."""
    ...
```

### 1.5 Deliverables

- [ ] Snowflake connector with full metadata extraction
- [ ] Databricks connector with Unity Catalog support
- [ ] SQL Server connector with schema introspection
- [ ] Semantic Analyzer module with entity detection
- [ ] 4 new MCP tools for source analysis
- [ ] Unit and integration tests (>80% coverage)
- [ ] Updated CLAUDE.md with new tool documentation

---

## Phase 2: Data Warehouse Model Generation (Weeks 5-8)

### Goal
Transform the approved canonical model into deployable Snowflake objects using the established pipeline: `TBL_0 → VW_1 → DT_2 → DT_3A → DT_3`.

### 2.1 Warehouse Modeler Module

**Location**: `apps/databridge-v3/src/warehouse_modeler/`

**Current State**: The `sql_generator/` module generates VW_1 tier views. This phase extends it to the full pipeline.

**Object Pipeline**:

| Object | Purpose | Template |
|--------|---------|----------|
| `TBL_0_{name}` | Source hierarchy table | `templates/snowflake/table.j2` |
| `VW_1_{name}` | Unnested mapping view | `templates/snowflake/view.j2` ✅ exists |
| `DT_2_{name}` | Dimension join table | `templates/snowflake/dynamic_table.j2` ✅ exists |
| `DT_3A_{name}` | Pre-aggregation table | `templates/snowflake/aggregation.j2` ✅ exists |
| `DT_3_{name}` | Final output table | `templates/snowflake/output_table.j2` ✅ exists |

**Implementation**:

```python
# apps/databridge-v3/src/warehouse_modeler/modeler.py
class WarehouseModeler:
    """
    Generates the full Snowflake object pipeline from a canonical model.
    """

    def __init__(self, canonical_model: CanonicalModel, dialect: SQLDialect = SQLDialect.SNOWFLAKE):
        self.model = canonical_model
        self.dialect = dialect
        self.generator = ViewGenerator(dialect)  # Existing class

    def generate_dimension_pipeline(self, dimension: DimensionSpec) -> List[GeneratedObject]:
        """
        Generate TBL_0 → VW_1 → DT_2 for a dimension.
        """
        objects = []

        # TBL_0: Source table from hierarchy
        objects.append(self._generate_tbl_0(dimension))

        # VW_1: Unnested view (uses existing view_generator)
        objects.append(self.generator.generate_view(dimension))

        # DT_2: Dynamic table joining to source
        objects.append(self._generate_dt_2(dimension))

        return objects

    def generate_fact_pipeline(self, fact: FactSpec) -> List[GeneratedObject]:
        """
        Generate DT_3A → DT_3 for a fact table.
        """
        objects = []

        # DT_3A: Pre-aggregation
        objects.append(self._generate_dt_3a(fact))

        # DT_3: Final output with union
        objects.append(self._generate_dt_3(fact))

        return objects

    def generate_full_warehouse(self) -> WarehouseSpec:
        """
        Generate all objects for the complete data warehouse.
        Returns dependency-ordered list of DDL statements.
        """
        ...
```

### 2.2 Dual-Mode Output Generation

**Basic Mode**: Direct SQL files for immediate execution.

```python
# apps/databridge-v3/src/warehouse_modeler/generators/sql_generator.py
class SQLScriptGenerator:
    """Generates plain SQL scripts for Basic Mode deployment."""

    def generate(self, warehouse: WarehouseSpec, output_dir: Path) -> List[Path]:
        """
        Creates dependency-ordered SQL files:
        - 01_dimensions.sql
        - 02_facts.sql
        - 03_views.sql
        - 04_dynamic_tables.sql
        - 05_grants.sql
        """
        ...
```

**Advanced Mode**: dbt project with full version control.

```python
# apps/databridge-v3/src/warehouse_modeler/generators/dbt_generator.py
class DbtProjectGenerator:
    """Generates a complete dbt project for Advanced Mode deployment."""

    def generate(self, warehouse: WarehouseSpec, project_name: str) -> DbtProject:
        """
        Creates dbt project structure:

        {project_name}/
        ├── dbt_project.yml
        ├── profiles.yml.example
        ├── packages.yml
        ├── models/
        │   ├── staging/
        │   │   ├── stg_{source}.sql
        │   │   └── _stg_{source}.yml
        │   ├── intermediate/
        │   │   ├── int_{entity}.sql
        │   │   └── _int_{entity}.yml
        │   └── marts/
        │       ├── dim_{dimension}.sql
        │       ├── fct_{fact}.sql
        │       └── _schema.yml
        ├── macros/
        │   └── generate_schema_name.sql
        ├── seeds/
        │   └── hierarchy_mappings.csv
        └── tests/
            └── generic/
        """
        ...

    def _generate_model_sql(self, obj: GeneratedObject) -> str:
        """Generate dbt-compatible SQL with {{ ref() }} and {{ source() }}."""
        ...

    def _generate_schema_yml(self, models: List[GeneratedObject]) -> str:
        """Generate _schema.yml with column definitions and tests."""
        ...
```

### 2.3 GitHub Integration (Advanced Mode)

**Location**: `apps/databridge-v3/src/integrations/github.py`

```python
# apps/databridge-v3/src/integrations/github.py
from github import Github
from github.Repository import Repository

class GitHubIntegration:
    """
    Manages GitHub repository operations for version-controlled dbt projects.
    """

    def __init__(self, token: str):
        self.client = Github(token)

    def create_repository(
        self,
        name: str,
        description: str,
        private: bool = True
    ) -> Repository:
        """Create a new GitHub repository."""
        ...

    def push_dbt_project(
        self,
        repo: Repository,
        project: DbtProject,
        commit_message: str = "Initial dbt project from DataBridge AI"
    ) -> str:
        """Push dbt project to repository. Returns commit SHA."""
        ...

    def setup_github_actions(self, repo: Repository) -> None:
        """
        Add GitHub Actions workflow for:
        - dbt build on push
        - dbt test on PR
        - dbt docs generate on release
        """
        ...
```

### 2.4 New MCP Tools for Phase 2

**Add to** `apps/databridge-v3/src/mcp/tools/warehouse.py`:

```python
@mcp.tool()
def design_warehouse_model(
    project_id: str,
    output_mode: Literal["basic", "dbt"] = "basic"
) -> dict:
    """
    Generate data warehouse design from approved canonical model.

    Args:
        project_id: The hierarchy project to generate from
        output_mode: "basic" for SQL scripts, "dbt" for dbt project

    Returns:
        - objects: List of generated database objects
        - dependencies: Object dependency graph
        - preview: Sample DDL statements
    """
    ...

@mcp.tool()
def preview_generated_ddl(
    project_id: str,
    object_name: str
) -> dict:
    """Preview the DDL for a specific generated object."""
    ...

@mcp.tool()
def create_github_repository(
    repo_name: str,
    description: str = "",
    private: bool = True
) -> dict:
    """
    Create a new GitHub repository for the dbt project.
    Requires GitHub OAuth token in credentials.
    """
    ...

@mcp.tool()
def push_dbt_project_to_github(
    project_id: str,
    repo_name: str,
    branch: str = "main"
) -> dict:
    """
    Push generated dbt project to GitHub repository.
    Returns commit URL.
    """
    ...
```

### 2.5 Testing Strategy

**Unit Tests**:
```python
# apps/databridge-v3/tests/unit/warehouse_modeler/test_dbt_generator.py
class TestDbtProjectGenerator:
    def test_generates_valid_dbt_project_yml(self): ...
    def test_generates_staging_models_with_source_refs(self): ...
    def test_generates_schema_yml_with_tests(self): ...
    def test_generates_github_actions_workflow(self): ...
```

**Integration Tests**:
```python
# apps/databridge-v3/tests/integration/test_dbt_generation.py
def test_generated_dbt_project_passes_dbt_parse():
    """Verify generated project passes `dbt parse`."""
    ...

def test_github_push_creates_valid_commit():
    """Integration test with GitHub API (mock or sandbox repo)."""
    ...
```

### 2.6 Deliverables

- [ ] WarehouseModeler class with full pipeline support
- [ ] DbtProjectGenerator with dbt best practices
- [ ] GitHubIntegration for repository management
- [ ] 4 new MCP tools for warehouse design
- [ ] Jinja2 templates for all object types
- [ ] Unit tests for generators (>85% coverage)
- [ ] Integration test with dbt CLI validation

---

## Phase 3: Deployment Orchestration (Weeks 9-12)

### Goal
Implement robust deployment pipelines for both Basic and Advanced modes with proper error handling, rollback, and validation.

### 3.1 Deployment Orchestrator

**Location**: `apps/databridge-v3/src/deployment/orchestrator.py`

```python
# apps/databridge-v3/src/deployment/orchestrator.py
from enum import Enum
from typing import Optional
import asyncio

class DeploymentMode(Enum):
    BASIC = "basic"      # Direct SQL execution
    ADVANCED = "advanced"  # dbt + GitHub

class DeploymentOrchestrator:
    """
    Manages the deployment lifecycle for both Basic and Advanced modes.
    """

    def __init__(
        self,
        project_id: str,
        mode: DeploymentMode,
        connector: Optional[BaseConnector] = None,
        github: Optional[GitHubIntegration] = None
    ):
        self.project_id = project_id
        self.mode = mode
        self.connector = connector
        self.github = github
        self.state = DeploymentState()

    async def deploy(self) -> DeploymentResult:
        """
        Execute the deployment based on mode.

        Basic Mode Pipeline:
        1. connect()
        2. begin_transaction()
        3. execute_ddl() - Create objects
        4. execute_etl() - Load data
        5. validate() - Run checks
        6. commit_transaction() or rollback()
        7. disconnect()

        Advanced Mode Pipeline:
        1. generate_dbt_project()
        2. git_commit()
        3. git_push()
        4. trigger_dbt_run() (optional)
        5. monitor_dbt_job()
        6. report_results()
        """
        if self.mode == DeploymentMode.BASIC:
            return await self._deploy_basic()
        else:
            return await self._deploy_advanced()

    async def _deploy_basic(self) -> DeploymentResult:
        """Direct SQL execution with transaction management."""
        try:
            self.connector.connect()
            self.connector.begin_transaction()

            # Execute DDL in dependency order
            for obj in self.state.objects:
                self.connector.execute(obj.ddl)
                self.state.mark_deployed(obj)

            # Run ETL statements
            for etl in self.state.etl_statements:
                self.connector.execute(etl)

            # Validate
            validation_result = await self._validate()
            if not validation_result.passed:
                self.connector.rollback()
                return DeploymentResult(success=False, errors=validation_result.errors)

            self.connector.commit()
            return DeploymentResult(success=True, deployed_objects=self.state.objects)

        except Exception as e:
            self.connector.rollback()
            return DeploymentResult(success=False, errors=[str(e)])
        finally:
            self.connector.disconnect()

    async def _deploy_advanced(self) -> DeploymentResult:
        """dbt + GitHub workflow."""
        try:
            # Generate and push to GitHub
            dbt_project = self._generate_dbt_project()
            commit_sha = self.github.push_dbt_project(
                repo=self.state.github_repo,
                project=dbt_project
            )

            # Optionally trigger dbt Cloud job
            if self.state.dbt_cloud_job_id:
                job_run = await self._trigger_dbt_cloud_run()
                result = await self._monitor_dbt_run(job_run)
                return DeploymentResult(
                    success=result.status == "success",
                    commit_sha=commit_sha,
                    dbt_run_id=job_run.id
                )

            return DeploymentResult(success=True, commit_sha=commit_sha)

        except Exception as e:
            return DeploymentResult(success=False, errors=[str(e)])
```

### 3.2 ETL Generator Enhancements

**Enhance existing** `apps/databridge-v3/src/sql_generator/`:

```python
# apps/databridge-v3/src/sql_generator/etl_generator.py
class ETLGenerator:
    """
    Generates data loading statements for both modes.
    """

    def generate_basic_etl(self, source: SourceSpec, target: TargetSpec) -> List[str]:
        """
        Generate INSERT INTO ... SELECT statements for Basic Mode.
        Handles:
        - Type conversions
        - NULL handling
        - Deduplication
        - Incremental loading
        """
        ...

    def generate_dbt_models(self, source: SourceSpec, target: TargetSpec) -> List[DbtModel]:
        """
        Generate dbt models for Advanced Mode.
        Uses:
        - {{ source() }} for raw data
        - {{ ref() }} for dependencies
        - Incremental materialization where appropriate
        """
        ...
```

### 3.3 Validation Suite

**Location**: `apps/databridge-v4/src/validation/`

```python
# apps/databridge-v4/src/validation/suite.py
class ValidationSuite:
    """
    Post-deployment validation for both modes.
    """

    def __init__(self, connector: BaseConnector, use_dbt: bool = False):
        self.connector = connector
        self.use_dbt = use_dbt

    async def validate(self, deployment: DeploymentResult) -> ValidationResult:
        """
        Run validation suite:
        1. Row count checks
        2. NULL value checks
        3. Referential integrity
        4. Business rule validation
        5. dbt tests (if Advanced Mode)
        """
        results = []

        # Standard validations
        results.extend(await self._check_row_counts())
        results.extend(await self._check_null_constraints())
        results.extend(await self._check_referential_integrity())

        # dbt-specific validations
        if self.use_dbt:
            dbt_results = await self._run_dbt_tests()
            results.extend(dbt_results)

        return ValidationResult(
            passed=all(r.passed for r in results),
            checks=results
        )

    async def _run_dbt_tests(self) -> List[CheckResult]:
        """Execute `dbt test` and parse results."""
        ...
```

### 3.4 New MCP Tools for Phase 3

**Add to** `apps/databridge-v3/src/mcp/tools/deployment.py`:

```python
@mcp.tool()
def deploy_warehouse(
    project_id: str,
    mode: Literal["basic", "advanced"] = "basic",
    target_schema: str = "ANALYTICS",
    dry_run: bool = False
) -> dict:
    """
    Deploy the data warehouse to the target environment.

    Args:
        project_id: Project containing the warehouse design
        mode: "basic" for direct SQL, "advanced" for dbt
        target_schema: Snowflake schema to deploy to
        dry_run: If True, preview changes without executing

    Returns:
        - status: "success" | "failed" | "dry_run"
        - deployed_objects: List of created objects
        - errors: Any deployment errors
    """
    ...

@mcp.tool()
def get_deployment_status(deployment_id: str) -> dict:
    """Check the status of an ongoing deployment."""
    ...

@mcp.tool()
def rollback_deployment(deployment_id: str) -> dict:
    """
    Rollback a failed deployment.
    Only available for Basic Mode deployments.
    """
    ...

@mcp.tool()
def validate_deployment(
    project_id: str,
    validation_level: Literal["basic", "comprehensive"] = "basic"
) -> dict:
    """
    Run post-deployment validation suite.

    Args:
        project_id: The deployed project
        validation_level: "basic" for row counts, "comprehensive" for full suite

    Returns:
        - passed: Overall pass/fail
        - checks: Individual check results
        - recommendations: Suggested fixes for failures
    """
    ...

@mcp.tool()
def trigger_dbt_run(
    project_id: str,
    dbt_command: Literal["run", "build", "test"] = "build"
) -> dict:
    """
    Trigger a dbt run for Advanced Mode deployments.
    Requires dbt Cloud integration or local dbt installation.
    """
    ...
```

### 3.5 Testing Strategy

**Unit Tests**:
```python
# apps/databridge-v3/tests/unit/deployment/test_orchestrator.py
class TestDeploymentOrchestrator:
    def test_basic_mode_commits_on_success(self): ...
    def test_basic_mode_rollbacks_on_failure(self): ...
    def test_advanced_mode_pushes_to_github(self): ...
    def test_validation_suite_catches_null_violations(self): ...
```

**Integration Tests**:
```python
# apps/databridge-v3/tests/integration/test_deployment.py
@pytest.mark.integration
def test_full_basic_deployment_workflow():
    """End-to-end: generate → deploy → validate."""
    ...

@pytest.mark.integration
def test_full_advanced_deployment_workflow():
    """End-to-end: generate → git push → dbt build → validate."""
    ...
```

### 3.6 Deliverables

- [ ] DeploymentOrchestrator with dual-mode support
- [ ] ETLGenerator for both modes
- [ ] ValidationSuite with dbt integration
- [ ] 5 new MCP tools for deployment
- [ ] Transaction management with rollback
- [ ] dbt Cloud API integration (optional)
- [ ] Comprehensive deployment logs

---

## Phase 4: V3-V4 Integration & Workflow Automation (Weeks 13-16)

### Goal
Create seamless integration between V3 (Hierarchy Builder) and V4 (Analytics Engine) with automated synchronization and high-level workflow tools.

### 4.1 Event-Driven Synchronization

**Current State**: V4 has a `V3Client` in `integration/v3_client.py` for manual sync.

**Enhancement**: Add event-driven synchronization using Redis Pub/Sub.

```python
# libs/databridge-core/src/databridge_core/events/publisher.py
import redis
from typing import Any

class EventPublisher:
    """Publishes events to Redis for cross-application communication."""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis = redis.from_url(redis_url)
        self.channel_prefix = "databridge:"

    def publish(self, event_type: str, payload: dict) -> None:
        """
        Publish an event.

        Event types:
        - deployment.completed
        - hierarchy.updated
        - validation.passed
        """
        channel = f"{self.channel_prefix}{event_type}"
        self.redis.publish(channel, json.dumps(payload))

# libs/databridge-core/src/databridge_core/events/subscriber.py
class EventSubscriber:
    """Subscribes to events for reactive processing."""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis = redis.from_url(redis_url)
        self.pubsub = self.redis.pubsub()

    def subscribe(self, event_type: str, handler: Callable) -> None:
        """Subscribe to an event type with a handler function."""
        channel = f"databridge:{event_type}"
        self.pubsub.subscribe(**{channel: handler})

    def listen(self) -> None:
        """Start listening for events (blocking)."""
        for message in self.pubsub.listen():
            if message["type"] == "message":
                # Handler is called automatically
                pass
```

**V4 Event Handlers**:

```python
# apps/databridge-v4/src/integration/event_handlers.py
class V4EventHandlers:
    """Handles events from V3."""

    def __init__(self, subscriber: EventSubscriber):
        self.subscriber = subscriber
        self._register_handlers()

    def _register_handlers(self):
        self.subscriber.subscribe("deployment.completed", self.on_deployment_completed)
        self.subscriber.subscribe("hierarchy.updated", self.on_hierarchy_updated)

    def on_deployment_completed(self, message: dict) -> None:
        """
        When V3 completes a deployment:
        1. Refresh metadata catalog
        2. Update knowledge base
        3. Re-index for NL-to-SQL
        """
        project_id = message["project_id"]
        schema = message["target_schema"]

        # Auto-sync metadata
        catalog = self.catalog_service.refresh(schema)
        self.knowledge_base.update_from_deployment(catalog)
```

### 4.2 High-Level Workflow MCP Tools

**Location**: `apps/databridge-v3/src/mcp/tools/workflow.py`

```python
@mcp.tool()
def workflow_start_warehouse_creation(
    source_config: dict,
    project_name: str
) -> dict:
    """
    Start the automated data warehouse creation workflow.

    Args:
        source_config: Connection details for source system
        project_name: Name for the new warehouse project

    Returns:
        - workflow_id: Unique identifier for tracking
        - status: "analyzing"
        - next_step: "review_proposed_model"
    """
    ...

@mcp.tool()
def workflow_get_proposed_model(workflow_id: str) -> dict:
    """
    Get the canonical model proposed by semantic analysis.

    Returns:
        - entities: Detected dimensions and facts
        - relationships: Proposed joins
        - warnings: Any potential issues
        - approval_required: List of items needing user confirmation
    """
    ...

@mcp.tool()
def workflow_approve_model(
    workflow_id: str,
    approved_model: dict,
    use_dbt: bool = True,
    github_repo: Optional[str] = None
) -> dict:
    """
    Approve the canonical model and trigger warehouse generation.

    Args:
        workflow_id: The workflow to approve
        approved_model: User-confirmed canonical model
        use_dbt: True for Advanced Mode, False for Basic Mode
        github_repo: Repository name for dbt project (Advanced Mode only)

    Returns:
        - status: "generating" | "deploying"
        - deployment_id: ID for tracking deployment
    """
    ...

@mcp.tool()
def workflow_get_status(workflow_id: str) -> dict:
    """
    Get comprehensive status of a workflow.

    Returns:
        - phase: "analysis" | "generation" | "deployment" | "validation" | "complete"
        - progress: Percentage complete
        - current_step: Description of current activity
        - artifacts: Generated files/objects
        - errors: Any issues encountered
    """
    ...

@mcp.tool()
def workflow_query_deployed_warehouse(
    workflow_id: str,
    question: str
) -> dict:
    """
    Query the deployed warehouse using natural language.
    Uses V4's NL-to-SQL capabilities.

    Args:
        workflow_id: Completed workflow
        question: Natural language question

    Returns:
        - sql: Generated SQL query
        - results: Query results (limited)
        - visualization_suggestion: Chart type recommendation
    """
    ...
```

### 4.3 Natural Language Interface Enhancement

**Enhance V4's NL-to-SQL** to work with dynamically deployed warehouses:

```python
# apps/databridge-v4/src/nlp/enhanced_nl_to_sql.py
class EnhancedNLToSQL:
    """
    Enhanced NL-to-SQL that integrates with V3 deployments.
    """

    def __init__(
        self,
        catalog: MetadataCatalog,
        knowledge_base: KnowledgeBase,
        v3_client: V3Client
    ):
        self.catalog = catalog
        self.kb = knowledge_base
        self.v3 = v3_client

    def translate(self, question: str, context: Optional[dict] = None) -> SQLQuery:
        """
        Translate natural language to SQL with full context awareness.

        Enhanced features:
        - Understands V3 hierarchy structures
        - Uses formula definitions from V3
        - Leverages business glossary
        - Applies FP&A domain knowledge
        """
        # Get hierarchy context from V3
        hierarchies = self.v3.get_hierarchies_for_project(context.get("project_id"))

        # Enrich catalog with hierarchy metadata
        enriched_catalog = self._enrich_catalog(hierarchies)

        # Standard NL-to-SQL with enriched context
        return self._translate_with_context(question, enriched_catalog)
```

### 4.4 Testing Strategy

**Integration Tests**:
```python
# apps/databridge-v4/tests/integration/test_v3_v4_sync.py
@pytest.mark.integration
def test_event_driven_catalog_refresh():
    """Verify V4 refreshes catalog when V3 deploys."""
    ...

@pytest.mark.integration
def test_full_workflow_end_to_end():
    """Complete workflow: source → analysis → deploy → query."""
    ...
```

**E2E Tests**:
```python
# tests/e2e/test_5_minute_warehouse.py
def test_5_minute_data_warehouse():
    """
    The signature demo:
    1. Upload source files
    2. Run semantic analysis
    3. Approve model
    4. Choose Advanced Mode
    5. Deploy with dbt
    6. Query with natural language
    """
    ...
```

### 4.5 Deliverables

- [ ] Event publisher/subscriber in databridge-core
- [ ] V4 event handlers for auto-sync
- [ ] 5 high-level workflow MCP tools
- [ ] Enhanced NL-to-SQL with V3 context
- [ ] End-to-end integration tests
- [ ] "5-Minute Data Warehouse" demo script

---

## Phase 5: Production Hardening & Documentation (Weeks 17-20)

### Goal
Prepare DataBridge AI for production deployment with comprehensive documentation, monitoring, and operational tooling.

### 5.1 Authentication & Authorization

**Location**: `libs/databridge-core/src/databridge_core/auth/`

```python
# libs/databridge-core/src/databridge_core/auth/provider.py
class AuthProvider:
    """
    Authentication provider supporting multiple methods.
    """

    def authenticate(self, credentials: dict) -> AuthResult:
        """
        Authenticate user.

        Supported methods:
        - API Key (simple)
        - OAuth 2.0 (GitHub, Google)
        - SAML (enterprise)
        """
        ...

    def authorize(self, user: User, resource: str, action: str) -> bool:
        """
        Check if user has permission for action on resource.

        Resources: project, hierarchy, deployment, query
        Actions: read, write, delete, deploy
        """
        ...
```

### 5.2 Monitoring & Observability

**Add Prometheus metrics and structured logging**:

```python
# libs/databridge-core/src/databridge_core/monitoring/metrics.py
from prometheus_client import Counter, Histogram, Gauge

# MCP Tool metrics
tool_calls = Counter(
    'databridge_mcp_tool_calls_total',
    'Total MCP tool invocations',
    ['tool_name', 'status']
)

tool_latency = Histogram(
    'databridge_mcp_tool_latency_seconds',
    'MCP tool execution time',
    ['tool_name']
)

# Deployment metrics
active_deployments = Gauge(
    'databridge_active_deployments',
    'Number of deployments in progress'
)

deployment_duration = Histogram(
    'databridge_deployment_duration_seconds',
    'Time to complete deployment',
    ['mode']  # basic or advanced
)
```

### 5.3 Documentation

**Documentation Structure**:

```
docs/
├── README.md                      # Quick start
├── DEPLOYMENT_PLAN.md            # This document
├── architecture/
│   ├── overview.md               # System architecture
│   ├── v3-hierarchy-builder.md   # V3 deep dive
│   ├── v4-analytics-engine.md    # V4 deep dive
│   └── data-flow.md              # Data flow diagrams
├── user-guide/
│   ├── getting-started.md        # First deployment
│   ├── connecting-sources.md     # Connector setup
│   ├── building-hierarchies.md   # V3 workflows
│   ├── analytics-queries.md      # V4 workflows
│   ├── deployment-modes.md       # Basic vs Advanced
│   └── templates-skills.md       # Using templates
├── api-reference/
│   ├── v3-mcp-tools.md           # 40+ V3 tools
│   ├── v4-mcp-tools.md           # 37+ V4 tools
│   └── workflow-tools.md         # Workflow tools
├── operations/
│   ├── docker-deployment.md      # Container setup
│   ├── kubernetes.md             # K8s deployment
│   ├── monitoring.md             # Observability
│   └── troubleshooting.md        # Common issues
└── tutorials/
    ├── 5-minute-warehouse.md     # Signature demo
    ├── oil-gas-hierarchy.md      # Industry example
    └── saas-metrics.md           # SaaS example
```

### 5.4 CLI Completion

**Complete** `apps/databridge-cli/`:

```python
# apps/databridge-cli/src/databridge_cli/main.py
import typer
from databridge_v3.cli import app as v3_app
from databridge_v4.cli import app as v4_app

app = typer.Typer(
    name="databridge",
    help="DataBridge AI - Automated Data Warehouse Platform"
)

# Mount V3 and V4 as subcommands
app.add_typer(v3_app, name="hierarchy", help="Hierarchy Builder (V3)")
app.add_typer(v4_app, name="analytics", help="Analytics Engine (V4)")

# Top-level workflow commands
@app.command()
def init(
    source: str = typer.Option(..., help="Source connection string"),
    project: str = typer.Option(..., help="Project name")
):
    """Initialize a new data warehouse project."""
    ...

@app.command()
def deploy(
    project: str = typer.Argument(..., help="Project to deploy"),
    mode: str = typer.Option("basic", help="basic or advanced"),
    dbt: bool = typer.Option(False, help="Use dbt (Advanced Mode)")
):
    """Deploy a data warehouse project."""
    ...

@app.command()
def query(
    project: str = typer.Argument(..., help="Project to query"),
    question: str = typer.Argument(..., help="Natural language question")
):
    """Query a deployed warehouse using natural language."""
    ...

if __name__ == "__main__":
    app()
```

### 5.5 Deliverables

- [ ] Authentication/authorization layer
- [ ] Prometheus metrics integration
- [ ] Structured logging with correlation IDs
- [ ] Complete CLI with all commands
- [ ] Comprehensive documentation (20+ pages)
- [ ] Tutorial videos/scripts
- [ ] Production deployment guide

---

## Summary: New MCP Tools by Phase

| Phase | New Tools | Total |
|-------|-----------|-------|
| **Phase 1** | `analyze_source_schema`, `review_canonical_model`, `approve_entity_mapping`, `merge_source_columns` | 4 |
| **Phase 2** | `design_warehouse_model`, `preview_generated_ddl`, `create_github_repository`, `push_dbt_project_to_github` | 4 |
| **Phase 3** | `deploy_warehouse`, `get_deployment_status`, `rollback_deployment`, `validate_deployment`, `trigger_dbt_run` | 5 |
| **Phase 4** | `workflow_start_warehouse_creation`, `workflow_get_proposed_model`, `workflow_approve_model`, `workflow_get_status`, `workflow_query_deployed_warehouse` | 5 |

**Total New Tools**: 18

**Final Tool Count**: 77 existing + 18 new = **95 MCP tools**

---

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| Connector API changes | Medium | Version pin dependencies, add retry logic |
| dbt version compatibility | Medium | Support dbt 1.5+ only, test against latest |
| GitHub API rate limits | Low | Implement caching, batch operations |
| Large schema analysis | Medium | Add pagination, async processing |
| Transaction deadlocks | High | Implement timeout, proper lock ordering |

---

## Success Criteria

### Phase 1
- [ ] All 6 warehouse connectors passing integration tests
- [ ] Semantic analyzer detects 90%+ of entities correctly
- [ ] Source analysis completes in <30 seconds for 100-table schemas

### Phase 2
- [ ] Generated dbt projects pass `dbt parse` validation
- [ ] GitHub integration creates valid repositories
- [ ] All Jinja2 templates produce syntactically correct SQL

### Phase 3
- [ ] Basic Mode deployments complete with proper rollback on failure
- [ ] Advanced Mode triggers successful dbt Cloud runs
- [ ] Validation suite catches 95%+ of data quality issues

### Phase 4
- [ ] Event-driven sync updates V4 within 5 seconds of V3 deployment
- [ ] NL-to-SQL accuracy >85% on deployed warehouses
- [ ] End-to-end workflow completes in <10 minutes for typical projects

### Phase 5
- [ ] All 95 MCP tools documented with examples
- [ ] CLI passes usability testing
- [ ] Production deployment guide validated by external tester

---

## Timeline Summary

| Phase | Duration | Focus |
|-------|----------|-------|
| **Phase 1** | Weeks 1-4 | Connectors & Semantic Analysis |
| **Phase 2** | Weeks 5-8 | Warehouse Model Generation |
| **Phase 3** | Weeks 9-12 | Deployment Orchestration |
| **Phase 4** | Weeks 13-16 | V3-V4 Integration |
| **Phase 5** | Weeks 17-20 | Production Hardening |

**Total Duration**: 20 weeks (~5 months)

---

*Document Version: 1.0*
*Created: 2026-01-30*
*Author: Claude Code (Opus 4.5)*
*Based on: DataBridge AI Monorepo Analysis*
