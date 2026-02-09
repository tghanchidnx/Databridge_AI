# DataBridge AI - Tool Manifest

> Auto-generated documentation for all MCP tools.
> Last updated: 2026-02-09

---

## Overview

DataBridge AI provides **348 tools** for data reconciliation, hierarchy management, and observability.

| Category | Tools |
|----------|-------|
| File Discovery | find_files, stage_file, get_working_directory |
| Data Loading | load_csv, load_json, query_database |
| Profiling | profile_data, detect_schema_drift |
| Comparison | compare_hashes, get_orphan_details, get_conflict_details |
| Fuzzy Matching | fuzzy_match_columns, fuzzy_deduplicate |
| PDF/OCR | extract_text_from_pdf, ocr_image, parse_table_from_text |
| Workflow | save_workflow_step, get_workflow, clear_workflow, get_audit_log |
| Transform | transform_column, merge_sources |
| Hierarchy Builder | create_hierarchy_project, create_hierarchy, import_hierarchy_csv, etc. |
| Templates & Skills | list_financial_templates, get_skill_prompt, etc. |
| AI Orchestrator | submit_orchestrated_task, send_agent_message, etc. |
| Cortex AI | cortex_complete, analyst_ask, cortex_reason, etc. |
| Wright (Mart Factory) | create_mart_config, generate_mart_pipeline, validate_hierarchy_data_quality, etc. |
| Data Catalog | catalog_create_asset, catalog_search, etc. |
| Versioning | version_create, version_diff, version_rollback, etc. |
| Lineage | track_column_lineage, analyze_change_impact, etc. |
| Git/CI-CD | git_commit, github_create_pr, etc. |
| dbt Integration | create_dbt_project, generate_dbt_model, etc. |
| Data Quality | generate_expectation_suite, run_validation, etc. |
| Diff Utilities | diff_text, diff_lists, diff_dicts, etc. |
| GraphRAG Engine | rag_search, rag_validate_output, rag_get_context, etc. |
| Data Observability | obs_record_metric, obs_create_alert_rule, obs_get_asset_health, etc. |

---

## Tool Reference

### `add_client_custom_prompt`

Add a custom prompt to a client's knowledge base.

        Custom prompts help store client-specific instructions that can be
        used when working on that client's data.

        Args:
            client_id: The client to add the prompt to.
            name: Name for the prompt (e.g., 'Revenue Recognition Rules').
            trigger: When to use this prompt (e.g., 'When mapping revenue accounts').
            content: The actual prompt content/instructions.
            category: Prompt category for organization.

        Returns:
            JSON with the updated client profile including the new prompt.

---

### `add_column_expectation`

Add a column expectation to a suite.

        Adds a specific data quality expectation for a column.

        Available expectation types:
        - not_null: Column values should not be null
        - unique: Column values should be unique
        - in_set: Values should be in specified set
        - match_regex: Values should match regex pattern
        - between: Values should be between min and max

        Args:
            suite_name: Name of the suite
            column: Column name
            expectation_type: Type of expectation (not_null, unique, in_set, match_regex, between)
            value_set: JSON array of allowed values (for in_set)
            regex: Regex pattern (for match_regex)
            min_value: Minimum value (for between)
            max_value: Maximum value (for between)
            severity: Failure severity (critical, high, medium, low, info)
            description: Human-readable description

        Returns:
            Added expectation details

        Example:
            add_column_expectation(
                suite_name="gl_accounts_suite",
                column="ACCOUNT_CODE",
                expectation_type="match_regex",
                regex="^[4-9][0-9]{3}$",
                severity="high"
            )

---

### `add_faux_object`

Add a faux object configuration to the project.

        Faux objects are standard Snowflake objects that wrap the semantic view:

        - **view**: Standard VIEW using SEMANTIC_VIEW() in the AS clause.
          Universal BI tool support. No parameters.

        - **stored_procedure**: Snowpark Python procedure with RETURNS TABLE.
          BI tools call: SELECT * FROM TABLE(proc(args)).
          Supports parameters for dynamic filtering.

        - **dynamic_table**: Auto-refreshing table from semantic view query.
          Requires warehouse and target_lag. Universal BI support.

        - **task**: Scheduled materialization via Snowflake Task + procedure.
          Creates a regular table refreshed on a CRON schedule.

        Args:
            project_id: The project ID
            name: Object name (e.g., "V_PL_BY_REGION", "GET_PL_DATA")
            faux_type: One of "view", "stored_procedure", "dynamic_table", "task"
            target_database: Database for the faux object
            target_schema: Schema for the faux object
            selected_dimensions: Comma-separated dimension names (empty = all)
            selected_metrics: Comma-separated metric names (empty = all)
            selected_facts: Comma-separated fact names (empty = all)
            parameters: JSON array of procedure parameters (stored_procedure only).
                        Each: {"name": "FISCAL_YEAR", "data_type": "INT", "default_value": "2025"}
            warehouse: Warehouse name (for dynamic_table/task)
            target_lag: Refresh interval (for dynamic_table, e.g., "2 hours")
            schedule: CRON schedule (for task, e.g., "USING CRON 0 */4 * * * America/Chicago")
            materialized_table: Target table for materialization (task only)
            where_clause: Static WHERE filter (e.g., "fiscal_year = 2025")
            comment: Object description

        Returns:
            JSON confirmation with the faux object details

---

### `add_faux_semantic_column`

Add a dimension, metric, or fact column to the semantic view.

        Columns define the business concepts exposed by the semantic view:
        - **dimension**: Descriptive attributes (region, product_name, fiscal_year)
        - **metric**: Calculated aggregations (total_revenue, gross_profit)
        - **fact**: Raw measure values (debit_amount, credit_amount)

        Args:
            project_id: The project ID
            name: Column name (e.g., "total_revenue", "account_name")
            column_type: One of "dimension", "metric", or "fact"
            data_type: Snowflake data type (VARCHAR, FLOAT, INT, NUMBER, DATE, etc.)
            table_alias: Table alias prefix (e.g., "accounts" for accounts.account_name)
            expression: SQL expression for metrics (e.g., "SUM(net_amount)")
            synonyms: Comma-separated synonyms (e.g., "GL account,ledger account")
            comment: Column description

        Returns:
            JSON confirmation with column counts by type

---

### `add_faux_semantic_relationship`

Add a relationship between tables in the semantic view.

        Relationships define how tables join together. These map to the
        RELATIONSHIPS clause in the CREATE SEMANTIC VIEW DDL.

        Args:
            project_id: The project ID
            from_table: Source table alias (e.g., "gl_entries")
            from_column: Source column name (e.g., "account_code")
            to_table: Target table alias (e.g., "accounts")
            to_column: Target column (optional, defaults to primary key)

        Returns:
            JSON confirmation

---

### `add_faux_semantic_table`

Add a table reference to the semantic view definition.

        Tables are the physical data sources referenced by the semantic view.
        Each table gets an alias used to qualify dimension/metric/fact references.

        Args:
            project_id: The project ID
            alias: Short alias for the table (e.g., "gl_entries", "accounts")
            fully_qualified_name: Full table path (e.g., "FINANCE.GL.FACT_JOURNAL_ENTRIES")
            primary_key: Optional primary key column name

        Returns:
            JSON confirmation with current table count

---

### `add_formula_rule`

Add a rule to an existing formula group.

        Args:
            project_id: Project UUID
            main_hierarchy_id: Hierarchy with the formula
            operation: SUM, SUBTRACT, MULTIPLY, DIVIDE, AVERAGE
            source_hierarchy_id: Hierarchy to include in calculation
            precedence: Order of operations (1, 2, 3...)
            constant_number: Optional constant for multiply/divide

        Returns:
            JSON with updated hierarchy

---

### `add_hierarchy_property`

Add a property to a hierarchy node.

        Properties control how dimensions are built, facts are designed,
        and filters are configured. Properties can be inherited by children.

        AUTO-SYNC: When enabled, automatically syncs to backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID (slug)
            name: Property name (e.g., 'aggregation_type', 'measure_type', 'color')
            value: Property value (JSON string for complex values)
            category: Property category:
                - dimension: Controls dimension building
                - fact: Controls fact/measure design
                - filter: Controls filter behavior
                - display: Controls UI display
                - validation: Data validation rules
                - security: Row-level security
                - custom: User-defined
            level: Specific level this applies to (empty = hierarchy level, number = specific LEVEL_X)
            inherit: Whether children inherit this property ("true"/"false")
            override_allowed: Whether children can override ("true"/"false")
            description: Property description

        Returns:
            JSON with updated hierarchy and property details.

        Examples:
            # Add aggregation type
            add_hierarchy_property(project_id, "REVENUE_1", "aggregation_type", "SUM", "dimension")

            # Add measure type
            add_hierarchy_property(project_id, "NET_INCOME", "measure_type", "derived", "fact")

            # Add display color
            add_hierarchy_property(project_id, "REVENUE_1", "color", "#22c55e", "display")

            # Add custom property
            add_hierarchy_property(project_id, "WELL_1", "regulatory_reporting", "true", "custom")

---

### `add_lineage_node`

Add a node to the lineage graph.

        Nodes represent data objects like tables, views, hierarchies,
        and data marts that participate in data lineage.

        Args:
            graph_name: Name of the lineage graph (creates if not exists)
            node_name: Node name (object name)
            node_type: Type of node (TABLE, VIEW, DYNAMIC_TABLE, HIERARCHY, DATA_MART, DBT_MODEL)
            database: Database name
            schema_name: Schema name
            columns: JSON array of column definitions [{"name": "col1", "data_type": "VARCHAR"}]
            description: Node description
            tags: Comma-separated tags

        Returns:
            Created node details

        Example:
            add_lineage_node(
                graph_name="finance_lineage",
                node_name="DIM_ACCOUNT",
                node_type="TABLE",
                database="ANALYTICS",
                schema_name="PUBLIC",
                columns='[{"name": "ACCOUNT_ID", "data_type": "NUMBER", "is_primary_key": true}]'
            )

---

### `add_mart_join_pattern`

Add a UNION ALL branch definition to a configuration.

        Each join pattern defines how hierarchy metadata joins to the fact table.
        Multiple patterns create UNION ALL branches in DT_3A.

        Args:
            config_name: Name of the configuration
            name: Branch name (e.g., "account", "deduct_product", "royalty")
            join_keys: Comma-separated DT_2 columns for join
            fact_keys: Comma-separated fact table columns for join
            filter: Optional WHERE clause filter (e.g., "ROYALTY_FILTER = 'Y'")
            description: Branch description

        Returns:
            Added pattern details

        Example:
            add_mart_join_pattern(
                config_name="upstream_gross",
                name="deduct_product",
                join_keys="LOS_DEDUCT_CODE_FILTER,LOS_PRODUCT_CODE_FILTER",
                fact_keys="FK_DEDUCT_KEY,FK_PRODUCT_KEY"
            )

---

### `add_semantic_table`

Add a logical table with dimensions and metrics to a semantic model.

        The base_table should be a fully qualified Snowflake table name.
        Dimensions, metrics, and facts define the business semantics.

        Args:
            model_name: Name of the semantic model
            table_name: Logical name for this table
            base_table: Fully qualified table (DATABASE.SCHEMA.TABLE)
            description: Table description
            dimensions: JSON array of dimension definitions
            time_dimensions: JSON array of time dimension definitions
            metrics: JSON array of metric definitions (with aggregations)
            facts: JSON array of fact/measure definitions

        Dimension format:
            [{"name": "region", "description": "Sales region", "expr": "REGION_NAME", "data_type": "VARCHAR"}]

        Metric format:
            [{"name": "total_revenue", "description": "Sum of revenue", "expr": "SUM(REVENUE)", "data_type": "NUMBER"}]

        Returns:
            Added table configuration

        Example:
            add_semantic_table(
                model_name="sales_analytics",
                table_name="sales",
                base_table="ANALYTICS.PUBLIC.SALES_FACT",
                dimensions='[{"name": "region", "expr": "region_name", "description": "Sales region", "data_type": "VARCHAR"}]',
                metrics='[{"name": "revenue", "expr": "SUM(amount)", "description": "Total revenue", "data_type": "NUMBER"}]'
            )

---

### `add_source_mapping`

Add a source mapping to a hierarchy.

        Maps a database column/value to a hierarchy node for data aggregation.

        AUTO-SYNC: When enabled, automatically adds the mapping in the backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Target hierarchy
            source_database: Database name (e.g., "WAREHOUSE")
            source_schema: Schema name (e.g., "FINANCE")
            source_table: Table name (e.g., "DIM_ACCOUNT")
            source_column: Column name (e.g., "ACCOUNT_CODE")
            source_uid: Specific value to match (e.g., "4100-500")
            precedence_group: Grouping for precedence logic (default "1")

        Returns:
            JSON with updated hierarchy (includes auto_sync status)

---

### `analyst_ask`

Ask a natural language question and get SQL + explanation.

        Uses Cortex Analyst to translate the question to SQL based on
        the semantic model's business context.

        Args:
            question: Natural language question
            semantic_model_file: Stage path to semantic model YAML
            connection_id: Optional Snowflake connection for context

        Returns:
            Generated SQL and explanation

        Example:
            analyst_ask(
                question="What was total revenue by region last quarter?",
                semantic_model_file="@ANALYTICS.PUBLIC.MODELS/sales.yaml"
            )

---

### `analyst_ask_and_run`

Ask a question, generate SQL, and execute it.

        Combines natural language understanding with query execution
        to return actual data results.

        Args:
            question: Natural language question
            semantic_model_file: Stage path to semantic model YAML
            connection_id: Snowflake connection ID for execution
            limit: Maximum rows to return (default 100)

        Returns:
            SQL, explanation, and query results

        Example:
            analyst_ask_and_run(
                question="Show top 5 regions by revenue",
                semantic_model_file="@ANALYTICS.PUBLIC.MODELS/sales.yaml",
                connection_id="snowflake-prod",
                limit=5
            )

---

### `analyst_conversation`

Have a multi-turn conversation with Cortex Analyst.

        Maintains conversation context for follow-up questions like
        "now break that down by month" or "filter to just Q4".

        Args:
            question: Natural language question or follow-up
            semantic_model_file: Stage path to semantic model YAML
            conversation_id: Optional ID to continue existing conversation

        Returns:
            SQL, explanation, and conversation context

        Example:
            # First question
            result1 = analyst_conversation(
                question="What was total revenue last year?",
                semantic_model_file="@ANALYTICS.PUBLIC.MODELS/sales.yaml"
            )
            conv_id = result1["conversation_id"]

            # Follow-up
            result2 = analyst_conversation(
                question="Break that down by quarter",
                semantic_model_file="@ANALYTICS.PUBLIC.MODELS/sales.yaml",
                conversation_id=conv_id
            )

---

### `analyze_book_with_researcher`

Analyze a Book using Researcher capabilities.

        This validates source mappings and/or profiles source data using
        the Researcher's database analytics.

        Args:
            book_name: Name of the registered Book
            connection_id: Database connection ID for validation
            analysis_type: Type of analysis:
                - "validate_sources": Validate all source mappings exist
                - "profile_sources": Profile all source columns (counts, samples)
                - "full": Both validation and profiling

        Returns:
            Analysis results including validation errors and profile data

        Example:
            analyze_book_with_researcher(
                book_name="My P&L",
                connection_id="snowflake-prod",
                analysis_type="validate_sources"
            )

---

### `analyze_change_impact`

Analyze impact of a proposed change.

        Identifies all objects affected by changes like column removal,
        column rename, hierarchy modifications, etc.

        Args:
            graph_name: Name of the lineage graph
            node: Node to change
            change_type: Type of change (REMOVE_COLUMN, RENAME_COLUMN, REMOVE_NODE, MODIFY_MAPPING, MODIFY_FORMULA)
            column: Column name (for column changes)
            new_column_name: New column name (for renames)

        Returns:
            Impact analysis with affected objects and severity

        Example:
            analyze_change_impact(
                graph_name="finance_lineage",
                node="DIM_ACCOUNT",
                change_type="REMOVE_COLUMN",
                column="ACCOUNT_CODE"
            )

---

### `analyze_group_filter_precedence`

Analyze GROUP_FILTER_PRECEDENCE patterns in mapping data.

        Detects multi-round filtering patterns:
        - Precedence 1: Primary dimension join
        - Precedence 2: Secondary filter
        - Precedence 3: Tertiary filter

        Args:
            mappings: JSON array of mapping records with GROUP_FILTER_PRECEDENCE

        Returns:
            Analysis with detected patterns and recommended SQL

        Example:
            analyze_group_filter_precedence(
                mappings='[{"FILTER_GROUP_1": "Deducts", "GROUP_FILTER_PRECEDENCE": 1}]'
            )

---

### `analyze_request`

Analyze a user request to understand intent and requirements.

        Uses AI to extract:
        - Primary intent (what the user wants to achieve)
        - Key entities (tables, schemas, files mentioned)
        - Constraints (time, quality, format requirements)
        - Ambiguities (unclear aspects needing clarification)

        Args:
            request: Natural language description of the task

        Returns:
            JSON analysis of the request

---

### `apply_property_template`

Apply a property template to a hierarchy.

        Templates provide pre-configured property sets for common use cases.
        Use list_property_templates to see available templates.

        AUTO-SYNC: When enabled, automatically syncs to backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID
            template_id: Template ID to apply
            merge: If "true", merge with existing properties. If "false", replace.

        Returns:
            JSON with updated hierarchy and applied template info.

        Examples:
            # Apply financial dimension template
            apply_property_template(project_id, "ACCOUNT", "financial_dimension")

            # Apply measure template
            apply_property_template(project_id, "REVENUE", "additive_measure")

            # Apply time dimension (replace existing)
            apply_property_template(project_id, "PERIOD", "time_dimension", "false")

---

### `broadcast_console_message`

Broadcast a message to all connected console clients.

        Sends a message to clients subscribed to the specified channel.
        Useful for sending system notifications, status updates, or
        custom messages to the dashboard.

        Args:
            message: The message text to broadcast
            level: Log level (debug, info, warning, error, success)
            source: Source identifier (e.g., tool name, agent name)
            channel: Channel to broadcast on (console, reasoning, agents, cortex)
            conversation_id: Optional conversation filter
            metadata: Optional additional metadata

        Returns:
            Broadcast result with recipient count

        Example:
            broadcast_console_message(
                message="Data reconciliation complete",
                level="success",
                source="reconciler"
            )

---

### `build_dependency_graph`

Build a dependency graph for visualization.

        Creates a hierarchical view of object dependencies that can be
        exported as Mermaid or DOT diagrams.

        Args:
            graph_name: Name of the lineage graph
            node: Root node name or ID
            direction: "upstream" or "downstream"
            max_depth: Maximum depth to traverse

        Returns:
            Dependency graph structure

        Example:
            build_dependency_graph(
                graph_name="finance_lineage",
                node="DT_3_UPSTREAM_GROSS",
                direction="upstream"
            )

---

### `bulk_set_property`

Set a property on multiple hierarchies at once.

        AUTO-SYNC: When enabled, automatically syncs to backend.

        Args:
            project_id: Project UUID
            hierarchy_ids: JSON array of hierarchy IDs (e.g., '["REVENUE_1", "COGS_1"]')
            name: Property name
            value: Property value
            category: Property category
            inherit: Whether children inherit

        Returns:
            JSON with success count and any errors.

        Example:
            bulk_set_property(
                project_id,
                '["REVENUE_1", "COGS_1", "EXPENSES_1"]',
                "aggregation_type",
                "SUM",
                "dimension"
            )

---

### `cancel_orchestrated_task`

Cancel a pending or running task.

        Args:
            task_id: The task ID to cancel

        Returns:
            JSON with cancellation result

---

### `catalog_create_asset`

Create a new data asset in the catalog.

        Data assets can be databases, schemas, tables, views, hierarchies,
        semantic models, dbt models, or other data objects.

        Args:
            name: Asset name
            asset_type: Type (database, schema, table, view, hierarchy, semantic_model, etc.)
            description: Business description
            database: Database name (for tables/views)
            schema_name: Schema name (for tables/views)
            classification: Data classification (public, internal, confidential, restricted, pii, phi, pci)
            quality_tier: Quality tier (gold, silver, bronze, unknown)
            tags: Comma-separated list of tags
            owner_name: Owner's display name
            owner_email: Owner's email

        Returns:
            Created asset details

        Example:
            catalog_create_asset(
                name="CUSTOMER_DIM",
                asset_type="table",
                database="ANALYTICS",
                schema_name="PUBLIC",
                description="Customer dimension table",
                classification="pii",
                tags="customer,dimension,core"
            )

---

### `catalog_create_term`

Create a business glossary term.

        Glossary terms define business concepts and can be linked to
        data assets and columns.

        Args:
            name: Term name
            definition: Business definition
            domain: Business domain (e.g., "Finance", "Sales")
            category: Term category
            synonyms: Comma-separated synonyms
            examples: Comma-separated examples
            owner_name: Term owner name

        Returns:
            Created term details

        Example:
            catalog_create_term(
                name="Revenue",
                definition="Total income from sales of goods and services",
                domain="Finance",
                synonyms="Sales,Income",
                examples="Product Revenue,Service Revenue"
            )

---

### `catalog_delete_asset`

Delete a data asset from the catalog.

        Args:
            asset_id: Asset ID to delete

        Returns:
            Deletion status

        Example:
            catalog_delete_asset(asset_id="abc-123")

---

### `catalog_get_asset`

Get details of a data asset.

        Can look up by ID or by name (with optional database/schema filters).

        Args:
            asset_id: Asset ID (preferred)
            name: Asset name
            database: Filter by database
            schema_name: Filter by schema

        Returns:
            Asset details including columns, tags, owners, quality metrics

        Example:
            catalog_get_asset(name="CUSTOMER_DIM", database="ANALYTICS")

---

### `catalog_get_stats`

Get data catalog statistics.

        Returns summary of assets, glossary terms, tags, and data quality.

        Returns:
            Catalog statistics

        Example:
            catalog_get_stats()

---

### `catalog_get_term`

Get glossary term details.

        Args:
            term_id: Term ID (preferred)
            name: Term name

        Returns:
            Term details including linked assets

        Example:
            catalog_get_term(name="Revenue")

---

### `catalog_link_term`

Link a glossary term to an asset or column.

        Args:
            term_id: Glossary term ID
            asset_id: Asset ID to link
            column_ref: Column reference (database.schema.table.column)

        Returns:
            Link status

        Example:
            catalog_link_term(
                term_id="term-123",
                column_ref="ANALYTICS.PUBLIC.SALES.REVENUE"
            )

---

### `catalog_list_assets`

List data assets with optional filters.

        Args:
            asset_type: Filter by type (table, view, hierarchy, etc.)
            database: Filter by database
            schema_name: Filter by schema
            tags: Comma-separated required tags
            classification: Filter by classification
            quality_tier: Filter by quality tier
            owner_id: Filter by owner user_id
            limit: Maximum results (default 50)
            offset: Pagination offset

        Returns:
            List of assets matching filters

        Example:
            catalog_list_assets(
                asset_type="table",
                database="ANALYTICS",
                tags="customer,validated"
            )

---

### `catalog_list_terms`

List glossary terms with filters.

        Args:
            domain: Filter by domain
            status: Filter by status (draft, pending_review, approved, deprecated)
            limit: Maximum results
            offset: Pagination offset

        Returns:
            List of glossary terms

        Example:
            catalog_list_terms(domain="Finance", status="approved")

---

### `catalog_manage_tags`

Manage tags in the catalog.

        Actions:
        - "list": List all tags
        - "add": Add a tag to an asset
        - "remove": Remove a tag from an asset
        - "create": Create a new tag definition

        Args:
            action: Action to perform (list, add, remove, create)
            asset_id: Asset ID (for add/remove)
            tag_name: Tag name
            tag_category: Tag category (for create)

        Returns:
            Action result

        Example:
            catalog_manage_tags(action="add", asset_id="abc-123", tag_name="validated")

---

### `catalog_refresh_asset`

Refresh metadata for an existing cataloged asset.

        Re-scans the source to update column info, row counts, etc.

        Args:
            asset_id: Asset ID to refresh

        Returns:
            Updated asset details

        Example:
            catalog_refresh_asset(asset_id="abc-123")

---

### `catalog_scan_connection`

Scan a data connection and catalog discovered assets.

        Automatically discovers databases, schemas, tables, and columns.
        Optionally profiles data and detects PII columns.

        Args:
            connection_id: Snowflake connection ID
            database: Specific database to scan (default: all)
            schema_pattern: Schema name pattern (e.g., "PROD_%")
            table_pattern: Table name pattern (e.g., "DIM_%")
            include_views: Include views in scan
            profile_columns: Collect column statistics (slower)
            detect_pii: Detect PII columns by name patterns

        Returns:
            Scan results with statistics

        Example:
            catalog_scan_connection(
                connection_id="snowflake-prod",
                database="ANALYTICS",
                schema_pattern="PUBLIC",
                detect_pii=True
            )

---

### `catalog_scan_table`

Scan a single table and add to catalog.

        Args:
            connection_id: Snowflake connection ID
            database: Database name
            schema_name: Schema name
            table_name: Table name
            profile: Collect column statistics
            detect_pii: Detect PII columns

        Returns:
            Created/updated asset details

        Example:
            catalog_scan_table(
                connection_id="snowflake-prod",
                database="ANALYTICS",
                schema_name="PUBLIC",
                table_name="CUSTOMER_DIM"
            )

---

### `catalog_search`

Search the data catalog.

        Searches asset names, descriptions, column names, and glossary terms.

        Args:
            query: Search text
            asset_types: Comma-separated asset types to include
            tags: Comma-separated required tags
            databases: Comma-separated database filter
            classification: Filter by classification
            include_glossary: Include glossary terms in results
            limit: Maximum results

        Returns:
            Search results ranked by relevance

        Example:
            catalog_search(
                query="customer revenue",
                asset_types="table,view",
                databases="ANALYTICS"
            )

---

### `catalog_update_asset`

Update a data asset's metadata.

        Args:
            asset_id: Asset ID to update
            description: New description
            classification: New classification
            quality_tier: New quality tier
            add_tags: Comma-separated tags to add
            remove_tags: Comma-separated tags to remove
            add_owner: Owner name to add (format: "Name <email>")

        Returns:
            Updated asset summary

        Example:
            catalog_update_asset(
                asset_id="abc-123",
                description="Updated customer dimension",
                classification="confidential",
                add_tags="validated,production"
            )

---

### `checkout_librarian_to_book`

Convert a Librarian project to a Book for in-memory manipulation.

        This "checks out" a Librarian project, creating a Book instance that can
        be manipulated using Book tools (add nodes, apply formulas, etc.).

        Args:
            project_id: Librarian project ID (UUID) to checkout
            book_name: Optional name for the Book (defaults to project name)

        Returns:
            Result with book info, hierarchy count, and any errors

        Example:
            checkout_librarian_to_book(project_id="abc-123")

---

### `clear_workflow`

Clear all steps from the current workflow.

    Returns:
        Confirmation message.

---

### `compare_book_to_database`

Compare Book hierarchy values with live database data.

        This extracts values from the Book's source mappings and compares
        them against the actual database values to find orphans and mismatches.

        Args:
            book_name: Name of the registered Book
            connection_id: Database connection ID
            hierarchy_filter: Optional hierarchy ID to compare (all if not specified)

        Returns:
            Comparison results showing matches, orphans, and statistics

        Example:
            compare_book_to_database(
                book_name="My P&L",
                connection_id="snowflake-prod"
            )

---

### `compare_database_schemas`

Compare schemas between two tables from different database connections.

        Identifies:
        - Columns present in source but not in target
        - Columns present in target but not in source
        - Columns with different data types
        - Columns with different nullability

        Args:
            source_connection_id: Source connection UUID
            source_database: Source database name
            source_schema: Source schema name
            source_table: Source table name
            target_connection_id: Target connection UUID
            target_database: Target database name
            target_schema: Target schema name
            target_table: Target table name

        Returns:
            JSON with detailed schema comparison results including column differences.

---

### `compare_ddl_content`

Compare generated DDL against baseline DDL.

        Identifies:
        - Overall similarity
        - Column additions/removals/modifications
        - JOIN clause changes
        - WHERE clause changes
        - Breaking changes and warnings

        Args:
            generated_ddl: The generated DDL content
            baseline_ddl: The baseline DDL to compare against
            generated_name: Name of generated file
            baseline_name: Name of baseline file

        Returns:
            Comparison result with differences

        Example:
            compare_ddl_content(
                generated_ddl="CREATE VIEW VW_1 AS SELECT col1 FROM table1",
                baseline_ddl="CREATE VIEW VW_1 AS SELECT col1, col2 FROM table1"
            )

---

### `compare_hashes`

Compare two CSV sources by hashing rows to identify orphans and conflicts.

    Args:
        source_a_path: Path to the first CSV file (source of truth).
        source_b_path: Path to the second CSV file (target).
        key_columns: Comma-separated column names that uniquely identify a row.
        compare_columns: Optional comma-separated columns to check for conflicts. Defaults to all non-key columns.

    Returns:
        JSON statistical summary with orphan and conflict counts (no raw data).

---

### `compare_pipeline_to_baseline`

Compare a generated pipeline against baseline DDL files.

        Compares all 4 pipeline objects (VW_1, DT_2, DT_3A, DT_3)
        against matching files in the baseline directory.

        Args:
            config_name: Name of the mart configuration
            baseline_dir: Directory containing baseline DDL files

        Returns:
            Comparison results for each pipeline object

        Example:
            compare_pipeline_to_baseline(
                config_name="upstream_gross",
                baseline_dir="C:/data/baseline_ddl"
            )

---

### `compare_table_data`

Compare data between two tables at the row level.

        Identifies:
        - Rows in source but not in target (orphans)
        - Rows in target but not in source (orphans)
        - Rows with same key but different values (conflicts)
        - Rows that match exactly

        Args:
            source_connection_id: Source connection UUID
            source_database: Source database name
            source_schema: Source schema name
            source_table: Source table name
            target_connection_id: Target connection UUID
            target_database: Target database name
            target_schema: Target schema name
            target_table: Target table name
            key_columns: Comma-separated list of key columns for matching rows
            compare_columns: Optional comma-separated list of columns to compare (default: all)

        Returns:
            JSON with comparison results including orphans, conflicts, and match statistics.

---

### `configure_auto_sync`

Enable or disable automatic synchronization between MCP and backend.

        When enabled, all write operations (create, update, delete) on projects
        and hierarchies automatically sync to the NestJS backend. This keeps
        the MCP server and Web UI in sync without manual sync_to_backend calls.

        Args:
            enabled: True to enable auto-sync, False to disable

        Returns:
            JSON with new sync configuration status

---

### `configure_cortex_agent`

Configure the Cortex Agent with a Snowflake connection.

        This must be called before using other Cortex tools. The connection_id
        should reference an existing Snowflake connection from list_backend_connections.

        Args:
            connection_id: ID of Snowflake connection (from list_backend_connections)
            cortex_model: Default model for COMPLETE (mistral-large, llama3-70b, etc.)
            max_reasoning_steps: Maximum steps in reasoning loop (1-50)
            temperature: Sampling temperature for COMPLETE (0.0-1.0)
            enable_console: Enable communication console
            console_outputs: Comma-separated outputs (cli, file, database)

        Returns:
            Configuration status

        Example:
            configure_cortex_agent(
                connection_id="snowflake-prod",
                cortex_model="mistral-large"
            )

---

### `configure_git`

Configure git integration settings.

        Sets up git client for a repository with optional GitHub authentication.

        Args:
            repo_path: Path to the git repository
            remote_url: GitHub/GitLab remote URL (e.g., https://github.com/owner/repo.git)
            username: Git username for commits
            email: Git email for commits
            token: GitHub personal access token (for PRs)
            default_branch: Default branch name
            branch_strategy: Branch naming strategy (feature, release, hotfix, deploy)
            auto_commit: Auto-commit generated files
            commit_prefix: Prefix for commit messages

        Returns:
            Configuration status

        Example:
            configure_git(
                repo_path="C:/projects/my-dbt",
                remote_url="https://github.com/myorg/my-dbt.git",
                username="databridge-bot",
                email="bot@example.com",
                token="ghp_xxxx",
                branch_strategy="feature"
            )

---

### `configure_planner`

Configure the PlannerAgent settings.

        Args:
            model: Claude model to use (e.g., "claude-sonnet-4-20250514")
            temperature: Sampling temperature (0.0-1.0, lower = more focused)
            max_steps: Maximum steps allowed in a plan
            enable_parallel: "true" or "false" to enable parallel optimization

        Returns:
            Updated configuration

---

### `configure_project_defaults`

Configure default source information for a project.

        These defaults are used during flexible import when source columns
        are not specified in the input data. Essential for Tier 1 and Tier 2
        imports where source info is not included.

        Args:
            project_id: Project UUID
            source_database: Default database name (e.g., "WAREHOUSE")
            source_schema: Default schema name (e.g., "FINANCE")
            source_table: Default table name (e.g., "DIM_ACCOUNT")
            source_column: Default column name (e.g., "ACCOUNT_CODE")

        Returns:
            JSON with configured defaults and completeness status.

        Example:
            configure_project_defaults(
                project_id="abc-123",
                source_database="WAREHOUSE",
                source_schema="FINANCE",
                source_table="DIM_ACCOUNT",
                source_column="ACCOUNT_CODE"
            )

---

### `convert_sql_format`

Convert SQL from one format to another.

        Supports conversions between:
        - SELECT query ↔ CREATE SEMANTIC VIEW DDL
        - CREATE VIEW ↔ CREATE SEMANTIC VIEW DDL
        - Any format → SELECT query

        Target formats:
        - "semantic_view_ddl": CREATE SEMANTIC VIEW statement
        - "create_view": CREATE VIEW wrapping SEMANTIC_VIEW() call
        - "select_query": Plain SELECT query

        Args:
            sql: Source SQL statement
            target_format: Target format ("semantic_view_ddl", "create_view", "select_query")
            name: Override semantic view name (optional)
            database: Override database (optional)
            schema_name: Override schema (optional)
            target_database: Target database for faux objects (optional)
            target_schema: Target schema for faux objects (optional)

        Returns:
            JSON with converted SQL

        Example:
            convert_sql_format('''
                SELECT region, SUM(amount) as total_sales
                FROM orders GROUP BY region
            ''', "semantic_view_ddl", name="sales_summary", database="ANALYTICS")

---

### `cortex_analyze_data`

AI-powered data analysis on a Snowflake table.

        Uses Cortex to analyze data quality, patterns, or statistics.

        Args:
            table_name: Fully qualified table name (DATABASE.SCHEMA.TABLE)
            analysis_type: Type of analysis (quality, patterns, statistics, anomalies)
            sample_size: Number of rows to sample for analysis
            focus_columns: Comma-separated column names to focus on (optional)

        Returns:
            Analysis results with AI insights

        Example:
            cortex_analyze_data(
                table_name="ANALYTICS.PUBLIC.CUSTOMERS",
                analysis_type="quality",
                sample_size=100
            )

---

### `cortex_clean_data`

AI-powered data cleaning with proposed changes.

        Uses Cortex to analyze and propose data cleaning transformations.

        Args:
            table_name: Fully qualified table name
            column_name: Column to clean
            cleaning_goal: What to clean (e.g., "standardize product names")
            preview_only: If True, only preview changes without applying
            limit: Number of rows to preview

        Returns:
            Proposed cleaning transformations

        Example:
            cortex_clean_data(
                table_name="ANALYTICS.PUBLIC.PRODUCTS",
                column_name="PRODUCT_NAME",
                cleaning_goal="Standardize product names and fix typos",
                preview_only=True
            )

---

### `cortex_complete`

Generate text using Snowflake Cortex COMPLETE() function.

        Uses an LLM to generate text based on the prompt. All processing
        happens within Snowflake - data never leaves the cloud.

        Args:
            prompt: The text prompt for generation
            model: Model to use (default from config: mistral-large)
            temperature: Sampling temperature (0.0-1.0)
            max_tokens: Maximum tokens to generate

        Returns:
            Generated text and query details

        Example:
            cortex_complete(
                prompt="Explain data reconciliation in one sentence",
                model="mistral-large"
            )

---

### `cortex_extract_answer`

Extract answer from context using Snowflake Cortex EXTRACT_ANSWER().

        Finds and extracts the answer to a question from provided context.

        Args:
            context: Text context to search
            question: Question to answer

        Returns:
            Extracted answer

        Example:
            cortex_extract_answer(
                context="The company was founded in 2010 by John Smith.",
                question="When was the company founded?"
            )
            # Returns: {"answer": "2010"}

---

### `cortex_reason`

Run the full reasoning loop for a complex goal.

        Uses the Observe → Plan → Execute → Reflect pattern to break down
        complex tasks into steps and execute them via Cortex functions.

        Args:
            goal: The goal to achieve (natural language)
            context: Optional JSON context (e.g., table names, constraints)

        Returns:
            Complete response with thinking steps

        Example:
            cortex_reason(
                goal="Analyze the data quality in PRODUCTS table and suggest improvements",
                context='{"table": "ANALYTICS.PUBLIC.PRODUCTS"}'
            )

---

### `cortex_sentiment`

Analyze sentiment using Snowflake Cortex SENTIMENT() function.

        Returns a sentiment score from -1 (negative) to 1 (positive).

        Args:
            text: Text to analyze

        Returns:
            Sentiment score and interpretation

        Example:
            cortex_sentiment(text="This product is amazing!")
            # Returns: {"sentiment": 0.85, "interpretation": "positive"}

---

### `cortex_summarize`

Summarize text using Snowflake Cortex SUMMARIZE() function.

        Generates a concise summary of the input text.

        Args:
            text: Text to summarize

        Returns:
            Summary and query details

        Example:
            cortex_summarize(
                text="Long document text here..."
            )

---

### `cortex_translate`

Translate text using Snowflake Cortex TRANSLATE() function.

        Translates text between languages.

        Args:
            text: Text to translate
            from_lang: Source language code (en, es, fr, de, etc.)
            to_lang: Target language code

        Returns:
            Translated text

        Example:
            cortex_translate(
                text="Hello, world!",
                from_lang="en",
                to_lang="es"
            )
            # Returns: {"translation": "¡Hola, mundo!"}

---

### `create_client_profile`

Create a new client knowledge base profile.

        Client profiles help store and retrieve client-specific information
        for consistent handling across sessions.

        Args:
            client_id: Unique identifier for the client (e.g., 'acme', 'client_001').
            client_name: Display name for the client (e.g., 'ACME Corporation').
            industry: Client's industry ('general', 'oil_gas', 'manufacturing', etc.).
            erp_system: Client's ERP system ('SAP', 'Oracle', 'NetSuite', etc.).

        Returns:
            JSON with the created client profile.

---

### `create_data_contract`

Create a data contract with quality expectations.

        Data contracts define the expected schema, quality rules, and SLAs
        for a data asset. They can be exported to YAML for version control.

        Args:
            name: Contract name
            version: Contract version (e.g., "1.0.0")
            owner: Data owner
            team: Responsible team
            database: Target database
            schema_name: Target schema
            table_name: Target table
            columns: JSON array of column definitions
            freshness_hours: Max data age in hours
            completeness_percent: Min completeness percentage
            validation_schedule: Cron schedule for validation

        Returns:
            Created contract details

        Example:
            create_data_contract(
                name="gl_accounts_contract",
                owner="finance-team",
                database="ANALYTICS",
                schema_name="FINANCE",
                table_name="GL_ACCOUNTS",
                freshness_hours=24,
                completeness_percent=99.5,
                validation_schedule="0 6 * * *"
            )

---

### `create_dbt_project`

Create a new dbt project scaffold.

        Generates a complete dbt project structure including:
        - dbt_project.yml configuration
        - profiles.yml template
        - Directory structure (models/, seeds/, tests/, etc.)
        - README and .gitignore
        - Optional CI/CD pipeline

        Args:
            name: Project name (will be converted to lowercase with underscores)
            profile: dbt profile name for database connections
            target_database: Target database name (optional)
            target_schema: Target schema name (optional)
            hierarchy_project_id: Link to DataBridge hierarchy project (optional)
            include_cicd: Whether to include GitHub Actions CI/CD workflow
            output_dir: Directory to write files (optional, for immediate export)

        Returns:
            Project details with generated file list

        Example:
            create_dbt_project(
                name="finance_analytics",
                profile="snowflake_prod",
                target_database="ANALYTICS",
                target_schema="FINANCE",
                hierarchy_project_id="revenue-pl",
                include_cicd=True
            )

---

### `create_faux_project`

Create a new Faux Objects project.

        A Faux Objects project contains:
        1. A Semantic View definition (the source of truth)
        2. One or more Faux Object configurations (views, stored procedures,
           dynamic tables, tasks) that wrap the Semantic View

        Faux Objects make Semantic Views accessible to BI tools (Power BI,
        Tableau, Excel) by generating standard Snowflake objects that BI tools
        already know how to query.

        Args:
            name: Project name (e.g., "P&L Analysis Wrappers")
            description: Optional project description

        Returns:
            JSON with the created project details

---

### `create_filter_group_backend`

Create a filter group via the NestJS backend.

        Filter groups allow you to define reusable filter criteria
        for hierarchy views and reports.

        Args:
            project_id: Project UUID
            group_name: Name for the filter group
            filters: JSON string of filter definitions

        Returns:
            JSON with created filter group details.

---

### `create_formula_group`

Create a formula group for calculated hierarchies.

        Args:
            project_id: Project UUID
            main_hierarchy_id: Hierarchy that stores the calculation result
            group_name: Name for the formula group
            rules: JSON array of formula rules

        Rules format example:
            [
                {"operation": "SUM", "hierarchy_id": "REVENUE_1", "precedence": 1},
                {"operation": "SUBTRACT", "hierarchy_id": "EXPENSES_1", "precedence": 2}
            ]

        Returns:
            JSON with updated hierarchy

---

### `create_hierarchy`

Create a new hierarchy node in a project.

        AUTO-SYNC: When enabled, automatically creates the hierarchy in the backend.

        Args:
            project_id: Target project UUID
            hierarchy_name: Display name for the hierarchy
            parent_id: Optional parent hierarchy ID for nesting
            description: Optional description
            flags: JSON string of hierarchy flags (include_flag, calculation_flag, etc.)

        Returns:
            JSON with created hierarchy details (includes auto_sync status)

        Example flags:
            {"calculation_flag": true, "active_flag": true, "is_leaf_node": false}

---

### `create_hierarchy_project`

Create a new hierarchy project.

        AUTO-SYNC: When enabled, automatically creates the project in the backend.

        Args:
            name: Project name (e.g., "Financial Reporting 2024")
            description: Optional project description

        Returns:
            JSON with project ID and details (includes auto_sync status)

---

### `create_mart_config`

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
            fact_table: Fact table for joins
            target_database: Target database for generated objects
            target_schema: Target schema for generated objects
            description: Configuration description

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

---

### `create_model_from_template`

Create a semantic model from a template.

        Loads a pre-built template and customizes it with your database/schema.
        Templates include standard dimensions, metrics, and relationships.

        Args:
            template_id: Template ID (from list_semantic_templates)
            model_name: Name for the new model
            database: Target database name
            schema_name: Target schema name
            deploy_to_stage: Optional stage path to deploy immediately

        Returns:
            Created model configuration

        Example:
            create_model_from_template(
                template_id="sales_analytics",
                model_name="my_sales_model",
                database="ANALYTICS",
                schema_name="PUBLIC"
            )

---

### `create_orchestrator_workflow`

Create a multi-step workflow for the orchestrator to execute.

        Workflows can include:
        - Sequential steps with dependencies
        - Parallel execution branches
        - Conditional logic
        - Human approval gates
        - Automatic retries

        Step types:
        - task: Execute an orchestrator task
        - parallel: Run multiple tasks in parallel
        - conditional: Branch based on condition
        - approval: Wait for human approval
        - wait: Pause for specified duration

        Args:
            name: Workflow name
            steps: JSON array of workflow step definitions
            description: Optional workflow description

        Returns:
            JSON with workflow ID and validation results

---

### `create_project_from_template`

Create a new hierarchy project pre-populated from a template.

        This creates a complete project with all hierarchies defined in the template,
        saving significant time compared to manual creation.

        Args:
            template_id: The template to use as the base.
            project_name: Name for the new hierarchy project.

        Returns:
            JSON with project details and list of created hierarchies.

---

### `create_semantic_model`

Create a new semantic model configuration for Cortex Analyst.

        A semantic model defines the business context (tables, dimensions, metrics)
        that Cortex Analyst uses to translate natural language to SQL.

        Args:
            name: Unique model name (used as filename)
            description: Human-readable description of the model
            database: Default Snowflake database for tables
            schema_name: Default Snowflake schema for tables

        Returns:
            Created model configuration

        Example:
            create_semantic_model(
                name="sales_analytics",
                description="Sales data for revenue analysis",
                database="ANALYTICS",
                schema_name="PUBLIC"
            )

---

### `create_unified_workflow`

Create a workflow plan spanning Book, Librarian, and Researcher.

        This creates a workflow definition that can be executed to perform
        complex multi-system operations.

        Args:
            workflow_name: Name for the workflow
            description: Description of what the workflow does
            steps: List of workflow steps, each with:
                - system: "book", "librarian", or "researcher"
                - action: Action to perform
                - params: Parameters for the action

        Returns:
            Created workflow definition

        Example:
            create_unified_workflow(
                workflow_name="Import and Deploy",
                description="Import CSV to Book, clean up, deploy to Snowflake",
                steps=[
                    {"system": "book", "action": "create_from_csv", "params": {"path": "data.csv"}},
                    {"system": "book", "action": "apply_formula", "params": {"formula": "SUM"}},
                    {"system": "researcher", "action": "validate_sources", "params": {}},
                    {"system": "librarian", "action": "promote", "params": {}},
                    {"system": "librarian", "action": "push_to_snowflake", "params": {}}
                ]
            )

---

### `define_faux_semantic_view`

Define the Semantic View that this project's faux objects will wrap.

        This sets up the semantic view metadata. After defining the view,
        add tables, columns (dimensions/metrics/facts), and relationships.

        Args:
            project_id: The project ID
            name: Semantic view name (e.g., "pl_analysis")
            database: Database name (e.g., "FINANCE")
            schema_name: Schema name (e.g., "SEMANTIC")
            comment: Optional description of the semantic view
            ai_sql_generation: Optional AI context for Cortex Analyst

        Returns:
            JSON with the updated project

        Example:
            define_faux_semantic_view("abc123", "pl_analysis", "FINANCE", "SEMANTIC",
                "Profit & Loss analysis for all business units")

---

### `delete_faux_project`

Delete a Faux Objects project.

        Args:
            project_id: The project ID to delete

        Returns:
            JSON with deletion status

---

### `delete_hierarchy`

Delete a hierarchy.

        AUTO-SYNC: When enabled, automatically deletes the hierarchy in the backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy to delete

        Returns:
            JSON with deletion status (includes auto_sync status)

---

### `delete_hierarchy_project`

Delete a project and all its hierarchies.

        AUTO-SYNC: When enabled, automatically deletes the project in the backend.

        Args:
            project_id: Project UUID to delete

        Returns:
            JSON with deletion status (includes auto_sync status)

---

### `deploy_semantic_model`

Deploy a semantic model YAML to a Snowflake stage.

        The semantic model must be deployed to a stage that Cortex Analyst
        can access. The stage_path should include the filename.

        Args:
            model_name: Name of the semantic model to deploy
            stage_path: Stage path with filename (e.g., @DB.SCHEMA.STAGE/models/model.yaml)
            connection_id: Optional Snowflake connection for deployment

        Returns:
            Deployment status with local and remote paths

        Example:
            deploy_semantic_model(
                model_name="sales_analytics",
                stage_path="@ANALYTICS.PUBLIC.MODELS/sales_analytics.yaml",
                connection_id="snowflake-prod"
            )

---

### `detect_hierarchy_format`

Detect the format and tier of hierarchy input data.

        Analyzes input content to determine:
        - Input format (CSV, Excel, JSON, text)
        - Complexity tier (tier_1 to tier_4)
        - Parent relationship strategy
        - Recommendations for import

        Tiers:
        - Tier 1: Ultra-simple (2-3 columns: source_value, group_name)
        - Tier 2: Basic (5-7 columns with parent names)
        - Tier 3: Standard (10-12 columns with explicit IDs)
        - Tier 4: Enterprise (28+ columns with LEVEL_X)

        Args:
            content: Input data content (CSV, JSON, or plain text)
            filename: Optional filename to help detect format from extension

        Returns:
            JSON with detected format, tier, columns, parent strategy,
            sample data, and recommendations.

        Example:
            detect_hierarchy_format("source_value,group_name\n4100,Revenue\n5100,COGS")
            -> {"format": "csv", "tier": "tier_1", "columns_found": ["source_value", "group_name"], ...}

---

### `detect_schema_drift`

Compare schemas between two CSV files to detect drift.

    Args:
        source_a_path: Path to first CSV (baseline).
        source_b_path: Path to second CSV (target).

    Returns:
        JSON with schema differences including added, removed, and type-changed columns.

---

### `detect_sql_format`

Detect the format of a SQL statement.

        Analyzes the SQL to determine if it's a CREATE VIEW, SELECT query,
        or CREATE SEMANTIC VIEW DDL statement.

        Args:
            sql: The SQL statement to analyze

        Returns:
            JSON with the detected format and description

        Example:
            detect_sql_format("SELECT * FROM orders GROUP BY region")
            # Returns: {"format": "select_query", "description": "SELECT query with aggregations"}

---

### `diff_book_and_librarian`

Show differences between a Book and a Librarian project.

        This compares the current state of a Book with a Librarian project
        and shows what would change if synced.

        Args:
            book_name: Name of the registered Book
            project_id: Librarian project ID to compare with

        Returns:
            Diff result showing book-only, librarian-only, modified, and identical items

        Example:
            diff_book_and_librarian(book_name="My P&L", project_id="abc-123")

---

### `diff_dicts`

Compare two dictionaries with value-level character diffs.

        For string values, provides character-level diff analysis showing
        exactly what changed. Useful for comparing record fields, configurations,
        or any key-value data structures.

        Args:
            dict_a: First dictionary to compare
            dict_b: Second dictionary to compare
            include_unchanged: Whether to include unchanged keys in differences list

        Returns:
            Dictionary with:
            - dict_a_keys, dict_b_keys: Key counts
            - added_keys: Keys in B but not in A
            - removed_keys: Keys in A but not in B
            - common_keys: Keys in both
            - changed_keys: Common keys with different values
            - unchanged_keys: Common keys with same values
            - differences: Detailed per-key comparison with:
                - key, value_a, value_b, status
                - similarity, opcodes (for string values)
            - overall_similarity: Weighted similarity score

        Example:
            >>> diff_dicts(
            ...     {"name": "John Smith", "age": 30},
            ...     {"name": "Jon Smyth", "age": 30, "city": "NYC"}
            ... )
            {
                "added_keys": ["city"],
                "changed_keys": ["name"],
                "unchanged_keys": ["age"],
                "differences": [
                    {
                        "key": "name",
                        "value_a": "John Smith",
                        "value_b": "Jon Smyth",
                        "status": "changed",
                        "similarity": 0.7273,
                        "opcodes": [...]
                    },
                    ...
                ]
            }

---

### `diff_lists`

Compare two lists and identify added, removed, and common items.

        Computes both Jaccard similarity (set-based) and sequence similarity.
        Useful for comparing column values, categories, or any ordered/unordered lists.

        Args:
            list_a: First list to compare
            list_b: Second list to compare
            max_items: Maximum items to show in added/removed/common lists

        Returns:
            Dictionary with:
            - list_a_count, list_b_count: Sizes of input lists
            - added: Items in B but not in A
            - removed: Items in A but not in B
            - common: Items in both lists
            - added_count, removed_count, common_count: Counts
            - jaccard_similarity: |A ∩ B| / |A ∪ B| (0.0 to 1.0)
            - jaccard_percent: Jaccard as percentage string
            - sequence_similarity: Order-aware similarity

        Example:
            >>> diff_lists(["a", "b", "c"], ["b", "c", "d"])
            {
                "added": ["d"],
                "removed": ["a"],
                "common": ["b", "c"],
                "jaccard_similarity": 0.5,
                "jaccard_percent": "50.0%"
            }

---

### `diff_text`

Compare two text strings with similarity scores and detailed diff analysis.

        Provides character-level comparison using Python's difflib module.
        Useful for comparing names, descriptions, or any text fields where
        you need to understand exactly what changed.

        Args:
            text_a: First text string to compare
            text_b: Second text string to compare
            detail_level: Amount of detail in response
                - "basic": Just similarity score and is_identical flag
                - "standard": Add opcodes and explanation
                - "detailed": Add matching blocks, unified diff, and ndiff
            include_html: Include HTML-formatted diff output

        Returns:
            Dictionary with comparison results including:
            - similarity: Float from 0.0 to 1.0
            - similarity_percent: String like "72.5%"
            - is_identical: Boolean
            - opcodes: List of operations (replace/delete/insert/equal)
            - explanation: Human-readable description of changes
            - matching_blocks: Where the strings match (detailed only)
            - unified_diff: Standard patch format (detailed only)
            - ndiff: Character-level +/-/? markers (detailed only)
            - html: HTML formatted diff (if include_html=True)

        Example:
            >>> diff_text("John Smith", "Jon Smyth")
            {
                "similarity": 0.7273,
                "similarity_percent": "72.7%",
                "is_identical": false,
                "opcodes": [
                    {"operation": "equal", "a_content": "Jo", "b_content": "Jo"},
                    {"operation": "replace", "a_content": "hn", "b_content": "n"},
                    ...
                ],
                "explanation": "Similarity: 72.7%\n  Changed: 'hn' -> 'n'\n  Changed: 'i' -> 'y'"
            }

---

### `discover_hierarchy_pattern`

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

---

### `execute_orchestrator_workflow`

Start executing a workflow.

        Args:
            workflow_id: The workflow ID to execute
            context: JSON object with initial context variables

        Returns:
            JSON with execution ID and initial status

---

### `execute_unified_workflow`

Execute a previously created unified workflow.

        This runs through each step of the workflow, passing results
        between steps as needed.

        Args:
            workflow_name: Name of the workflow to execute
            context_params: Optional parameters to pass to the workflow

        Returns:
            Execution results for each step

        Example:
            execute_unified_workflow(
                workflow_name="Import and Deploy",
                context_params={"book_name": "My P&L", "connection_id": "snowflake-prod"}
            )

---

### `explain_diff`

Generate a human-readable explanation of differences between two texts.

        Designed for non-technical users or for displaying to end users.
        Provides a natural language description of what changed.

        Args:
            text_a: First text (original/before)
            text_b: Second text (modified/after)
            context: Optional context for the comparison (e.g., "account name", "description")

        Returns:
            Dictionary with:
            - similarity: Float similarity score
            - similarity_percent: Percentage string
            - is_identical: Boolean
            - summary: One-line summary
            - explanation: Detailed natural language explanation
            - change_count: Number of distinct changes

        Example:
            >>> explain_diff("John Smith", "Jon Smyth", context="customer name")
            {
                "similarity": 0.7273,
                "is_identical": false,
                "summary": "The customer name values are 72.7% similar with 2 changes",
                "explanation": "Similarity: 72.7%\nChanges:\n  - Changed 'hn' to 'n'\n  - Changed 'i' to 'y'"
            }

---

### `explain_plan`

Generate a human-readable explanation of a workflow plan.

        Converts a JSON plan into a detailed markdown explanation
        that describes each step, dependencies, and reasoning.

        Args:
            plan_json: JSON string of a workflow plan (from plan_workflow)

        Returns:
            Markdown explanation of the plan

---

### `export_data_contract`

Export a data contract to YAML or JSON.

        Exports the contract definition including schema, quality rules,
        and SLAs to a file or returns the content.

        Args:
            contract_name: Name of the contract
            format: Output format (yaml, json)
            output_path: Optional file path to write to

        Returns:
            Exported contract content or file path

        Example:
            export_data_contract(
                contract_name="gl_accounts_contract",
                format="yaml",
                output_path="./contracts/gl_accounts.yml"
            )

---

### `export_dbt_project`

Export a dbt project to directory or ZIP file.

        Writes all generated files to disk.

        Args:
            project_name: Name of the dbt project
            output_dir: Directory to export to (default: ./dbt_export/{project_name})
            as_zip: Whether to create a ZIP archive

        Returns:
            Export details with file paths

        Example:
            export_dbt_project(
                project_name="finance",
                output_dir="./my_dbt_project",
                as_zip=True
            )

---

### `export_faux_scripts`

Export all generated SQL scripts to individual files.

        Creates one .sql file per object plus a deployment_bundle.sql
        containing everything in deployment order.

        Args:
            project_id: The project ID
            output_dir: Directory for output files (default: data/faux_objects/exports/{project_id})

        Returns:
            JSON with file paths for each exported script

---

### `export_hierarchy_csv`

Export all hierarchies to CSV format - exports HIERARCHY structure only.

        NOTE: For a complete export, you need TWO files:
        1. This tool exports: {PROJECT_NAME}_HIERARCHY.CSV (structure + sort orders)
        2. Also use export_mapping_csv for: {PROJECT_NAME}_HIERARCHY_MAPPING.CSV (mappings)

        CSV columns include:
        - HIERARCHY_ID, HIERARCHY_NAME, PARENT_ID, DESCRIPTION
        - LEVEL_1 through LEVEL_10 (hierarchy level values)
        - LEVEL_1_SORT through LEVEL_10_SORT (sort order for each level)
        - INCLUDE_FLAG, EXCLUDE_FLAG, TRANSFORM_FLAG, CALCULATION_FLAG, ACTIVE_FLAG, IS_LEAF_NODE
        - FORMULA_GROUP, SORT_ORDER

        Args:
            project_id: Project UUID

        Returns:
            JSON with CSV content and suggested filename

---

### `export_hierarchy_csv_backend`

Export hierarchy to CSV via the NestJS backend.

        This uses the backend's export functionality which may have
        different formatting than the local export.

        Args:
            project_id: Project UUID

        Returns:
            JSON with CSV content and suggested filename.

---

### `export_hierarchy_simplified`

Export project hierarchies in simplified format.

        Converts hierarchies to a simpler tier format for:
        - Sharing with non-technical users (Tier 1)
        - Easy editing and re-import (Tier 2)
        - Standard format with explicit IDs (Tier 3)

        Args:
            project_id: Project UUID
            target_tier: Target format ("tier_1", "tier_2", "tier_3")
                - tier_1: source_value, group_name (mappings only)
                - tier_2: hierarchy_name, parent_name, source_value, sort_order
                - tier_3: Standard with hierarchy_id, parent_id, flags

        Returns:
            JSON with:
            - format: Target tier
            - csv_content: Exported CSV data
            - row_count: Number of data rows
            - note: Usage guidance

        Example:
            export_hierarchy_simplified(project_id="abc-123", target_tier="tier_2")
            -> CSV with hierarchy_name, parent_name, source_value, sort_order

---

### `export_lineage_diagram`

Export lineage as a diagram.

        Generates Mermaid or DOT (Graphviz) diagram code for visualization.

        Args:
            graph_name: Name of the lineage graph
            node: Root node name or ID
            direction: "upstream" or "downstream"
            format: Output format - "mermaid" or "dot"
            max_depth: Maximum depth to traverse

        Returns:
            Diagram code string

        Example:
            export_lineage_diagram(
                graph_name="finance_lineage",
                node="DT_3_UPSTREAM_GROSS",
                direction="upstream",
                format="mermaid"
            )

---

### `export_mapping_csv`

Export all source mappings to CSV format - exports MAPPING data only.

        NOTE: For a complete export, you need TWO files:
        1. Use export_hierarchy_csv for: {PROJECT_NAME}_HIERARCHY.CSV (structure)
        2. This tool exports: {PROJECT_NAME}_HIERARCHY_MAPPING.CSV (mappings)

        Args:
            project_id: Project UUID

        Returns:
            JSON with CSV content and suggested filename

---

### `export_mart_config`

Export configuration to dbt YAML format.

        Exports the mart configuration as a YAML file that can be used
        with dbt vars or version controlled.

        Args:
            config_name: Name of the configuration
            output_path: Optional output file path

        Returns:
            Exported YAML content or file path

        Example:
            export_mart_config(
                config_name="upstream_gross",
                output_path="./configs/upstream_gross.yml"
            )

---

### `export_project_json`

Export complete project as JSON backup.

        Args:
            project_id: Project UUID

        Returns:
            JSON with full project backup

---

### `extract_text_from_pdf`

Extract text content from a PDF file.

    Args:
        file_path: Path to the PDF file.
        pages: Page numbers to extract ('all', or '1,2,3', or '1-5').

    Returns:
        JSON with extracted text per page.

---

### `find_files`

Search for files across common directories.

    Use this tool when you can't find a file or need to discover available files.
    It searches Downloads, Documents, Desktop, temp folders, and the DataBridge
    data directory.

    Args:
        pattern: Glob pattern to match (default "*.csv"). Examples:
                 - "*.csv" for all CSV files
                 - "*.xlsx" for Excel files
                 - "*" for all files
        search_name: Optional filename substring to filter results (case-insensitive)
        max_results: Maximum number of results to return (default 20)

    Returns:
        JSON with found files, their paths, sizes, and modification times.

    Example:
        find_files(pattern="*.csv", search_name="hierarchy")

---

### `find_similar_strings`

Find strings similar to a target from a list of candidates.

        Uses difflib's get_close_matches with exact similarity scoring.
        Useful for fuzzy lookups, typo correction, or finding related entries.

        Args:
            target: The string to match against
            candidates: List of candidate strings to search
            max_results: Maximum number of matches to return (default: 5)
            min_similarity: Minimum similarity threshold 0.0-1.0 (default: 0.6)

        Returns:
            Dictionary with:
            - target: The search string
            - candidates_searched: Number of candidates searched
            - matches: List of matches sorted by similarity, each with:
                - candidate: The matching string
                - similarity: Float similarity score
                - similarity_percent: Percentage string
                - rank: 1 = best match

        Example:
            >>> find_similar_strings("Revenue", ["Revnue", "Expenses", "Revenue Total", "Rev"])
            {
                "target": "Revenue",
                "matches": [
                    {"candidate": "Revenue Total", "similarity": 0.8571, "rank": 1},
                    {"candidate": "Revnue", "similarity": 0.8571, "rank": 2},
                    {"candidate": "Rev", "similarity": 0.6667, "rank": 3}
                ]
            }

---

### `fuzzy_deduplicate`

Find potential duplicate values within a single column using fuzzy matching.

    Args:
        source_path: Path to the CSV file.
        column: Column name to check for duplicates.
        threshold: Minimum similarity score (0-100). Default 90.
        limit: Maximum duplicate groups to return (max 10).

    Returns:
        JSON with potential duplicate groups.

---

### `fuzzy_match_columns`

Find fuzzy matches between two columns using RapidFuzz.

    Args:
        source_a_path: Path to the first CSV file.
        source_b_path: Path to the second CSV file.
        column_a: Column name in source A to match.
        column_b: Column name in source B to match against.
        threshold: Minimum similarity score (0-100). Default 80.
        limit: Maximum matches to return (max 10).

    Returns:
        JSON with fuzzy match results including similarity scores.

---

### `generate_cicd_pipeline`

Generate CI/CD pipeline configuration.

        Creates automated build and deploy workflows for the dbt project.

        Args:
            project_name: Name of the dbt project
            platform: CI/CD platform (github_actions, gitlab_ci, azure_devops)
            trigger_branches: Comma-separated list of trigger branches
            dbt_version: dbt version to use
            run_tests: Whether to run dbt tests
            run_docs: Whether to generate documentation

        Returns:
            Generated pipeline details

        Example:
            generate_cicd_pipeline(
                project_name="finance",
                platform="github_actions",
                trigger_branches="main,develop"
            )

---

### `generate_dbt_metrics`

Generate metrics.yml from formula groups or manual definitions.

        Creates dbt metrics for business calculations.

        Args:
            project_name: Name of the dbt project
            formula_groups: JSON string of DataBridge formula groups
            metrics: JSON list of metric definitions (alternative)

        Returns:
            Generated metrics details

        Example:
            generate_dbt_metrics(
                project_name="finance",
                metrics='[{"name": "total_revenue", "expression": "SUM(amount)", "type": "derived"}]'
            )

---

### `generate_dbt_model`

Generate a dbt model SQL file.

        Creates staging, intermediate, dimension, or fact models based on type.

        Args:
            project_name: Name of the dbt project
            model_name: Name for the model (prefix added automatically)
            model_type: Type of model (staging, intermediate, dimension, fact)
            source_name: Source name for staging models
            source_table: Source table for staging models
            ref_models: Comma-separated model references for non-staging models
            columns: JSON list of column names to include
            case_mappings: JSON list of CASE statement mappings
            description: Model description

        Returns:
            Generated model details

        Example:
            # Staging model
            generate_dbt_model(
                project_name="finance",
                model_name="gl_accounts",
                model_type="staging",
                source_name="raw",
                source_table="GL_ACCOUNTS"
            )

            # Dimension model
            generate_dbt_model(
                project_name="finance",
                model_name="account_hierarchy",
                model_type="dimension",
                ref_models="stg_gl_accounts"
            )

---

### `generate_dbt_schema`

Generate schema.yml with model documentation and tests.

        Creates schema definitions for all models in the project.

        Args:
            project_name: Name of the dbt project
            models: Optional JSON list of specific models to include

        Returns:
            Generated schema details

        Example:
            generate_dbt_schema(project_name="finance")

---

### `generate_dbt_sources`

Generate sources.yml from hierarchy mappings or manual configuration.

        Creates source definitions for dbt that reference raw tables.

        Args:
            project_name: Name of the dbt project
            source_name: Name for the source (e.g., "raw", "finance")
            mappings: JSON string of DataBridge hierarchy mappings
            tables: JSON list of table definitions (alternative to mappings)
            database: Database name for all tables
            schema_name: Schema name for all tables

        Returns:
            Generated sources details

        Example:
            generate_dbt_sources(
                project_name="finance",
                source_name="raw",
                database="RAW_DB",
                schema_name="FINANCE",
                tables='[{"name": "gl_accounts", "columns": ["account_code", "account_name"]}]'
            )

---

### `generate_dbt_workflow`

Generate a GitHub Actions workflow for dbt CI/CD.

        Creates a complete workflow with lint, build, test, and deploy jobs.

        Args:
            project_name: dbt project name
            dbt_version: dbt version to use
            database_type: Database type (snowflake, postgres, bigquery)
            run_commands: Comma-separated dbt commands (build, test, run, docs)
            environments: Comma-separated environments (dev, prod)
            output_path: Path to write workflow file (optional)

        Returns:
            Generated workflow YAML

        Example:
            generate_dbt_workflow(
                project_name="revenue_mart",
                dbt_version="1.7.0",
                database_type="snowflake",
                run_commands="build,test,docs generate",
                output_path=".github/workflows/dbt-ci.yml"
            )

---

### `generate_deploy_workflow`

Generate a GitHub Actions workflow for DataBridge deployments.

        Creates a workflow to deploy hierarchy DDL scripts to multiple environments.

        Args:
            project_name: Project name
            environments: Comma-separated environments
            output_path: Path to write workflow file (optional)

        Returns:
            Generated workflow YAML

        Example:
            generate_deploy_workflow(
                project_name="upstream_gross",
                environments="dev,staging,prod",
                output_path=".github/workflows/deploy.yml"
            )

---

### `generate_deployment_scripts`

Generate SQL deployment scripts for a hierarchy project via the backend.

        Args:
            project_id: Project UUID
            table_name: Target table name for INSERT statements
            view_name: Target view name for VIEW creation
            include_insert: Whether to include INSERT script
            include_view: Whether to include VIEW script

        Returns:
            JSON with generated SQL scripts ready for deployment.

---

### `generate_expectation_suite`

Generate an expectation suite from hierarchy mappings or configuration.

        Creates a suite of data quality expectations that can be validated against
        source data. Expectations are derived from hierarchy mappings (source_column,
        source_uid patterns) or can be configured manually.

        Args:
            name: Suite name (e.g., "gl_accounts_suite")
            hierarchy_project_id: Source hierarchy project ID (optional)
            mappings: JSON string of hierarchy mappings (optional)
            database: Target database name
            schema_name: Target schema name
            table_name: Target table name
            description: Suite description

        Returns:
            Generated suite details

        Example:
            generate_expectation_suite(
                name="gl_accounts_suite",
                database="ANALYTICS",
                schema_name="FINANCE",
                table_name="GL_ACCOUNTS"
            )

---

### `generate_faux_deployment_bundle`

Generate a complete deployment bundle with all faux objects.

        Creates a single SQL script containing all objects in deployment order,
        with headers and comments. This can be executed directly in Snowflake
        to deploy all faux objects at once.

        Args:
            project_id: The project ID

        Returns:
            JSON with the complete deployment SQL bundle

---

### `generate_faux_scripts`

Generate SQL scripts for all faux objects in a project.

        This generates the complete SQL for every configured faux object,
        plus the CREATE SEMANTIC VIEW DDL. Scripts are returned as a list
        with object name, type, and SQL content.

        Args:
            project_id: The project ID

        Returns:
            JSON with generated scripts for each object

---

### `generate_filter_precedence_sql`

Generate SQL for GROUP_FILTER_PRECEDENCE multi-round filtering.

        Generates DT_2 CTEs and UNION ALL branch definitions
        based on detected filter patterns.

        Args:
            mappings: JSON array of mapping records with GROUP_FILTER_PRECEDENCE

        Returns:
            SQL snippets for multi-round filtering

        Example:
            generate_filter_precedence_sql(
                mappings='[{"FILTER_GROUP_1": "Taxes", "GROUP_FILTER_PRECEDENCE": 2}]'
            )

---

### `generate_hierarchy_scripts`

Generate SQL scripts for hierarchy deployment.

        Args:
            project_id: Project UUID
            script_type: "insert", "view", or "all"
            table_name: Target table name
            view_name: Target view name

        Returns:
            JSON with generated SQL scripts

---

### `generate_mart_dbt_project`

Generate a complete dbt project from mart configuration.

        Creates dbt model files, schema.yml, and project configuration
        from the mart pipeline configuration.

        Args:
            config_name: Name of the mart configuration
            dbt_project_name: Name for the dbt project
            output_dir: Output directory for dbt files

        Returns:
            Generated file paths

        Example:
            generate_mart_dbt_project(
                config_name="upstream_gross",
                dbt_project_name="upstream_gross_marts",
                output_dir="./dbt_projects/upstream_gross"
            )

---

### `generate_mart_object`

Generate a single pipeline object.

        Generate just one layer of the pipeline for inspection or testing.

        Args:
            config_name: Name of the configuration
            layer: Pipeline layer - "VW_1", "DT_2", "DT_3A", or "DT_3"

        Returns:
            Generated DDL for the specified layer

        Example:
            generate_mart_object(
                config_name="upstream_gross",
                layer="VW_1"
            )

---

### `generate_mart_pipeline`

Generate the complete 4-object data mart pipeline.

        Creates:
        - VW_1: Translation View (CASE on ID_SOURCE)
        - DT_2: Granularity Dynamic Table (UNPIVOT, exclusions)
        - DT_3A: Pre-Aggregation Fact (UNION ALL branches)
        - DT_3: Data Mart (formula precedence, surrogates)

        Args:
            config_name: Name of the configuration to use
            output_format: Output format - "ddl" or "summary"
            include_formulas: Whether to include standard LOS formulas

        Returns:
            Generated pipeline objects

        Example:
            generate_mart_pipeline(
                config_name="upstream_gross",
                output_format="ddl"
            )

---

### `generate_mart_workflow`

Generate a GitHub Actions workflow for Mart Factory pipelines.

        Creates a workflow to auto-generate and deploy data mart DDL.

        Args:
            project_name: Project name
            hierarchy_table: Hierarchy table name (e.g., ANALYTICS.PUBLIC.TBL_0_HIERARCHY)
            mapping_table: Mapping table name
            output_path: Path to write workflow file (optional)

        Returns:
            Generated workflow YAML

        Example:
            generate_mart_workflow(
                project_name="upstream_gross",
                hierarchy_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY_",
                mapping_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY_MAPPING",
                output_path=".github/workflows/mart-factory.yml"
            )

---

### `generate_merge_sql_script`

Generate a MERGE SQL script for synchronizing data between two tables.

        Args:
            source_connection_id: Source connection UUID
            source_database: Source database name
            source_schema: Source schema name
            source_table: Source table name
            target_connection_id: Target connection UUID
            target_database: Target database name
            target_schema: Target schema name
            target_table: Target table name
            key_columns: Comma-separated list of key columns for matching rows
            script_type: Type of script to generate: MERGE, INSERT, UPDATE, DELETE

        Returns:
            JSON with generated SQL script ready for execution or review.

---

### `generate_model_from_faux`

Generate a semantic model from a Faux Objects project.

        Converts Faux Objects semantic view definitions into a
        Cortex Analyst semantic model.

        Args:
            faux_project_id: Faux Objects project ID
            model_name: Optional name for the model

        Returns:
            Generated model configuration

        Example:
            generate_model_from_faux(
                faux_project_id="abc-123",
                model_name="faux_semantic"
            )

---

### `generate_model_from_hierarchy`

Auto-generate a semantic model from a DataBridge hierarchy project.

        Maps hierarchy levels to dimensions, source mappings to base tables,
        and formula groups to metrics.

        Args:
            project_id: DataBridge hierarchy project ID
            model_name: Optional name for the model (defaults to project name)
            deploy_to_stage: Optional stage path to deploy immediately

        Returns:
            Generated model configuration

        Example:
            generate_model_from_hierarchy(
                project_id="revenue-pl",
                model_name="revenue_semantic",
                deploy_to_stage="@ANALYTICS.PUBLIC.MODELS/revenue.yaml"
            )

---

### `generate_model_from_schema`

Auto-generate a semantic model from Snowflake schema metadata.

        Scans tables and intelligently maps column types:
        - VARCHAR/TEXT -> Dimensions
        - DATE/TIMESTAMP -> Time Dimensions
        - NUMBER/FLOAT -> Facts (potential metrics)
        - Columns ending in _ID -> Join keys

        Also auto-detects relationships based on naming conventions.

        Args:
            connection_id: Snowflake connection ID
            database: Database name to scan
            schema_name: Schema name to scan
            tables: Optional comma-separated table names (defaults to all)
            model_name: Optional model name (defaults to schema name)
            include_sample_values: Fetch sample values for dimensions
            deploy_to_stage: Optional stage path to deploy immediately

        Returns:
            Generated model summary

        Example:
            generate_model_from_schema(
                connection_id="snowflake-prod",
                database="ANALYTICS",
                schema_name="PUBLIC",
                tables="SALES_FACT,DIM_CUSTOMER,DIM_PRODUCT",
                model_name="sales_model"
            )

---

### `generate_patch`

Generate a patch in unified, context, or ndiff format.

        Creates standard patch output that can be used with patch tools
        or for displaying changes in a familiar format.

        Args:
            text_a: Original text (before)
            text_b: Modified text (after)
            format: Patch format
                - "unified": Standard unified diff (default)
                - "context": Context diff format
                - "ndiff": Character-level with +/-/? markers
            from_label: Label for the original file/text
            to_label: Label for the modified file/text
            context_lines: Number of context lines (unified/context only)

        Returns:
            Dictionary with:
            - format: The format used
            - from_label, to_label: Labels used
            - patch: The patch content
            - line_count: Number of lines in patch
            - has_changes: Boolean indicating if there are differences

        Example:
            >>> generate_patch("line1\nline2", "line1\nline2 modified", format="unified")
            {
                "format": "unified",
                "patch": "--- original\n+++ modified\n@@ -1,2 +1,2 @@\n line1\n-line2\n+line2 modified",
                "has_changes": true
            }

---

### `generate_semantic_view_ddl`

Generate the CREATE SEMANTIC VIEW DDL from the project definition.

        This generates just the semantic view DDL (not the faux object wrappers).
        Useful for reviewing or deploying the semantic view independently.

        Args:
            project_id: The project ID

        Returns:
            JSON with the semantic view DDL

---

### `get_agent_details`

Get details of a registered agent.

        Args:
            agent_id: The agent ID to look up

        Returns:
            JSON with agent details, capabilities, and health status

---

### `get_agent_messages`

Get messages sent to an agent.

        Args:
            agent_id: The agent ID to get messages for
            limit: Maximum messages to return (default: 50)

        Returns:
            JSON with message list

---

### `get_application_documentation`

Read the content of a specific application documentation file.

        Use this to read user guides, API documentation, or other app docs
        to help answer user questions about the application.

        Args:
            doc_name: Name of the documentation file (e.g., 'USER_GUIDE.md', 'README.md').

        Returns:
            The full content of the documentation file, or error if not found.

---

### `get_audit_log`

Retrieve recent entries from the audit trail.

    Args:
        limit: Maximum entries to return (max 10).

    Returns:
        JSON with recent audit entries.

---

### `get_backend_connection`

Get detailed information about a specific database connection.

        Args:
            connection_id: Connection UUID

        Returns:
            JSON with connection details including configuration and metadata.

---

### `get_backend_hierarchy_tree`

Get hierarchy tree from the NestJS backend.

        Args:
            backend_project_id: Backend project ID

        Returns:
            JSON tree structure from the backend

---

### `get_backend_table_statistics`

Get profiling statistics for a table from the backend.

        Returns detailed statistics including:
        - Row count
        - Column statistics (min, max, distinct count, null count)
        - Data distribution information

        Args:
            connection_id: Connection UUID
            database: Database name
            schema: Schema name
            table: Table name

        Returns:
            JSON with comprehensive table statistics.

---

### `get_client_knowledge`

Get client-specific knowledge including COA patterns, prompts, and notes.

        Use this to understand a client's specific requirements and configurations.

        Args:
            client_id: The unique identifier of the client.

        Returns:
            JSON with full client knowledge base including custom prompts and mappings.

---

### `get_column_distinct_values`

Get distinct values from a specific column.

        Useful for understanding data distribution and selecting values for hierarchy mappings.

        Args:
            connection_id: Connection UUID
            database: Database name
            schema: Schema name
            table: Table name
            column: Column name to get values from
            limit: Maximum number of distinct values to return (default: 100)

        Returns:
            JSON array of distinct values from the column.

---

### `get_column_lineage`

Get lineage for a specific column.

        Shows what feeds into a column (upstream) or what a column feeds (downstream).

        Args:
            graph_name: Name of the lineage graph
            node: Node name or ID
            column: Column name
            direction: "upstream" (what feeds this column) or "downstream" (what this column feeds)

        Returns:
            Column lineage relationships

        Example:
            get_column_lineage(
                graph_name="finance_lineage",
                node="DT_3_UPSTREAM_GROSS",
                column="GROSS_AMOUNT",
                direction="upstream"
            )

---

### `get_conflict_details`

Retrieve details of conflicting records (same key, different values).

    Args:
        source_a_path: Path to the first CSV file.
        source_b_path: Path to the second CSV file.
        key_columns: Comma-separated column names that uniquely identify a row.
        compare_columns: Optional columns to compare. Defaults to all non-key columns.
        limit: Maximum conflicts to return (max 10).

    Returns:
        JSON with conflict details showing both versions side-by-side.

---

### `get_connection_columns`

Get column details for a table.

        Args:
            connection_id: Connection UUID
            database: Database name
            schema: Schema name
            table: Table name

        Returns:
            JSON array of columns with name, data type, nullable, and other metadata.

---

### `get_connection_databases`

List all databases available in a connection.

        Args:
            connection_id: Connection UUID

        Returns:
            JSON array of database names available in the connection.

---

### `get_connection_schemas`

List all schemas in a database.

        Args:
            connection_id: Connection UUID
            database: Database name

        Returns:
            JSON array of schema names in the specified database.

---

### `get_connection_tables`

List all tables in a schema.

        Args:
            connection_id: Connection UUID
            database: Database name
            schema: Schema name

        Returns:
            JSON array of table names in the specified schema.

---

### `get_console_connections`

List active WebSocket connections to the console server.

        Returns information about each connected client including:
        - Connection ID
        - Client IP address
        - Connection time
        - Subscribed channels
        - Message count
        - Last activity time

        Returns:
            List of connection info

        Example:
            get_console_connections()

---

### `get_console_server_status`

Get the current status of the console server.

        Returns detailed information about:
        - Server running state
        - Host and port configuration
        - Active connection count
        - Broadcaster backend (memory/redis)
        - Channel subscription counts
        - Message history size

        Returns:
            Server status information

        Example:
            get_console_server_status()

---

### `get_cortex_agent_status`

Get the current status of the Cortex Agent.

        Returns connection status, configuration, and statistics.

        Returns:
            Agent status including:
            - is_configured: Whether agent is configured
            - config: Current configuration
            - context_stats: Conversation statistics
            - console_status: Console output status

---

### `get_cortex_console_log`

Get recent console log entries.

        The console captures all agent communication for observability.

        Args:
            limit: Maximum entries to return
            conversation_id: Filter by conversation ID
            message_type: Filter by type (request, response, thinking, plan, error)

        Returns:
            List of console log entries

        Example:
            get_cortex_console_log(limit=20, message_type="thinking")

---

### `get_cortex_conversation`

Get full conversation with all thinking steps.

        Retrieves the complete conversation history including observations,
        plans, executions, and reflections.

        Args:
            conversation_id: The conversation ID

        Returns:
            Full conversation with thinking steps

        Example:
            get_cortex_conversation(conversation_id="abc-123-...")

---

### `get_dashboard_stats`

Get dashboard statistics from the NestJS backend.

        Returns:
            JSON with statistics including project count, hierarchy count,
            deployment stats, and activity summaries.

---

### `get_data_comparison_summary`

Get a statistical summary of data comparison between two tables.

        Faster than full comparison, returns only counts without row details.

        Args:
            source_connection_id: Source connection UUID
            source_database: Source database name
            source_schema: Source schema name
            source_table: Source table name
            target_connection_id: Target connection UUID
            target_database: Target database name
            target_schema: Target schema name
            target_table: Target table name
            key_columns: Comma-separated list of key columns for matching rows

        Returns:
            JSON with summary statistics: total rows, matches, orphans, conflicts.

---

### `get_deployment_history`

Get deployment history for a project.

        Args:
            project_id: Project UUID
            limit: Maximum number of entries to return (default: 50)

        Returns:
            JSON array of deployment history entries with timestamps and status.

---

### `get_downstream_impact`

Get all objects that would be impacted by changes to a node.

        Shows the "blast radius" of potential changes to a data object.

        Args:
            graph_name: Name of the lineage graph
            node: Node name or ID
            max_depth: Maximum depth to traverse

        Returns:
            All downstream objects that depend on this node

        Example:
            get_downstream_impact(
                graph_name="finance_lineage",
                node="TBL_0_GROSS_LOS_REPORT_HIERARCHY_"
            )

---

### `get_faux_project`

Get full details of a Faux Objects project.

        Returns the complete project including the semantic view definition,
        all faux object configurations, and column details.

        Args:
            project_id: The project ID

        Returns:
            JSON with full project details

---

### `get_hierarchy`

Get a specific hierarchy by ID.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID (slug)

        Returns:
            JSON with hierarchy details

---

### `get_hierarchy_project`

Get detailed information about a specific project.

        Args:
            project_id: Project UUID

        Returns:
            JSON with project details

---

### `get_hierarchy_properties`

Get properties for a hierarchy, optionally including inherited properties.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID (slug) or UUID
            category: Filter by category (empty = all categories)
            include_inherited: Include properties inherited from ancestors ("true"/"false")

        Returns:
            JSON with:
            - own_properties: Properties defined on this hierarchy
            - inherited_properties: Properties from ancestors (if include_inherited)
            - effective_properties: Final resolved properties
            - dimension_props, fact_props, filter_props, display_props: Type-specific props

        Property Categories:
            - dimension: aggregation_type, drill_enabled, sort_behavior, etc.
            - fact: measure_type, time_balance, format_string, etc.
            - filter: filter_behavior, default_value, cascading_parent_id, etc.
            - display: color, icon, tooltip, visible, etc.
            - custom: user-defined properties

---

### `get_hierarchy_tree`

Get the complete hierarchy tree for a project.

        Args:
            project_id: Project UUID

        Returns:
            JSON tree structure with all hierarchies and their children

---

### `get_id_source_alias_report`

Get a report of all ID_SOURCE aliases and mappings.

        Returns:
            Report with canonical mappings, aliases, and auto-detected corrections

        Example:
            get_id_source_alias_report()

---

### `get_inherited_mappings`

Get all mappings for a hierarchy including those inherited from children.

        Child mappings propagate UP to parent levels (not the other way).
        This allows parent nodes to aggregate all mappings from their descendants.

        Args:
            project_id: Project UUID
            hierarchy_uuid: The UUID (not hierarchy_id) of the hierarchy

        Returns:
            JSON with:
            - own_mappings: Mappings directly on this hierarchy
            - inherited_mappings: Mappings from all child hierarchies
            - by_precedence: All mappings grouped by precedence_group
            - child_counts: Mapping counts per immediate child
            - total_count: Total number of mappings

---

### `get_llm_validation_prompt`

Get a formatted prompt for LLM validation of recommendations.

        This tool generates recommendations and formats them as a structured
        prompt that you (the LLM) can use to validate and refine the
        suggestions based on the user's specific needs.

        Use this when you want to:
        1. Review DataBridge's automated recommendations
        2. Apply your knowledge to refine suggestions
        3. Explain the recommendations to the user
        4. Suggest modifications based on context

        Args:
            file_path: Path to CSV file to analyze
            content: Raw CSV content (alternative to file_path)
            user_intent: What the user wants to accomplish
            client_id: Client ID for knowledge base lookup
            industry: Known industry override

        Returns:
            Formatted markdown prompt with recommendations for LLM review

        Example:
            get_llm_validation_prompt(
                file_path="C:/data/chart_of_accounts.csv",
                user_intent="Create a standard P&L structure for manufacturing"
            )

---

### `get_mapping_summary`

Get mapping summary for entire project with inheritance info.

        Shows each hierarchy's own mappings and total mappings (including
        those inherited from children). Use this to understand the complete
        mapping coverage across your hierarchy tree.

        Args:
            project_id: Project UUID

        Returns:
            JSON with mapping summary for all hierarchies

---

### `get_mappings_by_precedence`

Get mappings filtered by precedence group.

        Precedence groups segregate mappings into separate logical groupings.
        Each unique precedence value represents a separate mapping context.

        Args:
            project_id: Project UUID
            hierarchy_uuid: The UUID of the hierarchy
            precedence_group: Optional - filter to specific precedence group

        Returns:
            JSON with mappings organized by precedence group

---

### `get_orchestrator_health`

Get the health status of the orchestrator.

        Returns:
            JSON with orchestrator health, agent counts, and task statistics

---

### `get_orphan_details`

Retrieve details of orphan records (records in one source but not the other).

    Args:
        source_a_path: Path to the first CSV file.
        source_b_path: Path to the second CSV file.
        key_columns: Comma-separated column names that uniquely identify a row.
        orphan_source: Which orphans to return: 'a', 'b', or 'both'.
        limit: Maximum orphans to return per source (max 10).

    Returns:
        JSON with orphan record details (limited to context sensitivity rules).

---

### `get_planner_status`

Get the current status of the PlannerAgent.

        Returns:
            JSON status including state, available agents, and history count

---

### `get_planning_history`

Get the history of recent workflow plans.

        Args:
            limit: Maximum number of plans to return (default 10)

        Returns:
            JSON list of recent plans

---

### `get_project_defaults`

Get configured source defaults for a project.

        Returns the default source information that will be used
        for Tier 1 and Tier 2 imports when source columns are not
        specified in the input data.

        Args:
            project_id: Project UUID

        Returns:
            JSON with defaults and completeness status.

---

### `get_properties_summary`

Get a summary of all properties used across a project.

        Returns:
            JSON with:
            - total_hierarchies: Total hierarchy count
            - hierarchies_with_properties: Count with properties
            - total_properties: Total property count
            - by_category: Property counts by category
            - by_name: Property usage by name with unique values

---

### `get_property_template`

Get detailed information about a property template.

        Args:
            template_id: Template ID (e.g., "financial_dimension", "additive_measure")

        Returns:
            JSON with full template details including all properties.

---

### `get_recent_activities`

Get recent activities from the backend dashboard.

        Args:
            limit: Maximum number of activities to return (default: 10)

        Returns:
            JSON array of recent activity entries with timestamps and details.

---

### `get_recommendation_context`

Get the full context available for recommendations.

        This tool shows what DataBridge knows that can be used for
        recommendations, including:
        - Available skills and their capabilities
        - Available templates for the industry
        - Client knowledge base (if client_id provided)

        Useful for understanding what recommendations are possible
        before importing data.

        Args:
            client_id: Client ID to show knowledge base
            industry: Industry to filter templates

        Returns:
            JSON with available context for recommendations

        Example:
            get_recommendation_context(industry="oil_gas")

---

### `get_schema_comparison_result`

Get the result of a previously executed schema comparison job.

        Args:
            job_id: Comparison job UUID

        Returns:
            JSON with full comparison results including column mappings and differences.

---

### `get_skill_details`

Get detailed information about a specific skill including capabilities.

        Use this to understand what a skill provides and when to use it.

        Args:
            skill_id: The unique identifier of the skill.

        Returns:
            JSON with skill details including capabilities, industries, and file references.

---

### `get_skill_prompt`

Get the system prompt for a skill to adopt that expertise.

        Use this prompt to configure the AI to act as a specialist in the given domain.

        Args:
            skill_id: The unique identifier of the skill.

        Returns:
            The full system prompt content for the skill, or error if not found.

---

### `get_smart_recommendations`

Get smart recommendations for importing a CSV file.

        This tool analyzes your CSV data and provides context-aware recommendations
        by combining:
        - Data profiling (column analysis, pattern detection)
        - Skill selection (domain expertise like FP&A, Manufacturing, Oil & Gas)
        - Template matching (industry-specific hierarchies)
        - Knowledge base lookups (client-specific patterns and preferences)

        The recommendations help you:
        1. Choose the right import tier (1-4 based on complexity)
        2. Select appropriate domain expertise (skills)
        3. Find matching templates for your industry
        4. Apply known patterns from previous work

        Args:
            file_path: Path to CSV file to analyze
            content: Raw CSV content (alternative to file_path)
            user_intent: What you want to accomplish (e.g., "Build a P&L hierarchy for oil & gas")
            client_id: Client ID for knowledge base lookup (optional)
            industry: Known industry override (e.g., "oil_gas", "manufacturing", "saas")
            target_database: Target database for deployment hints
            target_schema: Target schema for deployment hints
            target_table: Target table for deployment hints

        Returns:
            JSON containing:
            - data_profile: Analyzed data structure and patterns
            - import_tier: Recommended tier (1-4) with reasoning
            - skills: Top 3 skill recommendations with scores
            - templates: Top 3 template recommendations with scores
            - knowledge: Knowledge base matches (if client_id provided)
            - summary: Human-readable summary of recommendations

        Example:
            get_smart_recommendations(
                file_path="C:/data/gl_accounts.csv",
                user_intent="Build a P&L hierarchy for upstream oil and gas",
                industry="oil_gas"
            )

---

### `get_table_lineage`

Get lineage for a table/object.

        Shows all upstream sources and downstream consumers of a data object.

        Args:
            graph_name: Name of the lineage graph
            node: Node name or ID
            direction: "upstream", "downstream", or "both"
            max_depth: Maximum depth to traverse

        Returns:
            Table lineage with upstream and downstream objects

        Example:
            get_table_lineage(
                graph_name="finance_lineage",
                node="DT_3_UPSTREAM_GROSS",
                direction="both"
            )

---

### `get_task_status`

Get the current status of an orchestrated task.

        Returns progress percentage, assigned agent, checkpoints,
        and partial results if available.

        Args:
            task_id: The task ID to check

        Returns:
            JSON with task status, progress (0-100), and details

---

### `get_template_details`

Get full details of a template including hierarchy structure.

        Use this to examine a template's structure before creating a project from it.

        Args:
            template_id: The unique identifier of the template.

        Returns:
            JSON with complete template details including all hierarchy nodes and their relationships.

---

### `get_template_recommendations`

Get AI recommendations for which template to use based on context.

        Use this when a user describes their needs to suggest the best template.

        Args:
            industry: The user's industry (e.g., 'oil_gas', 'manufacturing', 'retail').
            statement_type: The type of statement needed ('pl', 'p&l', 'balance_sheet', 'cash_flow').

        Returns:
            JSON with ranked template recommendations and reasoning.

---

### `get_unified_context`

Get the current unified context across all systems.

        This shows all registered Books, active Librarian project,
        database connections, and recent operations.

        Returns:
            Current context state across Book, Librarian, and Researcher

        Example:
            get_unified_context()

---

### `get_upstream_dependencies`

Get all upstream dependencies of a node.

        Shows all source tables and objects that feed into a data object.

        Args:
            graph_name: Name of the lineage graph
            node: Node name or ID
            max_depth: Maximum depth to traverse

        Returns:
            All upstream objects that this node depends on

        Example:
            get_upstream_dependencies(
                graph_name="finance_lineage",
                node="DT_3_UPSTREAM_GROSS"
            )

---

### `get_user_guide_section`

Get a specific section from the user guide or the full guide.

        Use this to help users understand features and how to use the application.

        Args:
            section: Section to retrieve (e.g., 'Templates', 'Skills', 'Connections').
                    Leave empty to get the full guide.

        Returns:
            The requested section content or full user guide.

---

### `get_validation_results`

Get validation results for a suite.

        Returns historical validation results including status,
        timing, and statistics.

        Args:
            suite_name: Name of the suite
            limit: Maximum number of results to return

        Returns:
            List of validation results

        Example:
            get_validation_results(suite_name="gl_accounts_suite", limit=5)

---

### `get_workflow`

Retrieve the current workflow recipe.

    Returns:
        JSON with all workflow steps.

---

### `get_workflow_definition`

Convert a plan to a workflow definition for the Orchestrator.

        Transforms the AI-generated plan into a format compatible
        with the Orchestrator.create_workflow() method.

        Args:
            plan_json: JSON string of a workflow plan (from plan_workflow)

        Returns:
            JSON workflow definition for the Orchestrator

---

### `get_workflow_execution_status`

Get the status of a workflow execution.

        Args:
            execution_id: The execution ID to check

        Returns:
            JSON with execution status, current step, and results

---

### `get_working_directory`

Get the current working directory and DataBridge data directory paths.

    Use this to understand where DataBridge is looking for files and where
    to place files for easy access.

    Returns:
        JSON with working directory, data directory, and available files.

---

### `git_commit`

Commit changes to the repository.

        Stages and commits files with the specified message.

        Args:
            message: Commit message
            files: Comma-separated list of files to commit (or None for staged)
            all_files: Commit all changes (git add -A)
            repo_path: Repository path (uses configured path if not specified)

        Returns:
            Commit result with SHA

        Example:
            git_commit(
                message="Add dbt models for revenue hierarchy",
                files="models/staging/stg_revenue.sql,models/marts/fct_revenue.sql"
            )

---

### `git_create_branch`

Create a new git branch.

        Creates a branch with optional naming strategy prefix.

        Args:
            branch_name: Branch name (will be prefixed with strategy if configured)
            checkout: Switch to the new branch
            from_branch: Base branch to create from
            repo_path: Repository path (uses configured path if not specified)

        Returns:
            Branch creation result

        Example:
            git_create_branch(
                branch_name="add-revenue-hierarchy",
                from_branch="main"
            )

---

### `git_push`

Push commits to remote repository.

        Pushes the current or specified branch to the remote.

        Args:
            remote: Remote name (default: origin)
            branch: Branch to push (current if not specified)
            set_upstream: Set upstream tracking (-u flag)
            repo_path: Repository path (uses configured path if not specified)

        Returns:
            Push result

        Example:
            git_push(remote="origin", set_upstream=True)

---

### `git_status`

Get git repository status.

        Shows current branch, staged/modified/untracked files, and sync status.

        Args:
            repo_path: Repository path (uses configured path if not specified)

        Returns:
            Repository status

        Example:
            git_status(repo_path="C:/projects/my-dbt")

---

### `github_create_pr`

Create a GitHub pull request.

        Creates a PR from head branch to base branch with optional reviewers and labels.

        Args:
            title: PR title
            body: PR description (supports markdown)
            head_branch: Source branch with changes
            base_branch: Target branch (default: main)
            draft: Create as draft PR
            reviewers: Comma-separated list of reviewer usernames
            labels: Comma-separated list of labels

        Returns:
            PR creation result with URL

        Example:
            github_create_pr(
                title="Add revenue hierarchy models",
                body="## Summary\nAdds dbt models for revenue...",
                head_branch="feature/add-revenue-hierarchy",
                base_branch="main",
                reviewers="john,jane",
                labels="dbt,hierarchy"
            )

---

### `github_get_pr_status`

Get status of a pull request.

        Shows PR details, check status, and mergeability.

        Args:
            pr_number: Pull request number

        Returns:
            PR status with checks

        Example:
            github_get_pr_status(pr_number=42)

---

### `github_list_prs`

List pull requests.

        Lists PRs with optional filtering by state and branches.

        Args:
            state: PR state (open, closed, all)
            head_branch: Filter by source branch
            base_branch: Filter by target branch

        Returns:
            List of pull requests

        Example:
            github_list_prs(state="open", base_branch="main")

---

### `github_merge_pr`

Merge a pull request.

        Merges the PR using the specified method.

        Args:
            pr_number: Pull request number
            merge_method: Merge method (merge, squash, rebase)
            commit_message: Custom commit message for squash/merge

        Returns:
            Merge result

        Example:
            github_merge_pr(pr_number=42, merge_method="squash")

---

### `import_flexible_hierarchy`

Import hierarchies from flexible format with auto-detection.

        Supports four tiers of input complexity:
        - Tier 1: Ultra-simple (source_value, group_name)
        - Tier 2: Basic (hierarchy_name, parent_name, source_value, sort_order)
        - Tier 3: Standard (explicit IDs, full source info)
        - Tier 4: Enterprise (LEVEL_1-10, all flags, formulas)

        Auto-infers missing fields based on tier and project defaults.

        AUTO-SYNC: When enabled, created hierarchies sync to backend.

        Args:
            project_id: Target project UUID
            content: Input data (CSV, JSON, or text)
            format_type: Format hint ("auto", "csv", "json", "excel", "text")
            source_defaults: JSON string of source defaults (overrides project defaults)
            tier_hint: Tier hint ("auto", "tier_1", "tier_2", "tier_3", "tier_4")

        Returns:
            JSON with import results including:
            - detected_format, detected_tier
            - hierarchies_created, mappings_created
            - created_hierarchies (list with IDs and names)
            - inferred_fields (what was auto-generated)
            - errors (if any)

        Example:
            # Tier 1 import
            import_flexible_hierarchy(
                project_id="abc-123",
                content="source_value,group_name\n4100,Revenue\n4200,Revenue\n5100,COGS",
                source_defaults='{"database":"WAREHOUSE","schema":"FINANCE","table":"DIM_ACCOUNT","column":"ACCOUNT_CODE"}'
            )

            # Tier 2 import
            import_flexible_hierarchy(
                project_id="abc-123",
                content="hierarchy_name,parent_name,source_value\nRevenue,,4%\nProduct Rev,Revenue,41%"
            )

---

### `import_hierarchy_csv`

Import hierarchies from CSV - Step 1 of 2 for full hierarchy import.

        IMPORTANT - BEFORE CALLING THIS TOOL:
        1. Always ask the user: "Is this an older/legacy version CSV format?"
        2. Hierarchy imports require TWO CSV files:
           - Hierarchy structure CSV (filename usually ends with _HIERARCHY.CSV)
           - Mapping CSV (filename usually ends with HIERARCHY_MAPPING.CSV)
        3. Import the hierarchy CSV FIRST, then import the mapping CSV.
        4. Sort orders come from LEVEL_X_SORT columns in the HIERARCHY CSV (not mapping CSV)

        Args:
            project_id: Target project UUID
            csv_content: CSV content as string (the _HIERARCHY.CSV file)
            is_legacy_format: Set to True if user confirms this is an older version CSV

        Expected CSV columns:
            - HIERARCHY_ID, HIERARCHY_NAME, PARENT_ID, DESCRIPTION
            - LEVEL_1 through LEVEL_10 (hierarchy level values)
            - LEVEL_1_SORT through LEVEL_10_SORT (sort order for each level)
            - INCLUDE_FLAG, EXCLUDE_FLAG, TRANSFORM_FLAG, CALCULATION_FLAG, ACTIVE_FLAG, IS_LEAF_NODE
            - FORMULA_GROUP, SORT_ORDER

        Returns:
            JSON with import statistics (imported, skipped, errors)

---

### `import_hierarchy_csv_backend`

Import hierarchy from CSV via the NestJS backend.

        This uses the backend's import functionality which handles
        validation and database insertion.

        Args:
            project_id: Target project UUID
            csv_content: CSV content as string

        Returns:
            JSON with import statistics (imported, skipped, errors).

---

### `import_mapping_csv`

Import source mappings from CSV - Step 2 of 2 for full hierarchy import.

        IMPORTANT - BEFORE CALLING THIS TOOL:
        1. Ensure hierarchies have been imported first using import_hierarchy_csv
        2. The HIERARCHY_ID values in this CSV must match existing hierarchies
        3. Mapping CSV filename usually ends with HIERARCHY_MAPPING.CSV
        4. NOTE: Sort orders come from HIERARCHY CSV (LEVEL_X_SORT columns), NOT this mapping CSV

        Args:
            project_id: Target project UUID
            csv_content: CSV content as string (the HIERARCHY_MAPPING.CSV file)

        Expected CSV columns:
            HIERARCHY_ID, MAPPING_INDEX, SOURCE_DATABASE, SOURCE_SCHEMA,
            SOURCE_TABLE, SOURCE_COLUMN, SOURCE_UID, PRECEDENCE_GROUP,
            INCLUDE_FLAG, EXCLUDE_FLAG, TRANSFORM_FLAG, ACTIVE_FLAG

        Returns:
            JSON with import statistics (imported, skipped, errors)

---

### `list_agent_conversations`

List conversations an agent is participating in.

        Args:
            agent_id: The agent ID
            active_only: Only return active conversations (default: True)

        Returns:
            JSON with conversation list

---

### `list_application_documentation`

List all available application documentation files.

        Use this to discover what documentation is available for the DataBridge AI
        UI application including user guides, API docs, and setup instructions.

        Returns:
            JSON with list of documentation files and their descriptions.

---

### `list_available_agents`

List all agents available for workflow planning.

        Returns information about each agent including:
        - Name and type
        - Description
        - Available capabilities
        - Input/output schemas

        Returns:
            JSON list of available agents

---

### `list_available_skills`

List all AI expertise skills available for financial analysis.

        Skills provide specialized knowledge and prompts for different domains
        like general financial analysis or oil & gas FP&A.

        Returns:
            JSON with list of available skills including capabilities and target industries.

---

### `list_backend_connections`

List all database connections from the NestJS backend.

        Returns:
            JSON array of connections with their configuration details.
            Each connection includes: id, name, type, host, port, database, status.

---

### `list_backend_projects`

List all projects from the NestJS backend (Web App).

        Returns:
            JSON array of projects from the backend database

---

### `list_client_profiles`

List all client knowledge base profiles.

        Client profiles store client-specific information like COA patterns,
        custom prompts, and known GL mappings.

        Returns:
            JSON with list of client profiles including industry and ERP system.

---

### `list_expectation_suites`

List all available expectation suites.

        Returns a list of all configured suites with their metadata
        including name, description, expectations count, and target table.

        Returns:
            List of suite summaries

        Example:
            list_expectation_suites()

---

### `list_faux_projects`

List all Faux Objects projects.

        Returns a summary of each project including the semantic view name
        and number of configured faux objects.

        Returns:
            JSON array of project summaries

---

### `list_filter_groups_backend`

List all filter groups for a project via the backend.

        Args:
            project_id: Project UUID

        Returns:
            JSON array of filter groups with their configurations.

---

### `list_financial_templates`

List available financial statement templates.

        Use this tool to recommend templates when users want to build hierarchies.
        Templates provide pre-defined structures for common financial statements.

        Args:
            category: Filter by category ('income_statement', 'balance_sheet', 'cash_flow', 'custom').
                     Leave empty to show all.
            industry: Filter by industry (e.g., 'oil_gas', 'manufacturing', 'general').
                     Leave empty to show all.

        Returns:
            JSON with list of available templates including name, category, industry, and description.

---

### `list_formula_groups`

List all hierarchies with formula groups in a project.

        Args:
            project_id: Project UUID

        Returns:
            JSON array of hierarchies with formulas

---

### `list_hierarchies`

List all hierarchies in a project.

        Args:
            project_id: Project UUID

        Returns:
            JSON array of hierarchies

---

### `list_hierarchy_projects`

List all hierarchy projects with summary statistics.

        Returns:
            JSON array of projects with hierarchy counts

---

### `list_lineage_graphs`

List all lineage graphs.

        Returns:
            List of graph summaries

        Example:
            list_lineage_graphs()

---

### `list_mart_configs`

List all configured data mart configurations.

        Returns:
            List of configuration summaries

        Example:
            list_mart_configs()

---

### `list_orchestrator_tasks`

List tasks in the orchestrator queue.

        Args:
            status: Filter by status (pending, queued, in_progress, completed, failed)
            task_type: Filter by task type
            limit: Maximum number of tasks to return (default: 20)

        Returns:
            JSON with task list and counts by status

---

### `list_orchestrator_workflows`

List all defined workflows.

        Returns:
            JSON with workflow list

---

### `list_property_templates`

List available property templates.

        Property templates provide pre-configured property sets for common
        use cases like financial dimensions, time dimensions, measures, etc.

        Returns:
            JSON array of templates with:
            - id: Template ID
            - name: Template name
            - description: Description
            - category: Primary category (dimension, fact, filter)
            - tags: Tags for searching

        Available Templates:
            - financial_dimension: Standard financial reporting dimensions
            - time_dimension: Time/date dimensions with period handling
            - additive_measure: Standard summable measures
            - balance_measure: Semi-additive balance measures
            - ratio_measure: Non-additive ratios/percentages
            - currency_measure: Monetary measures
            - cascading_filter: Dependent filters
            - required_filter: Required single-select filters
            - oil_gas_dimension: Oil & gas operational hierarchies
            - volume_measure: Volume measures with units

---

### `list_registered_agents`

List agents registered with the orchestrator.

        Args:
            agent_type: Filter by agent type
            capability: Filter by capability (tool name)
            healthy_only: Only return healthy agents

        Returns:
            JSON with agent list and health statistics

---

### `list_schema_comparisons`

List all schema comparison jobs.

        Args:
            limit: Maximum number of jobs to return (default: 50)

        Returns:
            JSON array of comparison job summaries with status and timestamps.

---

### `list_semantic_models`

List all configured semantic models.

        Returns all models with their table counts and metadata.

        Returns:
            List of semantic models with summaries

        Example:
            list_semantic_models()

---

### `list_semantic_templates`

List available semantic model templates.

        Templates provide pre-built semantic models for common use cases
        like sales analytics, financial reporting, and industry-specific models.

        Args:
            domain: Optional filter by domain (sales, finance, operations, marketing)
            industry: Optional filter by industry (general, retail, oil_gas, etc.)

        Returns:
            List of available templates with metadata

        Example:
            list_semantic_templates(domain="finance")
            list_semantic_templates(industry="oil_gas")

---

### `load_csv`

Load a CSV file and return a preview with schema information.

    Args:
        file_path: Path to the CSV file.
        preview_rows: Number of rows to preview (max 10).

    Returns:
        JSON with schema info and sample data.

---

### `load_json`

Load a JSON file (array or object) and return a preview.

    Args:
        file_path: Path to the JSON file.
        preview_rows: Number of rows to preview (max 10).

    Returns:
        JSON with schema info and sample data.

---

### `merge_sources`

Merge two CSV sources on key columns.

    Args:
        source_a_path: Path to the first CSV file.
        source_b_path: Path to the second CSV file.
        key_columns: Comma-separated column names to join on.
        merge_type: Type of merge ('inner', 'left', 'right', 'outer').
        output_path: Optional path to save merged file.

    Returns:
        JSON with merge statistics and preview.

---

### `normalize_id_source_values`

Normalize ID_SOURCE values in mapping data.

        Corrects known typos like:
        - BILLING_CATEGRY_CODE → BILLING_CATEGORY_CODE
        - BILLING_CATEGORY_TYPE → BILLING_CATEGORY_TYPE_CODE

        Args:
            mappings: JSON array of mapping records
            auto_detect: Whether to use fuzzy matching for unknown values
            id_source_key: Key for ID_SOURCE field

        Returns:
            Normalized mappings and correction details

        Example:
            normalize_id_source_values(
                mappings='[{"ID_SOURCE": "BILLING_CATEGRY_CODE", "ID": "4100"}]'
            )

---

### `ocr_image`

Extract text from an image using OCR (Tesseract).

    Args:
        file_path: Path to the image file (PNG, JPG, etc.).
        language: Tesseract language code (default 'eng').

    Returns:
        JSON with extracted text.

---

### `optimize_plan`

Optimize a workflow plan for better performance.

        Analyzes step dependencies and identifies opportunities
        for parallel execution. Adds parallel_group hints to steps
        that can run concurrently.

        Args:
            plan_json: JSON string of a workflow plan (from plan_workflow)

        Returns:
            JSON optimized plan with parallelism hints

---

### `parse_table_from_text`

Attempt to parse tabular data from extracted text.

    Args:
        text: Raw text containing tabular data.
        delimiter: Column delimiter ('auto', 'tab', 'space', 'pipe', or custom).

    Returns:
        JSON with parsed table data.

---

### `plan_workflow`

Create a workflow plan from a natural language request using AI.

        The PlannerAgent uses Claude to analyze the request and generate
        an optimal sequence of steps using available agents.

        Args:
            request: Natural language description of what you want to accomplish.
                     Examples:
                     - "Extract hierarchies from SQL CASE statements and deploy to Snowflake"
                     - "Scan the FINANCE schema and design a star schema for reporting"
                     - "Reconcile data between the staging and production tables"
            context: Optional JSON context with additional information like:
                     {"schema": "FINANCE", "database": "WAREHOUSE", "constraints": [...]}

        Returns:
            JSON workflow plan with steps, agents, and execution order

---

### `preview_import`

Preview hierarchy import without creating anything.

        Shows what hierarchies and mappings would be created from the input
        without actually persisting them. Use this to verify data before
        committing to import.

        Args:
            content: Input data (CSV, JSON, or text)
            format_type: Format hint ("auto", "csv", "json", "excel", "text")
            source_defaults: JSON string of source defaults:
                {"database": "X", "schema": "Y", "table": "Z", "column": "W"}
            limit: Maximum rows to preview (default 10)

        Returns:
            JSON with detected format/tier, preview of hierarchies,
            inferred fields, and source defaults status.

        Example:
            preview_import(
                content="source_value,group_name\n4100,Revenue",
                source_defaults='{"database":"WAREHOUSE","schema":"FINANCE"}'
            )

---

### `profile_book_sources`

Profile data for all source mappings in a Book.

        This analyzes each source column to get row counts, distinct values,
        null counts, and sample values.

        Args:
            book_name: Name of the registered Book
            connection_id: Database connection ID

        Returns:
            Profile data for each source mapping

        Example:
            profile_book_sources(book_name="My P&L", connection_id="snowflake-prod")

---

### `profile_data`

Analyze data structure and quality. Identifies table type and anomalies.

    Args:
        source_path: Path to CSV file to profile.

    Returns:
        JSON with profiling statistics including structure type, cardinality, and data quality metrics.

---

### `promote_book_to_librarian`

Create or update a Librarian project from a Book.

        This "promotes" a Book to Librarian, making it available in the web UI
        and for deployment to databases.

        Args:
            book_name: Name of the registered Book to promote
            project_name: Name for the Librarian project (defaults to book name)
            project_description: Description for the project
            existing_project_id: If provided, updates existing project instead of creating new

        Returns:
            Result with project_id, created/updated counts, and any errors

        Example:
            promote_book_to_librarian(book_name="My P&L", project_name="P&L Hierarchy")

---

### `publish_orchestrator_event`

Publish an event to the orchestrator Event Bus.

        This allows notifying other agents and services of changes.

        Common channels:
        - hierarchy.updated: Hierarchy was modified
        - hierarchy.deployed: Hierarchy was deployed
        - task.completed: A task finished
        - sync.required: Synchronization needed

        Args:
            channel: Event channel name
            payload: JSON object with event data

        Returns:
            JSON with publication result

---

### `push_hierarchy_to_snowflake`

Deploy a hierarchy project to Snowflake.

        Args:
            project_id: Project UUID to deploy
            connection_id: Snowflake connection UUID
            target_database: Target database name
            target_schema: Target schema name
            target_table: Target table name

        Returns:
            JSON with deployment result including row counts and any errors.

---

### `query_database`

Execute a SQL query and return results.

    Args:
        connection_string: SQLAlchemy connection string (e.g., 'sqlite:///data.db').
        query: SQL SELECT query to execute.
        preview_rows: Maximum rows to return (max 10).

    Returns:
        JSON with query results and metadata.

---

### `register_agent`

Register an external agent with the orchestrator.

        Used by Excel plugins, Power BI connectors, and external AI agents
        to announce their presence and capabilities.

        Agent types:
        - mcp_native: Direct MCP tool access (Claude Code, etc.)
        - llm_agent: Claude/GPT with tool calling
        - specialized: Domain-specific agents (FP&A, DBA)
        - excel_plugin: Excel Add-in client
        - power_bi: Power BI connector
        - external: Third-party integrations

        Args:
            agent_id: Unique identifier for the agent
            name: Human-readable agent name
            agent_type: Type of agent
            capabilities: JSON array of capability objects [{tool, proficiency, constraints}]
            max_concurrent_tasks: Maximum concurrent tasks (default: 5)
            callback_url: Webhook URL for receiving messages

        Returns:
            JSON with registration confirmation and agent details

---

### `register_custom_agent`

Register a custom agent for workflow planning.

        Adds a new agent to the planner's registry so it can be
        included in generated workflow plans.

        Args:
            name: Unique agent name (e.g., "my_custom_agent")
            agent_type: Agent class name (e.g., "CustomProcessor")
            description: What the agent does
            capabilities: JSON array of capability names
            input_schema: JSON schema for agent inputs
            output_schema: JSON schema for agent outputs

        Returns:
            Confirmation message

---

### `remove_faux_object`

Remove a faux object from the project.

        Args:
            project_id: The project ID
            object_name: Name of the faux object to remove

        Returns:
            JSON confirmation

---

### `remove_hierarchy_property`

Remove a property from a hierarchy.

        AUTO-SYNC: When enabled, automatically syncs to backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID
            name: Property name to remove
            level: Level of the property (empty = hierarchy level)

        Returns:
            JSON with status.

---

### `remove_source_mapping`

Remove a source mapping by index.

        Args:
            project_id: Project UUID
            hierarchy_id: Target hierarchy
            mapping_index: Index of mapping to remove

        Returns:
            JSON with updated hierarchy

---

### `run_validation`

Run an expectation suite against data.

        Validates data against all expectations in a suite and returns
        detailed results including pass/fail status, unexpected values,
        and statistics.

        Args:
            suite_name: Name of the suite to run
            data: JSON array of row data (for in-memory validation)
            connection_id: Database connection ID (for database validation)

        Returns:
            Validation results with pass/fail details

        Example:
            # In-memory validation
            run_validation(
                suite_name="gl_accounts_suite",
                data='[{"ACCOUNT_CODE": "4100", "ACCOUNT_NAME": "Revenue"}]'
            )

            # Database validation (requires connection)
            run_validation(
                suite_name="gl_accounts_suite",
                connection_id="snowflake-prod"
            )

---

### `save_project_as_template`

Save an existing hierarchy project as a reusable template.

        Use this after building a hierarchy structure to make it reusable
        for future projects or other clients.

        Args:
            project_id: The project to convert to a template.
            template_name: Name for the new template.
            category: Template category ('income_statement', 'balance_sheet', 'cash_flow', 'custom').
            description: Description of what this template is for.
            industry: Target industry ('general', 'oil_gas', 'manufacturing', etc.).

        Returns:
            JSON with the created template details.

---

### `save_workflow_step`

Save a reconciliation step to the workflow recipe.

    Args:
        step_name: Descriptive name for this step.
        step_type: Type of operation (e.g., 'compare_hashes', 'fuzzy_match', 'transform').
        parameters: JSON string of parameters used for this step.

    Returns:
        Confirmation with updated workflow summary.

---

### `search_hierarchies_backend`

Search hierarchies within a project via the backend.

        Args:
            project_id: Project UUID
            query: Search query string

        Returns:
            JSON array of matching hierarchies with relevance scores.

---

### `send_agent_message`

Send a message to another agent via the AI-Link-Orchestrator.

        Use this for agent-to-agent communication including task handoffs,
        queries, status updates, and data sharing.

        Message types:
        - task_handoff: Pass task to another agent with context
        - query: Ask another agent for information
        - response: Reply to a query
        - status_update: Notify of progress
        - error: Escalate an error
        - approval_request: Request human approval
        - data_share: Share intermediate results

        Args:
            to_agent: Target agent ID or '*' for broadcast
            message_type: Type of message
            payload: JSON object with message content
            conversation_id: Existing conversation ID to continue (optional)
            requires_response: Whether to wait for response
            response_timeout: Response timeout in milliseconds (default: 30000)

        Returns:
            JSON with message ID, delivery status, and response if requested

---

### `set_dimension_properties`

Set dimension properties for a hierarchy.

        Dimension properties control how the hierarchy behaves as a dimension
        in reports and analytics.

        AUTO-SYNC: When enabled, automatically syncs to backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID
            props: JSON string of dimension properties:
                {
                    "aggregation_type": "SUM",      // SUM, AVG, COUNT, MIN, MAX, NONE
                    "display_format": null,         // Format string
                    "sort_behavior": "alpha",       // alpha, numeric, custom, natural
                    "drill_enabled": true,          // Allow drill-down
                    "drill_path": null,             // Custom drill path hierarchy IDs
                    "grouping_enabled": true,       // Allow grouping in reports
                    "totals_enabled": true,         // Show totals
                    "hierarchy_type": "standard",   // standard, ragged, parent-child, time
                    "all_member_name": "All",       // Name for 'All' member
                    "default_member": null          // Default member ID
                }

        Returns:
            JSON with updated hierarchy.

        Example:
            set_dimension_properties(project_id, "ACCOUNT", '{"aggregation_type": "SUM", "drill_enabled": true}')

---

### `set_display_properties`

Set display properties for a hierarchy.

        Display properties control how the hierarchy appears in the UI.

        AUTO-SYNC: When enabled, automatically syncs to backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID
            props: JSON string of display properties:
                {
                    "color": "#22c55e",             // Display color
                    "background_color": null,       // Background color
                    "icon": null,                   // Icon name or emoji
                    "tooltip": null,                // Hover tooltip
                    "visible": true,                // Visible in UI
                    "collapsed_by_default": false,  // Start collapsed
                    "highlight_condition": null,    // Condition for highlighting
                    "custom_css_class": null,       // Custom CSS class
                    "display_order": null           // Override display order
                }

        Returns:
            JSON with updated hierarchy.

        Examples:
            set_display_properties(project_id, "REVENUE", '{"color": "#22c55e", "icon": "dollar"}')
            set_display_properties(project_id, "EXPENSES", '{"color": "#ef4444", "collapsed_by_default": true}')

---

### `set_fact_properties`

Set fact/measure properties for a hierarchy.

        Fact properties control how the hierarchy behaves as a measure
        in reports and analytics.

        AUTO-SYNC: When enabled, automatically syncs to backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID
            props: JSON string of fact properties:
                {
                    "measure_type": "additive",     // additive, semi_additive, non_additive, derived
                    "aggregation_type": "SUM",      // SUM, AVG, COUNT, etc.
                    "time_balance": null,           // flow, first, last, average (for semi-additive)
                    "format_string": "#,##0.00",    // Number format
                    "decimal_places": 2,            // Decimal places
                    "currency_code": "USD",         // Currency code
                    "unit_of_measure": null,        // Unit (bbl, mcf, units)
                    "null_handling": "zero",        // zero, null, exclude
                    "negative_format": "minus",     // minus, parens, red
                    "calculation_formula": null,    // Formula for derived measures
                    "base_measure_ids": null        // IDs of measures used in calculation
                }

        Returns:
            JSON with updated hierarchy.

        Examples:
            # Additive measure (revenue, expenses)
            set_fact_properties(project_id, "REVENUE", '{"measure_type": "additive", "aggregation_type": "SUM"}')

            # Semi-additive balance (uses last value for time)
            set_fact_properties(project_id, "BALANCE", '{"measure_type": "semi_additive", "time_balance": "last"}')

            # Derived ratio
            set_fact_properties(project_id, "MARGIN_PCT", '{"measure_type": "non_additive", "format_string": "0.00%"}')

---

### `set_filter_properties`

Set filter properties for a hierarchy.

        Filter properties control how the hierarchy behaves as a filter
        in reports and dashboards.

        AUTO-SYNC: When enabled, automatically syncs to backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy ID
            props: JSON string of filter properties:
                {
                    "filter_behavior": "multi",     // single, multi, range, cascading, search, hierarchy
                    "default_value": null,          // Default filter value
                    "default_to_all": true,         // Default to all values
                    "allowed_values": null,         // Restrict to these values
                    "excluded_values": null,        // Exclude these values
                    "cascading_parent_id": null,    // Parent filter hierarchy ID
                    "required": false,              // Selection required
                    "visible": true,                // Show in filter panel
                    "search_enabled": true,         // Enable search
                    "show_all_option": true,        // Show 'All' option
                    "max_selections": null          // Max selections for multi-select
                }

        Returns:
            JSON with updated hierarchy.

        Examples:
            # Multi-select with search
            set_filter_properties(project_id, "ACCOUNT", '{"filter_behavior": "multi", "search_enabled": true}')

            # Cascading filter (depends on parent)
            set_filter_properties(project_id, "WELL", '{"filter_behavior": "cascading", "cascading_parent_id": "FIELD_1"}')

            # Required single-select
            set_filter_properties(project_id, "PERIOD", '{"filter_behavior": "single", "required": true}')

---

### `smart_import_csv`

Smart CSV import with automatic recommendations.

        This is an intelligent wrapper around the flexible hierarchy import
        that automatically:
        1. Profiles the CSV data
        2. Gets recommendations (skill, template, tier)
        3. Configures the import based on detected patterns
        4. Imports the hierarchy
        5. Suggests next steps

        Use this for a streamlined import experience.

        Args:
            file_path: Path to CSV file to import
            project_name: Name for the new project (auto-generated if empty)
            user_intent: What you want to accomplish
            client_id: Client ID for knowledge base patterns
            industry: Industry override (detected if empty)
            use_recommendations: "true" to apply recommendations, "false" to just analyze

        Returns:
            JSON with import results and recommendations

        Example:
            smart_import_csv(
                file_path="C:/data/los_hierarchy.csv",
                project_name="Q4 LOS Hierarchy",
                user_intent="Build Lease Operating Statement hierarchy",
                industry="oil_gas"
            )

---

### `stage_file`

Copy a file to the DataBridge data directory for easy access.

    Use this when you find a file with find_files() but it's in an inconvenient
    location. This copies it to the DataBridge data directory where all tools
    can easily access it.

    Args:
        source_path: Full path to the source file
        new_name: Optional new filename (keeps original name if not provided)

    Returns:
        JSON with the new file path and confirmation.

    Example:
        stage_file("/Users/john/Downloads/my_data.csv")
        stage_file("/tmp/upload123.csv", new_name="quarterly_report.csv")

---

### `start_console_server`

Start the WebSocket console server.

        Opens a web-based dashboard for real-time monitoring of:
        - Console log entries from all MCP tools
        - Reasoning loop steps (OBSERVE → PLAN → EXECUTE → REFLECT)
        - Agent activity and inter-agent messages
        - Cortex AI queries and responses

        Args:
            port: Port number (default: 8080)
            host: Host address (default: 0.0.0.0 for all interfaces)
            redis_url: Optional Redis URL for multi-instance broadcasting

        Returns:
            Server status with URL

        Example:
            start_console_server(port=8080)
            # Open http://localhost:8080 in browser

---

### `stop_console_server`

Stop the WebSocket console server.

        Gracefully shuts down the server and disconnects all clients.

        Returns:
            Success status

        Example:
            stop_console_server()

---

### `submit_orchestrated_task`

Submit a task to the AI Orchestrator for managed execution.

        The orchestrator will:
        1. Queue the task based on priority (critical > high > normal > low)
        2. Wait for dependencies to complete
        3. Assign to an appropriate agent
        4. Track progress and handle failures

        Task types:
        - hierarchy_build: Build or update hierarchy structures
        - data_reconciliation: Compare and reconcile datasets
        - sql_analysis: Analyze SQL for hierarchy extraction
        - mapping_suggestion: Suggest source mappings
        - report_generation: Generate reports
        - deployment: Deploy hierarchies to databases
        - agent_handoff: Hand off work to another agent
        - workflow_step: Execute as part of a workflow
        - custom: Custom task type

        Args:
            task_type: Type of task to execute
            payload: JSON string with task-specific parameters
            priority: Task priority (low, normal, high, critical)
            dependencies: JSON array of task IDs that must complete first
            callback_url: Webhook URL for completion notification

        Returns:
            JSON with task ID, status, and queue position

---

### `suggest_agents`

Suggest which agents could handle a specific request.

        Analyzes the request and returns a ranked list of agents
        with relevance scores and matched capabilities.

        Args:
            request: Natural language description of the task

        Returns:
            JSON list of agent suggestions with relevance scores

---

### `suggest_enrichment_after_hierarchy`

Suggest enrichment options after hierarchy import.

        Call this after importing a hierarchy to get recommendations for
        enriching the mapping file with additional detail columns from
        reference data (like Chart of Accounts).

        This helps complete the workflow:
        1. Import CSV → Hierarchy
        2. Suggest enrichment (this tool)
        3. Configure enrichment sources
        4. Enrich mapping file

        Args:
            project_id: The hierarchy project ID
            file_path: Original CSV file path (for context)
            user_intent: What the user wants to accomplish

        Returns:
            JSON with enrichment suggestions and next steps

        Example:
            suggest_enrichment_after_hierarchy(
                project_id="my-project",
                file_path="C:/data/gl_hierarchy.csv",
                user_intent="Add account names to the mapping export"
            )

---

### `suggest_mart_config`

Get AI-recommended configuration for a hierarchy.

        Analyzes the hierarchy and mapping tables to generate a complete
        mart configuration recommendation.

        Args:
            hierarchy_table: Fully qualified hierarchy table name
            mapping_table: Fully qualified mapping table name
            project_name: Optional project name (auto-generated if not provided)
            connection_id: Snowflake connection for queries

        Returns:
            Recommended configuration

        Example:
            suggest_mart_config(
                hierarchy_table="ANALYTICS.PUBLIC.TBL_0_NET_LOS_REPORT_HIERARCHY",
                mapping_table="ANALYTICS.PUBLIC.TBL_0_NET_LOS_REPORT_HIERARCHY_MAPPING"
            )

---

### `sync_backend_health`

Check if the NestJS backend is reachable and auto-sync status.

        Returns:
            JSON with connection status, backend URL, and auto-sync configuration

---

### `sync_book_and_librarian`

Synchronize a Book with a Librarian project.

        This performs a sync operation, detecting differences and applying
        changes based on the specified direction and conflict resolution.

        Args:
            book_name: Name of the registered Book
            project_id: Librarian project ID to sync with
            direction: Sync direction - "to_librarian", "from_librarian", or "bidirectional"
            conflict_resolution: How to resolve conflicts - "book_wins" or "librarian_wins"

        Returns:
            Sync result with pushed/pulled counts and any errors

        Example:
            sync_book_and_librarian(
                book_name="My P&L",
                project_id="abc-123",
                direction="bidirectional",
                conflict_resolution="book_wins"
            )

---

### `sync_from_backend`

Pull a project and its hierarchies from the NestJS backend to local MCP storage.

        This syncs Web App (MySQL database) -> MCP local storage.

        Args:
            backend_project_id: The backend project ID to sync from
            local_project_id: Optional local project ID (creates new if empty)

        Returns:
            JSON with sync statistics

---

### `sync_to_backend`

Push a local project and its hierarchies to the NestJS backend.

        This syncs MCP local storage -> Web App (MySQL database).

        Args:
            local_project_id: The local MCP project ID
            backend_project_id: Optional backend project ID (creates new if empty)

        Returns:
            JSON with sync statistics

---

### `test_backend_connection`

Test a database connection's health and connectivity.

        Args:
            connection_id: Connection UUID to test

        Returns:
            JSON with test results including success status, latency, and any error messages.

---

### `track_column_lineage`

Add column-level lineage relationship.

        Tracks how source column(s) transform into a target column.

        Args:
            graph_name: Name of the lineage graph
            source_node: Source node name or ID
            source_columns: Comma-separated source column names
            target_node: Target node name or ID
            target_column: Target column name
            transformation_type: Type of transformation (DIRECT, AGGREGATION, CALCULATION, FILTER, JOIN, CASE)
            transformation_expression: Optional expression used

        Returns:
            Created lineage details

        Example:
            track_column_lineage(
                graph_name="finance_lineage",
                source_node="DIM_ACCOUNT",
                source_columns="ACCOUNT_CODE,ACCOUNT_NAME",
                target_node="VW_1_GROSS_TRANSLATED",
                target_column="RESOLVED_VALUE",
                transformation_type="CASE",
                transformation_expression="CASE WHEN ID_SOURCE = 'ACCOUNT_CODE' THEN ..."
            )

---

### `transform_column`

Apply a transformation to a column and optionally save the result.

    Args:
        source_path: Path to the CSV file.
        column: Column name to transform.
        operation: Transformation operation ('upper', 'lower', 'strip', 'trim_spaces', 'remove_special').
        output_path: Optional path to save transformed file. If empty, returns preview only.

    Returns:
        JSON with transformation preview and status.

---

### `translate_sql_to_faux_project`

Parse SQL and create a complete FauxProject in one step.

        This combines SQL parsing with project creation. The SQL is analyzed
        to extract the semantic view structure, then a project is created
        with the semantic view definition populated.

        Args:
            sql: SQL statement to parse
            project_name: Name for the new project
            description: Project description (optional)
            faux_type: Optional faux object type to create: "view", "stored_procedure",
                       "dynamic_table", or "task". If provided, a faux object is added.
            target_database: Target database for faux object (uses semantic view database if empty)
            target_schema: Target schema for faux object (uses semantic view schema if empty)

        Returns:
            JSON with created project details including ID and semantic view info

        Example:
            translate_sql_to_faux_project('''
                SELECT region, SUM(amount) as total_sales
                FROM WAREHOUSE.SALES.ORDERS o
                GROUP BY region
            ''', "Sales Analysis", faux_type="view")

---

### `translate_sql_to_semantic_view`

Parse SQL into a SemanticViewDefinition.

        Accepts CREATE VIEW, SELECT query, or CREATE SEMANTIC VIEW DDL and
        reverse-engineers it into a structured semantic view definition with
        tables, dimensions, metrics, facts, and relationships.

        Column classification rules:
        - Columns in GROUP BY → DIMENSION
        - Columns with aggregations (SUM, COUNT, AVG, MIN, MAX) → METRIC
        - Raw columns not in GROUP BY → FACT

        Args:
            sql: SQL statement (CREATE VIEW, SELECT, or CREATE SEMANTIC VIEW)
            name: Override name for the semantic view (optional)
            database: Override database (optional)
            schema_name: Override schema (optional)

        Returns:
            JSON with tables, dimensions, metrics, facts, relationships, and warnings

        Example:
            translate_sql_to_semantic_view('''
                SELECT region, SUM(amount) as total_sales
                FROM orders
                GROUP BY region
            ''', name="sales_analysis")

---

### `update_client_knowledge`

Update a specific field in client knowledge base.

        Supported fields: client_name, industry, erp_system, chart_of_accounts_pattern,
        preferred_template_id, preferred_skill_id, notes, gl_patterns

        Args:
            client_id: The unique identifier of the client.
            field: The field to update.
            value: The new value (string for most fields, JSON string for gl_patterns).

        Returns:
            JSON with updated client knowledge base.

---

### `update_hierarchy`

Update an existing hierarchy.

        AUTO-SYNC: When enabled, automatically updates the hierarchy in the backend.

        Args:
            project_id: Project UUID
            hierarchy_id: Hierarchy to update
            updates: JSON string of fields to update

        Returns:
            JSON with updated hierarchy (includes auto_sync status)

---

### `update_manifest`

Regenerate the MANIFEST.md documentation from tool docstrings.

    Returns:
        Confirmation message with tool count.

---

### `validate_dbt_project`

Validate a dbt project structure and configuration.

        Checks for required files, valid YAML, and model references.

        Args:
            project_name: Name of the dbt project

        Returns:
            Validation results with errors and warnings

        Example:
            validate_dbt_project(project_name="finance")

---

### `validate_hierarchy_data_quality`

Validate hierarchy and mapping data for quality issues.

        Detects:
        - ID_SOURCE typos (e.g., BILLING_CATEGRY_CODE)
        - Duplicate hierarchy keys
        - Orphan nodes (no mappings)
        - Orphan mappings (no hierarchy)
        - FILTER_GROUP mismatches
        - Formula reference issues

        Args:
            hierarchies: JSON array of hierarchy records
            mappings: JSON array of mapping records
            hierarchy_table: Name of hierarchy table
            mapping_table: Name of mapping table

        Returns:
            Validation result with all detected issues

        Example:
            validate_hierarchy_data_quality(
                hierarchies='[{"HIERARCHY_ID": 1, "ACTIVE_FLAG": true}]',
                mappings='[{"FK_REPORT_KEY": 1, "ID_SOURCE": "BILLING_CATEGRY_CODE"}]'
            )

---

### `validate_hierarchy_project`

Validate a hierarchy project for issues.

        Checks for:
        - Orphaned hierarchies (invalid parent references)
        - Leaf nodes without source mappings
        - Invalid formula references
        - Circular dependencies

        Args:
            project_id: Project UUID

        Returns:
            JSON with validation results and recommendations

---

### `validate_lineage`

Validate lineage graph completeness.

        Checks for:
        - Orphan nodes (nodes with no connections)
        - Missing source lineage
        - Circular dependencies
        - Overall completeness score

        Args:
            graph_name: Name of the lineage graph

        Returns:
            Validation result with issues and completeness score

        Example:
            validate_lineage(graph_name="finance_lineage")

---

### `validate_mart_config`

Validate configuration completeness and consistency.

        Checks that:
        - All required fields are present
        - Join pattern key counts match
        - No duplicate ID_SOURCE values
        - Configuration is ready for pipeline generation

        Args:
            config_name: Name of the configuration to validate

        Returns:
            Validation result with errors and warnings

        Example:
            validate_mart_config(config_name="upstream_gross")

---

### `validate_mart_pipeline`

Test generated DDL against source data.

        Validates that:
        - Source tables exist and are accessible
        - Column references are valid
        - Generated DDL is syntactically correct
        - Join patterns produce expected row counts

        Args:
            config_name: Name of the configuration
            connection_id: Snowflake connection for validation queries

        Returns:
            Validation result with per-layer status

        Example:
            validate_mart_pipeline(
                config_name="upstream_gross",
                connection_id="snowflake-prod"
            )

---

### `validate_semantic_model`

Validate a semantic model configuration.

        Checks for required fields, valid references, and optionally
        validates against the live database.

        Args:
            model_name: Name of the model to validate
            connection_id: Optional Snowflake connection for live validation

        Returns:
            Validation results with errors and warnings

        Example:
            validate_semantic_model(
                model_name="sales_analytics",
                connection_id="snowflake-prod"
            )

---

### `version_create`

Create a versioned snapshot of any object.

        This is the core versioning tool that creates a new version record
        with a full snapshot of the object's current state.

        Args:
            object_type: Type of object (hierarchy_project, hierarchy, catalog_asset,
                        glossary_term, semantic_model, data_contract, expectation_suite,
                        formula_group, source_mapping)
            object_id: Unique identifier for the object
            snapshot: JSON string of the complete object state
            change_description: Human-readable description of changes
            changed_by: User who made the change
            version_bump: Type of version increment (major, minor, patch)
            change_type: Type of change (create, update, delete, restore)
            tags: Comma-separated tags (e.g., "release,approved,production")
            object_name: Human-readable name for the object

        Returns:
            Version record with id, version string, and metadata

---

### `version_create_catalog_asset`

Create a versioned snapshot of a data catalog asset.

        Args:
            asset_id: The catalog asset ID
            asset_data: JSON string with the asset metadata
            change_description: Description of what changed
            changed_by: User who made the change
            version_bump: Version increment type (major, minor, patch)
            asset_name: Human-readable asset name

        Returns:
            Version record for the asset

---

### `version_create_hierarchy`

Create a versioned snapshot of a hierarchy project with all its hierarchies.

        This is a convenience tool for versioning hierarchy projects that automatically
        includes all child hierarchies in the snapshot.

        Args:
            project_id: The hierarchy project ID
            hierarchies: JSON string with project data including hierarchies array
            change_description: Description of what changed
            changed_by: User who made the change
            version_bump: Version increment type (major, minor, patch)
            project_name: Human-readable project name

        Returns:
            Version record for the project

---

### `version_diff`

Compare two versions and show differences.

        Args:
            object_type: Type of object
            object_id: Object identifier
            from_version: Starting version (e.g., "1.0.0")
            to_version: Ending version (None = latest)

        Returns:
            Diff with added, removed, and modified fields

---

### `version_diff_latest`

Compare current (latest) version to a previous version.

        This is a convenience wrapper for version_diff that compares
        a specific historical version to the current state.

        Args:
            object_type: Type of object
            object_id: Object identifier
            compare_to_version: Historical version to compare against

        Returns:
            Diff showing what changed since that version

---

### `version_get`

Get a specific version or the latest version of an object.

        Args:
            object_type: Type of object
            object_id: Object identifier
            version: Specific version string (e.g., "1.2.3") or None for latest
            include_snapshot: Whether to include the full snapshot data

        Returns:
            Version record with optional snapshot

---

### `version_get_stats`

Get versioning statistics across all objects.

        Returns counts by object type, recent activity, and top contributors.

        Returns:
            Statistics including total counts, by-type breakdowns, and activity

---

### `version_list`

List version history for an object (most recent first).

        Args:
            object_type: Type of object
            object_id: Object identifier
            limit: Maximum number of versions to return

        Returns:
            List of versions without snapshot data

---

### `version_preview_rollback`

Preview what a rollback would restore without applying it.

        Use this to see the differences before committing to a rollback.

        Args:
            object_type: Type of object
            object_id: Object identifier
            to_version: Target version to preview

        Returns:
            Preview with diff and optional warnings

---

### `version_rollback`

Rollback an object to a previous version.

        This creates a new version with the state from the target version,
        recording it as a RESTORE change type. The caller is responsible
        for actually applying the snapshot to the underlying object.

        Args:
            object_type: Type of object
            object_id: Object identifier
            to_version: Target version to restore
            changed_by: User performing the rollback

        Returns:
            The snapshot data to apply and the new version record

---

### `version_search`

Search versions across all objects with filters.

        Args:
            object_type: Filter by object type
            object_id: Filter by specific object
            changed_by: Filter by user who made changes
            change_type: Filter by change type (create, update, delete, restore)
            tag: Filter by tag
            from_date: Start date (ISO format)
            to_date: End date (ISO format)
            is_major: Filter for major versions only
            limit: Maximum results

        Returns:
            List of matching versions

---

### `version_tag`

Add or remove tags on a version.

        Tags are useful for marking releases, approvals, or other milestones.
        Common tags: "release", "approved", "production", "staging", "archived"

        Args:
            object_type: Type of object
            object_id: Object identifier
            version: Version string to tag
            add_tags: Comma-separated tags to add
            remove_tags: Comma-separated tags to remove

        Returns:
            Updated version with current tags

---

