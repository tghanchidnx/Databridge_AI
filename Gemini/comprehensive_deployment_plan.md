# Comprehensive Deployment Plan: DataBridge Warehouse Automation

This document provides a comprehensive, phased deployment plan for enhancing the DataBridge AI platform with automated data warehouse creation capabilities. It is based on the concepts outlined in the `gemini_enhancement_plan.md` and details the pragmatic steps, documentation, demos, and testing strategies required for successful implementation.

The platform will support two modes of operation:
*   **Basic Mode:** For rapid development and simple projects, DataBridge will generate and execute SQL directly against a Snowflake database. This provides immediate results but offers limited version control and transformation complexity.
*   **Advanced (Version-Controlled) Mode:** As a recommended best practice, DataBridge will integrate with GitHub and dbt. In this mode, the application will generate a complete dbt project, push it to a user's GitHub repository, and optionally orchestrate dbt runs. This provides a robust, collaborative, and version-controlled environment for the data warehouse.

The following phases are designed to build out this dual-mode capability progressively.

---

## Phase 1: Source Intelligence & Semantic Analysis

**Goal:** To establish a robust foundation for ingesting and understanding metadata from diverse source systems, forming the basis for automated data modeling.

### Features & Implementation
1.  **Pluggable Source Connector Framework (Librarian):**
    *   Develop a Python-based framework with a unified interface (`BaseConnector`) for metadata extraction.
    *   Implement initial connectors for:
        *   **Databases (JDBC/ODBC):** Read schemas, tables, columns, and primary/foreign keys.
        *   **File Formats:** Implement parsers for `.xlsx`, `.csv`, and `.parquet` to infer schemas and data types.
        *   **PDF/Text (LLM-Powered):** Enhance existing OCR to use an LLM to identify and extract tabular data from unstructured financial reports.
2.  **Semantic Analyzer Module (Librarian):**
    *   Build a new module that uses heuristics (e.g., column name patterns like `_ID`, `_DATE`) and an LLM for deeper analysis.
    *   **Entity Detection:** Train or prompt the model to recognize the specified entities: "Employee," "Customer," "Vendor," "Cost Center," "Assets," "Equipment," "Inventory," "Location," "Department," "Product," "Company/Corp," "Date," and "Chart of Accounts."
    *   **Dimension Classifier:** Implement logic to group "Employee," "Customer," and "Vendor" under a "Business Associate" dimension. Add a classifier to detect potential Slowly Changing Dimension (SCD) types based on the presence of `start_date`/`end_date` or `version` columns.
    *   **Relationship Inferencer:** Develop an algorithm that suggests potential joins based on column name similarity, data type matching, and value distribution analysis.
3.  **Interactive Refinement Tools (Librarian CLI/MCP):**
    *   Create new CLI commands (`databridge source review`, `databridge source link`, `databridge source merge`) and corresponding MCP tools.
    *   These tools will present the inferred model to the user and allow them to confirm/reject joins, rename entities, and define merge operations (e.g., `CUST_ID` + `CustomerNumber` -> `customer_id`).

### Documentation Plan
*   **Internal:**
    *   API documentation for the `BaseConnector` interface.
    *   Architecture diagram of the Semantic Analyzer module.
*   **User Guide:**
    *   Add a "Connecting to Sources" chapter explaining how to configure the new connectors.
    *   Add a "Reviewing the Canonical Model" section detailing the new interactive refinement commands.

### Demo Plan
*   **Title:** "From Raw Source to Canonical Model"
*   **Script:** A 2-minute video showing the CLI connecting to a sample database, running the analysis, printing the inferred entities and relationships, and then using a CLI command to merge two customer ID columns.

### Testing Strategy
*   **Regression Tests:**
    *   Ensure existing Librarian CSV import functionality still works.
    *   Verify that Researcher can still connect to its original data sources for the FP&A workflow.
*   **New Feature Tests:**
    *   Unit tests for each connector (e.g., `test_snowflake_connector_get_schema`).
    *   Integration test to run the Semantic Analyzer on a test database and validate the generated canonical model against an expected output.
*   **User Acceptance Testing (UAT) Guide:**
    *   **Objective:** Validate that the system correctly analyzes a known data source.
    *   **Script:**
        1.  "Use the `databridge source add-db` command to connect to the provided test database."
        2.  "Run the `databridge source analyze` command."
        3.  "Does the output correctly identify the 'Customer' and 'Product' tables?"
        4.  "Is the join between `sales.product_id` and `products.id` suggested?"
        5.  "Use the `databridge source rename` command to rename the 'Customer' entity to 'Client'. Verify the change is saved."

---

## Phase 2: Data Model & Snowflake DDL Generation

**Goal:** To translate the user-approved canonical model into a concrete, multi-layered Snowflake data warehouse design, generating either basic SQL scripts or a complete, version-controlled dbt project.

### Features & Implementation
1.  **Advanced Hierarchy Management (Librarian):**
    *   Modify the `Hierarchy` model in Librarian to support two types: `GROUPING` and `XREF`.
    *   The `XREF` hierarchy will be the central metadata artifact linking all dimensions and facts.
2.  **Warehouse Modeler Module (Librarian):**
    *   Implement the core logic for designing the Snowflake objects.
    *   **Base Fact Logic:** Create logic to generate the `Fact_Financial_Actuals` table, identifying measures and dimensional keys from the canonical model. Acknowledge the "pre-fact processing" concept by allowing this module to source from views (which can be created manually for now).
    *   **Hierarchy-to-Object Logic:** Implement the specific generation pipeline for a given hierarchy (`MyReport`):
        1.  `TBL_0_MyReport`: Create a table from the Librarian hierarchy data.
        2.  `VW_1_MyReport`: Generate a view that unnests the hierarchy mappings.
        3.  `DT_2_MyReport`: Generate the `CREATE DYNAMIC TABLE` statement that joins all dimensions at their lowest grain, using the view above.
        4.  `DT_3A_MyReport`: Generate the DDL for the pre-aggregation DT, including `GROUP BY` clauses and aggregation functions (`SUM`, `AVG`).
        5.  `DT_3_MyReport`: Generate the DDL that unions `DT_3A` with the transactional data, grouped by user-selected "fact-based details" (e.g., invoice number, transaction ID).
3.  **DDL & dbt Project Generator (Librarian):**
    *   Create a template-based generator (e.g., using Jinja2) with two output modes:
        *   **Basic Mode:** Produces a dependency-ordered list of `.sql` files containing `CREATE TABLE`, `CREATE VIEW`, etc., statements.
        *   **Advanced Mode (dbt):** Generates a complete dbt project structure, including:
            *   `.sql` files for each model (dimensions, facts, dynamic tables) in the `models/` directory.
            *   `.yml` schema files in the `models/` directory defining sources, columns, data types, and tests (e.g., `not_null`, `unique`).
            *   A `dbt_project.yml` file configured for the user's Snowflake target.
4.  **GitHub Project Scaffolding (Optional, Librarian):**
    *   **Action:** Create a new set of tools to initialize a GitHub repository for the user's data warehouse project.
    *   **Details:** The tool will, with user permission (OAuth):
        *   Create a new private GitHub repository.
        *   Initialize it with a standard structure (`.github/workflows`, `README.md`).
        *   Push the generated dbt project (from step 3) as the initial commit.
        *   Optionally, set up a basic GitHub Actions workflow for running `dbt build` on push.

### Documentation Plan
*   **Internal:**
    *   Detailed document explaining the logic and purpose of the `TBL_0` -> `DT_3` object pipeline.
    *   Documentation for the dbt project templates.
*   **User Guide:**
    *   Add a "Data Warehouse Modeling" chapter. Explain the two output modes (SQL Scripts vs. dbt Project).
    *   Add a "Version Control with GitHub" section explaining how to connect a GitHub account and save projects.

### Demo Plan
*   **Title:** "Designing and Versioning a Snowflake Data Mart"
*   **Script:** Start with the approved canonical model. Run `databridge model design --project <id> --dbt --github-repo my-new-datamart`. Show the CLI creating a new GitHub repo, and then show the populated dbt project in the GitHub UI, including the generated `.sql` and `.yml` files.

### Testing Strategy
*   **Regression Tests:**
    *   Verify that creating a simple Librarian "grouping" hierarchy for the FP&A workflow has not been broken.
    *   Ensure the "Basic Mode" SQL script generation still works as expected.
*   **New Feature Tests:**
    *   Unit test the dbt project generator to ensure all necessary files (`.sql`, `.yml`, `dbt_project.yml`) are created correctly.
    *   Integration test for the GitHub scaffolding tool using a mock GitHub API.
*   **User Acceptance Testing (UAT) Guide:**
    *   **Objective:** Ensure the generated dbt project is correct and can be saved to GitHub.
    *   **Script:**
        1.  "Run the `databridge model design --dbt` command."
        2.  "The assistant will ask if you want to save this to a new GitHub repository. Confirm 'yes' and provide a repository name."
        3.  "Follow the authentication prompts."
        4.  "Check your GitHub account. Is the new repository created?"
        5.  "Does the repository contain a `models` directory with `.sql` and `.yml` files?"

---

## Phase 3: Snowflake Deployment & ETL Generation

**Goal:** To automate the execution of the generated model, either by running SQL directly for simple projects or by orchestrating a dbt project for robust, version-controlled deployments.

### Features & Implementation
1.  **Dual-Mode Deployment Orchestrator (Librarian):**
    *   Enhance the workflow engine to support two distinct deployment strategies:
        *   **Basic Mode:** Manages a direct pipeline: `connect -> begin_transaction -> execute_ddl -> execute_etl -> commit_transaction -> disconnect`. This is suitable for users without dbt/Git.
        *   **Advanced Mode (dbt):** The orchestrator's role shifts to that of a Git and dbt client. The pipeline becomes: `git_commit -> git_push -> (optional) trigger_dbt_run`.
    *   Implement robust error handling and logging for both modes.
2.  **Dual-Mode ETL Generator Module (Librarian):**
    *   This module will now generate transformations based on the chosen mode:
        *   **Basic Mode:** Generates `INSERT INTO ... SELECT ...` statements for direct execution, suitable for simpler, non-versioned transformations.
        *   **Advanced Mode (dbt):** Generates `.sql` files within the dbt project's `models/` directory. It will produce models for staging, intermediate transformations, and the final dimension/fact tables, leveraging dbt features like sources, refs, and macros.
3.  **Deployment CLI/MCP Tools (Librarian):**
    *   Enhance the `databridge model deploy` command.
    *   If a dbt project is detected, the command will automatically use the Advanced Mode workflow (commit, push, and optionally trigger a run).
    *   Add flags to control Git and dbt behavior, such as `--no-push` or `--dbt-target <target_name>`.

### Documentation Plan
*   **Internal:**
    *   Sequence diagrams for both the Basic and Advanced deployment workflows.
*   **User Guide:**
    *   Update the "Deploying Your Data Warehouse" chapter to clearly explain the two modes.
    *   Provide examples for both `databridge model deploy` with and without a dbt project.
    *   Add a section on how to set up credentials for dbt Cloud or select a local dbt profile.

### Demo Plan
*   **Title:** "Two Paths to Production: Basic vs. dbt Deployment"
*   **Script:**
    1.  **Basic Demo:** Show a project being deployed directly to Snowflake using the simple `deploy` command.
    2.  **Advanced Demo:** For a project configured with GitHub, run `databridge model deploy`. Show the CLI committing and pushing the dbt models to the repository. Then, switch to the dbt Cloud UI to show a new job being triggered and running successfully.

### Testing Strategy
*   **Regression Tests:**
    *   Ensure the original Librarian `deploy` command (if it had different functionality) still works as expected. The "Basic Mode" should cover this.
*   **New Feature Tests:**
    *   Integration test for the "Advanced Mode" that:
        1.  Generates a dbt project.
        2.  Commits it to a temporary local Git repository.
        3.  Runs `dbt build` using a locally installed dbt Core and asserts a successful run.
*   **User Acceptance Testing (UAT) Guide:**
    *   **Objective:** Confirm both deployment modes work as expected.
    *   **Script (Basic):**
        1.  "Run the `databridge model deploy` command for a project without a Git repository."
        2.  "Does the command complete successfully and show logs of SQL execution?"
        3.  "Verify in Snowflake that the tables are created and populated."
    *   **Script (Advanced):**
        1.  "Run `databridge model deploy` for the project you linked to GitHub in Phase 2."
        2.  "Does the command complete successfully and show logs of Git commands?"
        3.  "Check your GitHub repository. Is there a new commit with the latest model changes?"

---

## Phase 4 & 5: Integration, Validation, and Usability

**Goal:** To create a seamless, end-to-end experience, from natural language prompt to a fully validated and version-controlled data warehouse.

### Features & Implementation
1.  **Automated Researcher Knowledge Base Sync (Librarian -> Researcher):**
    *   Implement an event-driven notification (e.g., using Redis Pub/Sub) or a simple webhook that Librarian calls upon successful deployment or a successful `dbt run`.
    *   Enhance Researcher with a listener that, upon receiving a notification, automatically runs its metadata extraction process on the appropriate Snowflake schema (or dbt target).
2.  **dbt-Aware Validation Suite (Researcher):**
    *   Develop a standard suite of post-deployment validation steps.
    *   If a dbt project is used, enhance the Researcher validation command (`databridge analytics validate`) to trigger `dbt test` and parse the results, providing a user-friendly summary of data quality tests.
    *   For both modes, continue to support direct validation queries (row counts, null checks, etc.).
3.  **High-Level Orchestration MCP Tools (Librarian/Researcher):**
    *   Create a new set of "workflow" tools that abstract the entire process.
    *   `workflow_start_dw_creation(source_config)`: Kicks off Phase 1.
    *   `workflow_get_proposed_model(workflow_id)`: Returns the canonical model for review.
    *   `workflow_approve_model(workflow_id, approved_model, use_dbt: bool, github_repo: str = None)`: Triggers Phase 2 and 3, with parameters to select the desired workflow.
    *   `workflow_get_deployment_status(workflow_id)`: Checks the status of the deployment.
4.  **Natural Language Interface (Gemini + Researcher):**
    *   Train Gemini (by providing examples) to use the new workflow tools and to ask the user clarifying questions to guide them through the process. For example: "I have designed the data warehouse schema. Would you like me to deploy it directly, or create a version-controlled dbt project in a new GitHub repository for you?"

### Documentation Plan
*   **Internal:**
    *   Documentation for the new "workflow" MCP tools, including the `use_dbt` and `github_repo` parameters.
*   **User Guide:**
    *   Add a new "Automated Workflow" chapter that explains how to use natural language to build a data warehouse from start to finish. Provide example prompts and explain the choices (Basic vs. Advanced).

### Demo Plan
*   **Title:** "The 5-Minute Data Warehouse, Your Way"
*   **Script:** A full, end-to-end screen recording.
    1.  Start with only a set of source files (CSVs, XLSX).
    2.  Give a single prompt to Gemini: "Analyze these files and propose a data warehouse model for me."
    3.  Show Gemini presenting the proposed model.
    4.  Show the user typing "Looks good, proceed."
    5.  **Key Step:** Show Gemini asking: "Deploy directly to Snowflake (Basic) or create a version-controlled dbt project in GitHub (Advanced)?"
    6.  User types: "Advanced, create a new repo named `my-company-datamart`".
    7.  Show the deployment logs, including `git push` and `dbt build` commands.
    8.  End with Gemini confirming the deployment and validation success, and then immediately using the Researcher NL-to-SQL to query the new warehouse.

### Testing Strategy
*   **Regression Tests:**
    *   Run the original `e2e_test.py` script to ensure the basic FP&A query workflow in Researcher still functions correctly against a manually configured data source.
*   **New Feature Tests:**
    *   A full end-to-end integration test that invokes the `workflow_start_dw_creation` tool with the `use_dbt=True` flag and checks that a valid dbt project is created and pushed to a mock Git server.
*   **User Acceptance Testing (UAT) Guide:**
    *   **Objective:** Validate the entire automated workflow, including the choice between Basic and Advanced modes.
    *   **Script:**
        1.  "Open a chat with the DataBridge AI assistant."
        2.  "Upload the provided set of sales and customer spreadsheets."
        3.  "Type the prompt: 'Build a sales data mart for me from these files.'"
        4.  "Follow the assistant's instructions to approve the proposed data model."
        5.  "When the assistant asks you to choose between Basic and Advanced deployment, choose 'Advanced' and provide a name for a new GitHub repository."
        6.  "Once the assistant confirms completion, check your GitHub account to verify the repository was created."
        7.  "Ask the assistant: 'Show me my top 5 customers by sales amount.' Does it provide a correct-looking answer?"