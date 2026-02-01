# Gemini's Enhancement Plan for DataBridge AI

This document outlines a detailed plan to enhance the DataBridge AI platform to fulfill its new primary purpose: **automated data warehouse creation**. This plan focuses on augmenting the existing V3 and V4 architecture to support this new functionality while keeping the current FP&A workflows intact.

The plan is divided into five phases:
1.  **Source Intelligence & Schema Analysis**
2.  **Data Model & DDL Generation**
3.  **Snowflake Deployment & Execution Engine**
4.  **Hierarchy-Driven Data Flow**
5.  **Validation, Usability & V4 Integration**

---

### Phase 1: Source Intelligence & Schema Analysis (V3 Enhancement)

**Goal:** Enhance V3 to ingest and understand various data sources beyond simple CSVs, and to infer relationships and data types automatically.

*   **1.1. Create a "Source Connector" Framework:**
    *   **Action:** In V3, develop a pluggable connector framework for ingesting metadata from different sources.
    *   **Details:** Create connectors for:
        *   **Databases:** Read schemas, tables, columns, and foreign keys from accounting systems (e.g., via JDBC).
        *   **File Formats:** Parse structured files like `.xlsx`, `.csv`, and `.parquet` to infer schemas.
        *   **Financial Reports:** Use LLM-powered text extraction (building on the existing V3 OCR capability) to identify tables and data points from unstructured `.pdf` reports.

*   **1.2. Develop the "Semantic Analyzer" Module:**
    *   **Action:** Create a new module in V3 that analyzes the metadata collected by the connectors.
    *   **Details:** This module will use a combination of heuristics and an LLM to:
        *   **Identify Entities:** Detect common business entities like ("Employee," "Customer," "Vendor,") -> "Business Associate," "Cost Center," "Assets," "Equipment," "Inventory,"  "Location," "Department,"  "Product," "Company/Corp," multiple "Date," and "Chart of Accounts."
        *   **Classify Data:** Tag columns as measures (e.g., amount, quantity) or dimensions (e.g., region, department) and SCD Dimensions.
        *   **Infer Relationships:** Suggest joins and relationships between tables, even where foreign keys are not explicitly defined.
        *   **Propose a Canonical Model:** Generate a high-level entity-relationship diagram as an intermediate representation.

*   **1.3. User Interaction for Refinement:**
    *   **Action:** Create a new set of CLI/MCP tools in V3 for users to review and refine the aoutomatically generated model.
    *   **Details:** Users should be able to:
        *   Confirm or reject suggested relationships.
        *   Rename entities and attributes.
        *   Merge different source columns into a single dimension (e.g., `CUST_ID` and `CustomerNumber` both become `customer_id`).

---

### Phase 2: Data Model & DDL Generation (V3 Enhancement)

**Goal:** Translate the refined canonical model from Phase 1 into a concrete Snowflake data warehouse design, including DDL (Data Definition Language).

*   **2.1. Create the "Warehouse Modeler" Module:**
    *   **Action:** Build a new module in V3 responsible for designing the target Snowflake schema.
    *   **Details:** This module will take the user-approved model and:
        *   Design standard "common" dimensions (e.g., `DIM_DATE` (there can be multiple date dimensions that should all be dertivateive of the 
    `DIM_DATE` dimension), `DIM_CHART_OF_ACCOUNTS`, `DIM_VENDOR`).
        *   There are two types of Hierarchy dimensions:
                1. one that stores groupings across dimensions 
                2. one used to store cross referencing and relationships between all dimensions and facts throughout the data modeling and warehousing process and is used as the central pillar to structure relationships The second one is called Xref Hierarhcy Dimension.
        *   Hierarchy dimensions can be many for each report or for multiple internal system cross referencing or multi system mapping and cross referencing. That is why we focus so much on buiulding hierarchies. Even though hierarhcy manager or builder is not the best naming choicve for marketing and coolness.
        *   We build base Fact Dynamic table Fact_Financial_Actuals, after that we build variations like data marts based on different hierahcy projects of that base fact from accounting general ledger and other ledgers as required by the client or industry standard practice and also based on ERP setup.
        *   Define the structure for the dynamic fact tables `Fact_Financial_Actuals`, and then based on that create variations of this Base Fact table depending on reporting hierarchies or department views. During structure difinition include process to choose and name measures and dimensional foreign keys, and fact based detail columns it will contain. This fact can be based of 1 or more transaction or ledger tables from the source system and that data might have been also wrapped in views (for simpler logic and smaller datasets) or dynamic tables to confirm to similar columns and get union on which our base fact table is built. That pre Fact table process is another identify and data engineering and discovery process we will build in future phases that you can keep in mind and remind yourself of and me. 
        *   The hierarchies get included in the snowflake TRANSFORMATION databse CONFIGURATION schema. in which hierarchies are stored as tables TBL_0_hierarhcy_Name
        * the views `VW_1_Hierarchy_Name` to create a fit the needs of the respective `DT_2_Hierarchy_Name` which expands all the hierachy mapping at the most detail granulairty of that dimension and do that for all the dimensions used in the hierarchy 
        * Then `DT_3A_Hierarchy_Name` is created that stored all aggregated and calculated values based on the hierahcy and formuals used in hierarhcy. This is like aggregations create in SSAS and other cube pre aggregation engines. 
        * Next `DT_3_Hierarchy_Name` is generated that will UNION `DT_3A_Hierarchy_Name` at the end but on the top follow the same concept, except for transactional data and store it at a granularity user selected earlier in phase 1. In Phase 1 user gets to select the granularity  based on count of rows with non dimensional fact based details are grouped. These are fact based dimensions that does not have a its own dimension table and are just treated as additional details in the fact table. If a dimension is generated from these then they stop being fact based dimensions. These detail attributes are invoice number, voucher no, check no, transaction ID, batch id, transaction description, transaction comments, transaction notes, etc. 
        
*   **2.2. Develop the "Snowflake DDL Generator":**
    *   **Action:** Create a component that generates the `CREATE TABLE`, `CREATE VIEW`, `CREATE DYNAMIC TABLE`, and `CREATE SEQUENCE` SQL statements for Snowflake.
    *   **Details:**
        *   The generator will use Snowflake-specific syntax and best practices (e.g., defining clustering keys, using `VARIANT` for semi-structured data).
        *   It will produce a dependency-ordered list of SQL scripts to ensure tables are created before views or foreign keys that depend on them.

---

### Phase 3: Snowflake Deployment & Execution Engine (V3 Enhancement)

**Goal:** Enhance V3's existing deployment capabilities to execute the generated DDL and populate the new data warehouse.

*   **3.1. Create the "Deployment Orchestrator":**
    *   **Action:** Build a robust workflow engine in V3 to manage the deployment process.
    *   **Details:** This orchestrator will handle:
        *   Connecting to the target Snowflake environment.
        *   Executing the DDL scripts in the correct order.
        *   Managing transactions to ensure deployments are atomic (all-or-nothing).
        *   Logging every step of the deployment for audit and debugging purposes.

*   **3.2. Develop the "ETL Generator" Module:**
    *   **Action:** Create a module that generates the SQL `INSERT` statements or `COPY INTO` commands needed to populate the new warehouse from the original sources.
    *   **Details:**
        *   This module will map the source columns (from Phase 1) to the target tables (from Phase 2).
        *   It will handle basic transformations (e.g., data type casting, string manipulation) as defined during the user refinement step.
        *   For complex transformations, it can generate skeletons for Snowflake Tasks and Streams or dbt models.

---

### Phase 4: Hierarchy-Driven Data Flow (V3 & V4 Integration)

**Goal:** Solidify the "hierarchy dimension" as the central component that governs data structure and user queries.

*   **4.1. Enhance V3 Hierarchy Management:**
    *   **Action:** Add a new "Data Model" tab or view within a V3 project.
    *   **Details:** This view will show the generated data warehouse schema and visually link each table and column back to the hierarchy nodes that defined them. This creates a clear data lineage trail.

*   **4.2. Enhance V4 Knowledge Base Integration:**
    *   **Action:** When a new data warehouse is deployed by V3, it should automatically notify V4.
    *   **Details:** V4 will then automatically:
        *   Connect to the new Snowflake schema.
        *   Ingest the entire schema, including the new dynamic fact tables and common dimensions, into its knowledge base.
        *   Link the business terms in its glossary directly to the newly created tables and columns, using the hierarchy dimension as the "source of truth."

---

### Phase 5: Validation, Usability & V4 Integration

**Goal:** Use the V4 engine to validate the new warehouse and make the entire process accessible via natural language.

*   **5.1. Automated Data Validation with V4:**
    *   **Action:** After V3 deploys the warehouse, automatically trigger a suite of validation queries using the V4 engine.
    *   **Details:** V4 can run queries like:
        *   "Count rows in `DT_2` and compare against the source table."
        *   "Check for null foreign keys in the fact tables."
        *   "Show me the sum of `amount` grouped by the new `hierarchy_level_1`."
    *   A summary report will be generated to confirm the success of the deployment.

*   **5.2. Natural Language Orchestration:**
    *   **Action:** Expose the new functionality through high-level MCP tools that V4 can use.
    *   **Details:** This will allow the user to initiate the entire workflow with a single prompt to Gemini, for example:
        > "Here are my Excel-based financial reports for the last quarter (attach files). Analyze them, propose a Snowflake data warehouse schema, and once I approve it, build and populate it for me."

    *   The AI would then use the new tools from V3 and V4 to orchestrate the entire process, asking for user confirmation at key steps (like schema approval). This makes the powerful warehouse creation capability accessible to a much wider audience.