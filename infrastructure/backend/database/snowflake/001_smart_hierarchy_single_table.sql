-- =====================================================
-- SMART HIERARCHY BUILDER - SNOWFLAKE SINGLE TABLE SETUP
-- Version: 2.0 - Simplified Single Table Approach
-- Date: 2025-11-27
-- 
-- Key Features:
-- 1. Single master table with all properties as JSON
-- 2. Multiple source mappings from different databases/tables
-- 3. Each mapping has its own flags
-- 4. Project-based hierarchy groups
-- 5. Dynamic VIEW and DT generation based on configuration
-- =====================================================

USE WAREHOUSE COMPUTE_WH; -- Change to your warehouse
USE DATABASE TRANSFORMATION; -- Change to your database
USE SCHEMA CORE_FINANCIAL; -- Change to your schema

-- =====================================================
-- SINGLE MASTER TABLE - All Configuration in JSON
-- =====================================================

CREATE OR REPLACE TABLE TBL_HIERARCHY_MASTER (
    -- Identity (Project-based)
    PROJECT_NAME VARCHAR(255) NOT NULL COMMENT 'Project identifier (hierarchy group)',
    HIERARCHY_ID VARCHAR(50) NOT NULL COMMENT 'Unique hierarchy identifier within project',
    HIERARCHY_PARENT_ID VARCHAR(50) COMMENT 'Parent hierarchy ID for tree structure',
    
    -- Hierarchy Levels (Dynamic 2-15 levels)
    HIERARCHY_LEVEL VARIANT NOT NULL COMMENT '
    JSON Structure:
    {
        "level_1": "Total Revenue",
        "level_2": "Operating Revenue",
        "level_3": null,
        ...
        "level_15": null,
        "level_1_sort": 100,
        "level_2_sort": 200,
        ...
        "level_15_sort": null
    }',
    
    -- Behavior Flags
    FLAGS VARIANT NOT NULL COMMENT '
    JSON Structure:
    {
        "active_flag": true,
        "calculation_flag": false,
        "volume_flag": false,
        "exclusion_flag": false,
        "sign_change_flag": false,
        "id_unpivot_flag": false,
        "id_row_flag": true,
        "do_not_expand_flag": false,
        "create_new_column": false
    }',
    
    -- Formula Configuration
    FORMULA_CONFIG VARIANT COMMENT '
    JSON Structure:
    {
        "formula_group": "GRP_REV_001",
        "formula_precedence": 1,
        "formula_param_ref": null,
        "arithmetic_logic": "ADD",
        "formula_param2_const_number": 1.0
    }',
    
    -- Filter Configuration
    FILTER_CONFIG VARIANT COMMENT '
    JSON Structure:
    {
        "filter_group_1": "REVENUE",
        "filter_group_2": "OPERATING",
        "filter_group_3": null,
        "filter_group_4": null,
        "excluded_accounts": ["ACCT001", "ACCT002"]
    }',
    
    -- Source Mapping Array (Multiple databases/tables with individual flags)
    MAPPING VARIANT COMMENT '
    JSON Array Structure:
    [
        {
            "mapping_index": 1,
            "source_database": "TRANSFORMATION",
            "source_schema": "CORE_FINANCIAL",
            "source_table": "DIM_ACCOUNT",
            "source_column": "ACCOUNT_CODE",
            "source_type": "ACCOUNT_CODE",
            "filter_expression": "ACCOUNT_MAJOR_CODE = ''400''",
            "transformation": "DIRECT",
            "flags": {
                "include_flag": true,
                "exclude_flag": false,
                "transform_flag": false,
                "active_flag": true
            }
        },
        {
            "mapping_index": 2,
            "source_database": "FINANCE_DB",
            "source_schema": "GL_DATA",
            "source_table": "GL_ACCOUNTS",
            "source_column": "GL_ACCOUNT_NUMBER",
            "source_type": "GL_ACCOUNT",
            "filter_expression": "ACCOUNT_TYPE = ''REVENUE''",
            "transformation": "UPPER",
            "flags": {
                "include_flag": true,
                "exclude_flag": false,
                "transform_flag": true,
                "active_flag": true
            }
        }
    ]',
    
    -- Pivot Configuration (Optional)
    PIVOT_CONFIG VARIANT COMMENT '
    JSON Structure:
    {
        "pivot_source_column_name": "MONTH",
        "pivot_source_table_name": "FACT_SALES",
        "pivot_source_schema_name": "SALES",
        "pivot_source_database_name": "ANALYTICS"
    }',
    
    -- Metadata
    CREATED_BY VARCHAR(100) DEFAULT CURRENT_USER(),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    
    -- Primary Key
    CONSTRAINT pk_hierarchy PRIMARY KEY (PROJECT_NAME, HIERARCHY_ID),
    
    -- Foreign Key for parent-child relationship
    CONSTRAINT fk_parent FOREIGN KEY (PROJECT_NAME, HIERARCHY_PARENT_ID) 
        REFERENCES TBL_HIERARCHY_MASTER(PROJECT_NAME, HIERARCHY_ID)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_project_name ON TBL_HIERARCHY_MASTER(PROJECT_NAME);
CREATE INDEX IF NOT EXISTS idx_hierarchy_id ON TBL_HIERARCHY_MASTER(HIERARCHY_ID);
CREATE INDEX IF NOT EXISTS idx_parent_id ON TBL_HIERARCHY_MASTER(HIERARCHY_PARENT_ID);
CREATE INDEX IF NOT EXISTS idx_active ON TBL_HIERARCHY_MASTER(IS_ACTIVE);
CREATE INDEX IF NOT EXISTS idx_project_active ON TBL_HIERARCHY_MASTER(PROJECT_NAME, IS_ACTIVE);

-- =====================================================
-- SAMPLE DATA - Multiple Source Mappings Example
-- =====================================================

INSERT INTO TBL_HIERARCHY_MASTER VALUES (
    'PROJECT_ALPHA',                    -- PROJECT_NAME
    'H001',                             -- HIERARCHY_ID
    NULL,                               -- HIERARCHY_PARENT_ID (root)
    PARSE_JSON('{
        "level_1": "Total Revenue",
        "level_2": "Operating Revenue",
        "level_3": null,
        "level_4": null,
        "level_5": null,
        "level_6": null,
        "level_7": null,
        "level_8": null,
        "level_9": null,
        "level_10": null,
        "level_11": null,
        "level_12": null,
        "level_13": null,
        "level_14": null,
        "level_15": null,
        "level_1_sort": 100,
        "level_2_sort": 200,
        "level_3_sort": null,
        "level_4_sort": null,
        "level_5_sort": null,
        "level_6_sort": null,
        "level_7_sort": null,
        "level_8_sort": null,
        "level_9_sort": null,
        "level_10_sort": null,
        "level_11_sort": null,
        "level_12_sort": null,
        "level_13_sort": null,
        "level_14_sort": null,
        "level_15_sort": null
    }'),
    PARSE_JSON('{
        "active_flag": true,
        "calculation_flag": false,
        "volume_flag": false,
        "exclusion_flag": false,
        "sign_change_flag": false,
        "id_unpivot_flag": false,
        "id_row_flag": true,
        "do_not_expand_flag": false,
        "create_new_column": false
    }'),
    PARSE_JSON('{
        "formula_group": "GRP_REV_001",
        "formula_precedence": 1,
        "formula_param_ref": null,
        "arithmetic_logic": "ADD",
        "formula_param2_const_number": 1.0
    }'),
    PARSE_JSON('{
        "filter_group_1": "REVENUE",
        "filter_group_2": "OPERATING",
        "filter_group_3": null,
        "filter_group_4": null,
        "excluded_accounts": []
    }'),
    -- Multiple source mappings from different databases
    PARSE_JSON('[
        {
            "mapping_index": 1,
            "source_database": "TRANSFORMATION",
            "source_schema": "CORE_FINANCIAL",
            "source_table": "DIM_ACCOUNT",
            "source_column": "ACCOUNT_CODE",
            "source_type": "ACCOUNT_CODE",
            "filter_expression": "ACCOUNT_MAJOR_CODE = ''400''",
            "transformation": "DIRECT",
            "flags": {
                "include_flag": true,
                "exclude_flag": false,
                "transform_flag": false,
                "active_flag": true
            }
        },
        {
            "mapping_index": 2,
            "source_database": "FINANCE_DB",
            "source_schema": "GL_DATA",
            "source_table": "GL_ACCOUNTS",
            "source_column": "GL_ACCOUNT_NUMBER",
            "source_type": "GL_ACCOUNT",
            "filter_expression": "ACCOUNT_TYPE = ''REVENUE''",
            "transformation": "UPPER",
            "flags": {
                "include_flag": true,
                "exclude_flag": false,
                "transform_flag": true,
                "active_flag": true
            }
        }
    ]'),
    NULL,                               -- PIVOT_CONFIG
    CURRENT_USER(),                     -- CREATED_BY
    CURRENT_TIMESTAMP(),                -- CREATED_AT
    CURRENT_TIMESTAMP(),                -- UPDATED_AT
    TRUE                                -- IS_ACTIVE
);

-- Child hierarchy with calculation flag
INSERT INTO TBL_HIERARCHY_MASTER VALUES (
    'PROJECT_ALPHA',
    'H002',
    'H001',                             -- Parent is H001
    PARSE_JSON('{
        "level_1": "Total Revenue",
        "level_2": "Net Revenue",
        "level_3": null,
        "level_1_sort": 100,
        "level_2_sort": 300
    }'),
    PARSE_JSON('{
        "active_flag": true,
        "calculation_flag": true,        -- Calculation row, no account join
        "volume_flag": false,
        "exclusion_flag": false,
        "sign_change_flag": true,
        "id_unpivot_flag": false,
        "id_row_flag": false,
        "do_not_expand_flag": false,
        "create_new_column": true
    }'),
    PARSE_JSON('{
        "formula_group": "GRP_NET_REV",
        "formula_precedence": 2,
        "formula_param_ref": "GRP_REV_001",
        "arithmetic_logic": "SUBTRACT",
        "formula_param2_const_number": 1.0
    }'),
    PARSE_JSON('{
        "filter_group_1": null,
        "filter_group_2": null,
        "filter_group_3": null,
        "filter_group_4": null,
        "excluded_accounts": []
    }'),
    NULL,                               -- No mapping for calculation rows
    NULL,
    CURRENT_USER(),
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    TRUE
);

-- =====================================================
-- MASTER VIEW - Flatten JSON Properties
-- =====================================================

CREATE OR REPLACE VIEW VW_HIERARCHY_MASTER_COMPLETE AS
SELECT 
    -- Identity
    h.PROJECT_NAME,
    h.HIERARCHY_ID,
    h.HIERARCHY_PARENT_ID,
    
    -- Expanded Levels (Up to 15)
    h.HIERARCHY_LEVEL:level_1::VARCHAR AS LEVEL_1,
    h.HIERARCHY_LEVEL:level_2::VARCHAR AS LEVEL_2,
    h.HIERARCHY_LEVEL:level_3::VARCHAR AS LEVEL_3,
    h.HIERARCHY_LEVEL:level_4::VARCHAR AS LEVEL_4,
    h.HIERARCHY_LEVEL:level_5::VARCHAR AS LEVEL_5,
    h.HIERARCHY_LEVEL:level_6::VARCHAR AS LEVEL_6,
    h.HIERARCHY_LEVEL:level_7::VARCHAR AS LEVEL_7,
    h.HIERARCHY_LEVEL:level_8::VARCHAR AS LEVEL_8,
    h.HIERARCHY_LEVEL:level_9::VARCHAR AS LEVEL_9,
    h.HIERARCHY_LEVEL:level_10::VARCHAR AS LEVEL_10,
    h.HIERARCHY_LEVEL:level_11::VARCHAR AS LEVEL_11,
    h.HIERARCHY_LEVEL:level_12::VARCHAR AS LEVEL_12,
    h.HIERARCHY_LEVEL:level_13::VARCHAR AS LEVEL_13,
    h.HIERARCHY_LEVEL:level_14::VARCHAR AS LEVEL_14,
    h.HIERARCHY_LEVEL:level_15::VARCHAR AS LEVEL_15,
    
    -- Sort Orders
    h.HIERARCHY_LEVEL:level_1_sort::NUMBER AS LEVEL_1_SORT,
    h.HIERARCHY_LEVEL:level_2_sort::NUMBER AS LEVEL_2_SORT,
    h.HIERARCHY_LEVEL:level_3_sort::NUMBER AS LEVEL_3_SORT,
    h.HIERARCHY_LEVEL:level_4_sort::NUMBER AS LEVEL_4_SORT,
    h.HIERARCHY_LEVEL:level_5_sort::NUMBER AS LEVEL_5_SORT,
    h.HIERARCHY_LEVEL:level_6_sort::NUMBER AS LEVEL_6_SORT,
    h.HIERARCHY_LEVEL:level_7_sort::NUMBER AS LEVEL_7_SORT,
    h.HIERARCHY_LEVEL:level_8_sort::NUMBER AS LEVEL_8_SORT,
    h.HIERARCHY_LEVEL:level_9_sort::NUMBER AS LEVEL_9_SORT,
    h.HIERARCHY_LEVEL:level_10_sort::NUMBER AS LEVEL_10_SORT,
    h.HIERARCHY_LEVEL:level_11_sort::NUMBER AS LEVEL_11_SORT,
    h.HIERARCHY_LEVEL:level_12_sort::NUMBER AS LEVEL_12_SORT,
    h.HIERARCHY_LEVEL:level_13_sort::NUMBER AS LEVEL_13_SORT,
    h.HIERARCHY_LEVEL:level_14_sort::NUMBER AS LEVEL_14_SORT,
    h.HIERARCHY_LEVEL:level_15_sort::NUMBER AS LEVEL_15_SORT,
    
    -- Flags
    h.FLAGS:active_flag::BOOLEAN AS ACTIVE_FLAG,
    h.FLAGS:calculation_flag::BOOLEAN AS CALCULATION_FLAG,
    h.FLAGS:volume_flag::BOOLEAN AS VOLUME_FLAG,
    h.FLAGS:exclusion_flag::BOOLEAN AS EXCLUSION_FLAG,
    h.FLAGS:sign_change_flag::BOOLEAN AS SIGN_CHANGE_FLAG,
    h.FLAGS:id_unpivot_flag::BOOLEAN AS ID_UNPIVOT_FLAG,
    h.FLAGS:id_row_flag::BOOLEAN AS ID_ROW_FLAG,
    h.FLAGS:do_not_expand_flag::BOOLEAN AS DO_NOT_EXPAND_FLAG,
    h.FLAGS:create_new_column::BOOLEAN AS CREATE_NEW_COLUMN,
    
    -- Formula Config
    h.FORMULA_CONFIG:formula_group::VARCHAR AS FORMULA_GROUP,
    h.FORMULA_CONFIG:formula_precedence::NUMBER AS FORMULA_PRECEDENCE,
    h.FORMULA_CONFIG:formula_param_ref::VARCHAR AS FORMULA_PARAM_REF,
    h.FORMULA_CONFIG:arithmetic_logic::VARCHAR AS ARITHMETIC_LOGIC,
    h.FORMULA_CONFIG:formula_param2_const_number::NUMBER AS FORMULA_PARAM2_CONST_NUMBER,
    
    -- Filter Config
    h.FILTER_CONFIG:filter_group_1::VARCHAR AS FILTER_GROUP_1,
    h.FILTER_CONFIG:filter_group_2::VARCHAR AS FILTER_GROUP_2,
    h.FILTER_CONFIG:filter_group_3::VARCHAR AS FILTER_GROUP_3,
    h.FILTER_CONFIG:filter_group_4::VARCHAR AS FILTER_GROUP_4,
    
    -- Mapping Count
    ARRAY_SIZE(h.MAPPING) AS MAPPING_COUNT,
    
    -- Full JSON (for complex operations)
    h.HIERARCHY_LEVEL AS HIERARCHY_LEVEL_JSON,
    h.FLAGS AS FLAGS_JSON,
    h.FORMULA_CONFIG AS FORMULA_CONFIG_JSON,
    h.FILTER_CONFIG AS FILTER_CONFIG_JSON,
    h.MAPPING AS MAPPING_JSON,
    h.PIVOT_CONFIG AS PIVOT_CONFIG_JSON,
    
    -- Metadata
    h.CREATED_BY,
    h.CREATED_AT,
    h.UPDATED_AT,
    h.IS_ACTIVE
    
FROM TBL_HIERARCHY_MASTER h
WHERE h.IS_ACTIVE = TRUE;

-- =====================================================
-- FLATTENED MAPPING VIEW - Unfold Mapping Array
-- =====================================================

CREATE OR REPLACE VIEW VW_HIERARCHY_MAPPING_FLATTENED AS
SELECT 
    h.PROJECT_NAME,
    h.HIERARCHY_ID,
    h.HIERARCHY_PARENT_ID,
    -- Flatten mapping array with LATERAL FLATTEN
    m.VALUE:mapping_index::NUMBER AS MAPPING_INDEX,
    m.VALUE:source_database::VARCHAR AS SOURCE_DATABASE,
    m.VALUE:source_schema::VARCHAR AS SOURCE_SCHEMA,
    m.VALUE:source_table::VARCHAR AS SOURCE_TABLE,
    m.VALUE:source_column::VARCHAR AS SOURCE_COLUMN,
    m.VALUE:source_type::VARCHAR AS SOURCE_TYPE,
    m.VALUE:filter_expression::VARCHAR AS FILTER_EXPRESSION,
    m.VALUE:transformation::VARCHAR AS TRANSFORMATION,
    -- Mapping-level flags
    m.VALUE:flags:include_flag::BOOLEAN AS MAPPING_INCLUDE_FLAG,
    m.VALUE:flags:exclude_flag::BOOLEAN AS MAPPING_EXCLUDE_FLAG,
    m.VALUE:flags:transform_flag::BOOLEAN AS MAPPING_TRANSFORM_FLAG,
    m.VALUE:flags:active_flag::BOOLEAN AS MAPPING_ACTIVE_FLAG,
    m.INDEX AS ARRAY_POSITION
FROM TBL_HIERARCHY_MASTER h,
LATERAL FLATTEN(input => h.MAPPING) m
WHERE h.IS_ACTIVE = TRUE
  AND m.VALUE:flags:active_flag::BOOLEAN = TRUE;

-- =====================================================
-- QUERY EXAMPLES
-- =====================================================

-- 1. View all hierarchies with flattened properties
SELECT * FROM VW_HIERARCHY_MASTER_COMPLETE;

-- 2. View all mappings flattened
SELECT * FROM VW_HIERARCHY_MAPPING_FLATTENED
ORDER BY PROJECT_NAME, HIERARCHY_ID, MAPPING_INDEX;

-- 3. Get hierarchy tree structure
SELECT 
    parent.PROJECT_NAME,
    parent.HIERARCHY_ID AS PARENT_ID,
    parent.LEVEL_1 AS PARENT_LEVEL,
    child.HIERARCHY_ID AS CHILD_ID,
    child.LEVEL_2 AS CHILD_LEVEL
FROM VW_HIERARCHY_MASTER_COMPLETE parent
LEFT JOIN VW_HIERARCHY_MASTER_COMPLETE child
    ON parent.PROJECT_NAME = child.PROJECT_NAME
    AND parent.HIERARCHY_ID = child.HIERARCHY_PARENT_ID;

-- 4. Count mappings per hierarchy
SELECT 
    PROJECT_NAME,
    HIERARCHY_ID,
    COUNT(*) AS MAPPING_COUNT
FROM VW_HIERARCHY_MAPPING_FLATTENED
GROUP BY PROJECT_NAME, HIERARCHY_ID;

-- 5. Get hierarchies with calculation flag
SELECT 
    PROJECT_NAME,
    HIERARCHY_ID,
    LEVEL_1,
    LEVEL_2,
    FORMULA_GROUP
FROM VW_HIERARCHY_MASTER_COMPLETE
WHERE CALCULATION_FLAG = TRUE;

-- 6. Get all mappings from specific database
SELECT *
FROM VW_HIERARCHY_MAPPING_FLATTENED
WHERE SOURCE_DATABASE = 'TRANSFORMATION'
ORDER BY PROJECT_NAME, HIERARCHY_ID, MAPPING_INDEX;

-- =====================================================
-- STORED PROCEDURE - Generate Dynamic Table Script
-- =====================================================

CREATE OR REPLACE PROCEDURE SP_GENERATE_DT_SCRIPT(
    P_PROJECT_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_sql VARCHAR;
BEGIN
    v_sql := '
-- Auto-Generated Dynamic Table for Project: ' || :P_PROJECT_NAME || '
-- Generated: ' || CURRENT_TIMESTAMP()::VARCHAR || '

CREATE OR REPLACE DYNAMIC TABLE DT_' || REPLACE(:P_PROJECT_NAME, ' ', '_') || '_HIERARCHY_EXPANSION
TARGET_LAG = ''1 hour''
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = COMPUTE_WH
AS
WITH HIERARCHY_BASE AS (
    SELECT * FROM VW_HIERARCHY_MASTER_COMPLETE
    WHERE PROJECT_NAME = ''' || :P_PROJECT_NAME || '''
),
MAPPING_FLATTENED AS (
    SELECT * FROM VW_HIERARCHY_MAPPING_FLATTENED
    WHERE PROJECT_NAME = ''' || :P_PROJECT_NAME || '''
)
-- Add your account join logic here based on mappings
SELECT 
    h.*,
    m.MAPPING_INDEX,
    m.SOURCE_DATABASE,
    m.SOURCE_TABLE,
    m.FILTER_EXPRESSION
FROM HIERARCHY_BASE h
LEFT JOIN MAPPING_FLATTENED m
    ON h.PROJECT_NAME = m.PROJECT_NAME
    AND h.HIERARCHY_ID = m.HIERARCHY_ID
WHERE h.ACTIVE_FLAG = TRUE
ORDER BY h.LEVEL_1_SORT, h.LEVEL_2_SORT;
';
    
    RETURN v_sql;
END;
$$;

-- Example: Generate DT script for a project
CALL SP_GENERATE_DT_SCRIPT('PROJECT_ALPHA');

-- =====================================================
-- COMPLETION MESSAGE
-- =====================================================

SELECT 'Smart Hierarchy Builder (Single Table) created successfully!' AS STATUS;

-- Next Steps:
-- 1. Insert your hierarchy data using the JSON format
-- 2. Query VW_HIERARCHY_MASTER_COMPLETE for flattened view
-- 3. Query VW_HIERARCHY_MAPPING_FLATTENED for mapping details
-- 4. Generate Dynamic Tables using SP_GENERATE_DT_SCRIPT
-- 5. Export project as JSON for backup/import
