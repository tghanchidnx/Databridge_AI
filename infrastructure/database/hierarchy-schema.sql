-- ============================================================================
-- HIERARCHY SYSTEM - SNOWFLAKE SCHEMA
-- ============================================================================
-- Purpose: Proper separation of hierarchies, nodes, details, and formulas
-- Project-scoped tables for multi-tenant support
-- ============================================================================

-- 1. HIERARCHIES TABLE (Container Level)
-- Stores hierarchy definitions with formulas and flags at container level
CREATE TABLE IF NOT EXISTS HIERARCHIES (
  ID VARCHAR(36) PRIMARY KEY,
  PROJECT_ID VARCHAR(36) NOT NULL,
  NAME VARCHAR(500) NOT NULL,
  DESCRIPTION TEXT,
  
  -- Hierarchy-level flags (apply to all nodes unless overridden)
  DEFAULT_DISPLAY_FORMAT VARCHAR(100),
  DEFAULT_AGGREGATION VARCHAR(50),
  
  -- Hierarchy-level formulas (can reference {NODE_VALUE} placeholder)
  BASE_FORMULA TEXT,
  BASE_SQL_SCRIPT TEXT,
  
  -- Metadata
  CREATED_BY VARCHAR(100),
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  
  -- Indexes
  INDEX idx_hierarchies_project (PROJECT_ID)
);

-- 2. HIERARCHY_NODES TABLE (Node Level)
-- Simplified - just tree structure and basic properties
CREATE TABLE IF NOT EXISTS HIERARCHY_NODES (
  ID VARCHAR(36) PRIMARY KEY,
  HIERARCHY_ID VARCHAR(36) NOT NULL,
  NAME VARCHAR(500) NOT NULL,
  PARENT_ID VARCHAR(36),
  ORDER_INDEX INT DEFAULT 0,
  NODE_TYPE VARCHAR(100) DEFAULT 'node',
  DESCRIPTION TEXT,
  
  -- Node can override hierarchy defaults
  OVERRIDE_FORMULA TEXT,
  
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  
  FOREIGN KEY (HIERARCHY_ID) REFERENCES HIERARCHIES(ID) ON DELETE CASCADE,
  INDEX idx_nodes_hierarchy (HIERARCHY_ID),
  INDEX idx_nodes_parent (PARENT_ID)
);

-- 3. NODE_DETAILS TABLE (Source Mappings & Precedence)
-- One-to-many: A node can have multiple detail entries (precedence groups)
CREATE TABLE IF NOT EXISTS NODE_DETAILS (
  ID VARCHAR(36) PRIMARY KEY,
  NODE_ID VARCHAR(36) NOT NULL,
  
  -- Precedence grouping
  PRECEDENCE_GROUP INT DEFAULT 1,
  GROUP_ORDER INT DEFAULT 0,
  
  -- Source configuration
  SOURCE_DATABASE VARCHAR(255),
  SOURCE_SCHEMA VARCHAR(255),
  SOURCE_TABLE VARCHAR(255),
  SOURCE_COLUMN VARCHAR(255),
  SOURCE_UID VARCHAR(100), -- UUID/ID from source table
  
  -- Exclusion flag (like reference image)
  EXCLUSION_FLAG BOOLEAN DEFAULT FALSE,
  ACTIVE_FLAG BOOLEAN DEFAULT TRUE,
  
  -- Custom filters (JSON object)
  CUSTOM_FILTERS TEXT,
  
  -- Linking to other procedures/nodes
  LINKED_PROCEDURE_ID VARCHAR(36),
  
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  
  FOREIGN KEY (NODE_ID) REFERENCES HIERARCHY_NODES(ID) ON DELETE CASCADE,
  INDEX idx_details_node (NODE_ID),
  INDEX idx_details_precedence (NODE_ID, PRECEDENCE_GROUP, GROUP_ORDER)
);

-- 4. HIERARCHY_FORMULAS TABLE (Formula Templates)
-- Reusable formula definitions at hierarchy level
CREATE TABLE IF NOT EXISTS HIERARCHY_FORMULAS (
  ID VARCHAR(36) PRIMARY KEY,
  HIERARCHY_ID VARCHAR(36) NOT NULL,
  
  -- Formula type
  FORMULA_TYPE VARCHAR(50) NOT NULL, -- 'SQL', 'EXPRESSION', 'AGGREGATE'
  FORMULA_NAME VARCHAR(200),
  
  -- Formula content
  FORMULA_TEXT TEXT NOT NULL,
  
  -- Variables that can be substituted
  VARIABLES TEXT, -- JSON array of {name, description, defaultValue}
  
  -- Enabled/disabled
  IS_ENABLED BOOLEAN DEFAULT TRUE,
  
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  
  FOREIGN KEY (HIERARCHY_ID) REFERENCES HIERARCHIES(ID) ON DELETE CASCADE,
  INDEX idx_formulas_hierarchy (HIERARCHY_ID),
  INDEX idx_formulas_type (FORMULA_TYPE)
);

-- 5. NODE_FORMULA_APPLICATIONS TABLE (Applied Formulas)
-- Tracks which formulas are applied to which nodes with specific values
CREATE TABLE IF NOT EXISTS NODE_FORMULA_APPLICATIONS (
  ID VARCHAR(36) PRIMARY KEY,
  NODE_ID VARCHAR(36) NOT NULL,
  FORMULA_ID VARCHAR(36) NOT NULL,
  
  -- Variable substitutions (JSON object {varName: value})
  VARIABLE_VALUES TEXT,
  
  -- Order of application (if multiple formulas per node)
  APPLICATION_ORDER INT DEFAULT 0,
  
  IS_ACTIVE BOOLEAN DEFAULT TRUE,
  
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  
  FOREIGN KEY (NODE_ID) REFERENCES HIERARCHY_NODES(ID) ON DELETE CASCADE,
  FOREIGN KEY (FORMULA_ID) REFERENCES HIERARCHY_FORMULAS(ID) ON DELETE CASCADE,
  INDEX idx_applications_node (NODE_ID),
  INDEX idx_applications_formula (FORMULA_ID)
);

-- ============================================================================
-- HELPER VIEWS
-- ============================================================================

-- View: Complete hierarchy with nodes and details
CREATE OR REPLACE VIEW V_HIERARCHY_COMPLETE AS
SELECT 
  h.ID as HIERARCHY_ID,
  h.NAME as HIERARCHY_NAME,
  h.BASE_FORMULA,
  n.ID as NODE_ID,
  n.NAME as NODE_NAME,
  n.PARENT_ID,
  n.ORDER_INDEX,
  nd.ID as DETAIL_ID,
  nd.PRECEDENCE_GROUP,
  nd.GROUP_ORDER,
  nd.SOURCE_DATABASE,
  nd.SOURCE_SCHEMA,
  nd.SOURCE_TABLE,
  nd.SOURCE_COLUMN,
  nd.SOURCE_UID,
  nd.EXCLUSION_FLAG,
  nd.ACTIVE_FLAG
FROM HIERARCHIES h
LEFT JOIN HIERARCHY_NODES n ON h.ID = n.HIERARCHY_ID
LEFT JOIN NODE_DETAILS nd ON n.ID = nd.NODE_ID
WHERE nd.ACTIVE_FLAG = TRUE OR nd.ACTIVE_FLAG IS NULL
ORDER BY h.NAME, n.ORDER_INDEX, nd.PRECEDENCE_GROUP, nd.GROUP_ORDER;

-- View: Node with applied formulas
CREATE OR REPLACE VIEW V_NODE_FORMULAS AS
SELECT 
  n.ID as NODE_ID,
  n.NAME as NODE_NAME,
  f.FORMULA_TYPE,
  f.FORMULA_NAME,
  f.FORMULA_TEXT,
  nfa.VARIABLE_VALUES,
  nfa.APPLICATION_ORDER
FROM HIERARCHY_NODES n
JOIN NODE_FORMULA_APPLICATIONS nfa ON n.ID = nfa.NODE_ID
JOIN HIERARCHY_FORMULAS f ON nfa.FORMULA_ID = f.ID
WHERE nfa.IS_ACTIVE = TRUE
ORDER BY n.NAME, nfa.APPLICATION_ORDER;

-- ============================================================================
-- SAMPLE DATA (for testing)
-- ============================================================================

-- Insert sample hierarchy
INSERT INTO HIERARCHIES (ID, PROJECT_ID, NAME, DESCRIPTION, BASE_FORMULA)
VALUES (
  'h1',
  '7015facd-5eb5-4055-8042-57d1912b2e56',
  'EnP LOS Summary',
  'Energy Product Line of Service Summary',
  'SELECT SUM({NODE_VALUE}) as TOTAL'
);

-- Insert sample nodes
INSERT INTO HIERARCHY_NODES (ID, HIERARCHY_ID, NAME, PARENT_ID, ORDER_INDEX, NODE_TYPE)
VALUES 
  ('n1', 'h1', 'Financial Report', NULL, 0, 'group'),
  ('n2', 'h1', 'Revenue', 'n1', 0, 'node'),
  ('n3', 'h1', 'Net Sales', 'n2', 0, 'node'),
  ('n4', 'h1', 'Expenses', 'n1', 1, 'node');

-- Insert sample node details (precedence groups)
INSERT INTO NODE_DETAILS (ID, NODE_ID, PRECEDENCE_GROUP, GROUP_ORDER, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, SOURCE_UID, ACTIVE_FLAG)
VALUES
  ('d1', 'n3', 1, 0, 'EDW', 'FINANCIAL', 'DIM_ACCOUNT', 'MINOR_CODE', '4-10-150-510', TRUE),
  ('d2', 'n3', 1, 1, 'EDW', 'FINANCIAL', 'DIM_ACCOUNT', 'MINOR_CODE', '4-10-100-503', TRUE);

-- Insert sample formulas
INSERT INTO HIERARCHY_FORMULAS (ID, HIERARCHY_ID, FORMULA_TYPE, FORMULA_NAME, FORMULA_TEXT, VARIABLES, IS_ENABLED)
VALUES (
  'f1',
  'h1',
  'SQL',
  'Sum by Account Code',
  'SELECT SUM(amount) FROM {TABLE} WHERE account_code = {ACCOUNT_CODE}',
  '[{"name": "TABLE", "description": "Source table"}, {"name": "ACCOUNT_CODE", "description": "Account code to filter"}]',
  TRUE
);

-- ============================================================================
-- CLEANUP (for development - removes all data)
-- ============================================================================
-- TRUNCATE TABLE NODE_FORMULA_APPLICATIONS;
-- TRUNCATE TABLE HIERARCHY_FORMULAS;
-- TRUNCATE TABLE NODE_DETAILS;
-- TRUNCATE TABLE HIERARCHY_NODES;
-- TRUNCATE TABLE HIERARCHIES;

-- DROP TABLE IF EXISTS NODE_FORMULA_APPLICATIONS;
-- DROP TABLE IF EXISTS HIERARCHY_FORMULAS;
-- DROP TABLE IF EXISTS NODE_DETAILS;
-- DROP TABLE IF EXISTS HIERARCHY_NODES;
-- DROP TABLE IF EXISTS HIERARCHIES;
-- DROP VIEW IF EXISTS V_HIERARCHY_COMPLETE;
-- DROP VIEW IF EXISTS V_NODE_FORMULAS;
