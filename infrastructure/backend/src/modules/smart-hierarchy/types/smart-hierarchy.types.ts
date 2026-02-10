// Smart Hierarchy TypeScript Types
// Matches Snowflake JSON structure

export interface HierarchyLevel {
  level_1?: string;
  level_2?: string;
  level_3?: string;
  level_4?: string;
  level_5?: string;
  level_6?: string;
  level_7?: string;
  level_8?: string;
  level_9?: string;
  level_10?: string;
  level_11?: string;
  level_12?: string;
  level_13?: string;
  level_14?: string;
  level_15?: string;
}

export interface HierarchyFlags {
  include_flag: boolean;
  exclude_flag: boolean;
  transform_flag: boolean;
  calculation_flag?: boolean;
  active_flag: boolean;
  is_leaf_node: boolean;
  customFlags?: Record<string, boolean>;
}

export interface SourceMappingFlags {
  include_flag: boolean;
  exclude_flag: boolean;
  transform_flag: boolean;
  active_flag: boolean;
  customFlags?: Record<string, boolean>;
}

// FP&A: Join key configuration for multi-table joins
export interface JoinKeyConfig {
  source_column: string;
  target_column: string;
  operator?: '=' | 'LIKE' | 'IN';
}

// FP&A: System types for multi-system comparison
export type SystemType = 'ACTUALS' | 'BUDGET' | 'FORECAST' | 'PRIOR_YEAR' | 'CUSTOM';

// FP&A: Dimension role for join prioritization
export type DimensionRole = 'PRIMARY' | 'SECONDARY' | 'OPTIONAL';

// FP&A: Join type for fact table generation
export type JoinType = 'INNER' | 'LEFT' | 'RIGHT' | 'FULL';

export interface SourceMapping {
  mapping_index: number;
  source_database: string;
  source_schema: string;
  source_table: string;
  source_column: string;
  source_uid?: string;
  hierarchy_key_column?: string;
  precedence_group?: string;

  // FP&A: New fields for multi-system mapping and fact table generation
  join_type?: JoinType;          // Default: 'LEFT'
  system_type?: SystemType;       // Default: 'ACTUALS'
  dimension_role?: DimensionRole; // Default based on mapping_index
  fact_table_ref?: string;        // Reference to fact table
  join_keys?: JoinKeyConfig[];    // Multi-column join keys

  flags: SourceMappingFlags;
}

export interface FormulaRule {
  operation: 'SUM' | 'SUBTRACT' | 'MULTIPLY' | 'DIVIDE' | 'AVERAGE';
  hierarchyId: string;
  hierarchyName: string;
  FORMULA_PRECEDENCE?: number;
  FORMULA_PARAM_REF?: string;
  FORMULA_PARAM2_CONST_NUMBER?: number;
}

export interface FormulaGroup {
  id: string; // Unique formula group ID
  groupName: string;
  mainHierarchy: string;
  mainHierarchyId: string;
  rules: FormulaRule[];
  formula_params?: Record<string, any>;
  createdAt?: Date;
  updatedAt?: Date;
}

export interface FormulaGroupReference {
  formulaGroupId: string;
  role?: 'MAIN' | 'CONTRIBUTOR'; // MAIN = stores result, CONTRIBUTOR = provides input
  FORMULA_PRECEDENCE?: number;
  FORMULA_PARAM_REF?: string;
  FORMULA_PARAM2_CONST_NUMBER?: number;
}

export interface FormulaConfig {
  formula_type: 'SQL' | 'EXPRESSION' | 'AGGREGATE';
  formula_text: string;
  variables?: Record<string, any>;
  formula_group_ref?: FormulaGroupReference; // Reference to formula group
}

export interface FilterGroupReference {
  filterGroupId: string;
  filterGroupName: string;
}

export interface FilterGroup {
  id: string; // Unique filter group ID
  groupName: string;
  filter_group_1?: string;
  filter_group_2?: string;
  filter_group_3?: string;
  filter_group_4?: string;
  filter_conditions: FilterCondition[];
  custom_sql?: string;
  assignedHierarchies?: string[]; // Array of hierarchy IDs using this filter
  createdAt?: Date;
  updatedAt?: Date;
}

export interface FilterCondition {
  column: string;
  operator: string;
  value: any;
  logic?: 'AND' | 'OR';
}

export interface FilterConfig {
  filter_group_1?: string;
  filter_group_2?: string;
  filter_group_3?: string;
  filter_group_4?: string;
  filter_conditions: FilterCondition[];
  custom_sql?: string;
  filter_group_ref?: FilterGroupReference; // Reference to filter group
}

export interface PivotConfig {
  pivot_columns: string[];
  aggregate_functions: Record<string, string>;
}

export interface SmartHierarchyMaster {
  id?: string;
  projectId: string;
  hierarchyId: string;
  hierarchyName: string;
  description?: string;
  // Tree structure fields
  parentId?: string | null;
  isRoot?: boolean;
  sortOrder?: number;
  // JSON fields
  hierarchyLevel: HierarchyLevel;
  flags: HierarchyFlags;
  mapping: SourceMapping[];
  formulaConfig?: FormulaConfig;
  filterConfig?: FilterConfig;
  pivotConfig?: PivotConfig;
  metadata?: Record<string, any>;
  createdBy?: string;
  createdAt?: Date;
  updatedAt?: Date;
}

export interface ProjectExportData {
  project_info: {
    id: string;
    name: string;
    description?: string;
    created_at: Date;
  };
  hierarchies: SmartHierarchyMaster[];
  metadata?: Record<string, any>;
}

export interface SmartHierarchyExport {
  id?: string;
  projectId: string;
  exportName: string;
  description?: string;
  exportData: ProjectExportData;
  exportType: 'manual' | 'auto-backup' | 'snapshot';
  version?: string;
  createdBy?: string;
  createdAt?: Date;
}

export interface SmartHierarchyScript {
  id?: string;
  projectId: string;
  hierarchyId: string;
  scriptType: 'insert' | 'view' | 'dt' | 'all';
  scriptContent: string;
  generatedAt?: Date;
  deployedAt?: Date;
  deploymentStatus?: 'pending' | 'success' | 'failed';
  errorMessage?: string;
}

// ============================================================================
// FP&A PLATFORM TYPES (Phase 11-16)
// ============================================================================

// Variance comparison configuration
export interface VarianceComparison {
  name: string;              // "Actual vs Budget"
  minuend: SystemType;       // ACTUALS (what we subtract from)
  subtrahend: SystemType;    // BUDGET (what we subtract)
  includePercent: boolean;   // Calculate variance %
}

export interface VarianceConfig {
  enabled: boolean;
  comparisons: VarianceComparison[];
}

// Fact table configuration
export interface FactTableConfig {
  id?: string;
  projectId: string;
  factTableName: string;
  databaseName?: string;
  schemaName?: string;
  includeVariance: boolean;
  varianceConfig?: VarianceConfig;
  systemColumns?: SystemType[];    // Which system types to include
  createdAt?: Date;
  updatedAt?: Date;
}

// Fact table script output
export interface FactTableScript {
  createTableScript: string;       // DDL for fact table
  insertScript: string;            // MERGE/INSERT statement
  joinLogic: string;               // JOIN clauses with proper types
  varianceColumns: string[];       // Calculated variance columns
}

// Query execution result
export interface PreviewQueryResult {
  columns: Array<{
    name: string;
    type: string;
    nullable: boolean;
  }>;
  rows: Record<string, any>[];
  rowCount: number;
  executionTimeMs: number;
  truncated: boolean;
  query: string;
}

// Query cache entry
export interface QueryCacheEntry {
  id?: string;
  cacheKey: string;              // SHA-256 hash of query + params
  connectionId: string;
  queryHash: string;
  resultData: PreviewQueryResult;
  rowCount: number;
  executionTimeMs: number;
  createdAt?: Date;
  expiresAt?: Date;
}

// Excel import result
export interface ExcelParseResult {
  fileName: string;
  sheets: Array<{
    name: string;
    rowCount: number;
    columnCount: number;
    headers: string[];
    sampleRows: Record<string, any>[];
  }>;
  detectedFormat: 'hierarchy' | 'mapping' | 'combined' | 'unknown';
  suggestions: {
    hierarchyColumns: string[];
    mappingColumns: string[];
    levelColumns: string[];
    idColumn?: string;
    parentColumn?: string;
    nameColumn?: string;
  };
}

// Excel import mapping
export interface ExcelColumnMapping {
  excelColumn: string;
  targetField: string;
  transform?: 'none' | 'uppercase' | 'lowercase' | 'trim' | 'number';
}

// Excel import history
export interface ExcelImportHistory {
  id?: string;
  projectId: string;
  fileName: string;
  fileSize?: number;
  sheetsImported: string[];
  hierarchiesCreated: number;
  mappingsCreated: number;
  importStatus: 'pending' | 'success' | 'partial' | 'failed';
  errorMessage?: string;
  importedBy?: string;
  importedAt?: Date;
}

// AI mapping pattern (for learning)
export interface MappingPattern {
  id?: string;
  sourcePattern: string;           // e.g., "account*", "gl_*"
  targetHierarchyPattern: string;  // e.g., "P&L > Revenue"
  confidence: number;
  usageCount: number;
  lastUsed?: Date;
  createdAt?: Date;
}

// AI mapping suggestion
export interface AIMappingSuggestion {
  id?: string;
  hierarchyId: string;
  suggestedMapping: Partial<SourceMapping>;
  confidenceScore: number;
  userFeedback?: 'accepted' | 'rejected' | 'modified' | null;
  createdAt?: Date;
}

// AI chat history
export interface AIChatMessage {
  id?: string;
  projectId: string;
  userId: string;
  messageRole: 'user' | 'assistant' | 'system';
  messageContent: string;
  contextSnapshot?: Record<string, any>;
  createdAt?: Date;
}
