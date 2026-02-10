import axiosInstance from "@/lib/axios";

// ============================================================================
// Types
// ============================================================================

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

export interface SourceMapping {
  mapping_index: number;
  source_database: string;
  source_schema: string;
  source_table: string;
  source_column: string;
  source_uid?: string;
  precedence_group?: string;
  flags: SourceMappingFlags;
}

export interface FormulaRule {
  operation: "SUM" | "SUBTRACT" | "MULTIPLY" | "DIVIDE" | "AVERAGE";
  hierarchyId: string;
  hierarchyName: string;
  FORMULA_PRECEDENCE?: number;
  FORMULA_PARAM_REF?: string;
  FORMULA_PARAM2_CONST_NUMBER?: number;
}

export interface FormulaGroup {
  id?: string; // Optional for creating, required after creation
  groupName: string;
  mainHierarchy: string;
  mainHierarchyId: string;
  rules: FormulaRule[];
  formula_params?: Record<string, any>;
  createdAt?: string;
  updatedAt?: string;
}

export interface FormulaGroupReference {
  formulaGroupId: string;
  role?: "MAIN" | "CONTRIBUTOR"; // MAIN = stores result, CONTRIBUTOR = provides input
  FORMULA_PRECEDENCE?: number;
  FORMULA_PARAM_REF?: string;
  FORMULA_PARAM2_CONST_NUMBER?: number;
}

export interface FormulaConfig {
  formula_type: "SQL" | "EXPRESSION" | "AGGREGATE";
  formula_text: string;
  variables?: Record<string, any>;
  formula_group_ref?: FormulaGroupReference; // Reference to formula group
}

export interface FilterCondition {
  column: string;
  operator: string;
  value: any;
  logic?: "AND" | "OR";
}

export interface FilterGroupReference {
  filterGroupId: string;
  filterGroupName: string;
}

export interface FilterGroup {
  id?: string; // Optional for creating, required after creation
  groupName: string;
  filter_group_1?: string;
  filter_group_1_type?: string;
  filter_group_2?: string;
  filter_group_2_type?: string;
  filter_group_3?: string;
  filter_group_3_type?: string;
  filter_group_4?: string;
  filter_group_4_type?: string;
  filter_conditions: FilterCondition[];
  custom_sql?: string;
  assignedHierarchies?: string[]; // Array of hierarchy IDs using this filter
  createdAt?: string;
  updatedAt?: string;
}

export interface FilterConfig {
  filter_group_1?: string;
  filter_group_1_type?: string;
  filter_group_2?: string;
  filter_group_2_type?: string;
  filter_group_3?: string;
  filter_group_3_type?: string;
  filter_group_4?: string;
  filter_group_4_type?: string;
  filter_conditions: FilterCondition[];
  custom_sql?: string;
  filter_group_ref?: FilterGroupReference; // Reference to filter group
  total_formula?: TotalFormula;
}

export interface TotalFormula {
  mainHierarchyId: string;
  mainHierarchyName: string;
  aggregation: "SUM" | "AVERAGE" | "COUNT" | "MIN" | "MAX";
  children: TotalFormulaChild[];
}

export interface TotalFormulaChild {
  hierarchyId: string;
  hierarchyName: string;
  level?: number;
}

export interface HierarchyFormulaGroup {
  mainHierarchyId: string;
  mainHierarchyName: string;
  rules: HierarchyFormulaRule[];
}

export interface HierarchyFormulaRule {
  hierarchyId: string;
  hierarchyName: string;
  operation:
    | "ADD"
    | "SUBTRACT"
    | "MULTIPLY"
    | "DIVIDE"
    | "SUM"
    | "AVERAGE"
    | "COUNT"
    | "MIN"
    | "MAX";
  precedence: number;
  parameterReference?: string;
  constantNumber?: number;
  formulaRefSource?: string;
  formulaRefTable?: string;
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
  parentId?: string | null;
  sortOrder?: number;
  isRoot?: boolean;
  hierarchyLevel: HierarchyLevel;
  flags: HierarchyFlags;
  mapping: SourceMapping[];
  formulaConfig?: FormulaConfig;
  filterConfig?: FilterConfig;
  pivotConfig?: PivotConfig;
  metadata?: Record<string, any>;
  createdBy?: string;
  updatedBy?: string;
  createdAt?: Date;
  updatedAt?: Date;
  children?: SmartHierarchyMaster[];
}

// Lightweight version for list view (minimal data for performance)
export interface SmartHierarchyListItem {
  id?: string;
  projectId: string;
  hierarchyId: string;
  hierarchyName: string;
  description?: string;
  parentId?: string | null;
  sortOrder?: number;
  isRoot?: boolean;
  flags: {
    active_flag: boolean;
  };
  levelCount: number;
  mappingCount: number;
  createdAt?: Date;
  updatedAt?: Date;
}

export interface HierarchyTree {
  id: string;
  name: string;
  description?: string;
  levels: string[];
  mappingCount: number;
  flags: HierarchyFlags;
  hasFormula: boolean;
  hasFilter: boolean;
  hasPivot: boolean;
}

export interface ProjectExport {
  exportId: string;
  exportName: string;
  description?: string;
  exportType: "manual" | "auto-backup" | "snapshot";
  version?: string;
  createdBy?: string;
  createdAt: Date;
}

export interface ScriptGenerationResult {
  hierarchyId: string;
  hierarchyName: string;
  scriptType: "insert" | "view" | "mapping" | "dt";
  databaseType: string;
  script: string;
}

export interface SnowflakePushResult {
  success: number;
  failed: number;
  results: Array<{
    hierarchyId: string;
    scriptType: string;
    status: "success" | "failed";
    error?: string;
  }>;
}

// ============================================================================
// API Service
// ============================================================================

const BASE_PATH = "/smart-hierarchy";

// Helper function to sanitize boolean values in flags
function sanitizeFlags(flags: any): any {
  if (!flags || typeof flags !== "object") return flags;

  const sanitized: any = {};
  for (const [key, value] of Object.entries(flags)) {
    if (key === "customFlags" && value && typeof value === "object") {
      // Sanitize customFlags recursively
      sanitized[key] = {};
      for (const [customKey, customValue] of Object.entries(value)) {
        sanitized[key][customKey] =
          customValue === true || customValue === "true";
      }
    } else {
      // Convert to proper boolean - handle undefined/null as false
      if (value === undefined || value === null) {
        sanitized[key] = false;
      } else if (typeof value === "string") {
        sanitized[key] = value.toLowerCase() === "true";
      } else if (typeof value === "boolean") {
        sanitized[key] = value;
      } else {
        sanitized[key] = Boolean(value);
      }
    }
  }
  return sanitized;
}

// Helper function to sanitize hierarchy data before sending to backend
function sanitizeHierarchyData(data: any): any {
  const sanitized = { ...data };

  // Remove any UI-only fields that shouldn't be sent to backend
  delete sanitized.children;
  delete sanitized.id;
  delete sanitized.createdAt;
  delete sanitized.updatedAt;

  // Sanitize flags - ensure all required fields exist
  if (sanitized.flags) {
    const cleanFlags = sanitizeFlags(sanitized.flags);
    // Ensure required hierarchy flag fields exist
    sanitized.flags = {
      include_flag: cleanFlags.include_flag ?? false,
      exclude_flag: cleanFlags.exclude_flag ?? false,
      transform_flag: cleanFlags.transform_flag ?? false,
      active_flag: cleanFlags.active_flag ?? true,
      is_leaf_node: cleanFlags.is_leaf_node ?? false,
      ...(cleanFlags.calculation_flag !== undefined && {
        calculation_flag: cleanFlags.calculation_flag,
      }),
      ...(cleanFlags.customFlags && { customFlags: cleanFlags.customFlags }),
    };
  }

  // Sanitize filterConfig - ensure filter_conditions is an array and remove UI-only fields
  if (sanitized.filterConfig) {
    if (
      typeof sanitized.filterConfig === "object" &&
      !Array.isArray(sanitized.filterConfig)
    ) {
      const { groupName, ...cleanConfig } = sanitized.filterConfig;
      sanitized.filterConfig = {
        ...cleanConfig,
        filter_conditions: Array.isArray(cleanConfig.filter_conditions)
          ? cleanConfig.filter_conditions
          : [],
      };
    }
  }

  // Sanitize formulaConfig - remove formula_group if it exists (should use formula_group_ref instead)
  if (sanitized.formulaConfig) {
    if (
      typeof sanitized.formulaConfig === "object" &&
      !Array.isArray(sanitized.formulaConfig)
    ) {
      const { formula_group, ...cleanFormulaConfig } = sanitized.formulaConfig;

      // Sanitize formula_group_ref - only keep formulaGroupId and role
      if (cleanFormulaConfig.formula_group_ref) {
        const ref = cleanFormulaConfig.formula_group_ref;
        const validRoles = ["MAIN", "CONTRIBUTOR"];

        cleanFormulaConfig.formula_group_ref = {
          formulaGroupId: ref.formulaGroupId || ref.mainHierarchyId || "",
          // Only include role if it's valid, otherwise omit it
          ...(ref.role && validRoles.includes(ref.role) && { role: ref.role }),
        };
      }

      sanitized.formulaConfig = cleanFormulaConfig;
    }
  }

  // Sanitize mapping flags
  if (sanitized.mapping && Array.isArray(sanitized.mapping)) {
    sanitized.mapping = sanitized.mapping.map((m: any) => {
      const cleanMapping = { ...m };

      // Remove UI-only fields from mapping
      delete cleanMapping.id;

      if (!cleanMapping.flags) return cleanMapping;

      const cleanFlags = sanitizeFlags(cleanMapping.flags);
      // Ensure required mapping flag fields exist (no calculation_flag for mappings)
      cleanMapping.flags = {
        include_flag: cleanFlags.include_flag ?? false,
        exclude_flag: cleanFlags.exclude_flag ?? false,
        transform_flag: cleanFlags.transform_flag ?? false,
        active_flag: cleanFlags.active_flag ?? true,
        ...(cleanFlags.customFlags && {
          customFlags: cleanFlags.customFlags,
        }),
      };

      return cleanMapping;
    });
  }

  return sanitized;
}

export class SmartHierarchyService {
  // ============================================================================
  // CRUD Operations
  // ============================================================================

  async create(
    hierarchy: Omit<SmartHierarchyMaster, "id" | "createdAt" | "updatedAt">
  ): Promise<SmartHierarchyMaster> {
    const sanitized = sanitizeHierarchyData(hierarchy);
    const response = await axiosInstance.post(BASE_PATH, sanitized);
    return response.data;
  }

  async findAll(projectId: string): Promise<SmartHierarchyMaster[]> {
    const response = await axiosInstance.get(
      `${BASE_PATH}/project/${projectId}`
    );
    return response.data;
  }

  // Get minimal list data for performance (only essential fields for tree view)
  async findAllMinimal(projectId: string): Promise<any[]> {
    const response = await axiosInstance.get(
      `${BASE_PATH}/project/${projectId}?minimal=true`
    );
    return response.data;
  }

  async getTree(projectId: string): Promise<HierarchyTree[]> {
    const response = await axiosInstance.get(
      `${BASE_PATH}/project/${projectId}/tree`
    );
    return response.data;
  }

  async findOne(
    projectId: string,
    hierarchyId: string
  ): Promise<SmartHierarchyMaster> {
    const response = await axiosInstance.get(
      `${BASE_PATH}/project/${projectId}/${hierarchyId}`
    );
    return response.data;
  }

  async getDependencies(projectId: string, hierarchyId: string): Promise<any> {
    const response = await axiosInstance.get(
      `${BASE_PATH}/project/${projectId}/${hierarchyId}/dependencies`
    );
    return response.data;
  }

  async update(
    projectId: string,
    hierarchyId: string,
    updates: Partial<SmartHierarchyMaster>
  ): Promise<SmartHierarchyMaster> {
    const sanitized = sanitizeHierarchyData(updates);
    const response = await axiosInstance.put(
      `${BASE_PATH}/project/${projectId}/${hierarchyId}`,
      sanitized
    );
    return response.data;
  }

  async delete(projectId: string, hierarchyId: string): Promise<void> {
    await axiosInstance.delete(
      `${BASE_PATH}/project/${projectId}/${hierarchyId}`
    );
  }

  async bulkDelete(
    projectId: string,
    hierarchyIds: string[]
  ): Promise<{
    deleted: number;
    failed: number;
    errors: Array<{ hierarchyId: string; error: string }>;
  }> {
    const response = await axiosInstance.post(
      `${BASE_PATH}/project/${projectId}/bulk-delete`,
      { hierarchyIds }
    );
    return response.data;
  }

  async bulkUpdateOrder(
    projectId: string,
    updates: Array<{
      hierarchyId: string;
      parentId?: string | null;
      isRoot?: boolean;
      sortOrder: number;
    }>
  ): Promise<{
    updated: number;
    failed: number;
    errors: Array<{ hierarchyId: string; error: string }>;
  }> {
    const response = await axiosInstance.post(
      `${BASE_PATH}/project/${projectId}/bulk-update-order`,
      { updates }
    );
    return response.data;
  }

  // ============================================================================
  // Export/Import Operations
  // ============================================================================

  async exportProject(
    projectId: string,
    exportName: string,
    options?: {
      description?: string;
      version?: string;
      exportType?: "manual" | "auto-backup" | "snapshot";
      format?: "json" | "csv";
    }
  ): Promise<{
    exportId: string;
    exportData: any;
    format: string;
    contentType: string;
    createdAt: Date;
  }> {
    const response = await axiosInstance.post(`${BASE_PATH}/export`, {
      projectId,
      exportName,
      format: options?.format || "json",
      ...options,
    });
    return response.data;
  }

  async importProject(
    projectId: string,
    exportData: any,
    format?: "json" | "csv"
  ): Promise<{ imported: number; skipped: number }> {
    const response = await axiosInstance.post(`${BASE_PATH}/import`, {
      projectId,
      exportData,
      format: format || "json",
    });
    return response.data;
  }

  async listExports(projectId: string): Promise<ProjectExport[]> {
    const response = await axiosInstance.get(
      `${BASE_PATH}/exports/${projectId}`
    );
    return response.data;
  }

  async getExport(exportId: string): Promise<any> {
    const response = await axiosInstance.get(`${BASE_PATH}/export/${exportId}`);
    return response.data;
  }

  // ============================================================================
  // Script Generation
  // ============================================================================

  async generateScripts(
    projectId: string,
    scriptType: "insert" | "view" | "mapping" | "dt" | "all",
    hierarchyIds?: string[],
    databaseType:
      | "snowflake"
      | "postgres"
      | "mysql"
      | "sqlserver"
      | "all" = "snowflake"
  ): Promise<{ scripts: ScriptGenerationResult[] }> {
    const response = await axiosInstance.post(`${BASE_PATH}/generate-scripts`, {
      projectId,
      hierarchyIds,
      scriptType,
      databaseType,
    });
    return response.data;
  }

  // ============================================================================
  // Snowflake Deployment
  // ============================================================================

  async pushToSnowflake(
    projectId: string,
    hierarchyIds: string[], // Changed to array
    connectionId: string,
    database: string,
    schema: string,
    masterTableName: string,
    masterViewName: string,
    databaseType: "snowflake" | "postgres" | "mysql" | "sqlserver",
    deployedBy: string, // User email
    options?: {
      createTables?: boolean;
      createViews?: boolean;
      createDynamicTables?: boolean;
      saveAsDeploymentConfig?: boolean;
    }
  ): Promise<SnowflakePushResult> {
    const response = await axiosInstance.post(
      `${BASE_PATH}/push-to-snowflake`,
      {
        projectId,
        hierarchyIds,
        connectionId,
        database,
        schema,
        masterTableName,
        masterViewName,
        databaseType,
        deployedBy,
        ...options,
      }
    );
    return response.data;
  }

  async getDeploymentConfig(projectId: string): Promise<any> {
    const response = await axiosInstance.get(
      `${BASE_PATH}/projects/${projectId}/deployment-config`
    );
    return response.data;
  }

  async getDeploymentHistory(
    projectId: string,
    limit?: number,
    offset?: number
  ): Promise<any> {
    const response = await axiosInstance.get(
      `${BASE_PATH}/projects/${projectId}/deployment-history`,
      {
        params: { limit, offset },
      }
    );
    return response.data;
  }

  async getDeploymentById(id: string): Promise<any> {
    const response = await axiosInstance.get(
      `${BASE_PATH}/deployment-history/${id}`
    );
    return response.data;
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  createEmptyHierarchy(
    projectId: string,
    hierarchyId: string
  ): Omit<SmartHierarchyMaster, "id" | "createdAt" | "updatedAt"> {
    return {
      projectId,
      hierarchyId,
      hierarchyName: "New Hierarchy",
      hierarchyLevel: {},
      flags: {
        include_flag: true,
        exclude_flag: false,
        transform_flag: false,
        active_flag: true,
        is_leaf_node: false,
        customFlags: {},
      },
      mapping: [],
    };
  }

  createEmptyMapping(index: number): SourceMapping {
    return {
      mapping_index: index,
      source_database: "",
      source_schema: "",
      source_table: "",
      source_column: "",
      precedence_group: "",
      flags: {
        include_flag: true,
        exclude_flag: false,
        transform_flag: false,
        active_flag: true,
        customFlags: {},
      },
    };
  }

  downloadScript(script: string, filename: string): void {
    const blob = new Blob([script], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  downloadJSON(data: any, filename: string): void {
    const json = JSON.stringify(data, null, 2);
    const blob = new Blob([json], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  // ============================================================================
  // Formula Management - Works with formulaConfig in smart_hierarchy_master
  // ============================================================================

  // Get all hierarchies with formulas in a project
  async getProjectFormulas(projectId: string): Promise<SmartHierarchyMaster[]> {
    const response = await axiosInstance.get(
      `/smart-hierarchy/projects/${projectId}/formulas`
    );
    return response.data;
  }

  // Get formula from specific hierarchy
  async getFormulaFromHierarchy(
    projectId: string,
    hierarchyId: string
  ): Promise<{
    hierarchyId: string;
    hierarchyName: string;
    formula_type: string;
    formula_text: string;
    formula_group: FormulaGroup | null;
    formula_group_ref: any | null;
    isMainHierarchy: boolean;
    isContributor: boolean;
  } | null> {
    const response = await axiosInstance.get(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${hierarchyId}/formula`
    );
    return response.data;
  }

  // Save/create formula in hierarchy (stores in formulaConfig)
  async saveFormulaToHierarchy(
    projectId: string,
    hierarchyId: string,
    formulaGroup: {
      groupName: string;
      rules: Array<{
        operation: string;
        hierarchyId: string;
        hierarchyName: string;
        FORMULA_PRECEDENCE?: number;
        FORMULA_PARAM_REF?: string;
        FORMULA_PARAM2_CONST_NUMBER?: number;
      }>;
      formula_params?: Record<string, any>;
    }
  ): Promise<SmartHierarchyMaster> {
    const response = await axiosInstance.put(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${hierarchyId}/formula`,
      formulaGroup
    );
    return response.data;
  }

  // Remove formula from hierarchy
  async removeFormulaFromHierarchy(
    projectId: string,
    hierarchyId: string
  ): Promise<SmartHierarchyMaster> {
    const response = await axiosInstance.delete(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${hierarchyId}/formula`
    );
    return response.data;
  }

  // Link hierarchy to existing formula group (creates reference to main hierarchy)
  async linkHierarchyToFormulaGroup(
    projectId: string,
    hierarchyId: string,
    data: {
      mainHierarchyId: string; // Hierarchy that has the formula_group
      role?: string; // Operation: SUM, SUBTRACT, etc.
      FORMULA_PRECEDENCE?: number;
      FORMULA_PARAM_REF?: string;
      FORMULA_PARAM2_CONST_NUMBER?: number;
    }
  ): Promise<SmartHierarchyMaster> {
    const response = await axiosInstance.post(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${hierarchyId}/link-formula-group`,
      data
    );
    return response.data;
  }

  // Unlink hierarchy from formula group (removes reference)
  async unlinkHierarchyFromFormulaGroup(
    projectId: string,
    hierarchyId: string
  ): Promise<SmartHierarchyMaster> {
    const response = await axiosInstance.delete(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${hierarchyId}/unlink-formula-group`
    );
    return response.data;
  }

  // ============================================================================
  // Filter Group Management (NEW - Standardized)
  // ============================================================================

  // Create a new filter group (stored separately)
  async createFilterGroup(
    projectId: string,
    filterGroup: Omit<FilterGroup, "id" | "createdAt" | "updatedAt">
  ): Promise<FilterGroup> {
    const response = await axiosInstance.post(
      `/smart-hierarchy/filter-groups`,
      { projectId, ...filterGroup }
    );
    return response.data;
  }

  // Get filter group by ID
  async getFilterGroupById(filterGroupId: string): Promise<FilterGroup> {
    const response = await axiosInstance.get(
      `/smart-hierarchy/filter-groups/${filterGroupId}`
    );
    return response.data;
  }

  // Update existing filter group
  async updateFilterGroupById(
    filterGroupId: string,
    filterGroup: Partial<FilterGroup>
  ): Promise<FilterGroup> {
    const response = await axiosInstance.put(
      `/smart-hierarchy/filter-groups/${filterGroupId}`,
      filterGroup
    );
    return response.data;
  }

  // Delete filter group
  async deleteFilterGroup(filterGroupId: string): Promise<void> {
    await axiosInstance.delete(
      `/smart-hierarchy/filter-groups/${filterGroupId}`
    );
  }

  // List all filter groups for a project
  async listFilterGroups(projectId: string): Promise<FilterGroup[]> {
    const response = await axiosInstance.get(
      `/smart-hierarchy/projects/${projectId}/filter-groups-list`
    );
    return response.data;
  }

  // Assign hierarchy to filter group (creates reference)
  async assignHierarchyToFilterGroup(
    projectId: string,
    hierarchyId: string,
    data: {
      filterGroupId: string;
      filterGroupName: string;
    }
  ): Promise<SmartHierarchyMaster> {
    const response = await axiosInstance.post(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${hierarchyId}/assign-filter-group`,
      data
    );
    return response.data;
  }

  // Unassign hierarchy from filter group (removes reference)
  async unassignHierarchyFromFilterGroup(
    projectId: string,
    hierarchyId: string
  ): Promise<SmartHierarchyMaster> {
    const response = await axiosInstance.delete(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${hierarchyId}/unassign-filter-group`
    );
    return response.data;
  }

  // Get filter attributes from a specific hierarchy for copying
  async getHierarchyFilterAttributes(
    projectId: string,
    hierarchyId: string
  ): Promise<{
    hierarchyId: string;
    hierarchyName: string;
    filter_group_1?: string;
    filter_group_2?: string;
    filter_group_3?: string;
    filter_group_4?: string;
    filter_conditions: FilterCondition[];
    custom_sql?: string;
    filter_group_ref?: FilterGroupReference;
  }> {
    const response = await axiosInstance.get(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${hierarchyId}/filter-attributes`
    );
    return response.data;
  }

  // ============================================================================
  // Legacy Methods - Aliased to new methods for backward compatibility
  // ============================================================================

  // Alias: updateFormulaGroup → saveFormulaToHierarchy
  async updateFormulaGroup(
    projectId: string,
    hierarchyId: string,
    formulaGroup: FormulaGroup
  ): Promise<SmartHierarchyMaster> {
    return this.saveFormulaToHierarchy(projectId, hierarchyId, formulaGroup);
  }

  // Alias: assignToFormulaGroup → linkHierarchyToFormulaGroup
  async assignToFormulaGroup(
    projectId: string,
    hierarchyId: string,
    data: {
      mainHierarchyId: string;
      role: string;
      FORMULA_PRECEDENCE?: number;
      FORMULA_PARAM_REF?: string;
      FORMULA_PARAM2_CONST_NUMBER?: number;
    }
  ): Promise<SmartHierarchyMaster> {
    return this.linkHierarchyToFormulaGroup(projectId, hierarchyId, data);
  }

  // Alias: unlinkFromFormulaGroup → unlinkHierarchyFromFormulaGroup
  async unlinkFromFormulaGroup(
    projectId: string,
    hierarchyId: string
  ): Promise<SmartHierarchyMaster> {
    return this.unlinkHierarchyFromFormulaGroup(projectId, hierarchyId);
  }

  // Alias: getFormulaGroup → getFormulaFromHierarchy
  async getFormulaGroup(
    projectId: string,
    hierarchyId: string
  ): Promise<any | null> {
    return this.getFormulaFromHierarchy(projectId, hierarchyId);
  }

  // Alias: removeFormulaGroup → removeFormulaFromHierarchy
  async removeFormulaGroup(
    projectId: string,
    hierarchyId: string
  ): Promise<SmartHierarchyMaster> {
    return this.removeFormulaFromHierarchy(projectId, hierarchyId);
  }

  // Get all hierarchies with formulas (with optional search filtering on client side)
  async getFormulaGroups(
    projectId: string,
    searchTerm?: string
  ): Promise<
    Array<{
      hierarchyId: string;
      hierarchyName: string;
      formulaGroup: any;
    }>
  > {
    // Get all hierarchies with formulas
    const hierarchies = await this.getProjectFormulas(projectId);

    // Transform to expected format and filter if search term provided
    let results = hierarchies
      .filter((h) => h.formulaConfig && (h.formulaConfig as any).formula_group)
      .map((h) => ({
        hierarchyId: h.hierarchyId,
        hierarchyName: h.hierarchyName,
        formulaGroup: (h.formulaConfig as any).formula_group,
      }));

    // Client-side filtering if search term provided
    if (searchTerm) {
      const lowerSearch = searchTerm.toLowerCase();
      results = results.filter(
        (r) =>
          r.hierarchyName.toLowerCase().includes(lowerSearch) ||
          r.hierarchyId.toLowerCase().includes(lowerSearch) ||
          r.formulaGroup.groupName?.toLowerCase().includes(lowerSearch)
      );
    }

    return results;
  }

  async searchHierarchies(
    projectId: string,
    searchTerm?: string,
    excludeIds?: string[]
  ): Promise<SmartHierarchyMaster[]> {
    const params = new URLSearchParams();
    if (searchTerm) params.append("searchTerm", searchTerm);
    if (excludeIds && excludeIds.length > 0) {
      params.append("excludeIds", excludeIds.join(","));
    }

    const response = await axiosInstance.get(
      `/smart-hierarchy/projects/${projectId}/hierarchies/search?${params.toString()}`
    );
    return response.data;
  }

  async getFormulaGroupUsages(
    projectId: string,
    hierarchyId: string
  ): Promise<SmartHierarchyMaster[]> {
    const response = await axiosInstance.get(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${hierarchyId}/formula-usages`
    );
    return response.data;
  }

  // ============================================================================
  // CSV Export/Import (Separate Hierarchy & Mapping)
  // ============================================================================

  async exportHierarchyCSV(
    projectId: string
  ): Promise<{ content: string; filename: string; contentType: string }> {
    const response = await axiosInstance.get(
      `/smart-hierarchy/projects/${projectId}/export-hierarchy-csv`
    );
    return response.data;
  }

  async exportMappingCSV(
    projectId: string
  ): Promise<{ content: string; filename: string; contentType: string }> {
    const response = await axiosInstance.get(
      `/smart-hierarchy/projects/${projectId}/export-mapping-csv`
    );
    return response.data;
  }

  async importHierarchyCSV(
    projectId: string,
    csvContent: string,
    isLegacyFormat: boolean = true
  ): Promise<{ imported: number; skipped: number; errors: string[] }> {
    // Backend auto-detects format, so always use the same endpoint
    // The isLegacyFormat flag is kept for UI purposes but doesn't change the endpoint
    const endpoint = `/smart-hierarchy/projects/${projectId}/import-hierarchy-csv`;

    const response = await axiosInstance.post(endpoint, { csvContent });
    return response.data;
  }

  async importHierarchyCSV_OldFormat(
    projectId: string,
    csvContent: string
  ): Promise<{ imported: number; skipped: number; errors: string[] }> {
    // Deprecated: Use importHierarchyCSV with isLegacyFormat=true instead
    return this.importHierarchyCSV(projectId, csvContent, true);
  }

  async importMappingCSV(
    projectId: string,
    csvContent: string
  ): Promise<{ imported: number; skipped: number; errors: string[] }> {
    const response = await axiosInstance.post(
      `/smart-hierarchy/projects/${projectId}/import-mapping-csv`,
      { csvContent }
    );
    return response.data;
  }

  async analyzeHierarchyCSV(
    projectId: string,
    csvContent: string
  ): Promise<{
    totalRows: number;
    uniqueHierarchies: number;
    formulaGroups: { name: string; count: number }[];
    levelDistribution: { level: number; count: number }[];
    flagStatistics: { flag: string; trueCount: number; falseCount: number }[];
    warnings: string[];
  }> {
    const response = await axiosInstance.post(
      `/smart-hierarchy/projects/${projectId}/analyze-hierarchy-csv`,
      { csvContent }
    );
    return response.data;
  }

  // ============================================================================
  // Total Formula Management Methods
  // ============================================================================

  async createOrUpdateTotalFormula(
    projectId: string,
    mainHierarchyId: string,
    data: {
      mainHierarchyName: string;
      aggregation: "SUM" | "AVERAGE" | "COUNT" | "MIN" | "MAX";
      children: TotalFormulaChild[];
    }
  ): Promise<SmartHierarchyMaster> {
    const response = await axiosInstance.post(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${mainHierarchyId}/total-formula`,
      data
    );
    return response.data;
  }

  async getTotalFormula(
    projectId: string,
    hierarchyId: string
  ): Promise<TotalFormula | null> {
    const response = await axiosInstance.get(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${hierarchyId}/total-formula`
    );
    return response.data;
  }

  async listHierarchiesWithTotalFormulas(projectId: string): Promise<
    Array<{
      hierarchyId: string;
      hierarchyName: string;
      totalFormula: TotalFormula;
    }>
  > {
    const response = await axiosInstance.get(
      `/smart-hierarchy/projects/${projectId}/total-formulas`
    );
    return response.data;
  }

  async deleteTotalFormula(
    projectId: string,
    hierarchyId: string
  ): Promise<SmartHierarchyMaster> {
    const response = await axiosInstance.delete(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${hierarchyId}/total-formula`
    );
    return response.data;
  }

  async addChildToTotalFormula(
    projectId: string,
    mainHierarchyId: string,
    child: TotalFormulaChild
  ): Promise<SmartHierarchyMaster> {
    const response = await axiosInstance.post(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${mainHierarchyId}/total-formula/children`,
      child
    );
    return response.data;
  }

  async removeChildFromTotalFormula(
    projectId: string,
    mainHierarchyId: string,
    childHierarchyId: string
  ): Promise<SmartHierarchyMaster> {
    const response = await axiosInstance.delete(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${mainHierarchyId}/total-formula/children/${childHierarchyId}`
    );
    return response.data;
  }

  // ============================================================================
  // Formula Group Management (NEW approach)
  // ============================================================================

  async createOrUpdateFormulaGroup(
    projectId: string,
    mainHierarchyId: string,
    data: Omit<HierarchyFormulaGroup, "mainHierarchyId">
  ): Promise<SmartHierarchyMaster> {
    const response = await axiosInstance.post(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${mainHierarchyId}/formula-group`,
      data
    );
    return response.data;
  }

  async getHierarchyFormulaGroup(
    projectId: string,
    hierarchyId: string
  ): Promise<HierarchyFormulaGroup | null> {
    const response = await axiosInstance.get(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${hierarchyId}/formula-group`
    );
    return response.data;
  }

  async listHierarchiesWithFormulaGroups(projectId: string): Promise<
    Array<{
      hierarchyId: string;
      hierarchyName: string;
      formulaGroup: HierarchyFormulaGroup;
    }>
  > {
    const response = await axiosInstance.get(
      `/smart-hierarchy/projects/${projectId}/formula-groups`
    );
    return response.data;
  }

  async getHierarchyFormulaInfo(
    projectId: string,
    hierarchyId: string
  ): Promise<{
    isFormulaOwner: boolean;
    isContributor: boolean;
    ownFormula?: {
      groupName: string;
      mainHierarchyId: string;
      mainHierarchyName: string;
      rules: HierarchyFormulaRule[];
      formula_params?: any;
    };
    contributorOf?: {
      mainHierarchyId: string;
      mainHierarchyName: string;
      groupName: string;
      myRole: string;
      myPrecedence?: number;
      allRules: HierarchyFormulaRule[];
    };
  } | null> {
    const response = await axiosInstance.get(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${hierarchyId}/formula-info`
    );
    return response.data;
  }

  async deleteFormulaGroup(
    projectId: string,
    hierarchyId: string
  ): Promise<SmartHierarchyMaster> {
    const response = await axiosInstance.delete(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${hierarchyId}/formula-group`
    );
    return response.data;
  }

  async addRuleToFormulaGroup(
    projectId: string,
    mainHierarchyId: string,
    rule: HierarchyFormulaRule
  ): Promise<SmartHierarchyMaster> {
    const response = await axiosInstance.post(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${mainHierarchyId}/formula-group/rules`,
      rule
    );
    return response.data;
  }

  async removeRuleFromFormulaGroup(
    projectId: string,
    mainHierarchyId: string,
    ruleHierarchyId: string
  ): Promise<SmartHierarchyMaster> {
    const response = await axiosInstance.delete(
      `/smart-hierarchy/projects/${projectId}/hierarchies/${mainHierarchyId}/formula-group/rules/${ruleHierarchyId}`
    );
    return response.data;
  }

  // ============================================================================
  // Smart CSV Import with Analysis & Auto-Fix (NEW)
  // ============================================================================

  /**
   * Analyze a CSV file before import to detect issues and suggest fixes
   */
  async analyzeCSV(
    projectId: string,
    csvContent: string,
    fileName?: string
  ): Promise<{
    isValid: boolean;
    format: "hierarchy" | "mapping" | "legacy" | "unknown";
    formatConfidence: number;
    rowCount: number;
    columnCount: number;
    columns: Array<{
      name: string;
      detectedType: string;
      nullCount: number;
      nullPercentage: number;
      uniqueCount: number;
      sampleValues: string[];
      mappedTo?: string;
    }>;
    issues: Array<{
      type: "error" | "warning" | "info";
      code: string;
      message: string;
      row?: number;
      column?: string;
      canAutoFix: boolean;
    }>;
    suggestions: Array<{
      type: string;
      message: string;
      column?: string;
      priority: "high" | "medium" | "low";
    }>;
    canAutoFix: boolean;
    autoFixActions: Array<{
      type: string;
      description: string;
      affectedRows: number;
    }>;
    stats: {
      totalRows: number;
      validRows: number;
      invalidRows: number;
      duplicateIds: number;
      orphanParents: number;
      emptyRequired: number;
    };
  }> {
    const response = await axiosInstance.post(
      `/smart-hierarchy/projects/${projectId}/analyze-csv`,
      { csvContent, fileName }
    );
    return response.data;
  }

  /**
   * Smart import hierarchy with analysis, auto-fix, and detailed logging
   */
  async smartImportHierarchy(
    projectId: string,
    csvContent: string,
    fileName?: string,
    options?: {
      autoFix?: boolean;
      validateBeforeImport?: boolean;
      dryRun?: boolean;
    }
  ): Promise<{
    success: boolean;
    logId: string;
    message: string;
    rowsTotal: number;
    rowsImported: number;
    rowsFailed: number;
    issues: Array<{ type: string; message: string; resolved: boolean }>;
    autoFixesApplied: string[];
    report: string;
    duration: number;
  }> {
    const response = await axiosInstance.post(
      `/smart-hierarchy/projects/${projectId}/smart-import-hierarchy`,
      { csvContent, fileName, options }
    );
    return response.data;
  }

  /**
   * Smart import mapping with analysis, auto-fix, and detailed logging
   */
  async smartImportMapping(
    projectId: string,
    csvContent: string,
    fileName?: string,
    options?: {
      autoFix?: boolean;
      validateBeforeImport?: boolean;
      dryRun?: boolean;
    }
  ): Promise<{
    success: boolean;
    logId: string;
    message: string;
    rowsTotal: number;
    rowsImported: number;
    rowsFailed: number;
    issues: Array<{ type: string; message: string; resolved: boolean }>;
    autoFixesApplied: string[];
    report: string;
    duration: number;
  }> {
    const response = await axiosInstance.post(
      `/smart-hierarchy/projects/${projectId}/smart-import-mapping`,
      { csvContent, fileName, options }
    );
    return response.data;
  }

  /**
   * Smart import both hierarchy and mapping with full analysis
   */
  async smartImportBoth(
    projectId: string,
    hierarchyCSV: string,
    mappingCSV: string,
    hierarchyFileName?: string,
    mappingFileName?: string,
    options?: {
      autoFix?: boolean;
      validateBeforeImport?: boolean;
      dryRun?: boolean;
    }
  ): Promise<{
    hierarchy: {
      success: boolean;
      logId: string;
      message: string;
      rowsTotal: number;
      rowsImported: number;
      rowsFailed: number;
      issues: Array<{ type: string; message: string; resolved: boolean }>;
      autoFixesApplied: string[];
    };
    mapping: {
      success: boolean;
      logId: string;
      message: string;
      rowsTotal: number;
      rowsImported: number;
      rowsFailed: number;
      issues: Array<{ type: string; message: string; resolved: boolean }>;
      autoFixesApplied: string[];
    } | null;
    overallSuccess: boolean;
  }> {
    const response = await axiosInstance.post(
      `/smart-hierarchy/projects/${projectId}/smart-import-both`,
      { hierarchyCSV, mappingCSV, hierarchyFileName, mappingFileName, options }
    );
    return response.data;
  }

  // ============================================================================
  // Chunked Upload (for large files)
  // ============================================================================

  /**
   * Initialize a chunked upload session for large files
   */
  async initializeChunkedUpload(
    fileName: string,
    fileSize: number,
    totalChunks: number,
    projectId?: string
  ): Promise<{ uploadId: string; chunkSize: number }> {
    const response = await axiosInstance.post(`/smart-hierarchy/upload/initialize`, {
      fileName,
      fileSize,
      totalChunks,
      projectId,
    });
    return response.data;
  }

  /**
   * Upload a chunk of data
   */
  async uploadChunk(
    uploadId: string,
    chunkIndex: number,
    data: string
  ): Promise<{
    received: boolean;
    complete: boolean;
    progress: number;
    missingChunks: number[];
  }> {
    const response = await axiosInstance.post(
      `/smart-hierarchy/upload/${uploadId}/chunk/${chunkIndex}`,
      { data }
    );
    return response.data;
  }

  /**
   * Process completed chunked upload
   */
  async processChunkedUpload(
    uploadId: string,
    projectId: string,
    fileType: "hierarchy" | "mapping",
    options?: {
      autoFix?: boolean;
      validateBeforeImport?: boolean;
      dryRun?: boolean;
    }
  ): Promise<{
    success: boolean;
    logId: string;
    message: string;
    rowsTotal: number;
    rowsImported: number;
    rowsFailed: number;
    issues: Array<{ type: string; message: string; resolved: boolean }>;
    autoFixesApplied: string[];
    report: string;
    duration: number;
  }> {
    const response = await axiosInstance.post(
      `/smart-hierarchy/upload/${uploadId}/process`,
      { projectId, fileType, options }
    );
    return response.data;
  }

  /**
   * Get status of chunked upload
   */
  async getUploadStatus(uploadId: string): Promise<{
    uploadId: string;
    fileName: string;
    fileSize: number;
    progress: number;
    receivedChunks: number;
    totalChunks: number;
    expiresAt: string;
  } | null> {
    const response = await axiosInstance.get(`/smart-hierarchy/upload/${uploadId}/status`);
    return response.data.error ? null : response.data;
  }

  /**
   * Cancel a chunked upload
   */
  async cancelUpload(uploadId: string): Promise<{ cancelled: boolean }> {
    const response = await axiosInstance.delete(`/smart-hierarchy/upload/${uploadId}`);
    return response.data;
  }

  // ============================================================================
  // Upload Logs
  // ============================================================================

  /**
   * Get upload logs for a project
   */
  async getProjectUploadLogs(
    projectId: string,
    limit?: number
  ): Promise<
    Array<{
      id: string;
      timestamp: string;
      fileName: string;
      fileSize: number;
      fileType: string;
      status: string;
      rowsTotal: number;
      rowsImported: number;
      rowsFailed: number;
      issues: Array<{ type: string; message: string; resolved: boolean }>;
      autoFixesApplied: string[];
      duration?: number;
    }>
  > {
    const response = await axiosInstance.get(
      `/smart-hierarchy/projects/${projectId}/upload-logs`,
      { params: { limit } }
    );
    return response.data;
  }

  /**
   * Get a specific upload log
   */
  async getUploadLog(logId: string): Promise<{
    id: string;
    timestamp: string;
    fileName: string;
    fileSize: number;
    fileType: string;
    status: string;
    rowsTotal: number;
    rowsImported: number;
    rowsFailed: number;
    issues: Array<{ type: string; message: string; resolved: boolean }>;
    autoFixesApplied: string[];
    errorMessage?: string;
    duration?: number;
  } | null> {
    const response = await axiosInstance.get(`/smart-hierarchy/upload-logs/${logId}`);
    return response.data.error ? null : response.data;
  }

  /**
   * Get formatted report for an upload log
   */
  async getUploadLogReport(logId: string): Promise<{ report: string }> {
    const response = await axiosInstance.get(`/smart-hierarchy/upload-logs/${logId}/report`);
    return response.data;
  }

  // ============================================================================
  // Large File Import Helper
  // ============================================================================

  /**
   * Import a large CSV file using chunked upload
   * Automatically handles chunking for files > 5MB
   */
  async importLargeCSV(
    projectId: string,
    file: File,
    fileType: "hierarchy" | "mapping",
    options?: {
      autoFix?: boolean;
      validateBeforeImport?: boolean;
      dryRun?: boolean;
    },
    onProgress?: (progress: number) => void
  ): Promise<{
    success: boolean;
    logId: string;
    message: string;
    rowsTotal: number;
    rowsImported: number;
    rowsFailed: number;
    issues: Array<{ type: string; message: string; resolved: boolean }>;
    autoFixesApplied: string[];
    report: string;
    duration: number;
  }> {
    const CHUNK_SIZE = 1024 * 1024; // 1MB chunks
    const fileSize = file.size;
    const totalChunks = Math.ceil(fileSize / CHUNK_SIZE);

    // For small files, use direct import
    if (fileSize < 5 * 1024 * 1024) {
      const content = await file.text();
      if (fileType === "hierarchy") {
        return this.smartImportHierarchy(projectId, content, file.name, options);
      } else {
        return this.smartImportMapping(projectId, content, file.name, options);
      }
    }

    // Initialize chunked upload
    const { uploadId } = await this.initializeChunkedUpload(
      file.name,
      fileSize,
      totalChunks,
      projectId
    );

    try {
      // Upload chunks
      for (let i = 0; i < totalChunks; i++) {
        const start = i * CHUNK_SIZE;
        const end = Math.min(start + CHUNK_SIZE, fileSize);
        const chunk = file.slice(start, end);
        const chunkText = await chunk.text();

        const result = await this.uploadChunk(uploadId, i, chunkText);

        if (onProgress) {
          onProgress(result.progress);
        }
      }

      // Process the upload
      return await this.processChunkedUpload(uploadId, projectId, fileType, options);
    } catch (error) {
      // Cancel upload on error
      await this.cancelUpload(uploadId);
      throw error;
    }
  }
}

// Export singleton instance
export const smartHierarchyService = new SmartHierarchyService();
