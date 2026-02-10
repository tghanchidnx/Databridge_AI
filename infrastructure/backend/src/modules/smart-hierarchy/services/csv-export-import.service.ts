import { Injectable, NotFoundException, BadRequestException, Logger } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma/prisma.service';
import type { SmartHierarchyMaster } from '../types/smart-hierarchy.types';

interface HierarchyCSVRow {
  XREF_HIERARCHY_KEY: number;
  HIERARCHY_GROUP_NAME: string; // Uses DESCRIPTION
  LEVEL_0_SORT?: number; // For old ENP format
  LEVEL_0?: string; // For old ENP format
  LEVEL_1_SORT: number;
  LEVEL_1: string;
  LEVEL_2_SORT: number;
  LEVEL_2: string;
  LEVEL_3_SORT: number;
  LEVEL_3: string;
  LEVEL_4_SORT: number;
  LEVEL_4: string;
  LEVEL_5_SORT: number;
  LEVEL_5: string;
  LEVEL_6_SORT: number;
  LEVEL_6: string;
  LEVEL_7_SORT: number;
  LEVEL_7: string;
  LEVEL_8_SORT: number;
  LEVEL_8: string;
  LEVEL_9_SORT: number;
  LEVEL_9: string;
  GROUP_FILTER_PRECEDENCE: number;
  HIERARCHY_NAME: string;
  PARENT_XREF_KEY: number | null;
  SORT_ORDER: number;
  IS_ROOT: boolean;
  // Individual flag columns (not FLAGS_JSON)
  DO_NOT_EXPAND_FLAG: boolean;
  IS_SECURED_FLAG: boolean;
  SPLIT_ACTIVE_FLAG: boolean;
  EXCLUSION_FLAG: boolean;
  CALCULATION_FLAG: boolean;
  ACTIVE_FLAG: boolean;
  VOLUME_FLAG: boolean;
  ID_UNPIVOT_FLAG: boolean;
  ID_ROW_FLAG: boolean;
  REMOVE_FROM_TOTALS: boolean;
  HAS_MULTIPLE_TABLES: boolean;
  // Individual filter columns (not FILTER_CONFIG_JSON)
  FILTER_GROUP_1: string;
  FILTER_GROUP_1_TYPE: string;
  FILTER_GROUP_2: string;
  FILTER_GROUP_2_TYPE: string;
  FILTER_GROUP_3: string;
  FILTER_GROUP_3_TYPE: string;
  FILTER_GROUP_4: string;
  FILTER_GROUP_4_TYPE: string;
  // Formula-related columns
  FORMULA_GROUP: string;
  SIGN_CHANGE_FLAG: boolean;
  FORMULA_PRECEDENCE: number;
  FORMULA_PARAM_REF: string;
  ARITHMETIC_LOGIC: string;
  FORMULA_REF_SOURCE: string;
  FORMULA_REF_TABLE: string;
  FORMULA_PARAM2_CONST_NUMBER: string;
  CREATE_NEW_COLUMN: boolean;
  UPDATED_BY: string;
  UPDATED_AT: string;
  [key: string]: any; // Support dynamic custom flag columns
}

interface MappingCSVRow {
  FK_REPORT_KEY: number;
  HIERARCHY_NAME: string;
  LEVEL_NODE: string;
  SORT_ORDER: number;
  XREF_HIERARCHY_KEY: number;
  SOURCE_DATABASE: string;
  SOURCE_SCHEMA: string;
  SOURCE_TABLE: string;
  SOURCE_COLUMN: string;
  SOURCE_UID: string;
  PRECEDENCE_GROUP: string;
  ACTIVE_FLAG: boolean;
  EXCLUDE_FLAG: boolean;
  INCLUDE_FLAG: boolean;
  TRANSFORM_FLAG: boolean;
  [key: string]: any; // Support dynamic custom flag columns
}

@Injectable()
export class CsvExportImportService {
  private readonly logger = new Logger(CsvExportImportService.name);

  constructor(private readonly prisma: PrismaService) {}

  // ============================================================================
  // HIERARCHY CSV EXPORT/IMPORT
  // ============================================================================

  /**
   * Export hierarchies to CSV format matching the ENP_LOS_SUMMARY style with formula groups
   */
  async exportHierarchyCSV(projectId: string): Promise<string> {
    const project = await this.prisma.hierarchyProject.findUnique({
      where: { id: projectId },
    });

    if (!project) {
      throw new NotFoundException(`Project '${projectId}' not found`);
    }

    // Get all hierarchies with sequential numbering
    const hierarchies = await this.prisma.smartHierarchyMaster.findMany({
      where: { projectId },
      orderBy: [{ sortOrder: 'asc' }, { createdAt: 'asc' }],
    });

    // First pass: collect all unique custom flag keys (uppercase, excluding standard flags)
    const standardFlagNames = new Set([
      'active_flag',
      'exclude_flag',
      'include_flag',
      'transform_flag',
      'calculation_flag',
      'is_leaf_node',
      'do_not_expand_flag',
      'is_secured_flag',
      'split_active_flag',
      'volume_flag',
      'id_unpivot_flag',
      'id_row_flag',
      'remove_from_totals',
      'has_multiple_tables',
      'create_new_column',
      'sign_change_flag',
    ]);

    const customFlagKeys = new Set<string>();
    hierarchies.forEach((h) => {
      const flags = (h.flags as any) || {};
      const customFlags = flags.customFlags || {};
      Object.keys(customFlags).forEach((key) => {
        // Convert to uppercase and skip if it's a standard flag
        const lowerKey = key.toLowerCase();
        if (!standardFlagNames.has(lowerKey)) {
          customFlagKeys.add(key.toUpperCase());
        }
      });
    });

    // Build hierarchy map for XREF_KEY assignment and parent lookups
    const hierarchyKeyMap = new Map<string, number>();
    const hierarchyById = new Map<string, any>();
    const hierarchyByName = new Map<string, any>();

    hierarchies.forEach((h, index) => {
      const xrefKey = index + 1; // 1-based sequential
      hierarchyKeyMap.set(h.hierarchyId, xrefKey);
      const enriched = { ...h, xrefKey };
      hierarchyById.set(h.id, enriched);
      hierarchyByName.set(h.hierarchyName, enriched);
    });

    // Build map of child hierarchies to their total_formula group name
    // Key: hierarchyId, Value: mainHierarchyName (formula group name)
    const childToFormulaGroupMap = new Map<string, string>();

    for (const h of hierarchies) {
      const filterConfig = (h.filterConfig as any) || {};
      if (filterConfig.total_formula && filterConfig.total_formula.children) {
        const mainHierarchyName = filterConfig.total_formula.mainHierarchyName;
        for (const child of filterConfig.total_formula.children) {
          // Map child hierarchyId to the formula group name
          childToFormulaGroupMap.set(child.hierarchyId, mainHierarchyName);
          // Also map by the actual hierarchy's id if it matches the child
          const childHierarchy = hierarchies.find(
            (hierarchy) =>
              hierarchy.hierarchyId === child.hierarchyId || hierarchy.id === child.hierarchyId,
          );
          if (childHierarchy && childHierarchy.id) {
            childToFormulaGroupMap.set(childHierarchy.id, mainHierarchyName);
          }
        }
      }
    }

    // Helper function to build level path from root with sort orders
    const buildLevelPathWithSort = (hierarchy: any): Array<{ name: string; sortOrder: number }> => {
      const path: Array<{ name: string; sortOrder: number }> = [];
      let current = hierarchy;
      const fullPath: any[] = [];

      // Build full path from root to current
      while (current) {
        fullPath.unshift(current);
        if (current.parentId) {
          current = hierarchyById.get(current.parentId);
        } else {
          break;
        }
      }

      // For the original hierarchy (leaf), read all 9 sort values from its hierarchyLevel
      // These were saved during import from CSV LEVEL_1_SORT through LEVEL_9_SORT
      const leafHierarchy = hierarchy;
      const leafHierarchyLevel = leafHierarchy.hierarchyLevel || {};

      // Debug: Log all sort values from leaf hierarchy
      // Extract name for each level and sortOrder from the LEAF hierarchy's stored sorts
      fullPath.forEach((h, levelIndex) => {
        const hierarchyLevel = h.hierarchyLevel || {};
        const levelKey = `level_${levelIndex + 1}`;
        // Read sort value for this level position from the LEAF hierarchy's data
        const sortKey = `level_${levelIndex + 1}_sort`;
        const sortValue = leafHierarchyLevel[sortKey] || 0;
        path.push({
          name: hierarchyLevel[levelKey] || h.hierarchyName,
          sortOrder: sortValue,
        });
      });

      return path;
    };

    const rows: HierarchyCSVRow[] = [];
    let formulaIdCounter = 1000; // Start formula IDs from 1000

    // Export regular hierarchies (skip auto-created intermediate parents)
    for (const h of hierarchies) {
      // Skip intermediate parent hierarchies (auto-created during import)
      if (h.hierarchyId.startsWith('HIER_AUTO_')) {
        continue;
      }

      const xrefKey = hierarchyKeyMap.get(h.hierarchyId) || 0;
      const parentXrefKey = h.parentId ? hierarchyKeyMap.get(h.parentId) || null : null;

      const formulaConfig = (h.formulaConfig as any) || {};
      const filterConfig = (h.filterConfig as any) || {};
      const flags = (h.flags as any) || {};
      const customFlags = flags.customFlags || {};
      const metadata = (h.metadata as any) || {};

      // Debug: Log metadata for hierarchies with arithmetic_logic or formula_group
      if (metadata.arithmetic_logic || metadata.formula_group) {
        this.logger.log(
          `[EXPORT] ${h.hierarchyName}: metadata.arithmetic_logic="${metadata.arithmetic_logic}", metadata.formula_group="${metadata.formula_group}"`,
        );
      }

      // Build level path from parent chain with sort orders
      const levelPathWithSort = buildLevelPathWithSort(h);

      const row: HierarchyCSVRow = {
        XREF_HIERARCHY_KEY: xrefKey,
        HIERARCHY_GROUP_NAME: h.description || project.name || '',
        // LEVEL_0 contains project name (OLD ENP FORMAT COMPATIBILITY)
        LEVEL_0_SORT: 0,
        LEVEL_0: project.name || '',
        // Level columns shifted down by 1 to match old format
        LEVEL_1_SORT: levelPathWithSort.length >= 1 ? levelPathWithSort[0].sortOrder : 0,
        LEVEL_1: levelPathWithSort.length >= 1 ? levelPathWithSort[0].name : '',
        LEVEL_2_SORT: levelPathWithSort.length >= 2 ? levelPathWithSort[1].sortOrder : 0,
        LEVEL_2: levelPathWithSort.length >= 2 ? levelPathWithSort[1].name : '',
        LEVEL_3_SORT: levelPathWithSort.length >= 3 ? levelPathWithSort[2].sortOrder : 0,
        LEVEL_3: levelPathWithSort.length >= 3 ? levelPathWithSort[2].name : '',
        LEVEL_4_SORT: levelPathWithSort.length >= 4 ? levelPathWithSort[3].sortOrder : 0,
        LEVEL_4: levelPathWithSort.length >= 4 ? levelPathWithSort[3].name : '',
        LEVEL_5_SORT: levelPathWithSort.length >= 5 ? levelPathWithSort[4].sortOrder : 0,
        LEVEL_5: levelPathWithSort.length >= 5 ? levelPathWithSort[4].name : '',
        LEVEL_6_SORT: levelPathWithSort.length >= 6 ? levelPathWithSort[5].sortOrder : 0,
        LEVEL_6: levelPathWithSort.length >= 6 ? levelPathWithSort[5].name : '',
        LEVEL_7_SORT: levelPathWithSort.length >= 7 ? levelPathWithSort[6].sortOrder : 0,
        LEVEL_7: levelPathWithSort.length >= 7 ? levelPathWithSort[6].name : '',
        LEVEL_8_SORT: levelPathWithSort.length >= 8 ? levelPathWithSort[7].sortOrder : 0,
        LEVEL_8: levelPathWithSort.length >= 8 ? levelPathWithSort[7].name : '',
        LEVEL_9_SORT: levelPathWithSort.length >= 9 ? levelPathWithSort[8].sortOrder : 0,
        LEVEL_9: levelPathWithSort.length >= 9 ? levelPathWithSort[8].name : '',
        GROUP_FILTER_PRECEDENCE: formulaConfig.formula_group?.precedence || 1,
        HIERARCHY_NAME: h.hierarchyName || '',
        PARENT_XREF_KEY: parentXrefKey,
        SORT_ORDER: h.sortOrder || 0,
        IS_ROOT: h.isRoot ?? true,
        // Individual flag columns
        DO_NOT_EXPAND_FLAG: customFlags.do_not_expand_flag ?? false,
        IS_SECURED_FLAG: customFlags.is_secured_flag ?? false,
        SPLIT_ACTIVE_FLAG: customFlags.split_active_flag ?? false,
        EXCLUSION_FLAG: flags.exclude_flag ?? false,
        CALCULATION_FLAG: flags.calculation_flag ?? false,
        ACTIVE_FLAG: flags.active_flag ?? true,
        VOLUME_FLAG: customFlags.volume_flag ?? false,
        ID_UNPIVOT_FLAG: customFlags.id_unpivot_flag ?? false,
        ID_ROW_FLAG: customFlags.id_row_flag ?? false,
        REMOVE_FROM_TOTALS: customFlags.remove_from_totals ?? false,
        HAS_MULTIPLE_TABLES: customFlags.has_multiple_tables ?? false,
        // Individual filter columns
        FILTER_GROUP_1: filterConfig.filter_group_1 || '',
        FILTER_GROUP_1_TYPE: filterConfig.filter_group_1_type || '',
        FILTER_GROUP_2: filterConfig.filter_group_2 || '',
        FILTER_GROUP_2_TYPE: filterConfig.filter_group_2_type || '',
        FILTER_GROUP_3: filterConfig.filter_group_3 || '',
        FILTER_GROUP_3_TYPE: filterConfig.filter_group_3_type || '',
        FILTER_GROUP_4: filterConfig.filter_group_4 || '',
        FILTER_GROUP_4_TYPE: filterConfig.filter_group_4_type || '',
        // Formula fields - Read from metadata (for old ENP format) or formulaConfig/filterConfig
        FORMULA_GROUP: (
          metadata.formula_group ||
          formulaConfig.formula_group?.groupName ||
          (filterConfig.total_formula ? filterConfig.total_formula.mainHierarchyName : '') ||
          childToFormulaGroupMap.get(h.hierarchyId) ||
          childToFormulaGroupMap.get(h.id) ||
          ''
        ).replace(/^Formula Group:\s*/i, ''),
        SIGN_CHANGE_FLAG: customFlags.sign_change_flag ?? false,
        FORMULA_PRECEDENCE:
          metadata.formula_precedence || formulaConfig.formula_group?.precedence || 1,
        FORMULA_PARAM_REF:
          metadata.formula_param_ref ||
          (filterConfig.total_formula ? filterConfig.total_formula.mainHierarchyName : '') ||
          '',
        ARITHMETIC_LOGIC:
          metadata.arithmetic_logic ||
          (filterConfig.total_formula ? filterConfig.total_formula.aggregation : '') ||
          '',
        FORMULA_REF_SOURCE: '',
        FORMULA_REF_TABLE:
          metadata.formula_ref_table && metadata.formula_ref_table !== 'null'
            ? metadata.formula_ref_table
            : '',
        FORMULA_PARAM2_CONST_NUMBER: metadata.formula_param2_const_number || '',
        CREATE_NEW_COLUMN: customFlags.create_new_column ?? false,
        UPDATED_BY: h.createdBy || '',
        UPDATED_AT: h.updatedAt?.toISOString() || '',
      };

      // Add all custom flags as dynamic properties (default FALSE if not present)
      // Custom flags are stored in mixed case but exported as UPPERCASE
      customFlagKeys.forEach((flagKey) => {
        // Find the flag in customFlags regardless of case
        const matchingKey = Object.keys(customFlags).find((k) => k.toUpperCase() === flagKey);
        row[flagKey] = matchingKey ? (customFlags[matchingKey] ?? false) : false;
      });

      rows.push(row);
    }

    // OLD ENP FORMAT: Export formula rules as separate rows (one per operation)
    // EBITDA with Add + Subtract operations = 2+ rows with same hierarchy name but different ARITHMETIC_LOGIC
    for (const h of hierarchies) {
      const xrefKey = hierarchyKeyMap.get(h.hierarchyId) || 0;
      const formulaConfig = (h.formulaConfig as any) || {};
      const filterConfig = (h.filterConfig as any) || {};
      const metadata = (h.metadata as any) || {};
      const row = rows.find((r) => r.XREF_HIERARCHY_KEY === xrefKey);

      if (!row) continue;

      // Skip if this hierarchy has total_formula - those are already in main rows
      if (filterConfig.total_formula) {
        continue;
      }

      // Check if this hierarchy has formula_group structure with multiple operations
      if (formulaConfig.formula_group && formulaConfig.formula_group.rules) {
        const rules = formulaConfig.formula_group.rules || [];

        // Export additional rows ONLY for rules with DIFFERENT operations than the main row
        // Main row already has first operation from metadata
        const mainOperation = metadata.arithmetic_logic || row.ARITHMETIC_LOGIC;
        const mainParamRef = metadata.formula_param_ref || row.FORMULA_PARAM_REF;
        const mainRefTable = metadata.formula_ref_table || row.FORMULA_REF_TABLE;

        for (const rule of rules) {
          const ruleOperation = rule.operation || 'Add';
          const ruleParamRef =
            rule.originalFormulaParamRef ||
            rule.hierarchyName ||
            rule.parameterReference ||
            rule.tableReference ||
            '';
          const ruleRefTable = rule.isTableReference ? rule.tableReference : '';

          // Skip if this is the SAME operation already in main row (avoid true duplicates)
          const isSameAsMain =
            ruleOperation.toLowerCase() === mainOperation?.toLowerCase() &&
            ruleParamRef === mainParamRef &&
            ruleRefTable === mainRefTable;

          if (isSameAsMain) {
            continue; // Already exported in main row
          }

          // Create additional row for this different operation
          const formulaEntry: HierarchyCSVRow = {
            XREF_HIERARCHY_KEY: formulaIdCounter++,
            HIERARCHY_GROUP_NAME: h.description || project.name || '',
            // Same LEVEL_0 as main hierarchy
            LEVEL_0_SORT: row.LEVEL_0_SORT,
            LEVEL_0: row.LEVEL_0,
            // Same levels as main hierarchy
            LEVEL_1_SORT: row.LEVEL_1_SORT,
            LEVEL_1: row.LEVEL_1,
            LEVEL_2_SORT: row.LEVEL_2_SORT,
            LEVEL_2: row.LEVEL_2,
            LEVEL_3_SORT: row.LEVEL_3_SORT,
            LEVEL_3: row.LEVEL_3,
            LEVEL_4_SORT: row.LEVEL_4_SORT,
            LEVEL_4: row.LEVEL_4,
            LEVEL_5_SORT: row.LEVEL_5_SORT,
            LEVEL_5: row.LEVEL_5,
            LEVEL_6_SORT: row.LEVEL_6_SORT,
            LEVEL_6: row.LEVEL_6,
            LEVEL_7_SORT: row.LEVEL_7_SORT,
            LEVEL_7: row.LEVEL_7,
            LEVEL_8_SORT: row.LEVEL_8_SORT,
            LEVEL_8: row.LEVEL_8,
            LEVEL_9_SORT: row.LEVEL_9_SORT,
            LEVEL_9: row.LEVEL_9,
            GROUP_FILTER_PRECEDENCE: rule.precedence || row.GROUP_FILTER_PRECEDENCE,
            HIERARCHY_NAME: h.hierarchyName,
            PARENT_XREF_KEY: row.PARENT_XREF_KEY,
            SORT_ORDER: h.sortOrder || 0,
            IS_ROOT: false,
            // Copy flags from main hierarchy
            DO_NOT_EXPAND_FLAG: row.DO_NOT_EXPAND_FLAG,
            IS_SECURED_FLAG: row.IS_SECURED_FLAG,
            SPLIT_ACTIVE_FLAG: row.SPLIT_ACTIVE_FLAG,
            EXCLUSION_FLAG: row.EXCLUSION_FLAG,
            CALCULATION_FLAG: row.CALCULATION_FLAG,
            ACTIVE_FLAG: row.ACTIVE_FLAG,
            VOLUME_FLAG: row.VOLUME_FLAG,
            ID_UNPIVOT_FLAG: row.ID_UNPIVOT_FLAG,
            ID_ROW_FLAG: row.ID_ROW_FLAG,
            REMOVE_FROM_TOTALS: row.REMOVE_FROM_TOTALS,
            HAS_MULTIPLE_TABLES: row.HAS_MULTIPLE_TABLES,
            // Copy filters from main hierarchy
            FILTER_GROUP_1: row.FILTER_GROUP_1,
            FILTER_GROUP_1_TYPE: row.FILTER_GROUP_1_TYPE,
            FILTER_GROUP_2: row.FILTER_GROUP_2,
            FILTER_GROUP_2_TYPE: row.FILTER_GROUP_2_TYPE,
            FILTER_GROUP_3: row.FILTER_GROUP_3,
            FILTER_GROUP_3_TYPE: row.FILTER_GROUP_3_TYPE,
            FILTER_GROUP_4: row.FILTER_GROUP_4,
            FILTER_GROUP_4_TYPE: row.FILTER_GROUP_4_TYPE,
            // Formula fields - DIFFERENT values for this operation
            FORMULA_GROUP: row.FORMULA_GROUP,
            SIGN_CHANGE_FLAG: rule.originalSignChangeFlag ?? row.SIGN_CHANGE_FLAG,
            FORMULA_PRECEDENCE: rule.precedence || row.FORMULA_PRECEDENCE,
            FORMULA_PARAM_REF: rule.originalFormulaParamRef || ruleParamRef,
            ARITHMETIC_LOGIC: ruleOperation,
            FORMULA_REF_SOURCE: '',
            FORMULA_REF_TABLE: ruleRefTable,
            FORMULA_PARAM2_CONST_NUMBER: rule.constantNumber?.toString() || '',
            CREATE_NEW_COLUMN: row.CREATE_NEW_COLUMN,
            UPDATED_BY: h.createdBy || '',
            UPDATED_AT: h.updatedAt?.toISOString() || '',
          };

          // Copy custom flags from main hierarchy row
          customFlagKeys.forEach((flagKey) => {
            formulaEntry[flagKey] = row[flagKey] ?? false;
          });

          rows.push(formulaEntry);
        }
      }
    }

    // Sort by XREF_HIERARCHY_KEY to maintain order
    rows.sort((a, b) => a.XREF_HIERARCHY_KEY - b.XREF_HIERARCHY_KEY);

    return this.convertHierarchyToCSV(rows, Array.from(customFlagKeys));
  }

  /**
   * Export mappings to CSV format
   */
  async exportMappingCSV(projectId: string): Promise<string> {
    const project = await this.prisma.hierarchyProject.findUnique({
      where: { id: projectId },
    });

    if (!project) {
      throw new NotFoundException(`Project '${projectId}' not found`);
    }

    // Get all hierarchies with their mappings
    const hierarchies = await this.prisma.smartHierarchyMaster.findMany({
      where: { projectId },
      orderBy: [{ sortOrder: 'asc' }, { createdAt: 'asc' }],
    });

    // Build hierarchy map for XREF_KEY assignment
    const hierarchyKeyMap = new Map<string, number>();
    hierarchies.forEach((h, index) => {
      hierarchyKeyMap.set(h.hierarchyId, index + 1); // 1-based sequential
    });

    // First pass: collect all unique custom flag keys (uppercase, excluding standard flags)
    const standardMappingFlagNames = new Set([
      'active_flag',
      'exclude_flag',
      'include_flag',
      'transform_flag',
    ]);

    const customFlagKeys = new Set<string>();
    for (const h of hierarchies) {
      const mappings = (h.mapping as any[]) || [];
      mappings.forEach((mapping) => {
        const customFlags = mapping.flags?.customFlags || {};
        Object.keys(customFlags).forEach((key) => {
          // Convert to uppercase and skip if it's a standard flag
          const lowerKey = key.toLowerCase();
          if (!standardMappingFlagNames.has(lowerKey)) {
            customFlagKeys.add(key.toUpperCase());
          }
        });
      });
    }

    const rows: MappingCSVRow[] = [];

    for (const h of hierarchies) {
      const xrefKey = hierarchyKeyMap.get(h.hierarchyId) || 0;
      const mappings = (h.mapping as any[]) || [];
      const hierarchyLevel = (h.hierarchyLevel as any) || {};

      // Get the deepest level name for LEVEL_NODE
      let levelNode = '';
      for (let i = 15; i >= 1; i--) {
        if (hierarchyLevel[`level_${i}`]) {
          levelNode = hierarchyLevel[`level_${i}`];
          break;
        }
      }

      // Create a row for each mapping
      mappings.forEach((mapping, index) => {
        const flags = mapping.flags || {};
        const customFlags = flags.customFlags || {};

        const row: MappingCSVRow = {
          FK_REPORT_KEY: xrefKey,
          HIERARCHY_NAME: h.hierarchyName || '',
          LEVEL_NODE: levelNode,
          SORT_ORDER: h.sortOrder || 0,
          XREF_HIERARCHY_KEY: mapping.mapping_index || index + 1,
          SOURCE_DATABASE: mapping.source_database || '',
          SOURCE_SCHEMA: mapping.source_schema || '',
          SOURCE_TABLE: mapping.source_table || '',
          SOURCE_COLUMN: mapping.source_column || '',
          SOURCE_UID: mapping.source_uid || '',
          PRECEDENCE_GROUP: mapping.precedence_group || '',
          ACTIVE_FLAG: flags.active_flag ?? false,
          EXCLUDE_FLAG: flags.exclude_flag ?? false,
          INCLUDE_FLAG: flags.include_flag ?? false,
          TRANSFORM_FLAG: flags.transform_flag ?? false,
        };

        // Add all custom flags as columns (default FALSE if not present)
        // Custom flags are stored in mixed case but exported as UPPERCASE
        customFlagKeys.forEach((flagKey) => {
          // Find the flag in customFlags regardless of case
          const matchingKey = Object.keys(customFlags).find((k) => k.toUpperCase() === flagKey);
          row[flagKey] = matchingKey ? (customFlags[matchingKey] ?? false) : false;
        });

        rows.push(row);
      });
    }

    return this.convertMappingToCSV(rows, Array.from(customFlagKeys));
  }

  /**
   * Import hierarchies from CSV
   */
  async importHierarchyCSV(
    projectId: string,
    csvContent: string,
  ): Promise<{ imported: number; skipped: number; errors: string[] }> {
    const project = await this.prisma.hierarchyProject.findUnique({
      where: { id: projectId },
    });

    if (!project) {
      throw new NotFoundException(`Project '${projectId}' not found`);
    }

    const rows = this.parseHierarchyCSV(csvContent);

    let imported = 0;
    let skipped = 0;
    const errors: string[] = [];

    // Build mapping from XREF_HIERARCHY_KEY to hierarchyId
    const xrefToHierarchyIdMap = new Map<number, string>();
    const hierarchyNameToIdMap = new Map<string, string>();
    const levelPathToHierarchyIdMap = new Map<string, string>(); // Maps level path to hierarchyId

    // ENP FORMAT: Group rows by HIERARCHY_NAME to detect duplicate-row formula pattern
    // In ENP, multiple rows with SAME HIERARCHY_NAME but different XREF_KEYS represent formula rules
    const hierarchyGroupsMap = new Map<string, HierarchyCSVRow[]>();

    for (const row of rows) {
      const hierarchyName = row.HIERARCHY_NAME;
      if (!hierarchyGroupsMap.has(hierarchyName)) {
        hierarchyGroupsMap.set(hierarchyName, []);
      }
      hierarchyGroupsMap.get(hierarchyName)?.push(row);
    }

    // Helper function to extract level path from row
    const extractLevelPath = (row: HierarchyCSVRow): string[] => {
      const path: string[] = [];
      for (let i = 1; i <= 9; i++) {
        const levelKey = `LEVEL_${i}` as keyof HierarchyCSVRow;
        const levelValue = row[levelKey];
        if (levelValue && typeof levelValue === 'string' && levelValue.trim()) {
          path.push(levelValue.trim());
        } else {
          break;
        }
      }
      return path;
    };

    // Detect total_formula patterns before processing hierarchies
    // Group hierarchies by FORMULA_GROUP name
    const formulaGroupMap = new Map<string, HierarchyCSVRow[]>();
    const totalFormulaMap = new Map<
      string,
      {
        mainHierarchyName: string;
        aggregation: string;
        children: Array<{ hierarchyName: string }>;
      }
    >();

    for (const [hierarchyName, groupRows] of hierarchyGroupsMap.entries()) {
      const mainRow = groupRows[0];
      const formulaGroupName = mainRow.FORMULA_GROUP?.trim();

      if (formulaGroupName) {
        if (!formulaGroupMap.has(formulaGroupName)) {
          formulaGroupMap.set(formulaGroupName, []);
        }
        formulaGroupMap.get(formulaGroupName)?.push(mainRow);
      }
    }

    // For each formula group with multiple hierarchies, detect if it's a total_formula pattern
    for (const [formulaGroupName, mainRows] of formulaGroupMap.entries()) {
      if (mainRows.length <= 1) continue;

      // Check if all rows have DIFFERENT hierarchy names (Total Formula pattern)
      // vs same hierarchy name (Formula Group pattern with XREF 1000+)
      const uniqueHierarchyNames = new Set(mainRows.map((row) => row.HIERARCHY_NAME));

      if (uniqueHierarchyNames.size > 1) {
        // Total Formula: Multiple DIFFERENT hierarchies with same FORMULA_GROUP
        // Find the main hierarchy: the one with ARITHMETIC_LOGIC filled
        const mainHierarchyRow = mainRows.find((row) => row.ARITHMETIC_LOGIC?.trim());

        if (mainHierarchyRow) {
          // This is a total_formula pattern
          const children = mainRows
            .filter((row) => row.HIERARCHY_NAME !== mainHierarchyRow.HIERARCHY_NAME)
            .map((row) => ({ hierarchyName: row.HIERARCHY_NAME }));

          if (children.length > 0) {
            totalFormulaMap.set(mainHierarchyRow.HIERARCHY_NAME, {
              mainHierarchyName: formulaGroupName,
              aggregation: mainHierarchyRow.ARITHMETIC_LOGIC?.trim() || 'SUM',
              children: children,
            });
          }
        }
      }
      // If uniqueHierarchyNames.size === 1, it's a Formula Group (handled in Pass 1)
    }

    // Helper function to find parent hierarchyId from level path
    const findParentFromLevelPath = (levelPath: string[]): string | null => {
      if (levelPath.length <= 1) return null;

      // Parent is the hierarchy whose level path matches all but the last level
      const parentPath = levelPath.slice(0, -1);
      const parentPathKey = parentPath.join(' > ');

      return levelPathToHierarchyIdMap.get(parentPathKey) || null;
    };

    // Pass 1: Create all hierarchies without parent relationships
    for (const [hierarchyName, groupRows] of hierarchyGroupsMap.entries()) {
      try {
        // Sort by XREF_KEY to ensure main hierarchy (lowest XREF) is processed first
        groupRows.sort((a, b) => a.XREF_HIERARCHY_KEY - b.XREF_HIERARCHY_KEY);

        const mainRow = groupRows[0]; // Main hierarchy row (lowest XREF_KEY)
        const formulaRows = groupRows.slice(1); // Additional rows are formula rules

        const hierarchyId = `HIER_${mainRow.XREF_HIERARCHY_KEY}`;
        xrefToHierarchyIdMap.set(mainRow.XREF_HIERARCHY_KEY, hierarchyId);
        hierarchyNameToIdMap.set(hierarchyName, hierarchyId);

        // Extract level path to determine parent later
        const levelPath = extractLevelPath(mainRow);
        const levelPathKey = levelPath.join(' > ');
        levelPathToHierarchyIdMap.set(levelPathKey, hierarchyId);

        // Check if hierarchy already exists
        const existing = await this.prisma.smartHierarchyMaster.findUnique({
          where: {
            projectId_hierarchyId: {
              projectId,
              hierarchyId,
            },
          },
        });

        if (existing) {
          skipped++;
          continue;
        }

        // Build hierarchyLevel object from individual LEVEL columns
        const hierarchyLevel: any = {};
        if (mainRow.LEVEL_1) hierarchyLevel.level_1 = mainRow.LEVEL_1;
        if (mainRow.LEVEL_2) hierarchyLevel.level_2 = mainRow.LEVEL_2;
        if (mainRow.LEVEL_3) hierarchyLevel.level_3 = mainRow.LEVEL_3;
        if (mainRow.LEVEL_4) hierarchyLevel.level_4 = mainRow.LEVEL_4;
        if (mainRow.LEVEL_5) hierarchyLevel.level_5 = mainRow.LEVEL_5;
        if (mainRow.LEVEL_6) hierarchyLevel.level_6 = mainRow.LEVEL_6;
        if (mainRow.LEVEL_7) hierarchyLevel.level_7 = mainRow.LEVEL_7;
        if (mainRow.LEVEL_8) hierarchyLevel.level_8 = mainRow.LEVEL_8;
        if (mainRow.LEVEL_9) hierarchyLevel.level_9 = mainRow.LEVEL_9;

        // Determine if this is a root based on level path length and IS_ROOT flag
        const isRoot = mainRow.IS_ROOT === true || levelPath.length === 1;

        // Build formula configuration from duplicate rows (ENP pattern)
        const formulaConfig: any = {};

        if (mainRow.FORMULA_GROUP && formulaRows.length > 0) {
          // Check if this is NOT a total_formula (those are handled separately)
          if (!totalFormulaMap.has(hierarchyName)) {
            // Build formula_group rules from duplicate rows using NEW structure
            const rules = formulaRows.map((formulaRow) => {
              return {
                hierarchyId: '', // Will be resolved after all hierarchies are created
                hierarchyName: formulaRow.FORMULA_PARAM_REF || '',
                operation: formulaRow.ARITHMETIC_LOGIC || 'SUM',
                precedence: formulaRow.FORMULA_PRECEDENCE || 1,
                parameterReference: undefined, // Don't use FORMULA_PARAM_REF as parameterReference
                constantNumber: formulaRow.FORMULA_PARAM2_CONST_NUMBER
                  ? parseFloat(formulaRow.FORMULA_PARAM2_CONST_NUMBER)
                  : undefined,
              };
            });

            formulaConfig.formula_group = {
              mainHierarchyId: hierarchyId,
              mainHierarchyName: mainRow.FORMULA_GROUP,
              rules: rules,
            };
          }
        } else if (mainRow.FORMULA_GROUP && formulaRows.length === 0) {
          // Formula group name exists but no formula rows - create placeholder
          if (!totalFormulaMap.has(hierarchyName)) {
            formulaConfig.formula_group = {
              mainHierarchyId: hierarchyId,
              mainHierarchyName: mainRow.FORMULA_GROUP,
              rules: [],
            };
          }
        } // Build flags object from individual flag columns
        const flags: any = {
          active_flag: mainRow.ACTIVE_FLAG,
          exclude_flag: mainRow.EXCLUSION_FLAG,
          calculation_flag: mainRow.CALCULATION_FLAG,
          customFlags: {
            do_not_expand_flag: mainRow.DO_NOT_EXPAND_FLAG,
            is_secured_flag: mainRow.IS_SECURED_FLAG,
            split_active_flag: mainRow.SPLIT_ACTIVE_FLAG,
            volume_flag: mainRow.VOLUME_FLAG,
            id_unpivot_flag: mainRow.ID_UNPIVOT_FLAG,
            id_row_flag: mainRow.ID_ROW_FLAG,
            remove_from_totals: mainRow.REMOVE_FROM_TOTALS,
            has_multiple_tables: mainRow.HAS_MULTIPLE_TABLES,
            create_new_column: mainRow.CREATE_NEW_COLUMN,
          },
        };

        // Extract dynamic custom flags from non-standard columns
        const standardColumns = [
          'XREF_HIERARCHY_KEY',
          'FK_REPORT_KEY',
          'HIERARCHY_GROUP_NAME',
          'REPORT_TYPE',
          'SORT_ORDER',
          'LEVEL_0_SORT',
          'LEVEL_0',
          'LEVEL_1_SORT',
          'LEVEL_1',
          'LEVEL_2_SORT',
          'LEVEL_2',
          'LEVEL_3_SORT',
          'LEVEL_3',
          'LEVEL_4_SORT',
          'LEVEL_4',
          'LEVEL_5_SORT',
          'LEVEL_5',
          'LEVEL_6_SORT',
          'LEVEL_6',
          'LEVEL_7_SORT',
          'LEVEL_7',
          'LEVEL_8_SORT',
          'LEVEL_8',
          'LEVEL_9_SORT',
          'LEVEL_9',
          'GROUP_FILTER_PRECEDENCE',
          'HIERARCHY_NAME',
          'PARENT_XREF_KEY',
          'IS_ROOT',
          'DO_NOT_EXPAND_FLAG',
          'IS_SECURED_FLAG',
          'SPLIT_ACTIVE_FLAG',
          'EXCLUSION_FLAG',
          'CALCULATION_FLAG',
          'ACTIVE_FLAG',
          'VOLUME_FLAG',
          'ID_UNPIVOT_FLAG',
          'ID_ROW_FLAG',
          'REMOVE_FROM_TOTALS',
          'HAS_MULTIPLE_TABLES',
          'FILTER_GROUP_1',
          'FILTER_GROUP_1_TYPE',
          'FILTER_GROUP_2',
          'FILTER_GROUP_2_TYPE',
          'FILTER_GROUP_3',
          'FILTER_GROUP_3_TYPE',
          'FILTER_GROUP_4',
          'FILTER_GROUP_4_TYPE',
          'FORMULA_GROUP',
          'SIGN_CHANGE_FLAG',
          'FORMULA_PRECEDENCE',
          'FORMULA_PARAM_REF',
          'ARITHMETIC_LOGIC',
          'FORMULA_REF_SOURCE',
          'FORMULA_REF_TABLE',
          'FORMULA_PARAM2_CONST_NUMBER',
          'CREATE_NEW_COLUMN',
          'UPDATED_BY',
          'UPDATED_AT',
        ];
        const customFlagsData: any = {};
        Object.keys(mainRow).forEach((key) => {
          if (!standardColumns.includes(key) && typeof mainRow[key] === 'boolean') {
            customFlagsData[key] = mainRow[key];
          }
        });
        // Merge with existing customFlags
        flags.customFlags = { ...flags.customFlags, ...customFlagsData };

        // Build filterConfig from individual filter columns
        const filterConfig: any = {};
        if (mainRow.FILTER_GROUP_1) filterConfig.filter_group_1 = mainRow.FILTER_GROUP_1;
        if (mainRow.FILTER_GROUP_1_TYPE)
          filterConfig.filter_group_1_type = mainRow.FILTER_GROUP_1_TYPE;
        if (mainRow.FILTER_GROUP_2) filterConfig.filter_group_2 = mainRow.FILTER_GROUP_2;
        if (mainRow.FILTER_GROUP_2_TYPE)
          filterConfig.filter_group_2_type = mainRow.FILTER_GROUP_2_TYPE;
        if (mainRow.FILTER_GROUP_3) filterConfig.filter_group_3 = mainRow.FILTER_GROUP_3;
        if (mainRow.FILTER_GROUP_3_TYPE)
          filterConfig.filter_group_3_type = mainRow.FILTER_GROUP_3_TYPE;
        if (mainRow.FILTER_GROUP_4) filterConfig.filter_group_4 = mainRow.FILTER_GROUP_4;
        if (mainRow.FILTER_GROUP_4_TYPE)
          filterConfig.filter_group_4_type = mainRow.FILTER_GROUP_4_TYPE;

        // Add total_formula if this hierarchy is the main hierarchy of a total_formula
        const totalFormulaData = totalFormulaMap.get(hierarchyName);
        if (totalFormulaData) {
          // Resolve child hierarchy names to IDs (will be updated in Pass 1.5)
          const children = totalFormulaData.children.map((child) => ({
            hierarchyId: '', // Will be resolved later
            hierarchyName: child.hierarchyName,
          }));

          filterConfig.total_formula = {
            mainHierarchyId: hierarchyId,
            mainHierarchyName: totalFormulaData.mainHierarchyName,
            aggregation: totalFormulaData.aggregation,
            children: children,
          };
        }

        // Create hierarchy WITHOUT parent link (will be added in pass 2)
        await this.prisma.smartHierarchyMaster.create({
          data: {
            projectId,
            hierarchyId,
            hierarchyName: mainRow.HIERARCHY_NAME,
            description: mainRow.HIERARCHY_GROUP_NAME || null,
            parentId: null, // Will be set in pass 2
            sortOrder: mainRow.SORT_ORDER || 0,
            isRoot: isRoot, // Determined from level path and IS_ROOT flag
            hierarchyLevel: hierarchyLevel,
            flags: flags,
            mapping: [], // Mappings imported separately
            formulaConfig: Object.keys(formulaConfig).length > 0 ? formulaConfig : null,
            filterConfig: Object.keys(filterConfig).length > 0 ? filterConfig : null,
            pivotConfig: null,
            metadata: {},
            createdBy: mainRow.UPDATED_BY || 'system',
          },
        });

        imported++;
      } catch (error: any) {
        const errorMsg = `Failed to create hierarchy '${hierarchyName}': ${error.message}`;
        errors.push(errorMsg);
        skipped++;
      }
    }

    // Pass 1.5: Resolve formula hierarchy name references to hierarchyIds
    for (const [hierarchyName, groupRows] of hierarchyGroupsMap.entries()) {
      const mainRow = groupRows[0];
      const hierarchyId = hierarchyNameToIdMap.get(hierarchyName);

      if (!hierarchyId || !mainRow.FORMULA_GROUP) {
        continue;
      }

      // Skip if no formula rows (no rules to resolve)
      if (groupRows.length <= 1) {
        continue;
      }

      try {
        const hierarchy = await this.prisma.smartHierarchyMaster.findUnique({
          where: {
            projectId_hierarchyId: {
              projectId,
              hierarchyId,
            },
          },
        });

        if (hierarchy && hierarchy.formulaConfig) {
          const formulaConfig = hierarchy.formulaConfig as any;

          // Resolve hierarchy names to IDs in formula_group.rules[]
          if (formulaConfig.formula_group && formulaConfig.formula_group.rules) {
            const resolvedRules = formulaConfig.formula_group.rules.map((rule: any) => {
              const refHierarchyId = hierarchyNameToIdMap.get(rule.hierarchyName);
              return {
                ...rule,
                hierarchyId: refHierarchyId || rule.hierarchyId,
              };
            });

            formulaConfig.formula_group.rules = resolvedRules;

            // Update hierarchy with resolved formula references
            await this.prisma.smartHierarchyMaster.update({
              where: {
                projectId_hierarchyId: {
                  projectId,
                  hierarchyId,
                },
              },
              data: {
                formulaConfig: formulaConfig,
              },
            });
          }
        }

        // Resolve hierarchy names to IDs in total_formula children
        if (hierarchy && hierarchy.filterConfig) {
          const filterConfig = hierarchy.filterConfig as any;

          if (filterConfig.total_formula && filterConfig.total_formula.children) {
            const resolvedChildren = filterConfig.total_formula.children.map((child: any) => {
              const childHierarchyId = hierarchyNameToIdMap.get(child.hierarchyName);
              return {
                ...child,
                hierarchyId: childHierarchyId || child.hierarchyId,
              };
            });

            filterConfig.total_formula.children = resolvedChildren;

            // Update hierarchy with resolved total formula references
            await this.prisma.smartHierarchyMaster.update({
              where: {
                projectId_hierarchyId: {
                  projectId,
                  hierarchyId,
                },
              },
              data: {
                filterConfig: filterConfig,
              },
            });
          }
        }
      } catch (error: any) {
        const errorMsg = `Failed to resolve formula references for '${hierarchyName}': ${error.message}`;
        errors.push(errorMsg);
      }
    }

    // Pass 2: Update parent relationships using both PARENT_XREF_KEY and level path
    for (const [hierarchyName, groupRows] of hierarchyGroupsMap.entries()) {
      const mainRow = groupRows[0];
      const levelPath = extractLevelPath(mainRow);

      try {
        const childHierarchyId = xrefToHierarchyIdMap.get(mainRow.XREF_HIERARCHY_KEY);
        if (!childHierarchyId) continue;

        let parentHierarchyId: string | null = null;

        // First try: Use PARENT_XREF_KEY if available
        if (mainRow.PARENT_XREF_KEY) {
          parentHierarchyId = xrefToHierarchyIdMap.get(mainRow.PARENT_XREF_KEY) || null;
        }

        // Second try: Use level path matching if PARENT_XREF_KEY didn't work
        if (!parentHierarchyId && levelPath.length > 1) {
          parentHierarchyId = findParentFromLevelPath(levelPath);
        }

        if (parentHierarchyId) {
          // Get parent UUID
          const parent = await this.prisma.smartHierarchyMaster.findUnique({
            where: {
              projectId_hierarchyId: {
                projectId,
                hierarchyId: parentHierarchyId,
              },
            },
          });

          if (parent) {
            await this.prisma.smartHierarchyMaster.update({
              where: {
                projectId_hierarchyId: {
                  projectId,
                  hierarchyId: childHierarchyId,
                },
              },
              data: {
                parentId: parent.id,
                isRoot: false,
              },
            });
          } else {
            console.warn(`Parent hierarchy not found for ${hierarchyName}`);
          }
        }
      } catch (error: any) {
        const errorMsg = `Failed to link parent for '${hierarchyName}': ${error.message}`;
        errors.push(errorMsg);
      }
    }

    return { imported, skipped, errors };
  }

  /**
   * Import hierarchies from OLD ENP CSV format (ID_NAME column)
   * This handles the legacy format with ID_NAME instead of HIERARCHY_NAME
   */
  async importHierarchyCSV_OldFormat(
    projectId: string,
    csvContent: string,
  ): Promise<{ imported: number; skipped: number; errors: string[] }> {
    const project = await this.prisma.hierarchyProject.findUnique({
      where: { id: projectId },
    });

    if (!project) {
      throw new NotFoundException(`Project '${projectId}' not found`);
    }

    const rows = this.parseHierarchyCSV_OldFormat(csvContent);

    let imported = 0;
    let skipped = 0;
    let created = 0;
    const errors: string[] = [];

    // Build mapping from XREF_HIERARCHY_KEY to hierarchyId
    const xrefToHierarchyIdMap = new Map<number, string>();
    const hierarchyNameToIdMap = new Map<string, string>();
    const levelPathToHierarchyIdMap = new Map<string, string>();

    // Map to store hierarchies by their name AND depth for parent resolution
    // Key format: "depth:hierarchyName" e.g., "2:Net Sales Vol"
    const hierarchyByDepthAndName = new Map<string, string>();

    // Group rows by ID_NAME (same as HIERARCHY_NAME) to detect formula pattern
    const hierarchyGroupsMap = new Map<string, HierarchyCSVRow[]>();

    for (const row of rows) {
      const hierarchyName = row.HIERARCHY_NAME;
      if (!hierarchyGroupsMap.has(hierarchyName)) {
        hierarchyGroupsMap.set(hierarchyName, []);
      }
      hierarchyGroupsMap.get(hierarchyName)?.push(row);
    }

    // Helper function to extract level path from row (handles LEVEL_2 to LEVEL_9)
    // Old format: LEVEL_1 is project name, actual hierarchy starts from LEVEL_2
    const extractLevelPath = (row: HierarchyCSVRow): string[] => {
      const path: string[] = [];
      // Skip LEVEL_1 (project name), collect from LEVEL_2 onwards
      for (let i = 2; i <= 9; i++) {
        const levelKey = `LEVEL_${i}` as keyof HierarchyCSVRow;
        const levelValue = row[levelKey];
        if (levelValue && typeof levelValue === 'string' && levelValue.trim()) {
          path.push(levelValue.trim());
        }
      }
      return path;
    };

    // Helper function to find parent from level path
    const findParentFromLevelPath = (levelPath: string[]): string | null => {
      if (levelPath.length <= 1) return null;

      // The parent is at depth (levelPath.length - 1)
      // and has the name of the second-to-last element in the path
      const parentDepth = levelPath.length - 1;
      const parentName = levelPath[parentDepth - 1]; // Get parent's name

      const parentKey = `${parentDepth}:${parentName}`;
      return hierarchyByDepthAndName.get(parentKey) || null;
    };

    // Sort hierarchy groups by level depth (parents first)
    const sortedHierarchyEntries = Array.from(hierarchyGroupsMap.entries()).sort((a, b) => {
      const levelPathA = extractLevelPath(a[1][0]);
      const levelPathB = extractLevelPath(b[1][0]);
      return levelPathA.length - levelPathB.length;
    });

    // Pass 0.5: Auto-create missing intermediate parent nodes
    const allIntermediatePaths = new Set<string>();
    const existingPathsInCSV = new Set<string>();

    // First, collect all paths that exist in the CSV
    for (const [hierarchyName, groupRows] of sortedHierarchyEntries) {
      const mainRow = groupRows[0];
      const levelPath = extractLevelPath(mainRow);

      // Deduplicate consecutive same names
      const deduplicatedPath: string[] = [];
      for (let i = 0; i < levelPath.length; i++) {
        if (i === 0 || levelPath[i] !== levelPath[i - 1]) {
          deduplicatedPath.push(levelPath[i]);
        }
      }

      // Mark this exact path as existing in CSV
      const fullPathKey = deduplicatedPath.join(' > ');
      existingPathsInCSV.add(fullPathKey);

      // Collect all intermediate parent paths needed
      for (let depth = 1; depth < deduplicatedPath.length; depth++) {
        const parentPath = deduplicatedPath.slice(0, depth);
        const parentPathKey = parentPath.join(' > ');
        allIntermediatePaths.add(parentPathKey);
      }
    }

    // Filter to only create paths that DON'T exist in CSV
    const missingPaths = Array.from(allIntermediatePaths).filter(
      (path) => !existingPathsInCSV.has(path),
    );

    // Create missing parent hierarchies (sorted by depth)
    const sortedIntermediatePaths = missingPaths.sort(
      (a, b) => a.split(' > ').length - b.split(' > ').length,
    );

    for (const pathKey of sortedIntermediatePaths) {
      const pathParts = pathKey.split(' > ');
      const depth = pathParts.length;
      const parentName = pathParts[pathParts.length - 1];
      const depthNameKey = `${depth}:${parentName}`;

      // Skip if already exists
      if (hierarchyByDepthAndName.has(depthNameKey)) {
        continue;
      }

      // Generate a unique hierarchyId for this auto-created parent
      const autoHierarchyId = `AUTO_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

      // Check if exists in database
      const existingByName = await this.prisma.smartHierarchyMaster.findFirst({
        where: {
          projectId,
          hierarchyName: parentName,
        },
      });

      if (existingByName) {
        // Use existing hierarchy
        hierarchyByDepthAndName.set(depthNameKey, existingByName.hierarchyId);
        hierarchyNameToIdMap.set(parentName, existingByName.hierarchyId);
        levelPathToHierarchyIdMap.set(pathKey, existingByName.hierarchyId);
        continue;
      }

      // Build hierarchyLevel for auto-created parent
      const hierarchyLevel: any = {};
      for (let i = 0; i < pathParts.length; i++) {
        hierarchyLevel[`level_${i + 1}`] = pathParts[i];
      }

      const isRoot = pathParts.length === 1;

      // Create the auto-parent hierarchy
      const createdHierarchy = await this.prisma.smartHierarchyMaster.create({
        data: {
          hierarchyId: autoHierarchyId,
          hierarchyName: parentName,
          description: `Auto-created parent: ${parentName}`,
          sortOrder: 1,
          hierarchyLevel,
          isRoot,
          formulaConfig: null,
          mapping: [],
          filterConfig: null,
          pivotConfig: null,
          metadata: {},
          flags: {
            active_flag: true,
            exclude_flag: false,
            calculation_flag: false,
            customFlags: {},
          },
          createdBy: 'system',
          project: {
            connect: { id: projectId },
          },
        },
      });

      // Register the auto-created parent
      hierarchyByDepthAndName.set(depthNameKey, autoHierarchyId);
      hierarchyNameToIdMap.set(parentName, autoHierarchyId);
      levelPathToHierarchyIdMap.set(pathKey, autoHierarchyId);
      created++;
    }

    // Pass 1: Create all hierarchies without parent relationships
    for (const [hierarchyName, groupRows] of sortedHierarchyEntries) {
      try {
        groupRows.sort((a, b) => a.XREF_HIERARCHY_KEY - b.XREF_HIERARCHY_KEY);

        const mainRow = groupRows[0];
        const formulaRows = groupRows.slice(1);

        const hierarchyId = `HIER_${mainRow.XREF_HIERARCHY_KEY}`;

        // Extract level path
        const levelPath = extractLevelPath(mainRow);

        // Deduplicate consecutive same names in path
        const deduplicatedPath: string[] = [];
        for (let i = 0; i < levelPath.length; i++) {
          if (i === 0 || levelPath[i] !== levelPath[i - 1]) {
            deduplicatedPath.push(levelPath[i]);
          }
        }

        // Use deduplicated path for all processing
        const finalPath = deduplicatedPath;
        const levelPathKey = finalPath.join(' > ');
        const depth = finalPath.length;
        const currentLevelName = finalPath[finalPath.length - 1];
        const depthNameKey = `${depth}:${currentLevelName}`;

        // Check if this hierarchy is a duplicate of an already-created parent
        const existingParentId = hierarchyByDepthAndName.get(depthNameKey);
        if (existingParentId && existingParentId !== hierarchyId) {
          // Map the CSV hierarchyId to the existing parent for formula resolution
          xrefToHierarchyIdMap.set(mainRow.XREF_HIERARCHY_KEY, existingParentId);
          hierarchyNameToIdMap.set(hierarchyName, existingParentId);
          skipped++;
          continue;
        }

        xrefToHierarchyIdMap.set(mainRow.XREF_HIERARCHY_KEY, hierarchyId);
        hierarchyNameToIdMap.set(hierarchyName, hierarchyId);
        levelPathToHierarchyIdMap.set(levelPathKey, hierarchyId);
        hierarchyByDepthAndName.set(depthNameKey, hierarchyId);

        // Check if exists
        const existing = await this.prisma.smartHierarchyMaster.findUnique({
          where: {
            projectId_hierarchyId: { projectId, hierarchyId },
          },
        });

        if (existing) {
          skipped++;
          continue;
        }

        // Build hierarchyLevel object using deduplicated path
        // Map LEVEL_2 -> level_1, LEVEL_3 -> level_2, etc. (skip LEVEL_1 which is project name)
        const hierarchyLevel: any = {};
        for (let i = 0; i < finalPath.length; i++) {
          hierarchyLevel[`level_${i + 1}`] = finalPath[i];
        }

        // Determine isRoot:
        // Root nodes have exactly 1 level (depth 1)
        // Child nodes have 2+ levels
        const isRoot = finalPath.length === 1;

        // Build formula configuration
        const formulaConfig: any = {};
        if (mainRow.FORMULA_GROUP && mainRow.FORMULA_GROUP.trim()) {
          const rules = formulaRows.map((formulaRow) => {
            return {
              operation: formulaRow.ARITHMETIC_LOGIC || 'SUM',
              hierarchyId: null,
              hierarchyName: formulaRow.FORMULA_PARAM_REF || '',
              FORMULA_PRECEDENCE: formulaRow.FORMULA_PRECEDENCE || 1,
              FORMULA_PARAM2_CONST_NUMBER: formulaRow.FORMULA_PARAM2_CONST_NUMBER
                ? parseFloat(formulaRow.FORMULA_PARAM2_CONST_NUMBER)
                : undefined,
            };
          });

          // Set formula_text to the group name (displayed as single badge)
          formulaConfig.formula_group = {
            groupName: mainRow.FORMULA_GROUP.trim(),
            mainHierarchyId: hierarchyId,
            rules: rules,
            formula_params: {},
          };
        }

        // Build flags (map old format flags to new structure)
        const flags: any = {
          active_flag: mainRow.ACTIVE_FLAG ?? true,
          exclude_flag: mainRow.EXCLUSION_FLAG ?? false,
          calculation_flag: mainRow.CALCULATION_FLAG ?? false,
          customFlags: {
            do_not_expand_flag: mainRow.DO_NOT_EXPAND_FLAG ?? false,
            is_secured_flag: mainRow.IS_SECURED_FLAG ?? false,
            split_active_flag: mainRow.SPLIT_ACTIVE_FLAG ?? false,
            volume_flag: mainRow.VOLUME_FLAG ?? false,
            id_unpivot_flag: mainRow.ID_UNPIVOT_FLAG ?? false,
            id_row_flag: mainRow.ID_ROW_FLAG ?? false,
            remove_from_totals: mainRow.REMOVE_FROM_TOTALS ?? false,
            has_multiple_tables: mainRow.HAS_MULTIPLE_TABLES ?? false,
            create_new_column: mainRow.CREATE_NEW_COLUMN ?? false,
            sign_change_flag: mainRow.SIGN_CHANGE_FLAG ?? false,
          },
        };

        // Extract any custom flags from non-standard columns
        const standardColumns = [
          'XREF_HIERARCHY_KEY',
          'HIERARCHY_GROUP_NAME',
          'HIERARCHY_NAME',
          'ID_NAME',
          'LEVEL_0_SORT',
          'LEVEL_0',
          'LEVEL_1_SORT',
          'LEVEL_1',
          'LEVEL_2_SORT',
          'LEVEL_2',
          'LEVEL_3_SORT',
          'LEVEL_3',
          'LEVEL_4_SORT',
          'LEVEL_4',
          'LEVEL_5_SORT',
          'LEVEL_5',
          'LEVEL_6_SORT',
          'LEVEL_6',
          'LEVEL_7_SORT',
          'LEVEL_7',
          'LEVEL_8_SORT',
          'LEVEL_8',
          'LEVEL_9_SORT',
          'LEVEL_9',
          'GROUP_FILTER_PRECEDENCE',
          'PARENT_XREF_KEY',
          'SORT_ORDER',
          'IS_ROOT',
          'HAS_MULTIPLE_TABLES',
          'DO_NOT_EXPAND_FLAG',
          'IS_SECURED_FLAG',
          'SPLIT_ACTIVE_FLAG',
          'EXCLUSION_FLAG',
          'CALCULATION_FLAG',
          'ACTIVE_FLAG',
          'VOLUME_FLAG',
          'ID_UNPIVOT_FLAG',
          'ID_ROW_FLAG',
          'REMOVE_FROM_TOTALS',
          'FILTER_GROUP_1',
          'FILTER_GROUP_1_TYPE',
          'FILTER_GROUP_2',
          'FILTER_GROUP_2_TYPE',
          'FILTER_GROUP_3',
          'FILTER_GROUP_3_TYPE',
          'FILTER_GROUP_4',
          'FILTER_GROUP_4_TYPE',
          'FORMULA_GROUP',
          'SIGN_CHANGE_FLAG',
          'FORMULA_PRECEDENCE',
          'FORMULA_PARAM_REF',
          'ARITHMETIC_LOGIC',
          'FORMULA_REF_SOURCE',
          'FORMULA_REF_TABLE',
          'FORMULA_PARAM2_CONST_NUMBER',
          'CREATE_NEW_COLUMN',
          'UPDATED_BY',
          'UPDATED_AT',
          'ID',
          'ID_SOURCE',
          'ID_TABLE',
          'ID_SCHEMA',
          'ID_DATABASE', // Old format columns to ignore
        ];

        Object.keys(mainRow).forEach((key) => {
          if (!standardColumns.includes(key) && typeof mainRow[key] === 'boolean') {
            flags.customFlags[key] = mainRow[key];
          }
        });

        // Build filterConfig
        const filterConfig: any = {};
        if (mainRow.FILTER_GROUP_1) filterConfig.filter_group_1 = mainRow.FILTER_GROUP_1;
        if (mainRow.FILTER_GROUP_1_TYPE)
          filterConfig.filter_group_1_type = mainRow.FILTER_GROUP_1_TYPE;
        if (mainRow.FILTER_GROUP_2) filterConfig.filter_group_2 = mainRow.FILTER_GROUP_2;
        if (mainRow.FILTER_GROUP_2_TYPE)
          filterConfig.filter_group_2_type = mainRow.FILTER_GROUP_2_TYPE;
        if (mainRow.FILTER_GROUP_3) filterConfig.filter_group_3 = mainRow.FILTER_GROUP_3;
        if (mainRow.FILTER_GROUP_3_TYPE)
          filterConfig.filter_group_3_type = mainRow.FILTER_GROUP_3_TYPE;
        if (mainRow.FILTER_GROUP_4) filterConfig.filter_group_4 = mainRow.FILTER_GROUP_4;
        if (mainRow.FILTER_GROUP_4_TYPE)
          filterConfig.filter_group_4_type = mainRow.FILTER_GROUP_4_TYPE;

        // Create hierarchy
        await this.prisma.smartHierarchyMaster.create({
          data: {
            projectId,
            hierarchyId,
            hierarchyName: mainRow.HIERARCHY_NAME,
            description: mainRow.HIERARCHY_GROUP_NAME || null,
            parentId: null,
            sortOrder: mainRow.SORT_ORDER || 0,
            isRoot: isRoot,
            hierarchyLevel: hierarchyLevel,
            flags: flags,
            mapping: [],
            formulaConfig: Object.keys(formulaConfig).length > 0 ? formulaConfig : null,
            filterConfig: Object.keys(filterConfig).length > 0 ? filterConfig : null,
            pivotConfig: null,
            metadata: {},
            createdBy: mainRow.UPDATED_BY || 'system',
          },
        });

        imported++;
      } catch (error: any) {
        const errorMsg = `Failed to create hierarchy '${hierarchyName}': ${error.message}`;
        errors.push(errorMsg);
        skipped++;
      }
    }
    levelPathToHierarchyIdMap.forEach((hierarchyId, pathKey) => {});
    hierarchyByDepthAndName.forEach((hierarchyId, key) => {
      const [depth, name] = key.split(':');
    });
    hierarchyNameToIdMap.forEach((hierarchyId, name) => {});

    // Pass 1.5: Resolve formula references
    for (const [hierarchyName, groupRows] of sortedHierarchyEntries) {
      const mainRow = groupRows[0];
      const hierarchyId = hierarchyNameToIdMap.get(hierarchyName);

      if (!hierarchyId || !mainRow.FORMULA_GROUP || groupRows.length <= 1) {
        continue;
      }

      try {
        const hierarchy = await this.prisma.smartHierarchyMaster.findUnique({
          where: { projectId_hierarchyId: { projectId, hierarchyId } },
        });

        if (hierarchy && hierarchy.formulaConfig) {
          const formulaConfig = hierarchy.formulaConfig as any;

          if (formulaConfig.formula_group && formulaConfig.formula_group.rules) {
            const resolvedRules = formulaConfig.formula_group.rules.map((rule: any) => {
              const refHierarchyId = hierarchyNameToIdMap.get(rule.hierarchyName);
              return {
                ...rule,
                hierarchyId: refHierarchyId || rule.hierarchyId,
              };
            });

            formulaConfig.formula_group.rules = resolvedRules;

            await this.prisma.smartHierarchyMaster.update({
              where: { projectId_hierarchyId: { projectId, hierarchyId } },
              data: { formulaConfig: formulaConfig },
            });
          }
        }
      } catch (error: any) {
        const errorMsg = `Failed to resolve formulas for '${hierarchyName}': ${error.message}`;
        errors.push(errorMsg);
      }
    }

    // Pass 2: Update parent relationships using level path
    for (const [hierarchyName, groupRows] of sortedHierarchyEntries) {
      const mainRow = groupRows[0];
      const originalLevelPath = extractLevelPath(mainRow);

      // Deduplicate path (same logic as Pass 0.5 and Pass 1)
      const levelPath: string[] = [];
      for (let i = 0; i < originalLevelPath.length; i++) {
        if (i === 0 || originalLevelPath[i] !== originalLevelPath[i - 1]) {
          levelPath.push(originalLevelPath[i]);
        }
      }

      try {
        const childHierarchyId = xrefToHierarchyIdMap.get(mainRow.XREF_HIERARCHY_KEY);
        if (!childHierarchyId) {
          continue;
        }

        let parentHierarchyId: string | null = null;

        // Find parent from level path
        if (levelPath.length > 1) {
          const parentPath = levelPath.slice(0, -1);
          const parentPathKey = parentPath.join(' > ');
          const parentDepth = levelPath.length - 1;
          const parentName = levelPath[parentDepth - 1];
          const parentKey = `${parentDepth}:${parentName}`;

          parentHierarchyId = findParentFromLevelPath(levelPath);
        } else {
        }

        if (parentHierarchyId) {
          const parent = await this.prisma.smartHierarchyMaster.findUnique({
            where: { projectId_hierarchyId: { projectId, hierarchyId: parentHierarchyId } },
          });

          if (parent) {
            await this.prisma.smartHierarchyMaster.update({
              where: { projectId_hierarchyId: { projectId, hierarchyId: childHierarchyId } },
              data: {
                parentId: parent.id,
                isRoot: false,
              },
            });
          } else {
          }
        } else {
        }
      } catch (error: any) {
        const errorMsg = `Failed to link parent for '${hierarchyName}': ${error.message}`;
        errors.push(errorMsg);
      }
    }

    return { imported, skipped, errors };
  }

  /**
   * Import mappings from CSV
   */
  async importMappingCSV(
    projectId: string,
    csvContent: string,
  ): Promise<{ imported: number; skipped: number; errors: string[] }> {
    const project = await this.prisma.hierarchyProject.findUnique({
      where: { id: projectId },
    });

    if (!project) {
      throw new NotFoundException(`Project '${projectId}' not found`);
    }

    const rows = this.parseMappingCSV(csvContent);

    // Group by FK_REPORT_KEY
    const mappingsByHierarchy = new Map<number, MappingCSVRow[]>();
    rows.forEach((row) => {
      const existing = mappingsByHierarchy.get(row.FK_REPORT_KEY) || [];
      existing.push(row);
      mappingsByHierarchy.set(row.FK_REPORT_KEY, existing);
    });

    let imported = 0;
    let skipped = 0;
    const errors: string[] = [];

    // Update each hierarchy with its mappings
    for (const [fkReportKey, mappingRows] of mappingsByHierarchy.entries()) {
      try {
        const hierarchyId = `HIER_${fkReportKey}`;

        // Check if hierarchy exists
        const hierarchy = await this.prisma.smartHierarchyMaster.findUnique({
          where: {
            projectId_hierarchyId: {
              projectId,
              hierarchyId,
            },
          },
        });

        if (!hierarchy) {
          const errorMsg = `Hierarchy ${hierarchyId} (FK_REPORT_KEY ${fkReportKey}) not found`;
          console.warn(errorMsg);
          errors.push(errorMsg);
          skipped += mappingRows.length;
          continue;
        }

        // Build mapping array
        const mappings = mappingRows.map((row) => {
          // Extract custom flags (any columns not in standard set)
          const standardColumns = [
            'FK_REPORT_KEY',
            'HIERARCHY_NAME',
            'LEVEL_NODE',
            'SORT_ORDER',
            'XREF_HIERARCHY_KEY',
            'SOURCE_DATABASE',
            'SOURCE_SCHEMA',
            'SOURCE_TABLE',
            'SOURCE_COLUMN',
            'SOURCE_UID',
            'PRECEDENCE_GROUP',
            'ACTIVE_FLAG',
            'EXCLUDE_FLAG',
            'INCLUDE_FLAG',
            'TRANSFORM_FLAG',
          ];

          const customFlags: any = {};
          Object.keys(row).forEach((key) => {
            if (!standardColumns.includes(key) && typeof row[key] === 'boolean') {
              customFlags[key] = row[key];
            }
          });

          return {
            mapping_index: row.XREF_HIERARCHY_KEY,
            source_database: row.SOURCE_DATABASE,
            source_schema: row.SOURCE_SCHEMA,
            source_table: row.SOURCE_TABLE,
            source_column: row.SOURCE_COLUMN,
            source_uid: row.SOURCE_UID,
            precedence_group: row.PRECEDENCE_GROUP,
            flags: {
              active_flag: row.ACTIVE_FLAG,
              exclude_flag: row.EXCLUDE_FLAG,
              include_flag: row.INCLUDE_FLAG,
              transform_flag: row.TRANSFORM_FLAG,
              ...(Object.keys(customFlags).length > 0 ? { customFlags } : {}),
            },
          };
        });

        // Update hierarchy with mappings
        await this.prisma.smartHierarchyMaster.update({
          where: {
            projectId_hierarchyId: {
              projectId,
              hierarchyId,
            },
          },
          data: {
            mapping: mappings as any,
          },
        });

        imported += mappingRows.length;
      } catch (error: any) {
        const errorMsg = `Failed to import mappings for FK_REPORT_KEY ${fkReportKey}: ${error.message}`;
        errors.push(errorMsg);
        skipped += mappingRows.length;
      }
    }

    return { imported, skipped, errors };
  }

  // ============================================================================
  // HELPER METHODS
  // ============================================================================

  private async getXrefKeyFromUuid(
    uuid: string,
    keyMap: Map<string, number>,
  ): Promise<number | null> {
    const hierarchy = await this.prisma.smartHierarchyMaster.findUnique({
      where: { id: uuid },
    });

    if (!hierarchy) return null;
    return keyMap.get(hierarchy.hierarchyId) || null;
  }

  private convertHierarchyToCSV(rows: HierarchyCSVRow[], customFlagKeys: string[] = []): string {
    const headers = [
      'XREF_HIERARCHY_KEY',
      'HIERARCHY_GROUP_NAME',
      'LEVEL_1_SORT',
      'LEVEL_1',
      'LEVEL_2_SORT',
      'LEVEL_2',
      'LEVEL_3_SORT',
      'LEVEL_3',
      'LEVEL_4_SORT',
      'LEVEL_4',
      'LEVEL_5_SORT',
      'LEVEL_5',
      'LEVEL_6_SORT',
      'LEVEL_6',
      'LEVEL_7_SORT',
      'LEVEL_7',
      'LEVEL_8_SORT',
      'LEVEL_8',
      'LEVEL_9_SORT',
      'LEVEL_9',
      'GROUP_FILTER_PRECEDENCE',
      'HIERARCHY_NAME',
      'PARENT_XREF_KEY',
      'SORT_ORDER',
      'IS_ROOT',
      'HAS_MULTIPLE_TABLES',
      'DO_NOT_EXPAND_FLAG',
      'IS_SECURED_FLAG',
      'SPLIT_ACTIVE_FLAG',
      'EXCLUSION_FLAG',
      'CALCULATION_FLAG',
      'ACTIVE_FLAG',
      'VOLUME_FLAG',
      'ID_UNPIVOT_FLAG',
      'ID_ROW_FLAG',
      'REMOVE_FROM_TOTALS',
      'FILTER_GROUP_1',
      'FILTER_GROUP_1_TYPE',
      'FILTER_GROUP_2',
      'FILTER_GROUP_2_TYPE',
      'FILTER_GROUP_3',
      'FILTER_GROUP_3_TYPE',
      'FILTER_GROUP_4',
      'FILTER_GROUP_4_TYPE',
      'FORMULA_GROUP',
      'SIGN_CHANGE_FLAG',
      'FORMULA_PRECEDENCE',
      'FORMULA_PARAM_REF',
      'ARITHMETIC_LOGIC',
      'FORMULA_REF_SOURCE',
      'FORMULA_REF_TABLE',
      'FORMULA_PARAM2_CONST_NUMBER',
      'CREATE_NEW_COLUMN',
      'UPDATED_BY',
      'UPDATED_AT',
      ...customFlagKeys, // Add dynamic custom flag columns
    ];

    const csvRows = [headers.join(',')];

    rows.forEach((row) => {
      const values = [
        row.XREF_HIERARCHY_KEY.toString(),
        this.escapeCSV(row.HIERARCHY_GROUP_NAME),
        row.LEVEL_1_SORT.toString(),
        this.escapeCSV(row.LEVEL_1),
        row.LEVEL_2_SORT.toString(),
        this.escapeCSV(row.LEVEL_2),
        row.LEVEL_3_SORT.toString(),
        this.escapeCSV(row.LEVEL_3),
        row.LEVEL_4_SORT.toString(),
        this.escapeCSV(row.LEVEL_4),
        row.LEVEL_5_SORT.toString(),
        this.escapeCSV(row.LEVEL_5),
        row.LEVEL_6_SORT.toString(),
        this.escapeCSV(row.LEVEL_6),
        row.LEVEL_7_SORT.toString(),
        this.escapeCSV(row.LEVEL_7),
        row.LEVEL_8_SORT.toString(),
        this.escapeCSV(row.LEVEL_8),
        row.LEVEL_9_SORT.toString(),
        this.escapeCSV(row.LEVEL_9),
        row.GROUP_FILTER_PRECEDENCE.toString(),
        this.escapeCSV(row.HIERARCHY_NAME),
        row.PARENT_XREF_KEY?.toString() || '',
        row.SORT_ORDER.toString(),
        row.IS_ROOT ? 'TRUE' : 'FALSE',
        row.HAS_MULTIPLE_TABLES ? 'TRUE' : 'FALSE',
        row.DO_NOT_EXPAND_FLAG ? 'TRUE' : 'FALSE',
        row.IS_SECURED_FLAG ? 'TRUE' : 'FALSE',
        row.SPLIT_ACTIVE_FLAG ? 'TRUE' : 'FALSE',
        row.EXCLUSION_FLAG ? 'TRUE' : 'FALSE',
        row.CALCULATION_FLAG ? 'TRUE' : 'FALSE',
        row.ACTIVE_FLAG ? 'TRUE' : 'FALSE',
        row.VOLUME_FLAG ? 'TRUE' : 'FALSE',
        row.ID_UNPIVOT_FLAG ? 'TRUE' : 'FALSE',
        row.ID_ROW_FLAG ? 'TRUE' : 'FALSE',
        row.REMOVE_FROM_TOTALS ? 'TRUE' : 'FALSE',
        this.escapeCSV(row.FILTER_GROUP_1),
        this.escapeCSV(row.FILTER_GROUP_1_TYPE),
        this.escapeCSV(row.FILTER_GROUP_2),
        this.escapeCSV(row.FILTER_GROUP_2_TYPE),
        this.escapeCSV(row.FILTER_GROUP_3),
        this.escapeCSV(row.FILTER_GROUP_3_TYPE),
        this.escapeCSV(row.FILTER_GROUP_4),
        this.escapeCSV(row.FILTER_GROUP_4_TYPE),
        this.escapeCSV(row.FORMULA_GROUP),
        row.SIGN_CHANGE_FLAG ? 'TRUE' : 'FALSE',
        row.FORMULA_PRECEDENCE.toString(),
        this.escapeCSV(row.FORMULA_PARAM_REF),
        this.escapeCSV(row.ARITHMETIC_LOGIC),
        this.escapeCSV(row.FORMULA_REF_SOURCE),
        this.escapeCSV(row.FORMULA_REF_TABLE),
        this.escapeCSV(row.FORMULA_PARAM2_CONST_NUMBER),
        row.CREATE_NEW_COLUMN ? 'TRUE' : 'FALSE',
        this.escapeCSV(row.UPDATED_BY),
        this.escapeCSV(row.UPDATED_AT),
        // Add custom flag values
        ...customFlagKeys.map((key) => (row[key] ? 'TRUE' : 'FALSE')),
      ];
      csvRows.push(values.join(','));
    });

    return csvRows.join('\n');
  }

  private convertMappingToCSV(rows: MappingCSVRow[], customFlagKeys: string[] = []): string {
    const headers = [
      'FK_REPORT_KEY',
      'HIERARCHY_NAME',
      'LEVEL_NODE',
      'SORT_ORDER',
      'XREF_HIERARCHY_KEY',
      'SOURCE_DATABASE',
      'SOURCE_SCHEMA',
      'SOURCE_TABLE',
      'SOURCE_COLUMN',
      'SOURCE_UID',
      'PRECEDENCE_GROUP',
      'ACTIVE_FLAG',
      'EXCLUDE_FLAG',
      'INCLUDE_FLAG',
      'TRANSFORM_FLAG',
      ...customFlagKeys, // Add custom flag columns
    ];

    const csvRows = [headers.join(',')];

    rows.forEach((row) => {
      const values = [
        row.FK_REPORT_KEY.toString(),
        this.escapeCSV(row.HIERARCHY_NAME),
        this.escapeCSV(row.LEVEL_NODE),
        row.SORT_ORDER.toString(),
        row.XREF_HIERARCHY_KEY.toString(),
        this.escapeCSV(row.SOURCE_DATABASE),
        this.escapeCSV(row.SOURCE_SCHEMA),
        this.escapeCSV(row.SOURCE_TABLE),
        this.escapeCSV(row.SOURCE_COLUMN),
        this.escapeCSV(row.SOURCE_UID),
        this.escapeCSV(row.PRECEDENCE_GROUP),
        row.ACTIVE_FLAG ? 'TRUE' : 'FALSE',
        row.EXCLUDE_FLAG ? 'TRUE' : 'FALSE',
        row.INCLUDE_FLAG ? 'TRUE' : 'FALSE',
        row.TRANSFORM_FLAG ? 'TRUE' : 'FALSE',
        // Add custom flag values
        ...customFlagKeys.map((key) => (row[key] ? 'TRUE' : 'FALSE')),
      ];
      csvRows.push(values.join(','));
    });

    return csvRows.join('\n');
  }

  private parseHierarchyCSV(csvContent: string): HierarchyCSVRow[] {
    const lines = csvContent.split('\n').filter((line) => line.trim());
    if (lines.length < 2) return [];

    const headers = this.parseCSVLine(lines[0]);
    const rows: HierarchyCSVRow[] = [];

    // Identify standard columns for hierarchy (both uppercase and lowercase variants)
    const standardColumns = [
      'XREF_HIERARCHY_KEY',
      'FK_REPORT_KEY',
      'HIERARCHY_GROUP_NAME',
      'REPORT_TYPE',
      'SORT_ORDER',
      'LEVEL_0_SORT',
      'LEVEL_0',
      'LEVEL_1_SORT',
      'LEVEL_1',
      'LEVEL_2_SORT',
      'LEVEL_2',
      'LEVEL_3_SORT',
      'LEVEL_3',
      'LEVEL_4_SORT',
      'LEVEL_4',
      'LEVEL_5_SORT',
      'LEVEL_5',
      'LEVEL_6_SORT',
      'LEVEL_6',
      'LEVEL_7_SORT',
      'LEVEL_7',
      'LEVEL_8_SORT',
      'LEVEL_8',
      'LEVEL_9_SORT',
      'LEVEL_9',
      'GROUP_FILTER_PRECEDENCE',
      'HIERARCHY_NAME',
      'PARENT_XREF_KEY',
      'IS_ROOT',
      'DO_NOT_EXPAND_FLAG',
      'do_not_expand_flag',
      'IS_SECURED_FLAG',
      'is_secured_flag',
      'SPLIT_ACTIVE_FLAG',
      'split_active_flag',
      'EXCLUSION_FLAG',
      'CALCULATION_FLAG',
      'ACTIVE_FLAG',
      'VOLUME_FLAG',
      'volume_flag',
      'ID_UNPIVOT_FLAG',
      'id_unpivot_flag',
      'ID_ROW_FLAG',
      'id_row_flag',
      'REMOVE_FROM_TOTALS',
      'remove_from_totals',
      'HAS_MULTIPLE_TABLES',
      'has_multiple_tables',
      'FILTER_GROUP_1',
      'FILTER_GROUP_1_TYPE',
      'FILTER_GROUP_2',
      'FILTER_GROUP_2_TYPE',
      'FILTER_GROUP_3',
      'FILTER_GROUP_3_TYPE',
      'FILTER_GROUP_4',
      'FILTER_GROUP_4_TYPE',
      'FORMULA_GROUP',
      'SIGN_CHANGE_FLAG',
      'sign_change_flag',
      'FORMULA_PRECEDENCE',
      'FORMULA_PARAM_REF',
      'ARITHMETIC_LOGIC',
      'FORMULA_REF_SOURCE',
      'FORMULA_REF_TABLE',
      'FORMULA_PARAM2_CONST_NUMBER',
      'CREATE_NEW_COLUMN',
      'create_new_column',
      'UPDATED_BY',
      'UPDATED_AT',
    ];

    for (let i = 1; i < lines.length; i++) {
      const values = this.parseCSVLine(lines[i]);
      if (values.length !== headers.length) continue;

      const row: any = {};
      headers.forEach((header, index) => {
        row[header] = values[index];
      });

      // Detect non-standard columns as custom flags (exclude lowercase duplicates of standard flags)
      const standardFlagPatterns = [
        'do_not_expand_flag',
        'is_secured_flag',
        'split_active_flag',
        'volume_flag',
        'id_unpivot_flag',
        'id_row_flag',
        'remove_from_totals',
        'has_multiple_tables',
        'sign_change_flag',
        'create_new_column',
      ];

      headers.forEach((header, index) => {
        if (!standardColumns.includes(header)) {
          // Check if this is a lowercase duplicate of a standard flag
          const isStandardFlagDuplicate = standardFlagPatterns.includes(header.toLowerCase());
          if (!isStandardFlagDuplicate) {
            // This is a truly custom flag - store in uppercase
            row[header.toUpperCase()] = values[index]?.toUpperCase() === 'TRUE';
          }
        }
      });

      // Also handle lowercase standard flags in the row data (merge with uppercase)
      const do_not_expand =
        row.DO_NOT_EXPAND_FLAG?.toUpperCase() === 'TRUE' ||
        row.do_not_expand_flag?.toUpperCase() === 'TRUE';
      const is_secured =
        row.IS_SECURED_FLAG?.toUpperCase() === 'TRUE' ||
        row.is_secured_flag?.toUpperCase() === 'TRUE';
      const split_active =
        row.SPLIT_ACTIVE_FLAG?.toUpperCase() === 'TRUE' ||
        row.split_active_flag?.toUpperCase() === 'TRUE';
      const volume =
        row.VOLUME_FLAG?.toUpperCase() === 'TRUE' || row.volume_flag?.toUpperCase() === 'TRUE';
      const id_unpivot =
        row.ID_UNPIVOT_FLAG?.toUpperCase() === 'TRUE' ||
        row.id_unpivot_flag?.toUpperCase() === 'TRUE';
      const id_row =
        row.ID_ROW_FLAG?.toUpperCase() === 'TRUE' || row.id_row_flag?.toUpperCase() === 'TRUE';
      const remove_totals =
        row.REMOVE_FROM_TOTALS?.toUpperCase() === 'TRUE' ||
        row.remove_from_totals?.toUpperCase() === 'TRUE';
      const has_multiple =
        row.HAS_MULTIPLE_TABLES?.toUpperCase() === 'TRUE' ||
        row.has_multiple_tables?.toUpperCase() === 'TRUE';
      const sign_change =
        row.SIGN_CHANGE_FLAG?.toUpperCase() === 'TRUE' ||
        row.sign_change_flag?.toUpperCase() === 'TRUE';
      const create_new =
        row.CREATE_NEW_COLUMN?.toUpperCase() === 'TRUE' ||
        row.create_new_column?.toUpperCase() === 'TRUE';

      rows.push({
        XREF_HIERARCHY_KEY: parseInt(row.XREF_HIERARCHY_KEY) || 0,
        HIERARCHY_GROUP_NAME: row.HIERARCHY_GROUP_NAME || '',
        LEVEL_1_SORT: parseInt(row.LEVEL_1_SORT) || 0,
        LEVEL_1: row.LEVEL_1 || '',
        LEVEL_2_SORT: parseInt(row.LEVEL_2_SORT) || 0,
        LEVEL_2: row.LEVEL_2 || '',
        LEVEL_3_SORT: parseInt(row.LEVEL_3_SORT) || 0,
        LEVEL_3: row.LEVEL_3 || '',
        LEVEL_4_SORT: parseInt(row.LEVEL_4_SORT) || 0,
        LEVEL_4: row.LEVEL_4 || '',
        LEVEL_5_SORT: parseInt(row.LEVEL_5_SORT) || 0,
        LEVEL_5: row.LEVEL_5 || '',
        LEVEL_6_SORT: parseInt(row.LEVEL_6_SORT) || 0,
        LEVEL_6: row.LEVEL_6 || '',
        LEVEL_7_SORT: parseInt(row.LEVEL_7_SORT) || 0,
        LEVEL_7: row.LEVEL_7 || '',
        LEVEL_8_SORT: parseInt(row.LEVEL_8_SORT) || 0,
        LEVEL_8: row.LEVEL_8 || '',
        LEVEL_9_SORT: parseInt(row.LEVEL_9_SORT) || 0,
        LEVEL_9: row.LEVEL_9 || '',
        GROUP_FILTER_PRECEDENCE: parseInt(row.GROUP_FILTER_PRECEDENCE) || 1,
        HIERARCHY_NAME: row.HIERARCHY_NAME || '',
        PARENT_XREF_KEY: row.PARENT_XREF_KEY ? parseInt(row.PARENT_XREF_KEY) : null,
        SORT_ORDER: parseInt(row.SORT_ORDER) || 0,
        IS_ROOT: row.IS_ROOT?.toUpperCase() === 'TRUE',
        // Individual flags (merge uppercase and lowercase columns)
        DO_NOT_EXPAND_FLAG: do_not_expand,
        IS_SECURED_FLAG: is_secured,
        SPLIT_ACTIVE_FLAG: split_active,
        EXCLUSION_FLAG: row.EXCLUSION_FLAG?.toUpperCase() === 'TRUE',
        CALCULATION_FLAG: row.CALCULATION_FLAG?.toUpperCase() === 'TRUE',
        ACTIVE_FLAG: row.ACTIVE_FLAG?.toUpperCase() === 'TRUE',
        VOLUME_FLAG: volume,
        ID_UNPIVOT_FLAG: id_unpivot,
        ID_ROW_FLAG: id_row,
        REMOVE_FROM_TOTALS: remove_totals,
        HAS_MULTIPLE_TABLES: has_multiple,
        // Individual filters
        FILTER_GROUP_1: row.FILTER_GROUP_1 || '',
        FILTER_GROUP_1_TYPE: row.FILTER_GROUP_1_TYPE || '',
        FILTER_GROUP_2: row.FILTER_GROUP_2 || '',
        FILTER_GROUP_2_TYPE: row.FILTER_GROUP_2_TYPE || '',
        FILTER_GROUP_3: row.FILTER_GROUP_3 || '',
        FILTER_GROUP_3_TYPE: row.FILTER_GROUP_3_TYPE || '',
        FILTER_GROUP_4: row.FILTER_GROUP_4 || '',
        FILTER_GROUP_4_TYPE: row.FILTER_GROUP_4_TYPE || '',
        // Formula columns
        FORMULA_GROUP: row.FORMULA_GROUP || '',
        SIGN_CHANGE_FLAG: sign_change,
        FORMULA_PRECEDENCE: parseInt(row.FORMULA_PRECEDENCE) || 1,
        FORMULA_PARAM_REF: row.FORMULA_PARAM_REF || '',
        ARITHMETIC_LOGIC: row.ARITHMETIC_LOGIC || '',
        FORMULA_REF_SOURCE: row.FORMULA_REF_SOURCE || '',
        FORMULA_REF_TABLE: row.FORMULA_REF_TABLE || '',
        FORMULA_PARAM2_CONST_NUMBER: row.FORMULA_PARAM2_CONST_NUMBER || '',
        CREATE_NEW_COLUMN: create_new,
        UPDATED_BY: row.UPDATED_BY || '',
        UPDATED_AT: row.UPDATED_AT || '',
        // Add custom flags dynamically (already in UPPERCASE from header processing)
        ...Object.keys(row)
          .filter(
            (key) =>
              key === key.toUpperCase() &&
              !standardColumns.includes(key) &&
              !standardColumns.includes(key.toLowerCase()),
          )
          .reduce((acc, key) => ({ ...acc, [key]: row[key] }), {}),
      });
    }

    return rows;
  }

  /**
   * Parse OLD ENP format CSV (with ID_NAME column instead of HIERARCHY_NAME)
   */
  private parseHierarchyCSV_OldFormat(csvContent: string): HierarchyCSVRow[] {
    const lines = csvContent.split('\n').filter((line) => line.trim());
    if (lines.length < 2) return [];

    const headers = this.parseCSVLine(lines[0]);
    const rows: HierarchyCSVRow[] = [];

    // Standard columns for old ENP format
    const standardColumns = [
      'XREF_HIERARCHY_KEY',
      'HIERARCHY_GROUP_NAME',
      'ID_NAME',
      'ID',
      'ID_SOURCE',
      'ID_TABLE',
      'ID_SCHEMA',
      'ID_DATABASE',
      'SORT_ORDER',
      'LEVEL_0_SORT',
      'LEVEL_0',
      'LEVEL_1_SORT',
      'LEVEL_1',
      'LEVEL_2_SORT',
      'LEVEL_2',
      'LEVEL_3_SORT',
      'LEVEL_3',
      'LEVEL_4_SORT',
      'LEVEL_4',
      'LEVEL_5_SORT',
      'LEVEL_5',
      'LEVEL_6_SORT',
      'LEVEL_6',
      'LEVEL_7_SORT',
      'LEVEL_7',
      'LEVEL_8_SORT',
      'LEVEL_8',
      'LEVEL_9_SORT',
      'LEVEL_9',
      'GROUP_FILTER_PRECEDENCE',
      'IS_ROOT',
      'HAS_MULTIPLE_TABLES',
      'DO_NOT_EXPAND_FLAG',
      'IS_SECURED_FLAG',
      'SPLIT_ACTIVE_FLAG',
      'EXCLUSION_FLAG',
      'CALCULATION_FLAG',
      'ACTIVE_FLAG',
      'VOLUME_FLAG',
      'ID_UNPIVOT_FLAG',
      'ID_ROW_FLAG',
      'REMOVE_FROM_TOTALS',
      'FILTER_GROUP_1',
      'FILTER_GROUP_1_TYPE',
      'FILTER_GROUP_2',
      'FILTER_GROUP_2_TYPE',
      'FILTER_GROUP_3',
      'FILTER_GROUP_3_TYPE',
      'FILTER_GROUP_4',
      'FILTER_GROUP_4_TYPE',
      'FORMULA_GROUP',
      'SIGN_CHANGE_FLAG',
      'FORMULA_PRECEDENCE',
      'FORMULA_PARAM_REF',
      'ARITHMETIC_LOGIC',
      'FORMULA_REF_SOURCE',
      'FORMULA_REF_TABLE',
      'FORMULA_PARAM2_CONST_NUMBER',
      'CREATE_NEW_COLUMN',
      'UPDATED_BY',
      'UPDATED_AT',
    ];

    for (let i = 1; i < lines.length; i++) {
      const values = this.parseCSVLine(lines[i]);
      const row: any = {};

      headers.forEach((header, index) => {
        row[header] = values[index] || '';
      });

      // Build hierarchy name from the last non-empty level column
      // This is the actual node name in the hierarchy
      let hierarchyName = '';
      for (let level = 9; level >= 1; level--) {
        const levelValue = row[`LEVEL_${level}`];
        if (levelValue && levelValue.trim()) {
          hierarchyName = levelValue.trim();
          break;
        }
      }

      if (!hierarchyName) continue; // Skip rows with no level data

      rows.push({
        XREF_HIERARCHY_KEY: parseInt(row.XREF_HIERARCHY_KEY) || 0,
        HIERARCHY_GROUP_NAME: row.HIERARCHY_GROUP_NAME || '',
        HIERARCHY_NAME: hierarchyName, // Map ID_NAME to HIERARCHY_NAME
        PARENT_XREF_KEY: parseInt(row.PARENT_XREF_KEY) || null,
        SORT_ORDER: parseInt(row.SORT_ORDER) || 0,
        IS_ROOT: row.IS_ROOT?.toUpperCase() === 'TRUE',
        // Level columns (including LEVEL_0 for old format)
        LEVEL_0_SORT: parseInt(row.LEVEL_0_SORT) || 0,
        LEVEL_0: row.LEVEL_0 || '',
        LEVEL_1_SORT: parseInt(row.LEVEL_1_SORT) || 0,
        LEVEL_1: row.LEVEL_1 || '',
        LEVEL_2_SORT: parseInt(row.LEVEL_2_SORT) || 0,
        LEVEL_2: row.LEVEL_2 || '',
        LEVEL_3_SORT: parseInt(row.LEVEL_3_SORT) || 0,
        LEVEL_3: row.LEVEL_3 || '',
        LEVEL_4_SORT: parseInt(row.LEVEL_4_SORT) || 0,
        LEVEL_4: row.LEVEL_4 || '',
        LEVEL_5_SORT: parseInt(row.LEVEL_5_SORT) || 0,
        LEVEL_5: row.LEVEL_5 || '',
        LEVEL_6_SORT: parseInt(row.LEVEL_6_SORT) || 0,
        LEVEL_6: row.LEVEL_6 || '',
        LEVEL_7_SORT: parseInt(row.LEVEL_7_SORT) || 0,
        LEVEL_7: row.LEVEL_7 || '',
        LEVEL_8_SORT: parseInt(row.LEVEL_8_SORT) || 0,
        LEVEL_8: row.LEVEL_8 || '',
        LEVEL_9_SORT: parseInt(row.LEVEL_9_SORT) || 0,
        LEVEL_9: row.LEVEL_9 || '',
        GROUP_FILTER_PRECEDENCE: parseInt(row.GROUP_FILTER_PRECEDENCE) || 1,
        // Flags
        DO_NOT_EXPAND_FLAG: row.DO_NOT_EXPAND_FLAG?.toUpperCase() === 'TRUE',
        IS_SECURED_FLAG: row.IS_SECURED_FLAG?.toUpperCase() === 'TRUE',
        SPLIT_ACTIVE_FLAG: row.SPLIT_ACTIVE_FLAG?.toUpperCase() === 'TRUE',
        EXCLUSION_FLAG: row.EXCLUSION_FLAG?.toUpperCase() === 'TRUE',
        CALCULATION_FLAG: row.CALCULATION_FLAG?.toUpperCase() === 'TRUE',
        ACTIVE_FLAG: row.ACTIVE_FLAG?.toUpperCase() === 'TRUE',
        VOLUME_FLAG: row.VOLUME_FLAG?.toUpperCase() === 'TRUE',
        ID_UNPIVOT_FLAG: row.ID_UNPIVOT_FLAG?.toUpperCase() === 'TRUE',
        ID_ROW_FLAG: row.ID_ROW_FLAG?.toUpperCase() === 'TRUE',
        REMOVE_FROM_TOTALS: row.REMOVE_FROM_TOTALS?.toUpperCase() === 'TRUE',
        HAS_MULTIPLE_TABLES: row.HAS_MULTIPLE_TABLES?.toUpperCase() === 'TRUE',
        // Filters
        FILTER_GROUP_1: row.FILTER_GROUP_1 || '',
        FILTER_GROUP_1_TYPE: row.FILTER_GROUP_1_TYPE || '',
        FILTER_GROUP_2: row.FILTER_GROUP_2 || '',
        FILTER_GROUP_2_TYPE: row.FILTER_GROUP_2_TYPE || '',
        FILTER_GROUP_3: row.FILTER_GROUP_3 || '',
        FILTER_GROUP_3_TYPE: row.FILTER_GROUP_3_TYPE || '',
        FILTER_GROUP_4: row.FILTER_GROUP_4 || '',
        FILTER_GROUP_4_TYPE: row.FILTER_GROUP_4_TYPE || '',
        // Formulas
        FORMULA_GROUP: row.FORMULA_GROUP || '',
        SIGN_CHANGE_FLAG: row.SIGN_CHANGE_FLAG?.toUpperCase() === 'TRUE',
        FORMULA_PRECEDENCE: parseInt(row.FORMULA_PRECEDENCE) || 1,
        FORMULA_PARAM_REF: row.FORMULA_PARAM_REF || '',
        ARITHMETIC_LOGIC: row.ARITHMETIC_LOGIC || '',
        FORMULA_REF_SOURCE: row.FORMULA_REF_SOURCE || '',
        FORMULA_REF_TABLE: row.FORMULA_REF_TABLE || '',
        FORMULA_PARAM2_CONST_NUMBER: row.FORMULA_PARAM2_CONST_NUMBER || '',
        CREATE_NEW_COLUMN: row.CREATE_NEW_COLUMN?.toUpperCase() === 'TRUE',
        UPDATED_BY: row.UPDATED_BY || '',
        UPDATED_AT: row.UPDATED_AT || '',
        // Add custom flags dynamically (ignore old ID columns)
        ...Object.keys(row)
          .filter((key) => !standardColumns.includes(key))
          .reduce((acc, key) => ({ ...acc, [key]: row[key] }), {}),
      } as HierarchyCSVRow);
    }

    return rows;
  }

  private parseMappingCSV(csvContent: string): MappingCSVRow[] {
    const lines = csvContent.split('\n').filter((line) => line.trim());
    if (lines.length < 2) return [];

    const headers = this.parseCSVLine(lines[0]);
    const rows: MappingCSVRow[] = [];

    // Identify standard columns
    const standardColumns = [
      'FK_REPORT_KEY',
      'HIERARCHY_NAME',
      'LEVEL_NODE',
      'SORT_ORDER',
      'XREF_HIERARCHY_KEY',
      'SOURCE_DATABASE',
      'SOURCE_SCHEMA',
      'SOURCE_TABLE',
      'SOURCE_COLUMN',
      'SOURCE_UID',
      'PRECEDENCE_GROUP',
      'ACTIVE_FLAG',
      'EXCLUDE_FLAG',
      'INCLUDE_FLAG',
      'TRANSFORM_FLAG',
    ];

    for (let i = 1; i < lines.length; i++) {
      const values = this.parseCSVLine(lines[i]);
      if (values.length !== headers.length) continue;

      const row: any = {};
      headers.forEach((header, index) => {
        row[header] = values[index];
      });

      const parsedRow: MappingCSVRow = {
        FK_REPORT_KEY: parseInt(row.FK_REPORT_KEY) || 0,
        HIERARCHY_NAME: row.HIERARCHY_NAME || '',
        LEVEL_NODE: row.LEVEL_NODE || '',
        SORT_ORDER: parseInt(row.SORT_ORDER) || 0,
        XREF_HIERARCHY_KEY: parseInt(row.XREF_HIERARCHY_KEY) || 0,
        SOURCE_DATABASE: row.SOURCE_DATABASE || '',
        SOURCE_SCHEMA: row.SOURCE_SCHEMA || '',
        SOURCE_TABLE: row.SOURCE_TABLE || '',
        SOURCE_COLUMN: row.SOURCE_COLUMN || '',
        SOURCE_UID: row.SOURCE_UID || '',
        PRECEDENCE_GROUP: row.PRECEDENCE_GROUP || '',
        ACTIVE_FLAG: row.ACTIVE_FLAG === 'TRUE',
        EXCLUDE_FLAG: row.EXCLUDE_FLAG === 'TRUE',
        INCLUDE_FLAG: row.INCLUDE_FLAG === 'TRUE',
        TRANSFORM_FLAG: row.TRANSFORM_FLAG === 'TRUE',
      };

      // Parse any additional columns as custom flags
      headers.forEach((header, index) => {
        if (!standardColumns.includes(header)) {
          parsedRow[header] = values[index] === 'TRUE';
        }
      });

      rows.push(parsedRow);
    }

    return rows;
  }

  private parseCSVLine(line: string): string[] {
    const values: string[] = [];
    let current = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      const nextChar = line[i + 1];

      if (char === '"') {
        if (inQuotes && nextChar === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === ',' && !inQuotes) {
        values.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }

    values.push(current.trim());
    return values;
  }

  private escapeCSV(value: string): string {
    if (!value) return '""';
    const str = value.toString();
    if (str.includes(',') || str.includes('"') || str.includes('\n')) {
      return `"${str.replace(/"/g, '""')}"`;
    }
    return `"${str}"`;
  }

  private parseJSON(jsonString: string): any {
    if (!jsonString || jsonString.trim() === '') return null;
    try {
      return JSON.parse(jsonString);
    } catch (e) {
      console.warn('Failed to parse JSON:', jsonString);
      return null;
    }
  }
}
