import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma/prisma.service';

/**
 * Flexible CSV Row Type - handles dynamic columns
 * Core columns are strongly typed, additional columns are dynamic
 */
interface OldFormatCSVRow {
  // Core Identifiers
  XREF_HIERARCHY_KEY: number;
  HIERARCHY_GROUP_NAME: string;
  HIERARCHY_NAME?: string; // New format
  ID_NAME?: string; // Old format (maps to HIERARCHY_NAME)

  // Level Hierarchy (LEVEL_0 through LEVEL_9)
  LEVEL_0_SORT?: number; // New export format with project name
  LEVEL_0?: string; // New export format with project name
  LEVEL_1_SORT?: number;
  LEVEL_1?: string;
  LEVEL_2_SORT?: number;
  LEVEL_2?: string;
  LEVEL_3_SORT?: number;
  LEVEL_3?: string;
  LEVEL_4_SORT?: number;
  LEVEL_4?: string;
  LEVEL_5_SORT?: number;
  LEVEL_5?: string;
  LEVEL_6_SORT?: number;
  LEVEL_6?: string;
  LEVEL_7_SORT?: number;
  LEVEL_7?: string;
  LEVEL_8_SORT?: number;
  LEVEL_8?: string;
  LEVEL_9_SORT?: number;
  LEVEL_9?: string;

  // Hierarchy Structure
  SORT_ORDER?: number;
  IS_ROOT?: boolean;
  PARENT_XREF_KEY?: number;

  // Standard Flags
  CALCULATION_FLAG?: boolean;
  ACTIVE_FLAG?: boolean;
  EXCLUSION_FLAG?: boolean;
  VOLUME_FLAG?: boolean;
  DO_NOT_EXPAND_FLAG?: boolean;
  IS_SECURED_FLAG?: boolean;
  SPLIT_ACTIVE_FLAG?: boolean;
  ID_UNPIVOT_FLAG?: boolean;
  ID_ROW_FLAG?: boolean;
  REMOVE_FROM_TOTALS?: boolean;
  HAS_MULTIPLE_TABLES?: boolean;
  CREATE_NEW_COLUMN?: boolean;
  SIGN_CHANGE_FLAG?: boolean;

  // Filter Groups (up to 4 groups)
  FILTER_GROUP_1?: string;
  FILTER_GROUP_1_TYPE?: string;
  FILTER_GROUP_2?: string;
  FILTER_GROUP_2_TYPE?: string;
  FILTER_GROUP_3?: string;
  FILTER_GROUP_3_TYPE?: string;
  FILTER_GROUP_4?: string;
  FILTER_GROUP_4_TYPE?: string;

  // Formula Fields
  FORMULA_GROUP?: string;
  FORMULA_PRECEDENCE?: number;
  FORMULA_PARAM_REF?: string;
  ARITHMETIC_LOGIC?: string;
  FORMULA_REF_TABLE?: string;
  FORMULA_REF_SOURCE?: string;
  FORMULA_PARAM2_CONST_NUMBER?: string;
  GROUP_FILTER_PRECEDENCE?: number;

  // Metadata
  UPDATED_BY?: string;
  UPDATED_AT?: string;

  // Dynamic columns support
  [key: string]: any;
}

/**
 * Internal data structure for processing
 */
interface HierarchyData {
  xrefKey: number;
  hierarchyId: string;
  hierarchyName: string;
  levelPath: string[];
  levelSorts: number[];
  depth: number;
  isRoot: boolean;
  parentXrefKey: number | null;
  parentHierarchyId: string | null;
  sortOrder: number;
  flags: Record<string, any>;
  filterConfig: Record<string, any>;
  formulaGroup: string | null;
  formulaPrecedence: number;
  formulaParamRef: string | null;
  arithmeticLogic: string | null;
  formulaRefTable: string | null;
  formulaConstNumber: string | null;
  calculationFlag: boolean;
  metadata: Record<string, any>;
  originalRow: OldFormatCSVRow;
  formulaRows?: OldFormatCSVRow[]; // Additional rows with different operations (formula rules)
  mapping?: any[]; // Extracted mapping data from row
}

/**
 * Import result with detailed tracking
 */
export interface ImportResult {
  success: boolean;
  imported: number;
  skipped: number;
  errors: string[];
  warnings: string[];
  logs: string[];
}

/**
 * OLD ENP FORMAT IMPORT SERVICE V2
 *
 * Professional, modular service for importing legacy ENP CSV format
 * with comprehensive logging and flexible type handling.
 *
 * PHASES:
 * 1. Data Loading - Parse CSV and normalize into array
 * 2. Hierarchy Creation - Create all hierarchies based on level path
 * 3. Parent Linking - Resolve and link parent relationships
 * 4. Flag Resolution - Process all flags (standard + custom)
 * 5. Filter Configuration - Build filter groups
 * 6. Total Formula Processing - Handle SUM formulas with CALCULATION_FLAG
 * 7. Formula Group Processing - Handle operation formulas
 * 8. Formula Properties - Process PARAM_REF, ARITHMETIC_LOGIC, REF_TABLE, PRECEDENCE
 * 9. Final Resolution - Handle missing references and cleanup
 */
@Injectable()
export class OldFormatImportV2Service {
  private readonly logger = new Logger(OldFormatImportV2Service.name);

  constructor(private readonly prisma: PrismaService) {}

  /**
   * Main import entry point
   */
  async importHierarchyCSV(projectId: string, csvContent: string): Promise<ImportResult> {
    try {
      // PHASE 1: Data Loading
      const rows = await this.phase1_loadData(csvContent);

      // PHASE 2: Hierarchy Creation
      const hierarchies = await this.phase2_createHierarchies(projectId, rows);

      // PHASE 3: Parent Linking
      await this.phase3_linkParents(projectId, hierarchies);

      // PHASE 3.5: Rebuild Hierarchy Levels (full paths from root)
      await this.phase3_5_rebuildHierarchyLevels(projectId);

      // PHASE 4: Flag Resolution
      await this.phase4_resolveFlags(projectId, hierarchies);

      // PHASE 5: Filter Configuration
      await this.phase5_configureFilters(projectId, hierarchies);

      // PHASE 6: Total Formula Processing
      const totalFormulaResult = await this.phase6_processTotalFormulas(projectId, hierarchies);

      // PHASE 7: Formula Group Processing
      const formulaGroupResult = await this.phase7_processFormulaGroups(projectId, hierarchies);

      // PHASE 8: Formula Properties
      await this.phase8_processFormulaProperties(projectId, hierarchies);

      // PHASE 9: Final Resolution
      await this.phase9_finalResolution(projectId, hierarchies);

      return {
        success: true,
        imported: hierarchies.length,
        skipped: 0,
        errors: [],
        warnings: [],
        logs: [],
      };
    } catch (error) {
      return {
        success: false,
        imported: 0,
        skipped: 0,
        errors: [error.message],
        warnings: [],
        logs: [],
      };
    }
  }

  /**
   * PHASE 1: Load and normalize CSV data
   * Groups multiple rows by hierarchy name (for formula rules)
   */
  private async phase1_loadData(csvContent: string): Promise<HierarchyData[]> {
    const lines = csvContent.split('\n').filter((line) => line.trim());

    if (lines.length < 2) {
      throw new Error('CSV file is empty or contains only headers');
    }

    const headers = this.parseCSVLine(lines[0]);

    const allRows: OldFormatCSVRow[] = [];
    const duplicateCheck = new Set<number>();

    for (let i = 1; i < lines.length; i++) {
      const values = this.parseCSVLine(lines[i]);
      const row: OldFormatCSVRow = {
        XREF_HIERARCHY_KEY: 0,
        HIERARCHY_GROUP_NAME: '',
      };

      // Map values to headers
      headers.forEach((header, index) => {
        row[header] = values[index] || '';
      });

      // Normalize boolean fields
      this.normalizeBooleans(row);

      // Normalize numeric fields
      this.normalizeNumbers(row);

      // Skip rows with ID column filled (these are mapping rows, not hierarchy structure)
      if (row.ID && String(row.ID).trim()) {
        continue;
      }

      // Extract level path (skip LEVEL_1 which is project name in old format)
      const levelPath = this.extractLevelPath(row);
      const levelSorts = this.extractLevelSorts(row);
      const depth = levelPath.length;

      // Get hierarchy name - use ID_NAME if present, otherwise use last level from path
      let hierarchyName = row.ID_NAME || row.HIERARCHY_NAME || '';

      // If no ID_NAME/HIERARCHY_NAME, use the last level from the path
      if (!hierarchyName && levelPath.length > 0) {
        hierarchyName = levelPath[levelPath.length - 1];
      }

      if (!hierarchyName) {
        continue;
      }

      const xrefKey = row.XREF_HIERARCHY_KEY;
      duplicateCheck.add(xrefKey);

      allRows.push(row);
    }

    // Group rows by hierarchy path (multiple rows with same path = formula rules)
    const hierarchyGroups = new Map<string, OldFormatCSVRow[]>();
    for (const row of allRows) {
      const levelPath = this.extractLevelPath(row);
      const hierarchyName =
        levelPath[levelPath.length - 1] || row.ID_NAME || row.HIERARCHY_NAME || '';
      const pathKey = levelPath.join(' > ');

      if (!hierarchyGroups.has(pathKey)) {
        hierarchyGroups.set(pathKey, []);
      }
      hierarchyGroups.get(pathKey)!.push(row);
    }

    // Convert grouped rows to HierarchyData
    const hierarchies: HierarchyData[] = [];
    const usedXrefKeys = new Set<number>();
    let autoXrefCounter = 1000; // Auto-assign XREF_KEYs starting from 1000 for conflicts

    for (const [pathKey, groupRows] of hierarchyGroups.entries()) {
      // Sort by XREF_KEY (first row is main, rest are formula rules)
      groupRows.sort((a, b) => (a.XREF_HIERARCHY_KEY || 0) - (b.XREF_HIERARCHY_KEY || 0));

      const mainRow = groupRows[0];
      const formulaRows = groupRows.slice(1); // Additional rows are formula rules

      const levelPath = this.extractLevelPath(mainRow);
      const levelSorts = this.extractLevelSorts(mainRow);
      const depth = levelPath.length;
      const hierarchyName =
        levelPath[levelPath.length - 1] || mainRow.ID_NAME || mainRow.HIERARCHY_NAME || '';

      if (!hierarchyName) {
        continue;
      }

      let xrefKey = mainRow.XREF_HIERARCHY_KEY;

      // Handle duplicate XREF_KEYs - assign new one if already used by different hierarchy
      if (usedXrefKeys.has(xrefKey)) {
        // Find next available auto key
        while (usedXrefKeys.has(autoXrefCounter)) {
          autoXrefCounter++;
        }
        xrefKey = autoXrefCounter;
        autoXrefCounter++;
      }

      usedXrefKeys.add(xrefKey);
      duplicateCheck.add(xrefKey);

      // Build normalized hierarchy data from main row
      // Extract mapping from the main row (old format has mapping in hierarchy row)
      const mappingData = this.extractMappingFromRow(mainRow);

      const hierarchyData: HierarchyData = {
        xrefKey,
        hierarchyId: `HIER_${xrefKey}`,
        hierarchyName,
        levelPath,
        levelSorts,
        depth,
        isRoot: depth === 1,
        parentXrefKey: mainRow.PARENT_XREF_KEY || null,
        parentHierarchyId: null,
        sortOrder: mainRow.SORT_ORDER || 0,
        flags: this.extractFlags(mainRow),
        filterConfig: this.extractFilterConfig(mainRow),
        formulaGroup: mainRow.FORMULA_GROUP?.trim() || null,
        formulaPrecedence: mainRow.FORMULA_PRECEDENCE || 1,
        formulaParamRef: mainRow.FORMULA_PARAM_REF?.trim() || null,
        arithmeticLogic: mainRow.ARITHMETIC_LOGIC?.trim() || null,
        formulaRefTable: mainRow.FORMULA_REF_TABLE?.trim() || null,
        formulaConstNumber: mainRow.FORMULA_PARAM2_CONST_NUMBER?.trim() || null,
        calculationFlag: mainRow.CALCULATION_FLAG === true,
        metadata: {},
        originalRow: mainRow,
        formulaRows: formulaRows, // Store additional rows as formula rules
        mapping: mappingData, // Extracted mapping from row
      };

      hierarchies.push(hierarchyData);
    }

    return hierarchies;
  }

  /**
   * PHASE 2: Create all hierarchies in database
   */
  private async phase2_createHierarchies(
    projectId: string,
    hierarchies: HierarchyData[],
  ): Promise<HierarchyData[]> {
    // Sort by depth first (parents must exist before children), then by sortOrder within each level
    // This preserves the correct order for formula rows and calculated fields
    const sorted = [...hierarchies].sort((a, b) => {
      if (a.depth !== b.depth) return a.depth - b.depth;
      return (a.sortOrder || 0) - (b.sortOrder || 0);
    });

    const created: HierarchyData[] = [];
    const xrefToHierarchyId = new Map<number, string>();
    const pathToHierarchyId = new Map<string, string>();

    // Check for auto-created intermediate parents
    const missingParents = this.identifyMissingParents(sorted);

    if (missingParents.length > 0) {
      for (const parent of missingParents) {
        await this.createIntermediateParent(
          projectId,
          parent,
          xrefToHierarchyId,
          pathToHierarchyId,
        );
      }
    }

    for (let i = 0; i < sorted.length; i++) {
      const h = sorted[i];
      const pathKey = h.levelPath.join(' > ');

      // Check for duplicates
      if (pathToHierarchyId.has(pathKey)) {
        const existingId = pathToHierarchyId.get(pathKey)!;
        xrefToHierarchyId.set(h.xrefKey, existingId);
        continue;
      }

      // Build hierarchyLevel object with names AND sorts
      const hierarchyLevel: any = {};
      h.levelPath.forEach((level, idx) => {
        hierarchyLevel[`level_${idx + 1}`] = level;
      });

      // Save ALL 9 level sort values (not just for levels that exist)
      for (let i = 0; i < 9; i++) {
        hierarchyLevel[`level_${i + 1}_sort`] = h.levelSorts[i] || 0;
      }

      try {
        // Create in database
        await this.prisma.smartHierarchyMaster.create({
          data: {
            projectId,
            hierarchyId: h.hierarchyId,
            hierarchyName: h.hierarchyName,
            hierarchyLevel: hierarchyLevel as any,
            isRoot: h.isRoot,
            sortOrder: h.sortOrder,
            flags: h.flags as any,
            filterConfig: Object.keys(h.filterConfig).length > 0 ? (h.filterConfig as any) : null,
            mapping: h.mapping || [],
            metadata: {
              xref_key: h.xrefKey,
              formula_group: h.formulaGroup,
              formula_param_ref: h.formulaParamRef,
              arithmetic_logic: h.arithmeticLogic,
              formula_ref_table: h.formulaRefTable,
              formula_precedence: h.formulaPrecedence,
              formula_param2_const_number: h.formulaConstNumber,
            } as any,
            createdBy: 'system',
          },
        });

        xrefToHierarchyId.set(h.xrefKey, h.hierarchyId);
        pathToHierarchyId.set(pathKey, h.hierarchyId);
        created.push(h);
      } catch (error) {
        // Skip failed entries
      }
    }

    return created;
  }

  /**
   * PHASE 3: Link parent relationships
   */
  private async phase3_linkParents(projectId: string, hierarchies: HierarchyData[]): Promise<void> {
    // Fetch ALL hierarchies from database (including auto-created intermediate parents)
    const allHierarchies = await this.prisma.smartHierarchyMaster.findMany({
      where: { projectId },
      select: { hierarchyId: true, hierarchyName: true, hierarchyLevel: true },
    });

    const hierarchyIdToUuid = new Map<string, string>();
    const pathToHierarchyId = new Map<string, string>();

    // Build lookup maps from ALL hierarchies in database
    for (const dbHierarchy of allHierarchies) {
      const hierarchyLevel = dbHierarchy.hierarchyLevel as any;
      const levelPath: string[] = [];

      // Reconstruct level path from hierarchyLevel
      for (let i = 1; i <= 9; i++) {
        const levelKey = `level_${i}`;
        if (hierarchyLevel[levelKey]) {
          levelPath.push(hierarchyLevel[levelKey]);
        }
      }

      const pathKey = levelPath.join(' > ');
      pathToHierarchyId.set(pathKey, dbHierarchy.hierarchyId);
    }

    // Also get UUID mapping for updating parentId
    const allDbRecords = await this.prisma.smartHierarchyMaster.findMany({
      where: { projectId },
      select: { id: true, hierarchyId: true },
    });

    allDbRecords.forEach((record) => {
      hierarchyIdToUuid.set(record.hierarchyId, record.id);
    });

    let linked = 0;
    let orphans = 0;

    // Link ALL hierarchies (including intermediate parents)
    for (const dbHierarchy of allHierarchies) {
      const hierarchyLevel = dbHierarchy.hierarchyLevel as any;
      const levelPath: string[] = [];

      // Reconstruct level path
      for (let i = 1; i <= 9; i++) {
        const levelKey = `level_${i}`;
        if (hierarchyLevel[levelKey]) {
          levelPath.push(hierarchyLevel[levelKey]);
        }
      }

      // Root nodes have no parent
      if (levelPath.length <= 1) {
        continue;
      }

      let parentHierarchyId: string | null = null;

      // Build parent path (all levels except last)
      const parentPath = levelPath.slice(0, -1);
      const parentPathKey = parentPath.join(' > ');
      parentHierarchyId = pathToHierarchyId.get(parentPathKey) || null;

      if (parentHierarchyId) {
        const parentUuid = hierarchyIdToUuid.get(parentHierarchyId);

        if (parentUuid) {
          await this.prisma.smartHierarchyMaster.update({
            where: { projectId_hierarchyId: { projectId, hierarchyId: dbHierarchy.hierarchyId } },
            data: { parentId: parentUuid },
          });
          linked++;
        } else {
          orphans++;
        }
      } else {
        orphans++;
      }
    }
  }

  /**
   * PHASE 3.5: Rebuild hierarchy levels with full paths from root
   */
  private async phase3_5_rebuildHierarchyLevels(projectId: string): Promise<void> {
    // Get all hierarchies
    const allHierarchies = await this.prisma.smartHierarchyMaster.findMany({
      where: { projectId },
      select: {
        id: true,
        hierarchyId: true,
        hierarchyName: true,
        parentId: true,
        hierarchyLevel: true,
      },
    });

    // Build maps for quick lookup
    const hierarchyByUuid = new Map<string, any>();
    const hierarchyByHierarchyId = new Map<string, any>();

    allHierarchies.forEach((h) => {
      hierarchyByUuid.set(h.id, h);
      hierarchyByHierarchyId.set(h.hierarchyId, h);
    });

    // Function to build full path from root
    const buildFullPath = (hierarchy: any): string[] => {
      const path: string[] = [];
      let current = hierarchy;
      let depth = 0;
      const maxDepth = 20; // Prevent infinite loops

      while (current && depth < maxDepth) {
        path.unshift(current.hierarchyName);
        if (current.parentId) {
          current = hierarchyByUuid.get(current.parentId);
        } else {
          break;
        }
        depth++;
      }

      return path;
    };

    let updated = 0;

    for (const h of allHierarchies) {
      const fullPath = buildFullPath(h);

      // Build new hierarchyLevel object with full path
      // IMPORTANT: Preserve existing sort values from Phase 2
      const oldHierarchyLevel = (h.hierarchyLevel as any) || {};
      const newHierarchyLevel: any = {};

      // Copy level names
      for (let i = 0; i < fullPath.length && i < 9; i++) {
        newHierarchyLevel[`level_${i + 1}`] = fullPath[i];
      }

      // Preserve ALL 9 sort values from Phase 2 (don't overwrite them!)
      for (let i = 0; i < 9; i++) {
        const sortKey = `level_${i + 1}_sort`;
        if (oldHierarchyLevel[sortKey] !== undefined) {
          newHierarchyLevel[sortKey] = oldHierarchyLevel[sortKey];
        }
      }

      // Update if different from current
      const currentLevelStr = JSON.stringify(h.hierarchyLevel);
      const newLevelStr = JSON.stringify(newHierarchyLevel);

      if (currentLevelStr !== newLevelStr) {
        await this.prisma.smartHierarchyMaster.update({
          where: { id: h.id },
          data: { hierarchyLevel: newHierarchyLevel as any },
        });
        updated++;
      }
    }
  }

  /**
   * PHASE 4: Resolve and update flags
   */
  private async phase4_resolveFlags(
    projectId: string,
    hierarchies: HierarchyData[],
  ): Promise<void> {
    // Flags already set during creation
  }

  /**
   * PHASE 5: Configure filter groups
   */
  private async phase5_configureFilters(
    projectId: string,
    hierarchies: HierarchyData[],
  ): Promise<void> {
    // Filter groups already set during creation
  }

  /**
   * PHASE 6: Process Total Formulas (SUM with CALCULATION_FLAG pattern)
   */
  private async phase6_processTotalFormulas(
    projectId: string,
    hierarchies: HierarchyData[],
  ): Promise<{ processed: number }> {
    const formulaGroups = new Map<string, HierarchyData[]>();
    for (const h of hierarchies) {
      if (h.formulaGroup) {
        if (!formulaGroups.has(h.formulaGroup)) {
          formulaGroups.set(h.formulaGroup, []);
        }
        formulaGroups.get(h.formulaGroup)!.push(h);
      }
    }

    let processed = 0;

    for (const [groupName, members] of formulaGroups.entries()) {
      const mainRow = members.find((m) => m.calculationFlag === true);
      const childRows = members.filter((m) => m.calculationFlag === false);

      // Total Formula pattern: 1 main (true) + N children (false)
      const isTotalFormula = !!mainRow && childRows.length > 0;

      if (isTotalFormula) {
        const children = childRows.map((c) => ({
          hierarchyId: c.hierarchyId,
          hierarchyName: c.hierarchyName,
        }));

        const totalFormula = {
          mainHierarchyId: mainRow.hierarchyId,
          mainHierarchyName: mainRow.hierarchyName,
          aggregation: this.mapArithmeticToAggregation(mainRow.arithmeticLogic || 'Sum'),
          children,
        };

        // Update main hierarchy with total_formula in filterConfig (preserve metadata)
        const existingHierarchy = await this.prisma.smartHierarchyMaster.findUnique({
          where: { projectId_hierarchyId: { projectId, hierarchyId: mainRow.hierarchyId } },
        });

        await this.prisma.smartHierarchyMaster.update({
          where: { projectId_hierarchyId: { projectId, hierarchyId: mainRow.hierarchyId } },
          data: {
            filterConfig: {
              ...mainRow.filterConfig,
              total_formula: totalFormula,
            } as any,
            flags: {
              ...mainRow.flags,
              calculation_flag: true,
            } as any,
            metadata: (existingHierarchy?.metadata || null) as any,
          },
        });

        processed++;
      }
    }

    return { processed };
  }

  /**
   * PHASE 7: Process Formula Groups (operations with PARAM_REF)
   */
  private async phase7_processFormulaGroups(
    projectId: string,
    hierarchies: HierarchyData[],
  ): Promise<{ processed: number }> {
    const formulaGroups = new Map<string, HierarchyData[]>();

    // Collect all hierarchy data including formula rows
    const allFormulaData: Array<{ h: HierarchyData; row: OldFormatCSVRow }> = [];

    for (const h of hierarchies) {
      if (h.formulaGroup) {
        if (!formulaGroups.has(h.formulaGroup)) {
          formulaGroups.set(h.formulaGroup, []);
        }
        formulaGroups.get(h.formulaGroup)!.push(h);

        // Also add main row
        allFormulaData.push({ h, row: h.originalRow });

        // Add formula rows if they exist (these are the additional operations like Subtract)
        if (h.formulaRows && h.formulaRows.length > 0) {
          for (const formulaRow of h.formulaRows) {
            allFormulaData.push({ h, row: formulaRow });
          }
        }
      }
    }

    let processed = 0;

    for (const [groupName, members] of formulaGroups.entries()) {
      const mainRow = members.find((m) => m.calculationFlag === true);
      const childRows = members.filter((m) => m.calculationFlag === false);

      // Formula Group pattern: NOT a total formula (no children OR has table ref)
      const hasTableRef = members.some((m) => m.formulaRefTable);
      const hasDivideOp = members.some((m) => m.arithmeticLogic?.toLowerCase() === 'divide');
      const isFormulaGroup = !mainRow || childRows.length === 0 || hasTableRef || hasDivideOp;

      if (isFormulaGroup) {
        const targetRow = mainRow || members[0];

        const rules: any[] = [];

        // Process all formula data (main rows + formula rows)
        const groupFormulaData = allFormulaData.filter((fd) =>
          members.some((m) => m.hierarchyId === fd.h.hierarchyId),
        );

        for (const { h: member, row: formulaRow } of groupFormulaData) {
          const operation = this.mapArithmeticToOperation(formulaRow.ARITHMETIC_LOGIC || 'Add');
          const paramRef = formulaRow.FORMULA_PARAM_REF?.trim();
          const refTable = formulaRow.FORMULA_REF_TABLE?.trim();

          // Table reference
          if (refTable) {
            rules.push({
              operation,
              hierarchyId: null,
              hierarchyName: refTable,
              tableReference: refTable,
              precedence: formulaRow.FORMULA_PRECEDENCE || 1,
              constantNumber: formulaRow.FORMULA_PARAM2_CONST_NUMBER
                ? parseFloat(formulaRow.FORMULA_PARAM2_CONST_NUMBER)
                : undefined,
              isTableReference: true,
              // Store original CSV values for export
              originalFormulaParamRef: paramRef || refTable,
              originalSignChangeFlag: formulaRow.SIGN_CHANGE_FLAG === true,
            });
          }
          // Hierarchy reference
          else if (paramRef) {
            // Find target hierarchy
            const target = await this.prisma.smartHierarchyMaster.findFirst({
              where: { projectId, hierarchyName: paramRef },
            });

            rules.push({
              operation,
              hierarchyId: target?.hierarchyId || null,
              hierarchyName: paramRef,
              precedence: formulaRow.FORMULA_PRECEDENCE || 1,
              parameterReference: paramRef,
              constantNumber: formulaRow.FORMULA_PARAM2_CONST_NUMBER
                ? parseFloat(formulaRow.FORMULA_PARAM2_CONST_NUMBER)
                : undefined,
              isTableReference: false,
              // Store original CSV values for export
              originalFormulaParamRef: paramRef,
              originalSignChangeFlag: formulaRow.SIGN_CHANGE_FLAG === true,
            });
          }
        }

        if (rules.length > 0) {
          const formulaConfig = {
            formula_type: 'AGGREGATE',
            formula_text: groupName,
            formula_group: {
              mainHierarchyId: targetRow.hierarchyId,
              mainHierarchyName: targetRow.hierarchyName,
              rules,
            },
          };

          // Preserve metadata during update
          const existingHierarchy = await this.prisma.smartHierarchyMaster.findUnique({
            where: { projectId_hierarchyId: { projectId, hierarchyId: targetRow.hierarchyId } },
          });

          await this.prisma.smartHierarchyMaster.update({
            where: { projectId_hierarchyId: { projectId, hierarchyId: targetRow.hierarchyId } },
            data: {
              formulaConfig: formulaConfig as any,
              flags: {
                ...targetRow.flags,
                calculation_flag: true,
              } as any,
              metadata: (existingHierarchy?.metadata || null) as any,
            },
          });

          processed++;
        }
      }
    }

    return { processed };
  }

  /**
   * PHASE 8: Process formula properties for individual rows
   */
  private async phase8_processFormulaProperties(
    projectId: string,
    hierarchies: HierarchyData[],
  ): Promise<void> {
    let processed = 0;
    for (const h of hierarchies) {
      // Skip if already part of a formula group
      if (h.formulaGroup) continue;

      // Check if has formula properties
      if (!h.formulaParamRef && !h.formulaRefTable && !h.arithmeticLogic) continue;

      const rules: any[] = [];
      const operation = this.mapArithmeticToOperation(h.arithmeticLogic || 'Sum');

      if (h.formulaRefTable) {
        rules.push({
          operation,
          hierarchyId: null,
          hierarchyName: h.formulaRefTable,
          tableReference: h.formulaRefTable,
          precedence: h.formulaPrecedence,
          constantNumber: h.formulaConstNumber ? parseFloat(h.formulaConstNumber) : undefined,
          isTableReference: true,
        });
      }

      if (h.formulaParamRef) {
        const target = await this.prisma.smartHierarchyMaster.findFirst({
          where: { projectId, hierarchyName: h.formulaParamRef },
        });

        rules.push({
          operation,
          hierarchyId: target?.hierarchyId || null,
          hierarchyName: h.formulaParamRef,
          precedence: h.formulaPrecedence,
          constantNumber: h.formulaConstNumber ? parseFloat(h.formulaConstNumber) : undefined,
          isTableReference: false,
        });
      }

      if (rules.length > 0) {
        const formulaConfig = {
          formula_type: 'AGGREGATE',
          formula_text: h.hierarchyName,
          formula_group: {
            mainHierarchyId: h.hierarchyId,
            mainHierarchyName: h.hierarchyName,
            rules,
          },
        };

        // Preserve metadata during update
        const existingHierarchy = await this.prisma.smartHierarchyMaster.findUnique({
          where: { projectId_hierarchyId: { projectId, hierarchyId: h.hierarchyId } },
        });

        await this.prisma.smartHierarchyMaster.update({
          where: { projectId_hierarchyId: { projectId, hierarchyId: h.hierarchyId } },
          data: {
            formulaConfig: formulaConfig as any,
            flags: {
              ...h.flags,
              calculation_flag: true,
            } as any,
            metadata: (existingHierarchy?.metadata || null) as any,
          },
        });

        processed++;
      }
    }
  }

  /**
   * PHASE 9: Final resolution of missing references
   */
  private async phase9_finalResolution(
    projectId: string,
    hierarchies: HierarchyData[],
  ): Promise<void> {
    let resolved = 0;
    const allHierarchies = await this.prisma.smartHierarchyMaster.findMany({
      where: { projectId },
      select: { hierarchyId: true, hierarchyName: true, formulaConfig: true },
    });

    for (const h of allHierarchies) {
      const formulaConfig = h.formulaConfig as any;
      if (!formulaConfig?.formula_group?.rules) continue;

      let needsUpdate = false;
      const rules = formulaConfig.formula_group.rules;

      for (const rule of rules) {
        if (!rule.hierarchyId && rule.hierarchyName && !rule.isTableReference) {
          const target = allHierarchies.find((t) => t.hierarchyName === rule.hierarchyName);
          if (target) {
            rule.hierarchyId = target.hierarchyId;
            needsUpdate = true;
            resolved++;
          }
        }
      }

      if (needsUpdate) {
        await this.prisma.smartHierarchyMaster.update({
          where: { projectId_hierarchyId: { projectId, hierarchyId: h.hierarchyId } },
          data: { formulaConfig: formulaConfig as any },
        });
      }
    }
  }

  // ============================================================================
  // HELPER FUNCTIONS
  // ============================================================================

  private parseCSVLine(line: string): string[] {
    const values: string[] = [];
    let current = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];

      if (char === '"') {
        inQuotes = !inQuotes;
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

  private extractLevelPath(row: OldFormatCSVRow): string[] {
    const path: string[] = [];

    // AUTO-DETECT FORMAT:
    // OLD FORMAT: LEVEL_1 equals HIERARCHY_GROUP_NAME -> start from LEVEL_2 (EXCLUDE LEVEL_1)
    // NEW FORMAT: LEVEL_1 differs from HIERARCHY_GROUP_NAME -> start from LEVEL_1
    const level1Value = row.LEVEL_1 && typeof row.LEVEL_1 === 'string' ? row.LEVEL_1.trim() : '';
    const hierarchyGroupName =
      row.HIERARCHY_GROUP_NAME && typeof row.HIERARCHY_GROUP_NAME === 'string'
        ? row.HIERARCHY_GROUP_NAME.trim()
        : '';

    const isOldFormat = level1Value === hierarchyGroupName && level1Value !== '';
    const startLevel = isOldFormat ? 2 : 1; // Old format: skip LEVEL_1, start at LEVEL_2

    // Log format detection for first few rows (avoid spam)
    if (row.XREF_HIERARCHY_KEY <= 15) {
      this.logger.debug(
        `Format Detection [XREF:${row.XREF_HIERARCHY_KEY}]: ` +
          `${isOldFormat ? 'OLD' : 'NEW'} format detected. ` +
          `Starting from LEVEL_${startLevel}. ` +
          `(LEVEL_1="${level1Value}", GROUP_NAME="${hierarchyGroupName}")`,
      );
    }

    for (let i = startLevel; i <= 9; i++) {
      const levelKey = `LEVEL_${i}` as keyof OldFormatCSVRow;
      const value = row[levelKey];
      if (value && typeof value === 'string' && value.trim() && value !== 'N/A') {
        const trimmed = value.trim();
        // Skip if same as previous level (deduplication)
        if (path.length === 0 || path[path.length - 1] !== trimmed) {
          path.push(trimmed);
        }
      }
    }
    return path;
  }

  private extractLevelSorts(row: OldFormatCSVRow): number[] {
    const sorts: number[] = [];
    // Include LEVEL_0_SORT through LEVEL_9_SORT (LEVEL_0_SORT will be 0 or undefined for old format)
    for (let i = 0; i <= 9; i++) {
      const sortKey = `LEVEL_${i}_SORT` as keyof OldFormatCSVRow;
      sorts.push(Number(row[sortKey]) || 0);
    }
    return sorts;
  }

  private extractFlags(row: OldFormatCSVRow): Record<string, any> {
    return {
      active_flag: row.ACTIVE_FLAG === true,
      exclude_flag: row.EXCLUSION_FLAG === true,
      calculation_flag: row.CALCULATION_FLAG === true,
      customFlags: {
        do_not_expand_flag: row.DO_NOT_EXPAND_FLAG === true,
        is_secured_flag: row.IS_SECURED_FLAG === true,
        split_active_flag: row.SPLIT_ACTIVE_FLAG === true,
        volume_flag: row.VOLUME_FLAG === true,
        id_unpivot_flag: row.ID_UNPIVOT_FLAG === true,
        id_row_flag: row.ID_ROW_FLAG === true,
        remove_from_totals: row.REMOVE_FROM_TOTALS === true,
        has_multiple_tables: row.HAS_MULTIPLE_TABLES === true,
        create_new_column: row.CREATE_NEW_COLUMN === true,
        sign_change_flag: row.SIGN_CHANGE_FLAG === true,
      },
    };
  }

  private extractFilterConfig(row: OldFormatCSVRow): Record<string, any> {
    const config: Record<string, any> = {};

    if (row.FILTER_GROUP_1) config.filter_group_1 = row.FILTER_GROUP_1;
    if (row.FILTER_GROUP_1_TYPE) config.filter_group_1_type = row.FILTER_GROUP_1_TYPE;
    if (row.FILTER_GROUP_2) config.filter_group_2 = row.FILTER_GROUP_2;
    if (row.FILTER_GROUP_2_TYPE) config.filter_group_2_type = row.FILTER_GROUP_2_TYPE;
    if (row.FILTER_GROUP_3) config.filter_group_3 = row.FILTER_GROUP_3;
    if (row.FILTER_GROUP_3_TYPE) config.filter_group_3_type = row.FILTER_GROUP_3_TYPE;
    if (row.FILTER_GROUP_4) config.filter_group_4 = row.FILTER_GROUP_4;
    if (row.FILTER_GROUP_4_TYPE) config.filter_group_4_type = row.FILTER_GROUP_4_TYPE;

    return config;
  }

  private normalizeBooleans(row: OldFormatCSVRow): void {
    const boolFields = [
      'IS_ROOT',
      'CALCULATION_FLAG',
      'ACTIVE_FLAG',
      'EXCLUSION_FLAG',
      'VOLUME_FLAG',
      'DO_NOT_EXPAND_FLAG',
      'IS_SECURED_FLAG',
      'SPLIT_ACTIVE_FLAG',
      'ID_UNPIVOT_FLAG',
      'ID_ROW_FLAG',
      'REMOVE_FROM_TOTALS',
      'HAS_MULTIPLE_TABLES',
      'CREATE_NEW_COLUMN',
      'SIGN_CHANGE_FLAG',
    ];

    for (const field of boolFields) {
      if (field in row) {
        const val = String(row[field]).toLowerCase();
        row[field] = val === 'true' || val === '1' || val === 'yes';
      }
    }
  }

  private normalizeNumbers(row: OldFormatCSVRow): void {
    const numFields = [
      'XREF_HIERARCHY_KEY',
      'SORT_ORDER',
      'PARENT_XREF_KEY',
      'FORMULA_PRECEDENCE',
      'GROUP_FILTER_PRECEDENCE',
    ];

    for (let i = 0; i <= 9; i++) {
      numFields.push(`LEVEL_${i}_SORT`);
    }

    for (const field of numFields) {
      if (field in row) {
        row[field] = parseInt(String(row[field])) || 0;
      }
    }
  }

  private identifyMissingParents(hierarchies: HierarchyData[]): HierarchyData[] {
    const existingPaths = new Set<string>();
    const allPaths = new Set<string>();

    for (const h of hierarchies) {
      const pathKey = h.levelPath.join(' > ');
      existingPaths.add(pathKey);

      // Add all intermediate paths
      for (let i = 1; i < h.levelPath.length; i++) {
        const intermediatePath = h.levelPath.slice(0, i);
        allPaths.add(intermediatePath.join(' > '));
      }
    }

    // Filter to only missing paths and avoid duplicates
    // FIX: Use FULL PATH as deduplication key, not just depth:name
    // This ensures intermediate nodes with same name at same depth but different ancestors
    // (e.g., "Chemicals > Recurring" vs "Compression > Recurring") are all created
    const missingPaths = Array.from(allPaths).filter((p) => !existingPaths.has(p));
    const seenPaths = new Set<string>(); // Use full path for deduplication
    const missing: HierarchyData[] = [];

    for (const pathStr of missingPaths) {
      const path = pathStr.split(' > ');
      const name = path[path.length - 1];

      // Skip if we already processed this exact path
      if (seenPaths.has(pathStr)) {
        continue;
      }
      seenPaths.add(pathStr);

      missing.push({
        xrefKey: 1000000 + missing.length,
        hierarchyId: `HIER_AUTO_${1000000 + missing.length}`,
        hierarchyName: name,
        levelPath: path,
        levelSorts: path.map(() => 0),
        depth: path.length,
        isRoot: path.length === 1,
        parentXrefKey: null,
        parentHierarchyId: null,
        sortOrder: 0,
        flags: { active_flag: true, customFlags: {} },
        filterConfig: {},
        formulaGroup: null,
        formulaPrecedence: 1,
        formulaParamRef: null,
        arithmeticLogic: null,
        formulaRefTable: null,
        formulaConstNumber: null,
        calculationFlag: false,
        metadata: {},
        originalRow: { XREF_HIERARCHY_KEY: 0, HIERARCHY_GROUP_NAME: '' } as OldFormatCSVRow,
      });
    }

    return missing;
  }

  private async createIntermediateParent(
    projectId: string,
    parent: HierarchyData,
    xrefToId: Map<number, string>,
    pathToId: Map<string, string>,
  ): Promise<void> {
    const hierarchyLevel: any = {};
    parent.levelPath.forEach((level, idx) => {
      hierarchyLevel[`level_${idx + 1}`] = level;
    });

    await this.prisma.smartHierarchyMaster.create({
      data: {
        projectId,
        hierarchyId: parent.hierarchyId,
        hierarchyName: parent.hierarchyName,
        hierarchyLevel: hierarchyLevel as any,
        isRoot: parent.isRoot,
        sortOrder: 0,
        flags: parent.flags as any,
        mapping: [],
        createdBy: 'system',
      },
    });

    xrefToId.set(parent.xrefKey, parent.hierarchyId);
    pathToId.set(parent.levelPath.join(' > '), parent.hierarchyId);
  }

  private mapArithmeticToOperation(arithmetic: string | null): string {
    if (!arithmetic) return 'Add';

    const map: Record<string, string> = {
      sum: 'Sum',
      add: 'Add',
      subtract: 'Subtract',
      multiply: 'Multiply',
      divide: 'Divide',
      average: 'Average',
    };

    return map[arithmetic.toLowerCase()] || 'Add';
  }

  private mapArithmeticToAggregation(arithmetic: string | null): string {
    if (!arithmetic) return 'SUM';

    const map: Record<string, string> = {
      sum: 'SUM',
      add: 'SUM',
      average: 'AVG',
      avg: 'AVG',
      count: 'COUNT',
      min: 'MIN',
      max: 'MAX',
    };

    return map[arithmetic.toLowerCase()] || 'SUM';
  }

  /**
   * Extract mapping data from a hierarchy row
   * Old format stores mapping info directly in the hierarchy row
   */
  private extractMappingFromRow(row: OldFormatCSVRow): any[] {
    // Check if row has mapping data
    const hasMapping =
      row.ID_SOURCE ||
      row.ID_TABLE ||
      row.ID_SCHEMA ||
      row.ID_DATABASE ||
      row.ID ||
      row.ID_NAME;

    if (!hasMapping) {
      return [];
    }

    // Build mapping entry from row
    const mapping: any = {
      mapping_index: 1,
      // Old format source identifiers
      id_name: row.ID_NAME || '',
      id: row.ID || '',
      id_source: row.ID_SOURCE || '',
      id_table: row.ID_TABLE || '',
      id_schema: row.ID_SCHEMA || '',
      id_database: row.ID_DATABASE || '',
      // Map to new format
      source_database: row.ID_DATABASE || '',
      source_schema: row.ID_SCHEMA || '',
      source_table: row.ID_TABLE || '',
      source_column: row.ID_SOURCE || '',
      source_uid: row.ID || row.ID_NAME || '',
      // Precedence
      precedence_group: row.GROUP_FILTER_PRECEDENCE || 1,
      // Flags
      flags: {
        active_flag: row.ACTIVE_FLAG ?? true,
        exclude_flag: row.EXCLUSION_FLAG ?? false,
        include_flag: true,
        do_not_expand_flag: row.DO_NOT_EXPAND_FLAG ?? false,
        is_secured_flag: row.IS_SECURED_FLAG ?? false,
        split_active_flag: row.SPLIT_ACTIVE_FLAG ?? false,
        calculation_flag: row.CALCULATION_FLAG ?? false,
        volume_flag: row.VOLUME_FLAG ?? false,
        id_unpivot_flag: row.ID_UNPIVOT_FLAG ?? false,
        id_row_flag: row.ID_ROW_FLAG ?? false,
        remove_from_totals: row.REMOVE_FROM_TOTALS ?? false,
        create_new_column: row.CREATE_NEW_COLUMN ?? false,
        sign_change_flag: row.SIGN_CHANGE_FLAG ?? false,
      },
    };

    return [mapping];
  }
}
