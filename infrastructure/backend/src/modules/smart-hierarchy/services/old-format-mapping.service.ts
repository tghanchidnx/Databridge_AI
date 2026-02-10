import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma/prisma.service';

/**
 * Service for handling OLD ENP format mapping import
 * This service manages the import of mappings from legacy CSV format with proper:
 * - FK_REPORT_KEY to hierarchy linking
 * - Source database/schema/table/column mapping
 * - Precedence group management
 * - Flag management (active, exclude, include, transform flags)
 * - Custom source identifiers (ID_NAME, ID, ID_SOURCE, ID_TABLE, ID_SCHEMA, ID_DATABASE)
 */
@Injectable()
export class OldFormatMappingService {
  constructor(private readonly prisma: PrismaService) {}

  /**
   * Main import function for old format mapping CSV
   * Links mappings to hierarchies based on FK_REPORT_KEY (XREF_HIERARCHY_KEY)
   */
  async importMappingCSV(
    projectId: string,
    csvContent: string,
  ): Promise<{ imported: number; skipped: number; errors: string[] }> {
    // Validate project exists
    const project = await this.prisma.hierarchyProject.findUnique({
      where: { id: projectId },
    });

    if (!project) {
      throw new NotFoundException(`Project '${projectId}' not found`);
    }

    const rows = this.parseMappingCSV(csvContent);

    // Group by FK_REPORT_KEY (links to XREF_HIERARCHY_KEY)
    const mappingsByHierarchy = this.groupMappingsByHierarchy(rows);

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

        // Build mapping array with all source identifiers and flags
        const mappings = this.buildMappings(mappingRows);

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

  /**
   * Parse OLD ENP format mapping CSV
   * Expected columns: FK_REPORT_KEY, XREF_HIERARCHY_KEY, ID_NAME, ID, ID_SOURCE,
   * ID_TABLE, ID_SCHEMA, ID_DATABASE, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE,
   * SOURCE_COLUMN, SOURCE_UID, PRECEDENCE_GROUP, flags, etc.
   */
  private parseMappingCSV(csvContent: string): MappingCSVRow[] {
    const lines = csvContent.split('\n').filter((line) => line.trim());
    if (lines.length < 2) return [];

    const headers = this.parseCSVLine(lines[0]);
    const rows: MappingCSVRow[] = [];

    for (let i = 1; i < lines.length; i++) {
      const values = this.parseCSVLine(lines[i]);
      const row: any = {};

      headers.forEach((header, index) => {
        row[header] = values[index] || '';
      });

      rows.push({
        XREF_HIERARCHY_KEY: parseInt(row.XREF_HIERARCHY_KEY) || 0,
        FK_REPORT_KEY: parseInt(row.FK_REPORT_KEY) || 0,
        HIERARCHY_GROUP_NAME: row.HIERARCHY_GROUP_NAME || '',
        HIERARCHY_NAME: row.HIERARCHY_NAME || '',
        LEVEL_NODE: row.LEVEL_NODE || '',
        SORT_ORDER: parseInt(row.SORT_ORDER) || 0,
        // Source identifiers (old format specific)
        ID_NAME: row.ID_NAME || '',
        ID: row.ID || '',
        ID_SOURCE: row.ID_SOURCE || '',
        ID_TABLE: row.ID_TABLE || '',
        ID_SCHEMA: row.ID_SCHEMA || '',
        ID_DATABASE: row.ID_DATABASE || '',
        // New format source identifiers
        SOURCE_DATABASE: row.SOURCE_DATABASE || row.ID_DATABASE || '',
        SOURCE_SCHEMA: row.SOURCE_SCHEMA || row.ID_SCHEMA || '',
        SOURCE_TABLE: row.SOURCE_TABLE || row.ID_TABLE || '',
        SOURCE_COLUMN: row.SOURCE_COLUMN || row.ID_SOURCE || '', // ID_SOURCE is the column name
        SOURCE_UID: row.SOURCE_UID || row.ID || row.ID_NAME || '', // ID or ID_NAME is the actual value
        // Precedence and filtering
        PRECEDENCE_GROUP:
          parseInt(row.PRECEDENCE_GROUP) || parseInt(row.GROUP_FILTER_PRECEDENCE) || 1,
        GROUP_FILTER_PRECEDENCE: parseInt(row.GROUP_FILTER_PRECEDENCE) || 1,
        // Flags
        ACTIVE_FLAG: this.parseBoolean(row.ACTIVE_FLAG),
        EXCLUDE_FLAG: this.parseBoolean(row.EXCLUDE_FLAG) || this.parseBoolean(row.EXCLUSION_FLAG),
        EXCLUSION_FLAG: this.parseBoolean(row.EXCLUSION_FLAG),
        INCLUDE_FLAG: this.parseBoolean(row.INCLUDE_FLAG),
        TRANSFORM_FLAG: this.parseBoolean(row.TRANSFORM_FLAG),
        DO_NOT_EXPAND_FLAG: this.parseBoolean(row.DO_NOT_EXPAND_FLAG),
        IS_SECURED_FLAG: this.parseBoolean(row.IS_SECURED_FLAG),
        SPLIT_ACTIVE_FLAG: this.parseBoolean(row.SPLIT_ACTIVE_FLAG),
        CALCULATION_FLAG: this.parseBoolean(row.CALCULATION_FLAG),
        VOLUME_FLAG: this.parseBoolean(row.VOLUME_FLAG),
        ID_UNPIVOT_FLAG: this.parseBoolean(row.ID_UNPIVOT_FLAG),
        ID_ROW_FLAG: this.parseBoolean(row.ID_ROW_FLAG),
        REMOVE_FROM_TOTALS: this.parseBoolean(row.REMOVE_FROM_TOTALS),
        CREATE_NEW_COLUMN: this.parseBoolean(row.CREATE_NEW_COLUMN),
        SIGN_CHANGE_FLAG: this.parseBoolean(row.SIGN_CHANGE_FLAG),
        // Additional level information
        LEVEL_1: row.LEVEL_1 || '',
        LEVEL_2: row.LEVEL_2 || '',
        LEVEL_3: row.LEVEL_3 || '',
        LEVEL_4: row.LEVEL_4 || '',
        LEVEL_5: row.LEVEL_5 || '',
        LEVEL_6: row.LEVEL_6 || '',
        LEVEL_7: row.LEVEL_7 || '',
        LEVEL_8: row.LEVEL_8 || '',
        LEVEL_9: row.LEVEL_9 || '',
        // Filter groups
        FILTER_GROUP_1: row.FILTER_GROUP_1 || '',
        FILTER_GROUP_2: row.FILTER_GROUP_2 || '',
        FILTER_GROUP_3: row.FILTER_GROUP_3 || '',
        FILTER_GROUP_4: row.FILTER_GROUP_4 || '',
        // Formula information
        FORMULA_GROUP: row.FORMULA_GROUP || '',
        FORMULA_PRECEDENCE: parseInt(row.FORMULA_PRECEDENCE) || 1,
        FORMULA_PARAM_REF: row.FORMULA_PARAM_REF || '',
        ARITHMETIC_LOGIC: row.ARITHMETIC_LOGIC || '',
        FORMULA_PARAM2_CONST_NUMBER: row.FORMULA_PARAM2_CONST_NUMBER || '',
      } as MappingCSVRow);
    }

    return rows;
  }

  /**
   * Group mapping rows by FK_REPORT_KEY (hierarchy identifier)
   */
  private groupMappingsByHierarchy(rows: MappingCSVRow[]): Map<number, MappingCSVRow[]> {
    const groupsMap = new Map<number, MappingCSVRow[]>();

    for (const row of rows) {
      const existing = groupsMap.get(row.FK_REPORT_KEY) || [];
      existing.push(row);
      groupsMap.set(row.FK_REPORT_KEY, existing);
    }

    return groupsMap;
  }

  /**
   * Build mapping array from CSV rows
   * Includes source identifiers, precedence groups, and all flags
   */
  private buildMappings(mappingRows: MappingCSVRow[]): any[] {
    return mappingRows.map((row) => {
      // Extract custom flags (any columns not in standard set)
      const customFlags = this.extractCustomFlags(row);

      return {
        mapping_index: row.XREF_HIERARCHY_KEY,
        // Source identifiers (old format)
        id_name: row.ID_NAME,
        id: row.ID,
        id_source: row.ID_SOURCE,
        id_table: row.ID_TABLE,
        id_schema: row.ID_SCHEMA,
        id_database: row.ID_DATABASE,
        // Source identifiers (new format)
        source_database: row.SOURCE_DATABASE,
        source_schema: row.SOURCE_SCHEMA,
        source_table: row.SOURCE_TABLE,
        source_column: row.SOURCE_COLUMN, // This now correctly gets ID_SOURCE
        source_uid: row.SOURCE_UID, // This now correctly gets ID or ID_NAME
        // Precedence
        precedence_group: row.PRECEDENCE_GROUP,
        group_filter_precedence: row.GROUP_FILTER_PRECEDENCE,
        // Level information (for reference)
        level_node: row.LEVEL_NODE,
        hierarchy_name: row.HIERARCHY_NAME,
        // Filter groups (from mapping context)
        filter_groups: {
          filter_group_1: row.FILTER_GROUP_1,
          filter_group_2: row.FILTER_GROUP_2,
          filter_group_3: row.FILTER_GROUP_3,
          filter_group_4: row.FILTER_GROUP_4,
        },
        // Formula information (from mapping context)
        formula_info: {
          formula_group: row.FORMULA_GROUP,
          formula_precedence: row.FORMULA_PRECEDENCE,
          formula_param_ref: row.FORMULA_PARAM_REF,
          arithmetic_logic: row.ARITHMETIC_LOGIC,
          formula_param2_const_number: row.FORMULA_PARAM2_CONST_NUMBER,
        },
        // Flags
        flags: {
          active_flag: row.ACTIVE_FLAG ?? true,
          exclude_flag: row.EXCLUDE_FLAG ?? false,
          exclusion_flag: row.EXCLUSION_FLAG ?? false,
          include_flag: row.INCLUDE_FLAG ?? true,
          transform_flag: row.TRANSFORM_FLAG ?? false,
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
          ...(Object.keys(customFlags).length > 0 ? { customFlags } : {}),
        },
        // Sort order
        sort_order: row.SORT_ORDER,
      };
    });
  }

  /**
   * Extract custom flags from row (non-standard columns) - returns flags in UPPERCASE
   */
  private extractCustomFlags(row: MappingCSVRow): any {
    const standardColumns = [
      'FK_REPORT_KEY',
      'XREF_HIERARCHY_KEY',
      'HIERARCHY_GROUP_NAME',
      'HIERARCHY_NAME',
      'LEVEL_NODE',
      'SORT_ORDER',
      'ID_NAME',
      'ID',
      'ID_SOURCE',
      'ID_TABLE',
      'ID_SCHEMA',
      'ID_DATABASE',
      'SOURCE_DATABASE',
      'SOURCE_SCHEMA',
      'SOURCE_TABLE',
      'SOURCE_COLUMN',
      'SOURCE_UID',
      'PRECEDENCE_GROUP',
      'GROUP_FILTER_PRECEDENCE',
      'ACTIVE_FLAG',
      'EXCLUDE_FLAG',
      'EXCLUSION_FLAG',
      'INCLUDE_FLAG',
      'TRANSFORM_FLAG',
      'DO_NOT_EXPAND_FLAG',
      'IS_SECURED_FLAG',
      'SPLIT_ACTIVE_FLAG',
      'CALCULATION_FLAG',
      'VOLUME_FLAG',
      'ID_UNPIVOT_FLAG',
      'ID_ROW_FLAG',
      'REMOVE_FROM_TOTALS',
      'CREATE_NEW_COLUMN',
      'SIGN_CHANGE_FLAG',
      'LEVEL_1',
      'LEVEL_2',
      'LEVEL_3',
      'LEVEL_4',
      'LEVEL_5',
      'LEVEL_6',
      'LEVEL_7',
      'LEVEL_8',
      'LEVEL_9',
      'LEVEL_1_SORT',
      'LEVEL_2_SORT',
      'LEVEL_3_SORT',
      'LEVEL_4_SORT',
      'LEVEL_5_SORT',
      'LEVEL_6_SORT',
      'LEVEL_7_SORT',
      'LEVEL_8_SORT',
      'LEVEL_9_SORT',
      'FILTER_GROUP_1',
      'FILTER_GROUP_2',
      'FILTER_GROUP_3',
      'FILTER_GROUP_4',
      'FORMULA_GROUP',
      'FORMULA_PRECEDENCE',
      'FORMULA_PARAM_REF',
      'ARITHMETIC_LOGIC',
      'FORMULA_PARAM2_CONST_NUMBER',
    ];

    const customFlags: any = {};
    Object.keys(row).forEach((key) => {
      if (!standardColumns.includes(key) && typeof row[key] === 'boolean') {
        // Store custom flags in UPPERCASE
        customFlags[key.toUpperCase()] = row[key];
      }
    });

    return customFlags;
  }

  /**
   * Parse CSV line handling quoted values
   */
  private parseCSVLine(line: string): string[] {
    const result: string[] = [];
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
        result.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }

    result.push(current.trim());
    return result;
  }

  /**
   * Parse boolean from CSV string
   */
  private parseBoolean(value: any): boolean {
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') {
      const lower = value.toLowerCase().trim();
      return lower === 'true' || lower === '1' || lower === 'yes';
    }
    return false;
  }
}

/**
 * Interface for Mapping CSV Row
 * Represents a single mapping entry linking a hierarchy to source data
 */
interface MappingCSVRow {
  XREF_HIERARCHY_KEY: number;
  FK_REPORT_KEY: number; // Links to hierarchy's XREF_HIERARCHY_KEY
  HIERARCHY_GROUP_NAME: string;
  HIERARCHY_NAME: string;
  LEVEL_NODE: string;
  SORT_ORDER: number;
  // Old format source identifiers
  ID_NAME: string;
  ID: string;
  ID_SOURCE: string;
  ID_TABLE: string;
  ID_SCHEMA: string;
  ID_DATABASE: string;
  // New format source identifiers
  SOURCE_DATABASE: string;
  SOURCE_SCHEMA: string;
  SOURCE_TABLE: string;
  SOURCE_COLUMN: string;
  SOURCE_UID: string;
  // Precedence
  PRECEDENCE_GROUP: number;
  GROUP_FILTER_PRECEDENCE: number;
  // Flags
  ACTIVE_FLAG: boolean;
  EXCLUDE_FLAG: boolean;
  EXCLUSION_FLAG: boolean;
  INCLUDE_FLAG: boolean;
  TRANSFORM_FLAG: boolean;
  DO_NOT_EXPAND_FLAG: boolean;
  IS_SECURED_FLAG: boolean;
  SPLIT_ACTIVE_FLAG: boolean;
  CALCULATION_FLAG: boolean;
  VOLUME_FLAG: boolean;
  ID_UNPIVOT_FLAG: boolean;
  ID_ROW_FLAG: boolean;
  REMOVE_FROM_TOTALS: boolean;
  CREATE_NEW_COLUMN: boolean;
  SIGN_CHANGE_FLAG: boolean;
  // Level information
  LEVEL_1: string;
  LEVEL_2: string;
  LEVEL_3: string;
  LEVEL_4: string;
  LEVEL_5: string;
  LEVEL_6: string;
  LEVEL_7: string;
  LEVEL_8: string;
  LEVEL_9: string;
  // Filter groups
  FILTER_GROUP_1: string;
  FILTER_GROUP_2: string;
  FILTER_GROUP_3: string;
  FILTER_GROUP_4: string;
  // Formula information
  FORMULA_GROUP: string;
  FORMULA_PRECEDENCE: number;
  FORMULA_PARAM_REF: string;
  ARITHMETIC_LOGIC: string;
  FORMULA_PARAM2_CONST_NUMBER: string;
  [key: string]: any; // Allow dynamic properties for additional columns
}
