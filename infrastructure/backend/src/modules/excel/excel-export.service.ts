/**
 * Excel Export Service
 * Exports hierarchy data to Excel format
 */
import { Injectable, Logger } from '@nestjs/common';
import * as XLSX from 'xlsx';

export interface ExcelExportOptions {
  projectId: string;
  hierarchyIds?: string[];
  includeMappings?: boolean;
  includeFormulas?: boolean;
  includeVarianceConfig?: boolean;
}

export interface HierarchyData {
  id: string;
  hierarchyId: string;
  hierarchyName: string;
  parentId?: string;
  description?: string;
  hierarchyLevel: Record<string, string>;
  sortOrder?: number;
  flags: Record<string, boolean>;
  mapping: any[];
  formulaConfig?: any;
}

@Injectable()
export class ExcelExportService {
  private readonly logger = new Logger(ExcelExportService.name);

  /**
   * Export hierarchies to Excel buffer
   */
  async exportHierarchiesToExcel(
    hierarchies: HierarchyData[],
    options: ExcelExportOptions,
  ): Promise<Buffer> {
    this.logger.log(`Exporting ${hierarchies.length} hierarchies to Excel`);

    const workbook = XLSX.utils.book_new();

    // Sheet 1: Hierarchy Structure
    const hierarchySheet = this.createHierarchySheet(hierarchies);
    XLSX.utils.book_append_sheet(workbook, hierarchySheet, 'Hierarchies');

    // Sheet 2: Mappings (optional)
    if (options.includeMappings !== false) {
      const mappingSheet = this.createMappingSheet(hierarchies);
      XLSX.utils.book_append_sheet(workbook, mappingSheet, 'Mappings');
    }

    // Sheet 3: Formulas (optional)
    if (options.includeFormulas) {
      const formulaSheet = this.createFormulaSheet(hierarchies);
      XLSX.utils.book_append_sheet(workbook, formulaSheet, 'Formulas');
    }

    // Sheet 4: Variance Config (optional)
    if (options.includeVarianceConfig) {
      const varianceSheet = this.createVarianceConfigSheet();
      XLSX.utils.book_append_sheet(workbook, varianceSheet, 'Variance Config');
    }

    // Write to buffer
    const buffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
    return Buffer.from(buffer);
  }

  /**
   * Create hierarchy structure sheet
   */
  private createHierarchySheet(hierarchies: HierarchyData[]): XLSX.WorkSheet {
    // Define columns
    const headers = [
      'HIERARCHY_ID',
      'HIERARCHY_NAME',
      'PARENT_ID',
      'DESCRIPTION',
      'LEVEL_1',
      'LEVEL_2',
      'LEVEL_3',
      'LEVEL_4',
      'LEVEL_5',
      'LEVEL_6',
      'LEVEL_7',
      'LEVEL_8',
      'LEVEL_9',
      'LEVEL_10',
      'LEVEL_1_SORT',
      'LEVEL_2_SORT',
      'LEVEL_3_SORT',
      'LEVEL_4_SORT',
      'LEVEL_5_SORT',
      'INCLUDE_FLAG',
      'EXCLUDE_FLAG',
      'TRANSFORM_FLAG',
      'ACTIVE_FLAG',
      'SORT_ORDER',
    ];

    const rows = hierarchies.map((h) => [
      h.hierarchyId,
      h.hierarchyName,
      h.parentId || '',
      h.description || '',
      h.hierarchyLevel?.level_1 || '',
      h.hierarchyLevel?.level_2 || '',
      h.hierarchyLevel?.level_3 || '',
      h.hierarchyLevel?.level_4 || '',
      h.hierarchyLevel?.level_5 || '',
      h.hierarchyLevel?.level_6 || '',
      h.hierarchyLevel?.level_7 || '',
      h.hierarchyLevel?.level_8 || '',
      h.hierarchyLevel?.level_9 || '',
      h.hierarchyLevel?.level_10 || '',
      h.hierarchyLevel?.level_1_sort || '',
      h.hierarchyLevel?.level_2_sort || '',
      h.hierarchyLevel?.level_3_sort || '',
      h.hierarchyLevel?.level_4_sort || '',
      h.hierarchyLevel?.level_5_sort || '',
      h.flags?.include_flag ? 'TRUE' : 'FALSE',
      h.flags?.exclude_flag ? 'TRUE' : 'FALSE',
      h.flags?.transform_flag ? 'TRUE' : 'FALSE',
      h.flags?.active_flag ? 'TRUE' : 'FALSE',
      h.sortOrder || 0,
    ]);

    const data = [headers, ...rows];
    const sheet = XLSX.utils.aoa_to_sheet(data);

    // Set column widths
    sheet['!cols'] = headers.map((h) => ({ wch: Math.max(h.length, 15) }));

    return sheet;
  }

  /**
   * Create mappings sheet
   */
  private createMappingSheet(hierarchies: HierarchyData[]): XLSX.WorkSheet {
    const headers = [
      'HIERARCHY_ID',
      'MAPPING_INDEX',
      'SOURCE_DATABASE',
      'SOURCE_SCHEMA',
      'SOURCE_TABLE',
      'SOURCE_COLUMN',
      'SOURCE_UID',
      'JOIN_TYPE',
      'SYSTEM_TYPE',
      'DIMENSION_ROLE',
      'HIERARCHY_KEY_COLUMN',
      'PRECEDENCE_GROUP',
      'INCLUDE_FLAG',
      'EXCLUDE_FLAG',
      'TRANSFORM_FLAG',
      'ACTIVE_FLAG',
    ];

    const rows: any[][] = [];
    for (const h of hierarchies) {
      if (!h.mapping || h.mapping.length === 0) continue;

      for (const m of h.mapping) {
        rows.push([
          h.hierarchyId,
          m.mapping_index,
          m.source_database || '',
          m.source_schema || '',
          m.source_table || '',
          m.source_column || '',
          m.source_uid || '',
          m.join_type || 'LEFT',
          m.system_type || 'ACTUALS',
          m.dimension_role || 'SECONDARY',
          m.hierarchy_key_column || '',
          m.precedence_group || '',
          m.flags?.include_flag ? 'TRUE' : 'FALSE',
          m.flags?.exclude_flag ? 'TRUE' : 'FALSE',
          m.flags?.transform_flag ? 'TRUE' : 'FALSE',
          m.flags?.active_flag ? 'TRUE' : 'FALSE',
        ]);
      }
    }

    const data = [headers, ...rows];
    const sheet = XLSX.utils.aoa_to_sheet(data);
    sheet['!cols'] = headers.map((h) => ({ wch: Math.max(h.length, 15) }));

    return sheet;
  }

  /**
   * Create formulas sheet
   */
  private createFormulaSheet(hierarchies: HierarchyData[]): XLSX.WorkSheet {
    const headers = [
      'HIERARCHY_ID',
      'FORMULA_TYPE',
      'FORMULA_TEXT',
      'FORMULA_GROUP_ID',
      'FORMULA_ROLE',
      'FORMULA_PRECEDENCE',
    ];

    const rows: any[][] = [];
    for (const h of hierarchies) {
      if (!h.formulaConfig) continue;

      rows.push([
        h.hierarchyId,
        h.formulaConfig.formula_type || '',
        h.formulaConfig.formula_text || '',
        h.formulaConfig.formula_group_ref?.formulaGroupId || '',
        h.formulaConfig.formula_group_ref?.role || '',
        h.formulaConfig.formula_group_ref?.FORMULA_PRECEDENCE || '',
      ]);
    }

    const data = [headers, ...rows];
    const sheet = XLSX.utils.aoa_to_sheet(data);
    sheet['!cols'] = headers.map((h) => ({ wch: Math.max(h.length, 20) }));

    return sheet;
  }

  /**
   * Create variance config sheet
   */
  private createVarianceConfigSheet(): XLSX.WorkSheet {
    const headers = [
      'COMPARISON_NAME',
      'MINUEND_SYSTEM',
      'SUBTRAHEND_SYSTEM',
      'INCLUDE_PERCENT',
    ];

    // Default variance comparisons
    const rows = [
      ['Actual vs Budget', 'ACTUALS', 'BUDGET', 'TRUE'],
      ['Actual vs Forecast', 'ACTUALS', 'FORECAST', 'TRUE'],
      ['Budget vs Forecast', 'BUDGET', 'FORECAST', 'TRUE'],
      ['YoY Variance', 'ACTUALS', 'PRIOR_YEAR', 'TRUE'],
    ];

    const data = [headers, ...rows];
    const sheet = XLSX.utils.aoa_to_sheet(data);
    sheet['!cols'] = headers.map((h) => ({ wch: Math.max(h.length, 20) }));

    return sheet;
  }

  /**
   * Generate import template
   */
  async generateImportTemplate(): Promise<Buffer> {
    this.logger.log('Generating import template');

    const workbook = XLSX.utils.book_new();

    // Hierarchy template sheet
    const hierarchyHeaders = [
      'HIERARCHY_ID',
      'HIERARCHY_NAME',
      'PARENT_ID',
      'DESCRIPTION',
      'LEVEL_1',
      'LEVEL_2',
      'LEVEL_3',
      'LEVEL_4',
      'LEVEL_5',
      'INCLUDE_FLAG',
      'EXCLUDE_FLAG',
      'SORT_ORDER',
    ];

    const hierarchySample = [
      ['REVENUE_001', 'Total Revenue', '', 'Revenue hierarchy root', 'Revenue', '', '', '', '', 'TRUE', 'FALSE', '1'],
      ['REVENUE_002', 'Product Revenue', 'REVENUE_001', 'Product sales', 'Revenue', 'Product', '', '', '', 'TRUE', 'FALSE', '2'],
      ['REVENUE_003', 'Service Revenue', 'REVENUE_001', 'Service fees', 'Revenue', 'Service', '', '', '', 'TRUE', 'FALSE', '3'],
    ];

    const hierarchySheet = XLSX.utils.aoa_to_sheet([hierarchyHeaders, ...hierarchySample]);
    hierarchySheet['!cols'] = hierarchyHeaders.map((h) => ({ wch: Math.max(h.length, 15) }));
    XLSX.utils.book_append_sheet(workbook, hierarchySheet, 'Hierarchies Template');

    // Mapping template sheet
    const mappingHeaders = [
      'HIERARCHY_ID',
      'MAPPING_INDEX',
      'SOURCE_DATABASE',
      'SOURCE_SCHEMA',
      'SOURCE_TABLE',
      'SOURCE_COLUMN',
      'JOIN_TYPE',
      'SYSTEM_TYPE',
      'DIMENSION_ROLE',
    ];

    const mappingSample = [
      ['REVENUE_002', '1', 'FINANCE_DB', 'GL', 'FACT_SALES', 'AMOUNT', 'INNER', 'ACTUALS', 'PRIMARY'],
      ['REVENUE_002', '2', 'FINANCE_DB', 'GL', 'FACT_BUDGET', 'AMOUNT', 'LEFT', 'BUDGET', 'SECONDARY'],
      ['REVENUE_003', '1', 'FINANCE_DB', 'GL', 'FACT_SERVICES', 'AMOUNT', 'INNER', 'ACTUALS', 'PRIMARY'],
    ];

    const mappingSheet = XLSX.utils.aoa_to_sheet([mappingHeaders, ...mappingSample]);
    mappingSheet['!cols'] = mappingHeaders.map((h) => ({ wch: Math.max(h.length, 15) }));
    XLSX.utils.book_append_sheet(workbook, mappingSheet, 'Mappings Template');

    // Instructions sheet
    const instructionData = [
      ['DataBridge AI V2 - Excel Import Template'],
      [''],
      ['Instructions:'],
      ['1. Fill in the Hierarchies Template sheet with your hierarchy data'],
      ['2. Fill in the Mappings Template sheet with source mappings'],
      ['3. Save this file and import it into DataBridge AI'],
      [''],
      ['Column Descriptions - Hierarchies:'],
      ['HIERARCHY_ID - Unique identifier for the hierarchy'],
      ['HIERARCHY_NAME - Display name'],
      ['PARENT_ID - ID of parent hierarchy (leave empty for root)'],
      ['DESCRIPTION - Optional description'],
      ['LEVEL_1..LEVEL_10 - Hierarchy level values'],
      ['INCLUDE_FLAG - TRUE/FALSE to include in reports'],
      ['EXCLUDE_FLAG - TRUE/FALSE to exclude from reports'],
      ['SORT_ORDER - Numeric sort order'],
      [''],
      ['Column Descriptions - Mappings:'],
      ['HIERARCHY_ID - Links to hierarchy'],
      ['MAPPING_INDEX - Order of mapping (1, 2, 3...)'],
      ['SOURCE_DATABASE - Database name'],
      ['SOURCE_SCHEMA - Schema name'],
      ['SOURCE_TABLE - Table name'],
      ['SOURCE_COLUMN - Column name'],
      ['JOIN_TYPE - INNER, LEFT, RIGHT, or FULL'],
      ['SYSTEM_TYPE - ACTUALS, BUDGET, FORECAST, or PRIOR_YEAR'],
      ['DIMENSION_ROLE - PRIMARY, SECONDARY, or OPTIONAL'],
    ];

    const instructionSheet = XLSX.utils.aoa_to_sheet(instructionData);
    instructionSheet['!cols'] = [{ wch: 80 }];
    XLSX.utils.book_append_sheet(workbook, instructionSheet, 'Instructions');

    const buffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
    return Buffer.from(buffer);
  }
}
