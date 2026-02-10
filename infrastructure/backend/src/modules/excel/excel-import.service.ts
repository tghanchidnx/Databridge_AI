/**
 * Excel Import Service
 * Parses Excel files and converts to hierarchy structures
 */
import { Injectable, Logger, BadRequestException } from '@nestjs/common';
import * as XLSX from 'xlsx';

export interface SheetData {
  name: string;
  rowCount: number;
  columnCount: number;
  headers: string[];
  sampleRows: Record<string, any>[];
  rawData: any[][];
}

export interface ExcelParseResult {
  fileName: string;
  sheets: SheetData[];
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

export interface ColumnMapping {
  excelColumn: string;
  targetField: string;
  transform?: 'none' | 'uppercase' | 'lowercase' | 'trim' | 'number';
}

export interface ImportResult {
  success: boolean;
  hierarchiesCreated: number;
  mappingsCreated: number;
  warnings: string[];
  errors: string[];
}

@Injectable()
export class ExcelImportService {
  private readonly logger = new Logger(ExcelImportService.name);

  // Known column patterns for detection
  private readonly HIERARCHY_PATTERNS = {
    id: /^(hierarchy_?id|id|key|code)$/i,
    parent: /^(parent_?id|parent|parent_?key)$/i,
    name: /^(hierarchy_?name|name|description|label|title)$/i,
    level: /^level_?(\d+)$/i,
    sort: /^(sort|sort_?order|sequence|order|level_?\d+_?sort)$/i,
  };

  private readonly MAPPING_PATTERNS = {
    database: /^(source_?database|database|db|source_?db)$/i,
    schema: /^(source_?schema|schema)$/i,
    table: /^(source_?table|table|table_?name)$/i,
    column: /^(source_?column|column|column_?name|field)$/i,
    uid: /^(source_?uid|uid|unique_?id|id_?value)$/i,
  };

  /**
   * Parse Excel file buffer
   */
  async parseExcelFile(buffer: Buffer, fileName: string): Promise<ExcelParseResult> {
    this.logger.log(`Parsing Excel file: ${fileName}`);

    try {
      const workbook = XLSX.read(buffer, { type: 'buffer' });
      const sheets: SheetData[] = [];

      for (const sheetName of workbook.SheetNames) {
        const worksheet = workbook.Sheets[sheetName];
        const rawData = XLSX.utils.sheet_to_json<any[]>(worksheet, { header: 1 });

        if (rawData.length === 0) continue;

        const headers = (rawData[0] as string[]).map((h) => String(h || '').trim());
        const dataRows = rawData.slice(1).filter((row) => row.some((cell) => cell != null));

        const sampleRows = dataRows.slice(0, 5).map((row) => {
          const obj: Record<string, any> = {};
          headers.forEach((header, i) => {
            if (header) obj[header] = row[i];
          });
          return obj;
        });

        sheets.push({
          name: sheetName,
          rowCount: dataRows.length,
          columnCount: headers.length,
          headers,
          sampleRows,
          rawData,
        });
      }

      // Detect format and suggest mappings
      const suggestions = this.detectColumnMappings(sheets);
      const detectedFormat = this.detectFormat(suggestions);

      return {
        fileName,
        sheets,
        detectedFormat,
        suggestions,
      };
    } catch (error) {
      this.logger.error(`Failed to parse Excel file: ${error.message}`);
      throw new BadRequestException(`Failed to parse Excel file: ${error.message}`);
    }
  }

  /**
   * Detect column mappings based on patterns
   */
  private detectColumnMappings(sheets: SheetData[]): ExcelParseResult['suggestions'] {
    const allHeaders = sheets.flatMap((s) => s.headers);
    const suggestions: ExcelParseResult['suggestions'] = {
      hierarchyColumns: [],
      mappingColumns: [],
      levelColumns: [],
    };

    for (const header of allHeaders) {
      // Check for hierarchy ID
      if (this.HIERARCHY_PATTERNS.id.test(header)) {
        if (!suggestions.idColumn) suggestions.idColumn = header;
        suggestions.hierarchyColumns.push(header);
      }

      // Check for parent ID
      if (this.HIERARCHY_PATTERNS.parent.test(header)) {
        if (!suggestions.parentColumn) suggestions.parentColumn = header;
        suggestions.hierarchyColumns.push(header);
      }

      // Check for name
      if (this.HIERARCHY_PATTERNS.name.test(header)) {
        if (!suggestions.nameColumn) suggestions.nameColumn = header;
        suggestions.hierarchyColumns.push(header);
      }

      // Check for level columns
      if (this.HIERARCHY_PATTERNS.level.test(header)) {
        suggestions.levelColumns.push(header);
        suggestions.hierarchyColumns.push(header);
      }

      // Check for mapping columns
      if (
        this.MAPPING_PATTERNS.database.test(header) ||
        this.MAPPING_PATTERNS.schema.test(header) ||
        this.MAPPING_PATTERNS.table.test(header) ||
        this.MAPPING_PATTERNS.column.test(header)
      ) {
        suggestions.mappingColumns.push(header);
      }
    }

    // Sort level columns numerically
    suggestions.levelColumns.sort((a, b) => {
      const numA = parseInt(a.match(/\d+/)?.[0] || '0');
      const numB = parseInt(b.match(/\d+/)?.[0] || '0');
      return numA - numB;
    });

    return suggestions;
  }

  /**
   * Detect file format based on column analysis
   */
  private detectFormat(suggestions: ExcelParseResult['suggestions']): ExcelParseResult['detectedFormat'] {
    const hasHierarchyColumns =
      suggestions.idColumn || suggestions.nameColumn || suggestions.levelColumns.length > 0;
    const hasMappingColumns = suggestions.mappingColumns.length >= 2;

    if (hasHierarchyColumns && hasMappingColumns) return 'combined';
    if (hasHierarchyColumns) return 'hierarchy';
    if (hasMappingColumns) return 'mapping';
    return 'unknown';
  }

  /**
   * Convert Excel data to hierarchy structure
   */
  async convertToHierarchies(
    sheet: SheetData,
    columnMappings: ColumnMapping[],
  ): Promise<any[]> {
    this.logger.log(`Converting sheet "${sheet.name}" to hierarchies`);

    const hierarchies: any[] = [];
    const mappingLookup = new Map(columnMappings.map((m) => [m.targetField, m]));

    for (const row of sheet.rawData.slice(1)) {
      if (!row.some((cell) => cell != null)) continue;

      const rowData: Record<string, any> = {};
      sheet.headers.forEach((header, i) => {
        if (header) rowData[header] = row[i];
      });

      const hierarchy = this.mapRowToHierarchy(rowData, mappingLookup, sheet.headers);
      if (hierarchy) hierarchies.push(hierarchy);
    }

    return hierarchies;
  }

  /**
   * Map a row to hierarchy structure
   */
  private mapRowToHierarchy(
    rowData: Record<string, any>,
    mappingLookup: Map<string, ColumnMapping>,
    headers: string[],
  ): any | null {
    const getValue = (targetField: string): any => {
      const mapping = mappingLookup.get(targetField);
      if (!mapping) return undefined;

      let value = rowData[mapping.excelColumn];
      if (value === undefined || value === null) return undefined;

      // Apply transform
      switch (mapping.transform) {
        case 'uppercase':
          value = String(value).toUpperCase();
          break;
        case 'lowercase':
          value = String(value).toLowerCase();
          break;
        case 'trim':
          value = String(value).trim();
          break;
        case 'number':
          value = Number(value);
          break;
      }

      return value;
    };

    const hierarchyId = getValue('hierarchyId') || getValue('id');
    if (!hierarchyId) return null;

    const hierarchyLevel: Record<string, string> = {};
    for (let i = 1; i <= 15; i++) {
      const levelValue = getValue(`level_${i}`);
      if (levelValue !== undefined) {
        hierarchyLevel[`level_${i}`] = String(levelValue);
      }
    }

    return {
      hierarchyId: String(hierarchyId),
      hierarchyName: getValue('hierarchyName') || getValue('name') || '',
      parentId: getValue('parentId') || getValue('parent') || null,
      description: getValue('description') || '',
      hierarchyLevel,
      sortOrder: getValue('sortOrder') || getValue('sort') || 0,
      flags: {
        include_flag: true,
        exclude_flag: false,
        transform_flag: false,
        active_flag: true,
        is_leaf_node: false,
      },
      mapping: [],
    };
  }

  /**
   * Convert Excel data to mappings
   */
  async convertToMappings(
    sheet: SheetData,
    columnMappings: ColumnMapping[],
    hierarchyIdColumn: string,
  ): Promise<Map<string, any[]>> {
    this.logger.log(`Converting sheet "${sheet.name}" to mappings`);

    const mappingsByHierarchy = new Map<string, any[]>();
    const mappingLookup = new Map(columnMappings.map((m) => [m.targetField, m]));

    for (const row of sheet.rawData.slice(1)) {
      if (!row.some((cell) => cell != null)) continue;

      const rowData: Record<string, any> = {};
      sheet.headers.forEach((header, i) => {
        if (header) rowData[header] = row[i];
      });

      const hierarchyId = rowData[hierarchyIdColumn];
      if (!hierarchyId) continue;

      const getValue = (targetField: string): any => {
        const mapping = mappingLookup.get(targetField);
        if (!mapping) return undefined;
        return rowData[mapping.excelColumn];
      };

      const sourceMapping = {
        mapping_index: 1, // Will be adjusted when merging
        source_database: getValue('source_database') || '',
        source_schema: getValue('source_schema') || '',
        source_table: getValue('source_table') || '',
        source_column: getValue('source_column') || '',
        source_uid: getValue('source_uid') || '',
        join_type: getValue('join_type') || 'LEFT',
        system_type: getValue('system_type') || 'ACTUALS',
        dimension_role: getValue('dimension_role') || 'SECONDARY',
        flags: {
          include_flag: true,
          exclude_flag: false,
          transform_flag: false,
          active_flag: true,
        },
      };

      if (!mappingsByHierarchy.has(hierarchyId)) {
        mappingsByHierarchy.set(hierarchyId, []);
      }
      mappingsByHierarchy.get(hierarchyId)!.push(sourceMapping);
    }

    // Re-index mappings per hierarchy
    for (const [, mappings] of mappingsByHierarchy) {
      mappings.forEach((m, i) => {
        m.mapping_index = i + 1;
      });
    }

    return mappingsByHierarchy;
  }

  /**
   * Import Excel file to project
   */
  async importExcelToProject(
    projectId: string,
    parseResult: ExcelParseResult,
    sheetName: string,
    columnMappings: ColumnMapping[],
    conflictResolution: 'merge' | 'replace' | 'skip' = 'merge',
  ): Promise<ImportResult> {
    this.logger.log(`Importing Excel to project: ${projectId}`);

    const result: ImportResult = {
      success: true,
      hierarchiesCreated: 0,
      mappingsCreated: 0,
      warnings: [],
      errors: [],
    };

    try {
      const sheet = parseResult.sheets.find((s) => s.name === sheetName);
      if (!sheet) {
        throw new BadRequestException(`Sheet "${sheetName}" not found`);
      }

      // Convert to hierarchies
      const hierarchies = await this.convertToHierarchies(sheet, columnMappings);
      result.hierarchiesCreated = hierarchies.length;

      // In production, save to database here
      // For now, just return counts

      this.logger.log(`Imported ${hierarchies.length} hierarchies`);
      return result;
    } catch (error) {
      this.logger.error(`Import failed: ${error.message}`);
      result.success = false;
      result.errors.push(error.message);
      return result;
    }
  }
}
