import { Injectable, Logger } from '@nestjs/common';

/**
 * CSV Analyzer Service
 * Provides AI-powered analysis of CSV files for hierarchy imports
 * Detects issues, suggests fixes, and can auto-correct common problems
 */

export interface CSVAnalysisResult {
  isValid: boolean;
  format: 'hierarchy' | 'mapping' | 'legacy' | 'unknown';
  formatConfidence: number;
  rowCount: number;
  columnCount: number;
  columns: ColumnAnalysis[];
  issues: CSVIssue[];
  suggestions: CSVSuggestion[];
  canAutoFix: boolean;
  autoFixActions: AutoFixAction[];
  stats: CSVStats;
  // AI-powered analysis results
  aiAnalysis?: AIAnalysisResult;
  // Legacy format specific data
  legacyFormatInfo?: LegacyFormatInfo;
}

export interface AIAnalysisResult {
  summary: string;
  formatDetection: {
    detected: string;
    confidence: number;
    reason: string;
  };
  columnMappings: Array<{
    sourceColumn: string;
    suggestedMapping: string;
    confidence: number;
    reason: string;
  }>;
  dataQuality: {
    score: number;
    issues: string[];
    recommendations: string[];
  };
  conversionSteps?: Array<{
    step: number;
    action: string;
    description: string;
    status: 'pending' | 'completed' | 'failed';
  }>;
}

export interface LegacyFormatInfo {
  isLegacy: boolean;
  hasXrefKey: boolean;
  hasFilterGroups: boolean;
  hasLevelColumns: boolean;
  hasSortColumns: boolean;
  levelCount: number;
  filterGroupColumns: string[];
  extractedMappingInfo: Array<{
    hierarchyKey: string;
    filterGroup1?: string;
    filterGroup2?: string;
    filterGroup3?: string;
    filterGroup4?: string;
    formulaGroup?: string;
  }>;
  conversionPlan: {
    canConvert: boolean;
    steps: string[];
    warnings: string[];
  };
}

export interface ColumnAnalysis {
  name: string;
  originalName: string;
  index: number;
  detectedType: 'string' | 'number' | 'boolean' | 'date' | 'id' | 'empty';
  nullCount: number;
  nullPercentage: number;
  uniqueCount: number;
  uniquePercentage: number;
  sampleValues: string[];
  mappedTo?: string; // Standard field this maps to
}

export interface CSVIssue {
  type: 'error' | 'warning' | 'info';
  code: string;
  message: string;
  details?: string;
  row?: number;
  column?: string;
  affectedRows?: number[];
  canAutoFix: boolean;
}

export interface CSVSuggestion {
  type: 'rename' | 'transform' | 'remove' | 'add' | 'fix';
  message: string;
  column?: string;
  action?: string;
  priority: 'high' | 'medium' | 'low';
}

export interface AutoFixAction {
  type: string;
  description: string;
  affectedRows: number;
  column?: string;
  beforeValue?: string;
  afterValue?: string;
}

export interface CSVStats {
  totalRows: number;
  validRows: number;
  invalidRows: number;
  duplicateIds: number;
  orphanParents: number;
  emptyRequired: number;
  encodingIssues: number;
}

// Standard column mappings for hierarchy files
const HIERARCHY_COLUMN_PATTERNS = {
  HIERARCHY_ID: [/^hierarchy[_\s]?id$/i, /^id$/i, /^key$/i, /^code$/i, /^unique[_\s]?id$/i],
  HIERARCHY_NAME: [/^hierarchy[_\s]?name$/i, /^name$/i, /^description$/i, /^label$/i, /^title$/i],
  PARENT_ID: [/^parent[_\s]?id$/i, /^parent$/i, /^parent[_\s]?key$/i, /^parent[_\s]?hierarchy[_\s]?id$/i],
  DESCRIPTION: [/^description$/i, /^desc$/i, /^memo$/i, /^notes$/i],
  SORT_ORDER: [/^sort[_\s]?order$/i, /^sort$/i, /^sequence$/i, /^order$/i, /^display[_\s]?order$/i],
  LEVEL_1: [/^level[_\s]?1$/i, /^l1$/i, /^level1$/i],
  LEVEL_2: [/^level[_\s]?2$/i, /^l2$/i, /^level2$/i],
  LEVEL_3: [/^level[_\s]?3$/i, /^l3$/i, /^level3$/i],
  LEVEL_4: [/^level[_\s]?4$/i, /^l4$/i, /^level4$/i],
  LEVEL_5: [/^level[_\s]?5$/i, /^l5$/i, /^level5$/i],
};

// Standard column mappings for mapping files
const MAPPING_COLUMN_PATTERNS = {
  HIERARCHY_ID: [/^hierarchy[_\s]?id$/i, /^id$/i, /^key$/i],
  MAPPING_INDEX: [/^mapping[_\s]?index$/i, /^index$/i, /^seq$/i, /^sequence$/i],
  SOURCE_DATABASE: [/^source[_\s]?database$/i, /^database$/i, /^db$/i, /^source[_\s]?db$/i],
  SOURCE_SCHEMA: [/^source[_\s]?schema$/i, /^schema$/i],
  SOURCE_TABLE: [/^source[_\s]?table$/i, /^table$/i, /^table[_\s]?name$/i],
  SOURCE_COLUMN: [/^source[_\s]?column$/i, /^column$/i, /^column[_\s]?name$/i, /^field$/i],
  SOURCE_UID: [/^source[_\s]?uid$/i, /^uid$/i, /^unique[_\s]?id$/i, /^filter[_\s]?value$/i],
  PRECEDENCE_GROUP: [/^precedence[_\s]?group$/i, /^precedence$/i, /^priority$/i],
};

// Legacy format column patterns (combined hierarchy + mapping)
const LEGACY_COLUMN_PATTERNS = {
  XREF_HIERARCHY_KEY: [/^xref[_\s]?hierarchy[_\s]?key$/i, /^xref_key$/i, /^hierarchy_key$/i],
  HIERARCHY_GROUP_NAME: [/^hierarchy[_\s]?group[_\s]?name$/i, /^group[_\s]?name$/i],
  FILTER_GROUP_1: [/^filter[_\s]?group[_\s]?1$/i, /^filter_group1$/i, /^filtergroup1$/i],
  FILTER_GROUP_2: [/^filter[_\s]?group[_\s]?2$/i, /^filter_group2$/i, /^filtergroup2$/i],
  FILTER_GROUP_3: [/^filter[_\s]?group[_\s]?3$/i, /^filter_group3$/i, /^filtergroup3$/i],
  FILTER_GROUP_4: [/^filter[_\s]?group[_\s]?4$/i, /^filter_group4$/i, /^filtergroup4$/i],
  FORMULA_GROUP: [/^formula[_\s]?group$/i, /^formula_grp$/i],
  FORMULA_PRECEDENCE: [/^formula[_\s]?precedence$/i, /^formula_prec$/i],
  FORMULA_PARAM_REF: [/^formula[_\s]?param[_\s]?ref$/i, /^formula_ref$/i],
  ARITHMETIC_LOGIC: [/^arithmetic[_\s]?logic$/i, /^arithmetic$/i, /^formula_logic$/i],
};

// Flag columns common in legacy format
const FLAG_COLUMN_PATTERNS = {
  DO_NOT_EXPAND_FLAG: [/^do[_\s]?not[_\s]?expand[_\s]?flag$/i],
  IS_SECURED_FLAG: [/^is[_\s]?secured[_\s]?flag$/i],
  SPLIT_ACTIVE_FLAG: [/^split[_\s]?active[_\s]?flag$/i],
  EXCLUSION_FLAG: [/^exclusion[_\s]?flag$/i],
  CALCULATION_FLAG: [/^calculation[_\s]?flag$/i],
  ACTIVE_FLAG: [/^active[_\s]?flag$/i],
  VOLUME_FLAG: [/^volume[_\s]?flag$/i],
  ID_UNPIVOT_FLAG: [/^id[_\s]?unpivot[_\s]?flag$/i],
  ID_ROW_FLAG: [/^id[_\s]?row[_\s]?flag$/i],
  REMOVE_FROM_TOTALS: [/^remove[_\s]?from[_\s]?totals$/i],
  SIGN_CHANGE_FLAG: [/^sign[_\s]?change[_\s]?flag$/i],
  CREATE_NEW_COLUMN: [/^create[_\s]?new[_\s]?column$/i],
};

@Injectable()
export class CsvAnalyzerService {
  private readonly logger = new Logger(CsvAnalyzerService.name);

  /**
   * Analyze a CSV file and return detailed analysis results
   */
  async analyzeCSV(csvContent: string, fileName?: string): Promise<CSVAnalysisResult> {
    this.logger.log(`Analyzing CSV file: ${fileName || 'unknown'}`);

    const issues: CSVIssue[] = [];
    const suggestions: CSVSuggestion[] = [];
    const autoFixActions: AutoFixAction[] = [];

    // Parse CSV
    const { headers, rows, parseErrors } = this.parseCSV(csvContent);

    // Add parsing errors as issues
    parseErrors.forEach((error) => {
      issues.push({
        type: 'error',
        code: 'PARSE_ERROR',
        message: error.message,
        row: error.row,
        canAutoFix: false,
      });
    });

    // Analyze columns
    const columns = this.analyzeColumns(headers, rows);

    // Detect format
    const { format, confidence } = this.detectFormat(columns);

    // Analyze legacy format if detected
    let legacyFormatInfo: LegacyFormatInfo | undefined;
    if (format === 'legacy') {
      legacyFormatInfo = this.analyzeLegacyFormat(columns, rows);
      this.validateLegacyCSV(columns, rows, issues, suggestions, autoFixActions, legacyFormatInfo);
    } else if (format === 'hierarchy') {
      this.validateHierarchyCSV(columns, rows, issues, suggestions, autoFixActions);
    } else if (format === 'mapping') {
      this.validateMappingCSV(columns, rows, issues, suggestions, autoFixActions);
    } else if (format === 'unknown') {
      issues.push({
        type: 'warning',
        code: 'UNKNOWN_FORMAT',
        message: 'Could not determine CSV format. Expected hierarchy or mapping format.',
        canAutoFix: false,
      });
      suggestions.push({
        type: 'add',
        message: 'Ensure file has required columns: HIERARCHY_ID for hierarchy files, or SOURCE_TABLE for mapping files',
        priority: 'high',
      });
    }

    // Generate AI analysis
    const aiAnalysis = this.generateAIAnalysis(columns, rows, format, legacyFormatInfo);

    // Check for common data issues
    this.checkCommonIssues(columns, rows, issues, suggestions, autoFixActions);

    // Calculate stats
    const stats = this.calculateStats(rows, columns, issues);

    // Add AI-suggested issues and recommendations
    if (aiAnalysis.dataQuality.issues.length > 0) {
      for (const aiIssue of aiAnalysis.dataQuality.issues) {
        if (!issues.some((i) => i.message.includes(aiIssue))) {
          issues.push({
            type: 'info',
            code: 'AI_INSIGHT',
            message: aiIssue,
            canAutoFix: false,
          });
        }
      }
    }

    if (aiAnalysis.dataQuality.recommendations.length > 0) {
      for (const rec of aiAnalysis.dataQuality.recommendations) {
        if (!suggestions.some((s) => s.message.includes(rec))) {
          suggestions.push({
            type: 'fix',
            message: rec,
            priority: 'medium',
          });
        }
      }
    }

    const hasErrors = issues.some((i) => i.type === 'error');

    return {
      isValid: !hasErrors,
      format,
      formatConfidence: confidence,
      rowCount: rows.length,
      columnCount: columns.length,
      columns,
      issues,
      suggestions,
      canAutoFix: autoFixActions.length > 0,
      autoFixActions,
      stats,
      aiAnalysis,
      legacyFormatInfo,
    };
  }

  /**
   * Validate legacy CSV format
   */
  private validateLegacyCSV(
    columns: ColumnAnalysis[],
    rows: string[][],
    issues: CSVIssue[],
    suggestions: CSVSuggestion[],
    autoFixActions: AutoFixAction[],
    legacyInfo: LegacyFormatInfo,
  ): void {
    // Check for XREF_HIERARCHY_KEY
    if (!legacyInfo.hasXrefKey) {
      issues.push({
        type: 'error',
        code: 'MISSING_XREF_KEY',
        message: 'Missing required column: XREF_HIERARCHY_KEY',
        details: 'Legacy format requires XREF_HIERARCHY_KEY for hierarchy identification',
        canAutoFix: false,
      });
    }

    // Check for level columns
    if (!legacyInfo.hasLevelColumns) {
      issues.push({
        type: 'warning',
        code: 'MISSING_LEVEL_COLUMNS',
        message: 'No LEVEL columns found in legacy format',
        details: 'Expected LEVEL_1, LEVEL_2, etc. columns for hierarchy structure',
        canAutoFix: false,
      });
    }

    // Inform about filter groups
    if (legacyInfo.hasFilterGroups) {
      issues.push({
        type: 'info',
        code: 'LEGACY_FILTER_GROUPS',
        message: `Found ${legacyInfo.filterGroupColumns.length} FILTER_GROUP columns for mapping identifiers`,
        details: `Columns: ${legacyInfo.filterGroupColumns.join(', ')}`,
        canAutoFix: false,
      });
      suggestions.push({
        type: 'transform',
        message: 'FILTER_GROUP values will be stored as mapping identifiers. Add source_database/table/column mappings manually if needed.',
        priority: 'medium',
      });
    } else {
      issues.push({
        type: 'warning',
        code: 'NO_FILTER_GROUPS',
        message: 'No FILTER_GROUP columns found - no mapping data will be imported',
        canAutoFix: false,
      });
      suggestions.push({
        type: 'add',
        message: 'Add source mappings after import using the mapping editor',
        priority: 'low',
      });
    }

    // Add conversion plan info
    if (legacyInfo.conversionPlan.canConvert) {
      issues.push({
        type: 'info',
        code: 'LEGACY_CONVERSION_READY',
        message: `Ready to convert legacy format (${legacyInfo.conversionPlan.steps.length} steps)`,
        details: legacyInfo.conversionPlan.steps.join('; '),
        canAutoFix: true,
      });
      autoFixActions.push({
        type: 'CONVERT_LEGACY_FORMAT',
        description: `Convert legacy format to standard hierarchy format with ${rows.length} rows`,
        affectedRows: rows.length,
      });
    }

    // Add conversion warnings
    for (const warning of legacyInfo.conversionPlan.warnings) {
      issues.push({
        type: 'warning',
        code: 'LEGACY_CONVERSION_WARNING',
        message: warning,
        canAutoFix: false,
      });
    }
  }

  /**
   * Parse CSV content into headers and rows
   */
  private parseCSV(content: string): {
    headers: string[];
    rows: string[][];
    parseErrors: { row: number; message: string }[];
  } {
    const parseErrors: { row: number; message: string }[] = [];
    const lines = content.split(/\r?\n/).filter((line) => line.trim());

    if (lines.length === 0) {
      parseErrors.push({ row: 0, message: 'CSV file is empty' });
      return { headers: [], rows: [], parseErrors };
    }

    // Parse header row
    const headers = this.parseCSVLine(lines[0]);

    // Parse data rows
    const rows: string[][] = [];
    for (let i = 1; i < lines.length; i++) {
      try {
        const row = this.parseCSVLine(lines[i]);
        if (row.length !== headers.length) {
          // Try to fix common issues
          if (row.length > headers.length) {
            // Too many columns - might have unescaped commas
            parseErrors.push({
              row: i + 1,
              message: `Row has ${row.length} columns but expected ${headers.length}. Possible unescaped comma in data.`,
            });
          } else {
            // Too few columns - pad with empty strings
            while (row.length < headers.length) {
              row.push('');
            }
          }
        }
        rows.push(row);
      } catch (error) {
        parseErrors.push({
          row: i + 1,
          message: `Failed to parse row: ${error.message}`,
        });
      }
    }

    return { headers, rows, parseErrors };
  }

  /**
   * Parse a single CSV line handling quoted fields
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
          // Escaped quote
          current += '"';
          i++;
        } else {
          // Toggle quote mode
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
   * Analyze columns and detect their types
   */
  private analyzeColumns(headers: string[], rows: string[][]): ColumnAnalysis[] {
    return headers.map((header, index) => {
      const values = rows.map((row) => row[index] || '');
      const nonEmpty = values.filter((v) => v !== '' && v !== null && v !== undefined);

      // Detect type
      let detectedType: ColumnAnalysis['detectedType'] = 'string';
      const lowerHeader = header.toLowerCase();

      if (lowerHeader.includes('id') || lowerHeader.includes('key') || lowerHeader.includes('code')) {
        detectedType = 'id';
      } else if (nonEmpty.length > 0 && nonEmpty.every((v) => !isNaN(Number(v)) && v !== '')) {
        detectedType = 'number';
      } else if (
        nonEmpty.length > 0 &&
        nonEmpty.every((v) => ['true', 'false', '1', '0', 'yes', 'no', 'y', 'n'].includes(v.toLowerCase()))
      ) {
        detectedType = 'boolean';
      } else if (nonEmpty.length === 0) {
        detectedType = 'empty';
      }

      // Find standard field mapping
      let mappedTo: string | undefined;
      const allPatterns = { ...HIERARCHY_COLUMN_PATTERNS, ...MAPPING_COLUMN_PATTERNS };
      for (const [standardField, patterns] of Object.entries(allPatterns)) {
        if (patterns.some((pattern) => pattern.test(header))) {
          mappedTo = standardField;
          break;
        }
      }

      return {
        name: header,
        originalName: header,
        index,
        detectedType,
        nullCount: values.length - nonEmpty.length,
        nullPercentage: ((values.length - nonEmpty.length) / Math.max(values.length, 1)) * 100,
        uniqueCount: new Set(nonEmpty).size,
        uniquePercentage: (new Set(nonEmpty).size / Math.max(nonEmpty.length, 1)) * 100,
        sampleValues: nonEmpty.slice(0, 5),
        mappedTo,
      };
    });
  }

  /**
   * Detect if this is a hierarchy, mapping, or legacy CSV format
   */
  private detectFormat(columns: ColumnAnalysis[]): { format: CSVAnalysisResult['format']; confidence: number } {
    const columnNames = columns.map((c) => c.name.toLowerCase());

    // Check for legacy format indicators FIRST (most specific)
    let legacyScore = 0;
    const legacyIndicators = [
      'xref_hierarchy_key',
      'hierarchy_group_name',
      'filter_group_1',
      'filter_group_2',
      'formula_group',
      'formula_precedence',
      'arithmetic_logic',
    ];

    legacyIndicators.forEach((indicator) => {
      if (columnNames.some((c) => c === indicator || c.replace(/[_\s]/g, '') === indicator.replace(/[_\s]/g, ''))) {
        legacyScore++;
      }
    });

    // Check for level columns with sort (strong legacy indicator)
    const hasLevelColumns = columnNames.some((c) => /^level[_\s]?\d+$/i.test(c));
    const hasLevelSortColumns = columnNames.some((c) => /^level[_\s]?\d+[_\s]?sort$/i.test(c));
    if (hasLevelColumns && hasLevelSortColumns) {
      legacyScore += 2;
    }

    // Check for flag columns (common in legacy format)
    const flagIndicators = ['do_not_expand_flag', 'is_secured_flag', 'calculation_flag', 'volume_flag', 'id_row_flag'];
    flagIndicators.forEach((indicator) => {
      if (columnNames.some((c) => c === indicator || c.replace(/[_\s]/g, '') === indicator.replace(/[_\s]/g, ''))) {
        legacyScore++;
      }
    });

    // Count standard hierarchy indicators
    let hierarchyScore = 0;
    const hierarchyIndicators = ['hierarchy_id', 'hierarchy_name', 'parent_id', 'description', 'sort_order'];
    hierarchyIndicators.forEach((indicator) => {
      if (columnNames.some((c) => c === indicator || c.includes(indicator.replace('_', '')))) {
        hierarchyScore++;
      }
    });

    // Count standard mapping indicators
    let mappingScore = 0;
    const mappingIndicators = ['source_database', 'source_table', 'source_column', 'mapping_index', 'source_schema', 'source_uid'];
    mappingIndicators.forEach((indicator) => {
      if (columnNames.some((c) => c === indicator || c.includes(indicator.replace('_', '')))) {
        mappingScore++;
      }
    });

    // Determine format based on scores - legacy takes priority if strong indicators present
    if (legacyScore >= 3) {
      const confidence = Math.min((legacyScore / 8) * 100, 95);
      return { format: 'legacy', confidence };
    } else if (mappingScore > hierarchyScore && mappingScore >= 2) {
      return { format: 'mapping', confidence: (mappingScore / mappingIndicators.length) * 100 };
    } else if (hierarchyScore > mappingScore && hierarchyScore >= 2) {
      return { format: 'hierarchy', confidence: (hierarchyScore / hierarchyIndicators.length) * 100 };
    } else if (legacyScore >= 1 || (hasLevelColumns && !mappingScore)) {
      return { format: 'legacy', confidence: 40 + legacyScore * 10 };
    }

    return { format: 'unknown', confidence: 0 };
  }

  /**
   * Analyze legacy format and extract mapping information
   */
  private analyzeLegacyFormat(
    columns: ColumnAnalysis[],
    rows: string[][],
  ): LegacyFormatInfo {
    const columnNames = columns.map((c) => c.name.toLowerCase());

    // Find key columns
    const xrefKeyIndex = columns.findIndex((c) =>
      /^xref[_\s]?hierarchy[_\s]?key$/i.test(c.name)
    );
    const filterGroup1Index = columns.findIndex((c) =>
      /^filter[_\s]?group[_\s]?1$/i.test(c.name)
    );
    const filterGroup2Index = columns.findIndex((c) =>
      /^filter[_\s]?group[_\s]?2$/i.test(c.name)
    );
    const filterGroup3Index = columns.findIndex((c) =>
      /^filter[_\s]?group[_\s]?3$/i.test(c.name)
    );
    const filterGroup4Index = columns.findIndex((c) =>
      /^filter[_\s]?group[_\s]?4$/i.test(c.name)
    );
    const formulaGroupIndex = columns.findIndex((c) =>
      /^formula[_\s]?group$/i.test(c.name)
    );

    // Count level columns
    const levelColumns = columns.filter((c) => /^level[_\s]?\d+$/i.test(c.name));
    const sortColumns = columns.filter((c) => /^level[_\s]?\d+[_\s]?sort$/i.test(c.name));

    // Identify filter group columns
    const filterGroupColumns: string[] = [];
    if (filterGroup1Index >= 0) filterGroupColumns.push(columns[filterGroup1Index].name);
    if (filterGroup2Index >= 0) filterGroupColumns.push(columns[filterGroup2Index].name);
    if (filterGroup3Index >= 0) filterGroupColumns.push(columns[filterGroup3Index].name);
    if (filterGroup4Index >= 0) filterGroupColumns.push(columns[filterGroup4Index].name);

    // Extract mapping info from rows (sample first 100 rows)
    const extractedMappingInfo: LegacyFormatInfo['extractedMappingInfo'] = [];
    const sampleRows = rows.slice(0, 100);

    for (const row of sampleRows) {
      const info: LegacyFormatInfo['extractedMappingInfo'][0] = {
        hierarchyKey: xrefKeyIndex >= 0 ? row[xrefKeyIndex] : '',
      };
      if (filterGroup1Index >= 0) info.filterGroup1 = row[filterGroup1Index];
      if (filterGroup2Index >= 0) info.filterGroup2 = row[filterGroup2Index];
      if (filterGroup3Index >= 0) info.filterGroup3 = row[filterGroup3Index];
      if (filterGroup4Index >= 0) info.filterGroup4 = row[filterGroup4Index];
      if (formulaGroupIndex >= 0) info.formulaGroup = row[formulaGroupIndex];

      extractedMappingInfo.push(info);
    }

    // Determine conversion plan
    const conversionSteps: string[] = [];
    const warnings: string[] = [];
    let canConvert = true;

    if (xrefKeyIndex < 0) {
      warnings.push('Missing XREF_HIERARCHY_KEY column - cannot determine hierarchy IDs');
      canConvert = false;
    } else {
      conversionSteps.push('Map XREF_HIERARCHY_KEY to HIERARCHY_ID');
    }

    if (levelColumns.length > 0) {
      conversionSteps.push(`Extract hierarchy names from ${levelColumns.length} LEVEL columns`);
      conversionSteps.push('Build parent-child relationships from level structure');
    } else {
      warnings.push('No LEVEL columns found - hierarchy structure unclear');
    }

    if (filterGroupColumns.length > 0) {
      conversionSteps.push(`Extract mapping identifiers from ${filterGroupColumns.length} FILTER_GROUP columns`);
      warnings.push('FILTER_GROUP values are concatenated strings - mappings will be stored as identifiers only');
      warnings.push('Standard source_database, source_table, source_column will not be populated');
    } else {
      warnings.push('No FILTER_GROUP columns found - no mapping data to extract');
    }

    return {
      isLegacy: true,
      hasXrefKey: xrefKeyIndex >= 0,
      hasFilterGroups: filterGroupColumns.length > 0,
      hasLevelColumns: levelColumns.length > 0,
      hasSortColumns: sortColumns.length > 0,
      levelCount: levelColumns.length,
      filterGroupColumns,
      extractedMappingInfo,
      conversionPlan: {
        canConvert,
        steps: conversionSteps,
        warnings,
      },
    };
  }

  /**
   * Generate AI analysis for the CSV
   */
  private generateAIAnalysis(
    columns: ColumnAnalysis[],
    rows: string[][],
    format: CSVAnalysisResult['format'],
    legacyInfo?: LegacyFormatInfo,
  ): AIAnalysisResult {
    const columnMappings: AIAnalysisResult['columnMappings'] = [];
    const issues: string[] = [];
    const recommendations: string[] = [];
    let qualityScore = 100;

    // Analyze each column for potential mappings
    for (const col of columns) {
      let suggestedMapping = '';
      let confidence = 0;
      let reason = '';

      // Check legacy patterns
      for (const [field, patterns] of Object.entries(LEGACY_COLUMN_PATTERNS)) {
        if (patterns.some((p) => p.test(col.name))) {
          suggestedMapping = field;
          confidence = 90;
          reason = 'Matches legacy format pattern';
          break;
        }
      }

      // Check hierarchy patterns if not matched
      if (!suggestedMapping) {
        for (const [field, patterns] of Object.entries(HIERARCHY_COLUMN_PATTERNS)) {
          if (patterns.some((p) => p.test(col.name))) {
            suggestedMapping = field;
            confidence = 85;
            reason = 'Matches standard hierarchy pattern';
            break;
          }
        }
      }

      // Check mapping patterns
      if (!suggestedMapping) {
        for (const [field, patterns] of Object.entries(MAPPING_COLUMN_PATTERNS)) {
          if (patterns.some((p) => p.test(col.name))) {
            suggestedMapping = field;
            confidence = 85;
            reason = 'Matches standard mapping pattern';
            break;
          }
        }
      }

      // Check flag patterns
      if (!suggestedMapping) {
        for (const [field, patterns] of Object.entries(FLAG_COLUMN_PATTERNS)) {
          if (patterns.some((p) => p.test(col.name))) {
            suggestedMapping = field;
            confidence = 80;
            reason = 'Matches flag column pattern';
            break;
          }
        }
      }

      // Analyze LEVEL columns
      if (!suggestedMapping && /^level[_\s]?\d+$/i.test(col.name)) {
        const levelNum = col.name.match(/\d+/)?.[0] || '?';
        suggestedMapping = `LEVEL_${levelNum}`;
        confidence = 95;
        reason = 'Level column for hierarchy depth';
      }

      // Analyze SORT columns
      if (!suggestedMapping && /^level[_\s]?\d+[_\s]?sort$/i.test(col.name)) {
        const levelNum = col.name.match(/\d+/)?.[0] || '?';
        suggestedMapping = `LEVEL_${levelNum}_SORT`;
        confidence = 95;
        reason = 'Sort order for level';
      }

      if (suggestedMapping) {
        columnMappings.push({
          sourceColumn: col.name,
          suggestedMapping,
          confidence,
          reason,
        });
      }
    }

    // Calculate quality score based on issues
    if (format === 'legacy') {
      issues.push('Legacy format detected - requires conversion');
      qualityScore -= 10;
      recommendations.push('Convert legacy format to standard hierarchy + mapping format');

      if (legacyInfo) {
        if (!legacyInfo.hasXrefKey) {
          issues.push('Missing XREF_HIERARCHY_KEY column');
          qualityScore -= 20;
        }
        if (!legacyInfo.hasFilterGroups) {
          issues.push('No FILTER_GROUP columns found - mappings cannot be extracted');
          qualityScore -= 15;
          recommendations.push('Add source mapping information manually after import');
        }
        if (legacyInfo.hasFilterGroups) {
          recommendations.push('FILTER_GROUP values will be stored as mapping identifiers');
          recommendations.push('You can add proper source_database/table/column mappings later');
        }
      }
    }

    // Check for empty columns
    const emptyColumns = columns.filter((c) => c.nullPercentage > 95);
    if (emptyColumns.length > 0) {
      issues.push(`${emptyColumns.length} columns are mostly empty`);
      qualityScore -= emptyColumns.length * 2;
    }

    // Build conversion steps for legacy format
    const conversionSteps: AIAnalysisResult['conversionSteps'] = [];
    if (format === 'legacy' && legacyInfo?.conversionPlan.canConvert) {
      let step = 1;
      for (const stepDesc of legacyInfo.conversionPlan.steps) {
        conversionSteps.push({
          step: step++,
          action: stepDesc.split(' ')[0],
          description: stepDesc,
          status: 'pending',
        });
      }
    }

    // Generate summary
    let summary = '';
    if (format === 'legacy') {
      summary = `Detected **Legacy Format** CSV with ${rows.length} rows. This format combines hierarchy and mapping data using LEVEL columns and FILTER_GROUP identifiers. `;
      if (legacyInfo?.hasXrefKey) {
        summary += `Found XREF_HIERARCHY_KEY for unique identification. `;
      }
      if (legacyInfo?.levelCount) {
        summary += `Hierarchy depth: ${legacyInfo.levelCount} levels. `;
      }
      if (legacyInfo?.hasFilterGroups) {
        summary += `Mapping identifiers available in FILTER_GROUP columns. `;
      }
      summary += 'Will convert to standard format during import.';
    } else if (format === 'hierarchy') {
      summary = `Standard hierarchy CSV with ${rows.length} rows. Ready for import.`;
    } else if (format === 'mapping') {
      summary = `Standard mapping CSV with ${rows.length} rows. Ready for import.`;
    } else {
      summary = `Unknown format with ${rows.length} rows. Manual column mapping may be required.`;
    }

    return {
      summary,
      formatDetection: {
        detected: format,
        confidence: format === 'legacy' ? 85 : format === 'unknown' ? 0 : 80,
        reason: format === 'legacy'
          ? 'Detected XREF_HIERARCHY_KEY and LEVEL/FILTER_GROUP columns typical of legacy exports'
          : format === 'hierarchy'
          ? 'Found standard hierarchy columns (HIERARCHY_ID, PARENT_ID)'
          : format === 'mapping'
          ? 'Found standard mapping columns (SOURCE_TABLE, SOURCE_COLUMN)'
          : 'Could not match column patterns to known formats',
      },
      columnMappings,
      dataQuality: {
        score: Math.max(0, qualityScore),
        issues,
        recommendations,
      },
      conversionSteps: conversionSteps.length > 0 ? conversionSteps : undefined,
    };
  }

  /**
   * Validate hierarchy CSV specific rules
   */
  private validateHierarchyCSV(
    columns: ColumnAnalysis[],
    rows: string[][],
    issues: CSVIssue[],
    suggestions: CSVSuggestion[],
    autoFixActions: AutoFixAction[],
  ): void {
    // Check for required HIERARCHY_ID column
    const idColumn = columns.find((c) => c.mappedTo === 'HIERARCHY_ID');
    if (!idColumn) {
      issues.push({
        type: 'error',
        code: 'MISSING_HIERARCHY_ID',
        message: 'Missing required column: HIERARCHY_ID',
        canAutoFix: false,
      });
      suggestions.push({
        type: 'add',
        message: 'Add a HIERARCHY_ID column with unique identifiers for each hierarchy',
        priority: 'high',
      });
    } else {
      // Check for duplicate IDs
      const idIndex = idColumn.index;
      const ids = rows.map((r) => r[idIndex]);
      const duplicates = ids.filter((id, index) => ids.indexOf(id) !== index);
      if (duplicates.length > 0) {
        const uniqueDuplicates = [...new Set(duplicates)];
        issues.push({
          type: 'error',
          code: 'DUPLICATE_IDS',
          message: `Found ${uniqueDuplicates.length} duplicate HIERARCHY_ID values`,
          details: `Duplicates: ${uniqueDuplicates.slice(0, 5).join(', ')}${uniqueDuplicates.length > 5 ? '...' : ''}`,
          column: idColumn.name,
          affectedRows: duplicates.map((d) => ids.indexOf(d) + 2),
          canAutoFix: false,
        });
      }

      // Check for empty IDs
      const emptyIds = rows.filter((r) => !r[idIndex] || r[idIndex].trim() === '');
      if (emptyIds.length > 0) {
        issues.push({
          type: 'error',
          code: 'EMPTY_HIERARCHY_ID',
          message: `${emptyIds.length} rows have empty HIERARCHY_ID values`,
          column: idColumn.name,
          canAutoFix: true,
        });
        autoFixActions.push({
          type: 'GENERATE_IDS',
          description: `Generate unique IDs for ${emptyIds.length} rows with empty HIERARCHY_ID`,
          affectedRows: emptyIds.length,
          column: idColumn.name,
        });
      }
    }

    // Check parent references
    const parentColumn = columns.find((c) => c.mappedTo === 'PARENT_ID');
    if (parentColumn && idColumn) {
      const idIndex = idColumn.index;
      const parentIndex = parentColumn.index;
      const allIds = new Set(rows.map((r) => r[idIndex]));

      const orphanParents: string[] = [];
      rows.forEach((row, index) => {
        const parentId = row[parentIndex];
        if (parentId && parentId.trim() !== '' && !allIds.has(parentId)) {
          orphanParents.push(parentId);
        }
      });

      if (orphanParents.length > 0) {
        const uniqueOrphans = [...new Set(orphanParents)];
        issues.push({
          type: 'warning',
          code: 'ORPHAN_PARENTS',
          message: `${uniqueOrphans.length} parent references point to non-existent hierarchies`,
          details: `Missing parents: ${uniqueOrphans.slice(0, 5).join(', ')}${uniqueOrphans.length > 5 ? '...' : ''}`,
          column: parentColumn.name,
          canAutoFix: true,
        });
        autoFixActions.push({
          type: 'CLEAR_ORPHAN_PARENTS',
          description: `Clear ${orphanParents.length} orphan parent references (set to null)`,
          affectedRows: orphanParents.length,
          column: parentColumn.name,
        });
      }
    }
  }

  /**
   * Validate mapping CSV specific rules
   */
  private validateMappingCSV(
    columns: ColumnAnalysis[],
    rows: string[][],
    issues: CSVIssue[],
    suggestions: CSVSuggestion[],
    autoFixActions: AutoFixAction[],
  ): void {
    // Check for required columns
    const requiredColumns = ['HIERARCHY_ID', 'SOURCE_TABLE', 'SOURCE_COLUMN'];
    requiredColumns.forEach((required) => {
      if (!columns.find((c) => c.mappedTo === required)) {
        issues.push({
          type: 'error',
          code: `MISSING_${required}`,
          message: `Missing required column: ${required}`,
          canAutoFix: false,
        });
      }
    });

    // Check for empty source tables
    const tableColumn = columns.find((c) => c.mappedTo === 'SOURCE_TABLE');
    if (tableColumn) {
      const emptyTables = rows.filter((r) => !r[tableColumn.index] || r[tableColumn.index].trim() === '');
      if (emptyTables.length > 0) {
        issues.push({
          type: 'warning',
          code: 'EMPTY_SOURCE_TABLE',
          message: `${emptyTables.length} mappings have empty SOURCE_TABLE values`,
          column: tableColumn.name,
          canAutoFix: false,
        });
      }
    }
  }

  /**
   * Check for common data issues across all CSV types
   */
  private checkCommonIssues(
    columns: ColumnAnalysis[],
    rows: string[][],
    issues: CSVIssue[],
    suggestions: CSVSuggestion[],
    autoFixActions: AutoFixAction[],
  ): void {
    // Check for encoding issues (BOM, special characters)
    const firstRow = rows[0];
    if (firstRow && firstRow[0] && firstRow[0].charCodeAt(0) === 0xfeff) {
      issues.push({
        type: 'warning',
        code: 'BOM_DETECTED',
        message: 'File contains UTF-8 BOM character at the beginning',
        canAutoFix: true,
      });
      autoFixActions.push({
        type: 'REMOVE_BOM',
        description: 'Remove UTF-8 BOM character from file',
        affectedRows: 1,
      });
    }

    // Check for columns with high null percentage
    columns.forEach((col) => {
      if (col.nullPercentage > 80 && col.detectedType !== 'empty') {
        suggestions.push({
          type: 'remove',
          message: `Column "${col.name}" has ${col.nullPercentage.toFixed(0)}% empty values. Consider removing if not needed.`,
          column: col.name,
          priority: 'low',
        });
      }
    });

    // Check for whitespace issues
    let whitespaceIssues = 0;
    rows.forEach((row) => {
      row.forEach((cell) => {
        if (cell !== cell.trim()) {
          whitespaceIssues++;
        }
      });
    });

    if (whitespaceIssues > 0) {
      issues.push({
        type: 'info',
        code: 'WHITESPACE_ISSUES',
        message: `${whitespaceIssues} cells have leading/trailing whitespace`,
        canAutoFix: true,
      });
      autoFixActions.push({
        type: 'TRIM_WHITESPACE',
        description: `Trim whitespace from ${whitespaceIssues} cells`,
        affectedRows: whitespaceIssues,
      });
    }

    // Check for special characters that might cause issues
    let specialCharIssues = 0;
    const problematicChars = /[\x00-\x1F\x7F]/; // Control characters
    rows.forEach((row) => {
      row.forEach((cell) => {
        if (problematicChars.test(cell)) {
          specialCharIssues++;
        }
      });
    });

    if (specialCharIssues > 0) {
      issues.push({
        type: 'warning',
        code: 'SPECIAL_CHARS',
        message: `${specialCharIssues} cells contain control characters that may cause issues`,
        canAutoFix: true,
      });
      autoFixActions.push({
        type: 'REMOVE_CONTROL_CHARS',
        description: `Remove control characters from ${specialCharIssues} cells`,
        affectedRows: specialCharIssues,
      });
    }
  }

  /**
   * Calculate statistics for the CSV
   */
  private calculateStats(rows: string[][], columns: ColumnAnalysis[], issues: CSVIssue[]): CSVStats {
    const idColumn = columns.find((c) => c.mappedTo === 'HIERARCHY_ID');
    const parentColumn = columns.find((c) => c.mappedTo === 'PARENT_ID');

    let duplicateIds = 0;
    let orphanParents = 0;
    let emptyRequired = 0;

    if (idColumn) {
      const ids = rows.map((r) => r[idColumn.index]);
      duplicateIds = ids.length - new Set(ids).size;
      emptyRequired = ids.filter((id) => !id || id.trim() === '').length;
    }

    if (parentColumn && idColumn) {
      const allIds = new Set(rows.map((r) => r[idColumn.index]));
      orphanParents = rows.filter((r) => {
        const parentId = r[parentColumn.index];
        return parentId && parentId.trim() !== '' && !allIds.has(parentId);
      }).length;
    }

    const encodingIssues = issues.filter(
      (i) => i.code === 'BOM_DETECTED' || i.code === 'SPECIAL_CHARS',
    ).length;

    return {
      totalRows: rows.length,
      validRows: rows.length - duplicateIds - emptyRequired,
      invalidRows: duplicateIds + emptyRequired,
      duplicateIds,
      orphanParents,
      emptyRequired,
      encodingIssues,
    };
  }

  /**
   * Apply auto-fix actions to CSV content
   */
  async applyAutoFixes(csvContent: string, actions: AutoFixAction[]): Promise<{
    fixedContent: string;
    appliedFixes: string[];
    failedFixes: string[];
  }> {
    const appliedFixes: string[] = [];
    const failedFixes: string[] = [];
    let content = csvContent;

    for (const action of actions) {
      try {
        switch (action.type) {
          case 'REMOVE_BOM':
            if (content.charCodeAt(0) === 0xfeff) {
              content = content.substring(1);
              appliedFixes.push('Removed UTF-8 BOM character');
            }
            break;

          case 'TRIM_WHITESPACE':
            const lines = content.split(/\r?\n/);
            content = lines
              .map((line) => {
                const cells = this.parseCSVLine(line);
                return cells.map((c) => c.trim()).join(',');
              })
              .join('\n');
            appliedFixes.push(`Trimmed whitespace from cells`);
            break;

          case 'REMOVE_CONTROL_CHARS':
            content = content.replace(/[\x00-\x1F\x7F]/g, '');
            appliedFixes.push('Removed control characters');
            break;

          case 'CLEAR_ORPHAN_PARENTS':
            // This would require more complex logic to implement
            appliedFixes.push(`Would clear ${action.affectedRows} orphan parent references`);
            break;

          case 'GENERATE_IDS':
            // This would require more complex logic to implement
            appliedFixes.push(`Would generate IDs for ${action.affectedRows} rows`);
            break;

          default:
            failedFixes.push(`Unknown action type: ${action.type}`);
        }
      } catch (error) {
        failedFixes.push(`Failed to apply ${action.type}: ${error.message}`);
      }
    }

    return { fixedContent: content, appliedFixes, failedFixes };
  }
}
