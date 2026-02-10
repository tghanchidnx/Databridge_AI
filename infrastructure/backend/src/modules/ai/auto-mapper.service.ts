/**
 * Auto-Mapper Service
 * Automatically maps Excel columns to hierarchy fields using AI and pattern matching
 */
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

export interface ExcelColumn {
  name: string;
  index: number;
  dataType: 'string' | 'number' | 'date' | 'boolean' | 'unknown';
  sampleValues: any[];
  uniqueCount: number;
  nullCount: number;
}

export interface ExcelParseResult {
  fileName: string;
  sheets: Array<{
    name: string;
    columns: ExcelColumn[];
    rowCount: number;
  }>;
}

export interface HierarchyField {
  name: string;
  type: 'id' | 'name' | 'parent' | 'description' | 'level' | 'sort' | 'flag' | 'mapping' | 'system_type';
  required: boolean;
  pattern?: RegExp;
}

export interface ColumnMapping {
  excelColumn: string;
  hierarchyField: string;
  confidence: number;
  reasoning: string;
}

export interface AutoMappingResult {
  sheetName: string;
  columnMappings: ColumnMapping[];
  unmappedColumns: string[];
  unmappedFields: string[];
  detectedFormat: 'standard' | 'legacy' | 'custom' | 'unknown';
  systemTypeDetection?: {
    hasActuals: boolean;
    hasBudget: boolean;
    hasForecast: boolean;
    detectedColumns: Record<string, string>;
  };
  warnings: string[];
  suggestions: string[];
}

@Injectable()
export class AutoMapperService {
  private readonly logger = new Logger(AutoMapperService.name);
  private readonly anthropicApiKey: string;
  private readonly openaiApiKey: string;

  // Standard hierarchy fields
  private readonly hierarchyFields: HierarchyField[] = [
    { name: 'HIERARCHY_ID', type: 'id', required: true, pattern: /^(hierarchy[_\s]?id|id|code|key|unique[_\s]?id)$/i },
    { name: 'HIERARCHY_NAME', type: 'name', required: true, pattern: /^(hierarchy[_\s]?name|name|description|title|label)$/i },
    { name: 'PARENT_ID', type: 'parent', required: false, pattern: /^(parent[_\s]?id|parent|parent[_\s]?code|parent[_\s]?key)$/i },
    { name: 'DESCRIPTION', type: 'description', required: false, pattern: /^(description|desc|details|notes)$/i },
    { name: 'LEVEL_1', type: 'level', required: false, pattern: /^(level[_\s]?1|l1|tier[_\s]?1)$/i },
    { name: 'LEVEL_2', type: 'level', required: false, pattern: /^(level[_\s]?2|l2|tier[_\s]?2)$/i },
    { name: 'LEVEL_3', type: 'level', required: false, pattern: /^(level[_\s]?3|l3|tier[_\s]?3)$/i },
    { name: 'LEVEL_4', type: 'level', required: false, pattern: /^(level[_\s]?4|l4|tier[_\s]?4)$/i },
    { name: 'LEVEL_5', type: 'level', required: false, pattern: /^(level[_\s]?5|l5|tier[_\s]?5)$/i },
    { name: 'SORT_ORDER', type: 'sort', required: false, pattern: /^(sort[_\s]?order|sort|order|sequence|seq)$/i },
    { name: 'INCLUDE_FLAG', type: 'flag', required: false, pattern: /^(include[_\s]?flag|include|incl)$/i },
    { name: 'EXCLUDE_FLAG', type: 'flag', required: false, pattern: /^(exclude[_\s]?flag|exclude|excl)$/i },
    { name: 'ACTIVE_FLAG', type: 'flag', required: false, pattern: /^(active[_\s]?flag|active|is[_\s]?active|status)$/i },
  ];

  // Mapping-specific fields
  private readonly mappingFields: HierarchyField[] = [
    { name: 'SOURCE_DATABASE', type: 'mapping', required: false, pattern: /^(source[_\s]?database|database|db)$/i },
    { name: 'SOURCE_SCHEMA', type: 'mapping', required: false, pattern: /^(source[_\s]?schema|schema)$/i },
    { name: 'SOURCE_TABLE', type: 'mapping', required: false, pattern: /^(source[_\s]?table|table|tbl)$/i },
    { name: 'SOURCE_COLUMN', type: 'mapping', required: false, pattern: /^(source[_\s]?column|column|col|field)$/i },
    { name: 'SYSTEM_TYPE', type: 'system_type', required: false, pattern: /^(system[_\s]?type|system|data[_\s]?type)$/i },
    { name: 'JOIN_TYPE', type: 'mapping', required: false, pattern: /^(join[_\s]?type|join)$/i },
    { name: 'DIMENSION_ROLE', type: 'mapping', required: false, pattern: /^(dimension[_\s]?role|role|dim[_\s]?role)$/i },
  ];

  // System type detection patterns
  private readonly systemTypePatterns = {
    ACTUALS: /^(actual|actuals|act|real|current)$/i,
    BUDGET: /^(budget|bud|plan|planned|target)$/i,
    FORECAST: /^(forecast|fcst|fcast|projected|projection)$/i,
    PRIOR_YEAR: /^(prior[_\s]?year|py|last[_\s]?year|ly|prior)$/i,
  };

  constructor(private configService: ConfigService) {
    this.anthropicApiKey = this.configService.get<string>('ANTHROPIC_API_KEY') || '';
    this.openaiApiKey = this.configService.get<string>('OPENAI_API_KEY') || '';
  }

  /**
   * Auto-map Excel columns to hierarchy fields
   */
  async autoMapExcelToHierarchy(
    excelData: ExcelParseResult,
    sheetName?: string,
  ): Promise<AutoMappingResult> {
    const sheet = sheetName
      ? excelData.sheets.find(s => s.name === sheetName)
      : excelData.sheets[0];

    if (!sheet) {
      throw new Error(`Sheet "${sheetName || 'default'}" not found`);
    }

    this.logger.log(`Auto-mapping sheet: ${sheet.name} with ${sheet.columns.length} columns`);

    // First, try pattern-based mapping
    const patternMappings = this.mapByPatterns(sheet.columns);

    // Detect format
    const detectedFormat = this.detectFormat(sheet.columns, patternMappings);

    // Try AI-based mapping for unmapped columns
    let aiMappings: ColumnMapping[] = [];
    const unmappedColumns = sheet.columns
      .map(c => c.name)
      .filter(name => !patternMappings.some(m => m.excelColumn === name));

    if (unmappedColumns.length > 0 && (this.anthropicApiKey || this.openaiApiKey)) {
      aiMappings = await this.mapWithAI(
        sheet.columns.filter(c => unmappedColumns.includes(c.name)),
        [...this.hierarchyFields, ...this.mappingFields],
      );
    }

    // Combine mappings
    const allMappings = [...patternMappings, ...aiMappings];

    // Deduplicate - keep highest confidence for each field
    const uniqueMappings = this.deduplicateMappings(allMappings);

    // Find still unmapped columns
    const stillUnmapped = sheet.columns
      .map(c => c.name)
      .filter(name => !uniqueMappings.some(m => m.excelColumn === name));

    // Find unmapped required fields
    const mappedFields = new Set(uniqueMappings.map(m => m.hierarchyField));
    const unmappedFields = [...this.hierarchyFields, ...this.mappingFields]
      .filter(f => f.required && !mappedFields.has(f.name))
      .map(f => f.name);

    // Detect system types in data
    const systemTypeDetection = this.detectSystemTypes(sheet.columns);

    // Generate warnings and suggestions
    const warnings: string[] = [];
    const suggestions: string[] = [];

    if (unmappedFields.length > 0) {
      warnings.push(`Required fields not mapped: ${unmappedFields.join(', ')}`);
    }

    if (stillUnmapped.length > 0) {
      suggestions.push(`Consider mapping these columns: ${stillUnmapped.slice(0, 5).join(', ')}`);
    }

    if (!systemTypeDetection.hasActuals && !systemTypeDetection.hasBudget) {
      suggestions.push('No system type columns detected. Consider adding ACTUALS or BUDGET columns.');
    }

    return {
      sheetName: sheet.name,
      columnMappings: uniqueMappings,
      unmappedColumns: stillUnmapped,
      unmappedFields,
      detectedFormat,
      systemTypeDetection,
      warnings,
      suggestions,
    };
  }

  /**
   * Map columns using pattern matching
   */
  private mapByPatterns(columns: ExcelColumn[]): ColumnMapping[] {
    const mappings: ColumnMapping[] = [];
    const allFields = [...this.hierarchyFields, ...this.mappingFields];

    for (const column of columns) {
      const normalizedName = column.name.toLowerCase().replace(/[^a-z0-9]/g, '_');

      for (const field of allFields) {
        if (field.pattern && field.pattern.test(column.name)) {
          mappings.push({
            excelColumn: column.name,
            hierarchyField: field.name,
            confidence: 0.95,
            reasoning: 'Exact pattern match',
          });
          break;
        }
      }

      // Try fuzzy matching if no exact pattern match
      if (!mappings.some(m => m.excelColumn === column.name)) {
        const bestMatch = this.findBestFuzzyMatch(normalizedName, allFields);
        if (bestMatch && bestMatch.confidence > 0.6) {
          mappings.push({
            excelColumn: column.name,
            hierarchyField: bestMatch.field,
            confidence: bestMatch.confidence,
            reasoning: 'Fuzzy name match',
          });
        }
      }
    }

    return mappings;
  }

  /**
   * Find best fuzzy match for a column name
   */
  private findBestFuzzyMatch(
    columnName: string,
    fields: HierarchyField[],
  ): { field: string; confidence: number } | null {
    let bestMatch: { field: string; confidence: number } | null = null;

    for (const field of fields) {
      const fieldName = field.name.toLowerCase().replace(/[^a-z0-9]/g, '_');
      const similarity = this.calculateSimilarity(columnName, fieldName);

      if (similarity > 0.6 && (!bestMatch || similarity > bestMatch.confidence)) {
        bestMatch = { field: field.name, confidence: similarity };
      }

      // Also check common abbreviations
      const abbreviations = this.getAbbreviations(field.name);
      for (const abbr of abbreviations) {
        const abbrSimilarity = this.calculateSimilarity(columnName, abbr);
        if (abbrSimilarity > 0.8 && (!bestMatch || abbrSimilarity > bestMatch.confidence)) {
          bestMatch = { field: field.name, confidence: abbrSimilarity };
        }
      }
    }

    return bestMatch;
  }

  /**
   * Get common abbreviations for a field name
   */
  private getAbbreviations(fieldName: string): string[] {
    const abbreviations: Record<string, string[]> = {
      HIERARCHY_ID: ['hier_id', 'h_id', 'hid'],
      HIERARCHY_NAME: ['hier_name', 'h_name', 'hname'],
      PARENT_ID: ['p_id', 'pid', 'parent'],
      DESCRIPTION: ['desc', 'dsc'],
      SOURCE_DATABASE: ['src_db', 'database', 'db'],
      SOURCE_SCHEMA: ['src_schema', 'schema'],
      SOURCE_TABLE: ['src_table', 'table', 'tbl'],
      SOURCE_COLUMN: ['src_col', 'column', 'col'],
      SORT_ORDER: ['sort', 'order', 'seq'],
      INCLUDE_FLAG: ['include', 'incl', 'inc'],
      EXCLUDE_FLAG: ['exclude', 'excl', 'exc'],
    };

    return abbreviations[fieldName] || [];
  }

  /**
   * Map columns using AI
   */
  private async mapWithAI(
    columns: ExcelColumn[],
    fields: HierarchyField[],
  ): Promise<ColumnMapping[]> {
    const prompt = this.buildAIMappingPrompt(columns, fields);

    try {
      let response: string;

      if (this.anthropicApiKey) {
        response = await this.callClaude(prompt);
      } else {
        response = await this.callOpenAI(prompt);
      }

      return this.parseAIMappingResponse(response);
    } catch (error) {
      this.logger.error('AI mapping failed', error);
      return [];
    }
  }

  /**
   * Build AI mapping prompt
   */
  private buildAIMappingPrompt(
    columns: ExcelColumn[],
    fields: HierarchyField[],
  ): string {
    const columnsInfo = columns.map(c => ({
      name: c.name,
      type: c.dataType,
      samples: c.sampleValues.slice(0, 3),
    }));

    const fieldsList = fields.map(f => `${f.name} (${f.type}, ${f.required ? 'required' : 'optional'})`);

    return `You are a data mapping expert. Map these Excel columns to hierarchy fields.

EXCEL COLUMNS:
${JSON.stringify(columnsInfo, null, 2)}

AVAILABLE HIERARCHY FIELDS:
${fieldsList.join('\n')}

Map each Excel column to the most appropriate hierarchy field based on:
1. Column name similarity
2. Data type compatibility
3. Sample values context

Respond with JSON array:
[
  {
    "excelColumn": "...",
    "hierarchyField": "...",
    "confidence": 0.0-1.0,
    "reasoning": "..."
  }
]

Only include mappings you're confident about (> 0.5 confidence).`;
  }

  /**
   * Parse AI mapping response
   */
  private parseAIMappingResponse(response: string): ColumnMapping[] {
    try {
      const jsonMatch = response.match(/\[[\s\S]*\]/);
      if (!jsonMatch) return [];

      const parsed = JSON.parse(jsonMatch[0]);
      return parsed.map((m: any) => ({
        excelColumn: m.excelColumn,
        hierarchyField: m.hierarchyField,
        confidence: m.confidence || 0.7,
        reasoning: m.reasoning || 'AI suggestion',
      }));
    } catch (error) {
      this.logger.error('Failed to parse AI mapping response', error);
      return [];
    }
  }

  /**
   * Detect Excel format
   */
  private detectFormat(
    columns: ExcelColumn[],
    mappings: ColumnMapping[],
  ): 'standard' | 'legacy' | 'custom' | 'unknown' {
    const columnNames = columns.map(c => c.name.toUpperCase());

    // Standard format: has HIERARCHY_ID, HIERARCHY_NAME, LEVEL_* columns
    const hasStandardFields =
      columnNames.some(c => c.includes('HIERARCHY_ID') || c.includes('HIER_ID')) &&
      columnNames.some(c => c.includes('HIERARCHY_NAME') || c.includes('HIER_NAME'));

    if (hasStandardFields) {
      return 'standard';
    }

    // Legacy format: has CODE, DESCRIPTION, L1/L2/L3 pattern
    const hasLegacyFields =
      columnNames.some(c => c === 'CODE' || c === 'ID') &&
      columnNames.some(c => c === 'DESCRIPTION' || c === 'NAME') &&
      columnNames.some(c => /^L\d$/.test(c) || c.includes('LEVEL'));

    if (hasLegacyFields) {
      return 'legacy';
    }

    // Custom format: has some recognized fields
    if (mappings.length >= 2) {
      return 'custom';
    }

    return 'unknown';
  }

  /**
   * Detect system types in columns
   */
  private detectSystemTypes(columns: ExcelColumn[]): AutoMappingResult['systemTypeDetection'] {
    const result = {
      hasActuals: false,
      hasBudget: false,
      hasForecast: false,
      detectedColumns: {} as Record<string, string>,
    };

    for (const column of columns) {
      // Check column name for system type indicators
      for (const [systemType, pattern] of Object.entries(this.systemTypePatterns)) {
        if (pattern.test(column.name)) {
          result.detectedColumns[column.name] = systemType;
          if (systemType === 'ACTUALS') result.hasActuals = true;
          if (systemType === 'BUDGET') result.hasBudget = true;
          if (systemType === 'FORECAST') result.hasForecast = true;
        }
      }

      // Check sample values for system type indicators
      if (column.sampleValues.some(v => typeof v === 'string')) {
        for (const value of column.sampleValues) {
          if (typeof value !== 'string') continue;

          for (const [systemType, pattern] of Object.entries(this.systemTypePatterns)) {
            if (pattern.test(value)) {
              result.detectedColumns[column.name] = systemType;
              if (systemType === 'ACTUALS') result.hasActuals = true;
              if (systemType === 'BUDGET') result.hasBudget = true;
              if (systemType === 'FORECAST') result.hasForecast = true;
            }
          }
        }
      }
    }

    return result;
  }

  /**
   * Deduplicate mappings, keeping highest confidence
   */
  private deduplicateMappings(mappings: ColumnMapping[]): ColumnMapping[] {
    const byField = new Map<string, ColumnMapping>();
    const byColumn = new Map<string, ColumnMapping>();

    for (const mapping of mappings) {
      // Check field deduplication
      const existingByField = byField.get(mapping.hierarchyField);
      if (!existingByField || mapping.confidence > existingByField.confidence) {
        byField.set(mapping.hierarchyField, mapping);
      }

      // Check column deduplication
      const existingByColumn = byColumn.get(mapping.excelColumn);
      if (!existingByColumn || mapping.confidence > existingByColumn.confidence) {
        byColumn.set(mapping.excelColumn, mapping);
      }
    }

    // Return unique mappings where both field and column are unique
    const result: ColumnMapping[] = [];
    const usedFields = new Set<string>();
    const usedColumns = new Set<string>();

    // Sort by confidence descending
    const sortedMappings = mappings.sort((a, b) => b.confidence - a.confidence);

    for (const mapping of sortedMappings) {
      if (!usedFields.has(mapping.hierarchyField) && !usedColumns.has(mapping.excelColumn)) {
        result.push(mapping);
        usedFields.add(mapping.hierarchyField);
        usedColumns.add(mapping.excelColumn);
      }
    }

    return result;
  }

  /**
   * Calculate string similarity (Levenshtein-based)
   */
  private calculateSimilarity(a: string, b: string): number {
    if (a === b) return 1;
    if (a.length === 0 || b.length === 0) return 0;

    const matrix: number[][] = [];

    for (let i = 0; i <= b.length; i++) {
      matrix[i] = [i];
    }

    for (let j = 0; j <= a.length; j++) {
      matrix[0][j] = j;
    }

    for (let i = 1; i <= b.length; i++) {
      for (let j = 1; j <= a.length; j++) {
        if (b.charAt(i - 1) === a.charAt(j - 1)) {
          matrix[i][j] = matrix[i - 1][j - 1];
        } else {
          matrix[i][j] = Math.min(
            matrix[i - 1][j - 1] + 1,
            matrix[i][j - 1] + 1,
            matrix[i - 1][j] + 1,
          );
        }
      }
    }

    const maxLength = Math.max(a.length, b.length);
    return 1 - matrix[b.length][a.length] / maxLength;
  }

  /**
   * Call Claude API
   */
  private async callClaude(prompt: string): Promise<string> {
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': this.anthropicApiKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-3-haiku-20240307',
        max_tokens: 1024,
        messages: [{ role: 'user', content: prompt }],
      }),
    });

    if (!response.ok) {
      throw new Error(`Claude API error: ${response.statusText}`);
    }

    const data = await response.json();
    return data.content[0].text;
  }

  /**
   * Call OpenAI API
   */
  private async callOpenAI(prompt: string): Promise<string> {
    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${this.openaiApiKey}`,
      },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [
          { role: 'system', content: 'You are a data mapping expert.' },
          { role: 'user', content: prompt },
        ],
        temperature: 0.2,
      }),
    });

    if (!response.ok) {
      throw new Error(`OpenAI API error: ${response.statusText}`);
    }

    const data = await response.json();
    return data.choices[0].message.content;
  }
}
