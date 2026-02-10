/**
 * Natural Language Query Service
 * Translates natural language queries to SQL for FP&A analysis
 */
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

export interface SQLTranslation {
  sql: string;
  explanation: string;
  tables: string[];
  columns: string[];
  filters: string[];
  aggregations: string[];
  confidence: number;
  warnings?: string[];
}

export interface ProjectContext {
  projectId: string;
  hierarchies: Array<{
    id: string;
    name: string;
    hierarchyId: string;
    level: number;
    mappings: Array<{
      sourceDatabase: string;
      sourceSchema: string;
      sourceTable: string;
      sourceColumn: string;
      systemType?: string;
    }>;
  }>;
  availableSystems: string[];
  factTableName?: string;
}

export interface QueryIntent {
  type: 'comparison' | 'trend' | 'detail' | 'aggregation' | 'filter';
  timeRange?: {
    start?: string;
    end?: string;
    period?: 'YTD' | 'QTD' | 'MTD' | 'YoY' | 'QoQ' | 'MoM';
  };
  metrics: string[];
  dimensions: string[];
  filters: Array<{
    field: string;
    operator: string;
    value: string;
  }>;
  systems: string[];
  sortOrder?: 'asc' | 'desc';
  limit?: number;
}

@Injectable()
export class NLQueryService {
  private readonly logger = new Logger(NLQueryService.name);
  private readonly anthropicApiKey: string;
  private readonly openaiApiKey: string;

  constructor(private configService: ConfigService) {
    this.anthropicApiKey = this.configService.get<string>('ANTHROPIC_API_KEY') || '';
    this.openaiApiKey = this.configService.get<string>('OPENAI_API_KEY') || '';
  }

  /**
   * Translate natural language query to SQL
   */
  async translateToSQL(
    query: string,
    context: ProjectContext,
  ): Promise<SQLTranslation> {
    this.logger.log(`Translating query: "${query}"`);

    try {
      // First, parse the intent from the query
      const intent = await this.parseQueryIntent(query, context);

      // Generate SQL based on intent and context
      const sql = this.generateSQLFromIntent(intent, context);

      return sql;
    } catch (error) {
      this.logger.error('Failed to translate query', error);
      throw error;
    }
  }

  /**
   * Parse query intent using AI or pattern matching
   */
  private async parseQueryIntent(
    query: string,
    context: ProjectContext,
  ): Promise<QueryIntent> {
    // Try AI-based parsing first
    if (this.anthropicApiKey || this.openaiApiKey) {
      return this.parseIntentWithAI(query, context);
    }

    // Fall back to pattern-based parsing
    return this.parseIntentWithPatterns(query, context);
  }

  /**
   * Parse intent using AI
   */
  private async parseIntentWithAI(
    query: string,
    context: ProjectContext,
  ): Promise<QueryIntent> {
    const prompt = this.buildIntentParsingPrompt(query, context);

    try {
      let response: string;

      if (this.anthropicApiKey) {
        response = await this.callClaude(prompt);
      } else {
        response = await this.callOpenAI(prompt);
      }

      return this.parseIntentResponse(response);
    } catch (error) {
      this.logger.error('AI intent parsing failed, falling back to patterns', error);
      return this.parseIntentWithPatterns(query, context);
    }
  }

  /**
   * Build prompt for intent parsing
   */
  private buildIntentParsingPrompt(query: string, context: ProjectContext): string {
    const hierarchyList = context.hierarchies
      .map(h => `- ${h.name} (${h.hierarchyId})`)
      .slice(0, 20)
      .join('\n');

    const systemTypes = context.availableSystems.join(', ') || 'ACTUALS, BUDGET, FORECAST';

    return `You are a financial query parser. Extract the intent from this natural language query.

USER QUERY: "${query}"

AVAILABLE HIERARCHIES:
${hierarchyList}

AVAILABLE SYSTEMS: ${systemTypes}

Parse the query and respond with JSON:
{
  "type": "comparison" | "trend" | "detail" | "aggregation" | "filter",
  "timeRange": {
    "period": "YTD" | "QTD" | "MTD" | "YoY" | "QoQ" | "MoM" | null,
    "start": "YYYY-MM-DD" or null,
    "end": "YYYY-MM-DD" or null
  },
  "metrics": ["list of metrics/values to retrieve"],
  "dimensions": ["list of dimensions to group by"],
  "filters": [{"field": "...", "operator": "=|>|<|>=|<=|LIKE", "value": "..."}],
  "systems": ["ACTUALS", "BUDGET", etc.],
  "sortOrder": "asc" | "desc" | null,
  "limit": number or null
}

Examples:
- "Show Q3 actuals vs budget" -> type: comparison, systems: [ACTUALS, BUDGET], timeRange: {period: null, start: "2024-07-01", end: "2024-09-30"}
- "YoY variance for Operating Expenses" -> type: comparison, systems: [ACTUALS], timeRange: {period: YoY}, metrics: [Operating Expenses]
- "Top 10 accounts with highest budget variance" -> type: filter, sortOrder: desc, limit: 10`;
  }

  /**
   * Parse intent using pattern matching
   */
  private parseIntentWithPatterns(
    query: string,
    context: ProjectContext,
  ): QueryIntent {
    const lowerQuery = query.toLowerCase();
    const intent: QueryIntent = {
      type: 'aggregation',
      metrics: [],
      dimensions: [],
      filters: [],
      systems: [],
    };

    // Detect comparison queries
    if (lowerQuery.includes(' vs ') || lowerQuery.includes('versus') || lowerQuery.includes('compared to')) {
      intent.type = 'comparison';
    }

    // Detect trend queries
    if (lowerQuery.includes('trend') || lowerQuery.includes('over time') || lowerQuery.includes('monthly')) {
      intent.type = 'trend';
    }

    // Detect variance queries
    if (lowerQuery.includes('variance')) {
      intent.type = 'comparison';
    }

    // Detect time periods
    if (lowerQuery.includes('yoy') || lowerQuery.includes('year over year')) {
      intent.timeRange = { period: 'YoY' };
    } else if (lowerQuery.includes('qoq') || lowerQuery.includes('quarter over quarter')) {
      intent.timeRange = { period: 'QoQ' };
    } else if (lowerQuery.includes('mom') || lowerQuery.includes('month over month')) {
      intent.timeRange = { period: 'MoM' };
    } else if (lowerQuery.includes('ytd') || lowerQuery.includes('year to date')) {
      intent.timeRange = { period: 'YTD' };
    } else if (lowerQuery.includes('qtd') || lowerQuery.includes('quarter to date')) {
      intent.timeRange = { period: 'QTD' };
    } else if (lowerQuery.includes('mtd') || lowerQuery.includes('month to date')) {
      intent.timeRange = { period: 'MTD' };
    }

    // Detect quarter references
    const quarterMatch = lowerQuery.match(/q([1-4])/i);
    if (quarterMatch) {
      const quarter = parseInt(quarterMatch[1]);
      const year = new Date().getFullYear();
      const startMonth = (quarter - 1) * 3;
      intent.timeRange = {
        start: `${year}-${String(startMonth + 1).padStart(2, '0')}-01`,
        end: `${year}-${String(startMonth + 3).padStart(2, '0')}-${quarter === 1 || quarter === 4 ? '31' : '30'}`,
      };
    }

    // Detect systems
    if (lowerQuery.includes('actual')) intent.systems.push('ACTUALS');
    if (lowerQuery.includes('budget')) intent.systems.push('BUDGET');
    if (lowerQuery.includes('forecast')) intent.systems.push('FORECAST');
    if (lowerQuery.includes('prior year')) intent.systems.push('PRIOR_YEAR');

    // Default to actuals if no system specified
    if (intent.systems.length === 0) {
      intent.systems.push('ACTUALS');
    }

    // Extract metrics from hierarchy names
    for (const hierarchy of context.hierarchies) {
      if (lowerQuery.includes(hierarchy.name.toLowerCase())) {
        intent.metrics.push(hierarchy.name);
      }
    }

    // Detect sorting and limits
    const topMatch = lowerQuery.match(/top (\d+)/i);
    if (topMatch) {
      intent.limit = parseInt(topMatch[1]);
      intent.sortOrder = 'desc';
    }

    const bottomMatch = lowerQuery.match(/bottom (\d+)/i);
    if (bottomMatch) {
      intent.limit = parseInt(bottomMatch[1]);
      intent.sortOrder = 'asc';
    }

    // Detect filter conditions
    const greaterMatch = lowerQuery.match(/(greater than|more than|above|over|>) (\d+)/i);
    if (greaterMatch) {
      intent.filters.push({
        field: 'value',
        operator: '>',
        value: greaterMatch[2],
      });
    }

    const lessThanMatch = lowerQuery.match(/(less than|under|below|<) (\d+)/i);
    if (lessThanMatch) {
      intent.filters.push({
        field: 'value',
        operator: '<',
        value: lessThanMatch[2],
      });
    }

    return intent;
  }

  /**
   * Parse AI response into intent
   */
  private parseIntentResponse(response: string): QueryIntent {
    try {
      const jsonMatch = response.match(/\{[\s\S]*\}/);
      if (!jsonMatch) {
        throw new Error('No JSON found in response');
      }

      const parsed = JSON.parse(jsonMatch[0]);
      return {
        type: parsed.type || 'aggregation',
        timeRange: parsed.timeRange,
        metrics: parsed.metrics || [],
        dimensions: parsed.dimensions || [],
        filters: parsed.filters || [],
        systems: parsed.systems || ['ACTUALS'],
        sortOrder: parsed.sortOrder,
        limit: parsed.limit,
      };
    } catch (error) {
      this.logger.error('Failed to parse intent response', error);
      return {
        type: 'aggregation',
        metrics: [],
        dimensions: [],
        filters: [],
        systems: ['ACTUALS'],
      };
    }
  }

  /**
   * Generate SQL from parsed intent
   */
  private generateSQLFromIntent(
    intent: QueryIntent,
    context: ProjectContext,
  ): SQLTranslation {
    const warnings: string[] = [];
    const tables: string[] = [];
    const columns: string[] = [];
    const filters: string[] = [];
    const aggregations: string[] = [];

    // Build table reference
    const factTable = context.factTableName || 'fact_hierarchy_data';
    tables.push(factTable);

    // Build SELECT clause
    const selectParts: string[] = [];

    // Add dimension columns
    if (intent.dimensions.length > 0) {
      for (const dim of intent.dimensions) {
        selectParts.push(dim);
        columns.push(dim);
      }
    } else {
      selectParts.push('hierarchy_name');
      columns.push('hierarchy_name');
    }

    // Add metric columns based on systems
    for (const system of intent.systems) {
      const colName = `${system.toLowerCase()}_amount`;
      selectParts.push(colName);
      columns.push(colName);
    }

    // Add variance calculations if comparing systems
    if (intent.type === 'comparison' && intent.systems.length === 2) {
      const sys1 = intent.systems[0].toLowerCase();
      const sys2 = intent.systems[1].toLowerCase();
      selectParts.push(`(${sys1}_amount - ${sys2}_amount) AS variance_amount`);
      selectParts.push(`CASE WHEN ${sys2}_amount != 0 THEN ((${sys1}_amount - ${sys2}_amount) / ${sys2}_amount * 100) ELSE NULL END AS variance_percent`);
      aggregations.push('variance');
    }

    // Build WHERE clause
    const whereParts: string[] = [];

    // Add time filters
    if (intent.timeRange) {
      if (intent.timeRange.start) {
        whereParts.push(`period_date >= '${intent.timeRange.start}'`);
        filters.push(`period >= ${intent.timeRange.start}`);
      }
      if (intent.timeRange.end) {
        whereParts.push(`period_date <= '${intent.timeRange.end}'`);
        filters.push(`period <= ${intent.timeRange.end}`);
      }
      if (intent.timeRange.period) {
        // Add period-based filter (simplified)
        const periodFilter = this.getPeriodFilter(intent.timeRange.period);
        if (periodFilter) {
          whereParts.push(periodFilter);
          filters.push(`period = ${intent.timeRange.period}`);
        }
      }
    }

    // Add metric filters (hierarchy names)
    if (intent.metrics.length > 0) {
      const metricList = intent.metrics.map(m => `'${m}'`).join(', ');
      whereParts.push(`hierarchy_name IN (${metricList})`);
      filters.push(`hierarchy IN (${intent.metrics.join(', ')})`);
    }

    // Add user filters
    for (const filter of intent.filters) {
      whereParts.push(`${filter.field} ${filter.operator} ${filter.value}`);
      filters.push(`${filter.field} ${filter.operator} ${filter.value}`);
    }

    // Build GROUP BY
    const groupByParts = intent.dimensions.length > 0 ? intent.dimensions : ['hierarchy_name'];

    // Build ORDER BY
    let orderBy = '';
    if (intent.sortOrder) {
      const orderCol = intent.type === 'comparison' ? 'variance_amount' : `${intent.systems[0].toLowerCase()}_amount`;
      orderBy = `ORDER BY ${orderCol} ${intent.sortOrder.toUpperCase()}`;
    }

    // Build LIMIT
    let limitClause = '';
    if (intent.limit) {
      limitClause = `LIMIT ${intent.limit}`;
    }

    // Construct full SQL
    const sql = `SELECT
  ${selectParts.join(',\n  ')}
FROM ${factTable}
${whereParts.length > 0 ? `WHERE ${whereParts.join('\n  AND ')}` : ''}
GROUP BY ${groupByParts.join(', ')}
${orderBy}
${limitClause}`.trim();

    // Calculate confidence based on how well we understood the query
    let confidence = 0.7;
    if (intent.metrics.length > 0) confidence += 0.1;
    if (intent.systems.length > 0) confidence += 0.1;
    if (intent.timeRange) confidence += 0.1;

    // Generate explanation
    const explanation = this.generateExplanation(intent);

    return {
      sql,
      explanation,
      tables,
      columns,
      filters,
      aggregations,
      confidence: Math.min(confidence, 1),
      warnings: warnings.length > 0 ? warnings : undefined,
    };
  }

  /**
   * Get period-based filter
   */
  private getPeriodFilter(period: string): string {
    const now = new Date();
    const year = now.getFullYear();
    const month = now.getMonth() + 1;
    const quarter = Math.ceil(month / 3);

    switch (period) {
      case 'YTD':
        return `period_date >= '${year}-01-01' AND period_date <= CURRENT_DATE`;
      case 'QTD':
        const qStart = (quarter - 1) * 3 + 1;
        return `period_date >= '${year}-${String(qStart).padStart(2, '0')}-01' AND period_date <= CURRENT_DATE`;
      case 'MTD':
        return `period_date >= '${year}-${String(month).padStart(2, '0')}-01' AND period_date <= CURRENT_DATE`;
      case 'YoY':
        return `(YEAR(period_date) = ${year} OR YEAR(period_date) = ${year - 1})`;
      case 'QoQ':
        return `period_date >= DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH)`;
      case 'MoM':
        return `period_date >= DATE_SUB(CURRENT_DATE, INTERVAL 2 MONTH)`;
      default:
        return '';
    }
  }

  /**
   * Generate human-readable explanation
   */
  private generateExplanation(intent: QueryIntent): string {
    let explanation = 'This query will ';

    switch (intent.type) {
      case 'comparison':
        explanation += `compare ${intent.systems.join(' vs ')}`;
        break;
      case 'trend':
        explanation += 'show the trend';
        break;
      case 'aggregation':
        explanation += 'aggregate';
        break;
      case 'detail':
        explanation += 'show detailed data';
        break;
      case 'filter':
        explanation += 'filter and show';
        break;
    }

    if (intent.metrics.length > 0) {
      explanation += ` for ${intent.metrics.join(', ')}`;
    }

    if (intent.timeRange?.period) {
      explanation += ` (${intent.timeRange.period})`;
    } else if (intent.timeRange?.start && intent.timeRange?.end) {
      explanation += ` from ${intent.timeRange.start} to ${intent.timeRange.end}`;
    }

    if (intent.limit) {
      explanation += `, showing ${intent.sortOrder === 'desc' ? 'top' : 'bottom'} ${intent.limit} results`;
    }

    return explanation + '.';
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
          { role: 'system', content: 'You are a financial query parsing expert.' },
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
