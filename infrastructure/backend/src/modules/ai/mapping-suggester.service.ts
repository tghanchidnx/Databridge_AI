/**
 * AI-Powered Mapping Suggester Service
 * Suggests source mappings for hierarchy nodes using AI and pattern learning
 */
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

export interface MappingSuggestion {
  hierarchyId: string;
  suggestedMapping: {
    sourceDatabase: string;
    sourceSchema: string;
    sourceTable: string;
    sourceColumn: string;
    sourceUid?: string;
  };
  confidence: number;
  reasoning: string;
  source: 'ai' | 'pattern' | 'similar';
}

export interface HierarchyContext {
  id: string;
  name: string;
  hierarchyId: string;
  parentName?: string;
  level: number;
  existingMappings?: Array<{
    sourceDatabase: string;
    sourceTable: string;
    sourceColumn: string;
  }>;
}

export interface AvailableSource {
  database: string;
  schema: string;
  table: string;
  columns: Array<{
    name: string;
    dataType: string;
    description?: string;
  }>;
}

@Injectable()
export class MappingSuggesterService {
  private readonly logger = new Logger(MappingSuggesterService.name);
  private readonly anthropicApiKey: string;
  private readonly openaiApiKey: string;
  private readonly patternCache = new Map<string, MappingSuggestion[]>();

  constructor(private configService: ConfigService) {
    this.anthropicApiKey = this.configService.get<string>('ANTHROPIC_API_KEY') || '';
    this.openaiApiKey = this.configService.get<string>('OPENAI_API_KEY') || '';
  }

  /**
   * Generate mapping suggestions for a hierarchy node
   */
  async suggestMappings(
    hierarchy: HierarchyContext,
    availableSources: AvailableSource[],
    projectMappingHistory?: Array<{ hierarchyName: string; mapping: MappingSuggestion['suggestedMapping'] }>,
  ): Promise<MappingSuggestion[]> {
    const suggestions: MappingSuggestion[] = [];

    try {
      // 1. Check pattern cache first
      const patternKey = this.generatePatternKey(hierarchy.name);
      if (this.patternCache.has(patternKey)) {
        const cached = this.patternCache.get(patternKey)!;
        suggestions.push(...cached.map(s => ({ ...s, source: 'pattern' as const })));
      }

      // 2. Find similar hierarchy names from history
      if (projectMappingHistory?.length) {
        const similarMappings = this.findSimilarMappings(hierarchy.name, projectMappingHistory);
        suggestions.push(...similarMappings);
      }

      // 3. Use AI for intelligent suggestions
      if (this.anthropicApiKey || this.openaiApiKey) {
        const aiSuggestions = await this.getAISuggestions(hierarchy, availableSources);
        suggestions.push(...aiSuggestions);
      }

      // 4. Fall back to name-based matching
      const nameSuggestions = this.getNameBasedSuggestions(hierarchy, availableSources);
      suggestions.push(...nameSuggestions);

      // Deduplicate and sort by confidence
      const uniqueSuggestions = this.deduplicateSuggestions(suggestions);
      return uniqueSuggestions.sort((a, b) => b.confidence - a.confidence).slice(0, 5);
    } catch (error) {
      this.logger.error('Failed to generate mapping suggestions', error);
      return suggestions;
    }
  }

  /**
   * Learn from user-accepted mappings
   */
  learnFromAcceptedMapping(
    hierarchyName: string,
    mapping: MappingSuggestion['suggestedMapping'],
  ): void {
    const patternKey = this.generatePatternKey(hierarchyName);
    const existing = this.patternCache.get(patternKey) || [];

    existing.push({
      hierarchyId: '',
      suggestedMapping: mapping,
      confidence: 0.9,
      reasoning: 'Learned from user acceptance',
      source: 'pattern',
    });

    this.patternCache.set(patternKey, existing);
    this.logger.log(`Learned mapping pattern for: ${hierarchyName}`);
  }

  /**
   * Get AI-powered suggestions using Claude or OpenAI
   */
  private async getAISuggestions(
    hierarchy: HierarchyContext,
    availableSources: AvailableSource[],
  ): Promise<MappingSuggestion[]> {
    const prompt = this.buildMappingPrompt(hierarchy, availableSources);

    try {
      let response: string;

      if (this.anthropicApiKey) {
        response = await this.callClaude(prompt);
      } else if (this.openaiApiKey) {
        response = await this.callOpenAI(prompt);
      } else {
        return [];
      }

      return this.parseMappingSuggestions(response, hierarchy.id);
    } catch (error) {
      this.logger.error('AI suggestion failed', error);
      return [];
    }
  }

  /**
   * Build prompt for AI mapping suggestions
   */
  private buildMappingPrompt(
    hierarchy: HierarchyContext,
    availableSources: AvailableSource[],
  ): string {
    const sourcesDescription = availableSources
      .map(s => `Table: ${s.database}.${s.schema}.${s.table}\nColumns: ${s.columns.map(c => `${c.name} (${c.dataType})`).join(', ')}`)
      .join('\n\n');

    return `You are a data mapping expert. Suggest the best source table and column mapping for this hierarchy node.

HIERARCHY NODE:
- Name: ${hierarchy.name}
- ID: ${hierarchy.hierarchyId}
- Parent: ${hierarchy.parentName || 'None (root)'}
- Level: ${hierarchy.level}
${hierarchy.existingMappings?.length ? `- Existing mappings in siblings: ${JSON.stringify(hierarchy.existingMappings)}` : ''}

AVAILABLE DATA SOURCES:
${sourcesDescription}

Based on the hierarchy node name and context, suggest the most appropriate source table and column mapping.
Consider:
1. Name similarity between hierarchy and column names
2. Data type appropriateness
3. Patterns from sibling mappings
4. Common accounting/financial hierarchy patterns

Respond in JSON format:
{
  "suggestions": [
    {
      "database": "...",
      "schema": "...",
      "table": "...",
      "column": "...",
      "confidence": 0.0-1.0,
      "reasoning": "..."
    }
  ]
}`;
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
        temperature: 0.3,
      }),
    });

    if (!response.ok) {
      throw new Error(`OpenAI API error: ${response.statusText}`);
    }

    const data = await response.json();
    return data.choices[0].message.content;
  }

  /**
   * Parse AI response into suggestions
   */
  private parseMappingSuggestions(response: string, hierarchyId: string): MappingSuggestion[] {
    try {
      // Extract JSON from response
      const jsonMatch = response.match(/\{[\s\S]*\}/);
      if (!jsonMatch) return [];

      const parsed = JSON.parse(jsonMatch[0]);
      return (parsed.suggestions || []).map((s: any) => ({
        hierarchyId,
        suggestedMapping: {
          sourceDatabase: s.database,
          sourceSchema: s.schema,
          sourceTable: s.table,
          sourceColumn: s.column,
        },
        confidence: s.confidence || 0.7,
        reasoning: s.reasoning || 'AI suggestion',
        source: 'ai' as const,
      }));
    } catch (error) {
      this.logger.error('Failed to parse AI suggestions', error);
      return [];
    }
  }

  /**
   * Find similar mappings from history
   */
  private findSimilarMappings(
    hierarchyName: string,
    history: Array<{ hierarchyName: string; mapping: MappingSuggestion['suggestedMapping'] }>,
  ): MappingSuggestion[] {
    const normalizedName = hierarchyName.toLowerCase().replace(/[^a-z0-9]/g, '');

    return history
      .filter(h => {
        const normalizedHistoryName = h.hierarchyName.toLowerCase().replace(/[^a-z0-9]/g, '');
        return this.calculateSimilarity(normalizedName, normalizedHistoryName) > 0.7;
      })
      .map(h => ({
        hierarchyId: '',
        suggestedMapping: h.mapping,
        confidence: 0.75,
        reasoning: `Similar to existing mapping for "${h.hierarchyName}"`,
        source: 'similar' as const,
      }));
  }

  /**
   * Get name-based suggestions using fuzzy matching
   */
  private getNameBasedSuggestions(
    hierarchy: HierarchyContext,
    availableSources: AvailableSource[],
  ): MappingSuggestion[] {
    const suggestions: MappingSuggestion[] = [];
    const normalizedName = hierarchy.name.toLowerCase().replace(/[^a-z0-9]/g, '');

    for (const source of availableSources) {
      for (const column of source.columns) {
        const normalizedColumn = column.name.toLowerCase().replace(/[^a-z0-9]/g, '');
        const similarity = this.calculateSimilarity(normalizedName, normalizedColumn);

        if (similarity > 0.5) {
          suggestions.push({
            hierarchyId: hierarchy.id,
            suggestedMapping: {
              sourceDatabase: source.database,
              sourceSchema: source.schema,
              sourceTable: source.table,
              sourceColumn: column.name,
            },
            confidence: similarity * 0.8,
            reasoning: `Column name "${column.name}" is similar to hierarchy name`,
            source: 'pattern',
          });
        }
      }
    }

    return suggestions;
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
   * Generate pattern key for caching
   */
  private generatePatternKey(hierarchyName: string): string {
    return hierarchyName.toLowerCase().replace(/[^a-z0-9]/g, '').substring(0, 20);
  }

  /**
   * Deduplicate suggestions
   */
  private deduplicateSuggestions(suggestions: MappingSuggestion[]): MappingSuggestion[] {
    const seen = new Set<string>();
    return suggestions.filter(s => {
      const key = `${s.suggestedMapping.sourceTable}:${s.suggestedMapping.sourceColumn}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }
}
