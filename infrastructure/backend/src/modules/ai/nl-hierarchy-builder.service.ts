/**
 * Natural Language Hierarchy Builder Service
 * Generates hierarchies from natural language descriptions using AI
 */
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

export interface GeneratedHierarchy {
  id: string;
  name: string;
  description?: string;
  level: number;
  children?: GeneratedHierarchy[];
  suggestedFormula?: {
    type: string;
    text: string;
  };
  suggestedMapping?: {
    pattern: string;
    examples: string[];
  };
}

export interface GenerationRequest {
  description: string;
  industry?: string;
  hierarchyType?: 'income_statement' | 'balance_sheet' | 'cash_flow' | 'custom';
  maxLevels?: number;
  includeFormulas?: boolean;
  includeMappingHints?: boolean;
  refinementContext?: {
    previousStructure?: GeneratedHierarchy[];
    userFeedback?: string;
  };
}

export interface GenerationResult {
  success: boolean;
  hierarchy?: GeneratedHierarchy;
  stats?: {
    totalNodes: number;
    maxDepth: number;
    leafNodes: number;
    formulaNodes: number;
  };
  warnings?: string[];
  conversationId?: string;
}

@Injectable()
export class NlHierarchyBuilderService {
  private readonly logger = new Logger(NlHierarchyBuilderService.name);
  private readonly anthropicApiKey: string;
  private readonly openaiApiKey: string;
  private conversationContexts = new Map<string, Array<{ role: string; content: string }>>();

  constructor(private configService: ConfigService) {
    this.anthropicApiKey = this.configService.get<string>('ANTHROPIC_API_KEY') || '';
    this.openaiApiKey = this.configService.get<string>('OPENAI_API_KEY') || '';
  }

  /**
   * Generate hierarchy from natural language description
   */
  async generateHierarchy(request: GenerationRequest): Promise<GenerationResult> {
    try {
      const prompt = this.buildPrompt(request);
      let response: string;

      if (this.anthropicApiKey) {
        response = await this.callClaude(prompt, request.refinementContext?.userFeedback);
      } else if (this.openaiApiKey) {
        response = await this.callOpenAI(prompt, request.refinementContext?.userFeedback);
      } else {
        // Use rule-based generation as fallback
        return this.generateFallbackHierarchy(request);
      }

      const hierarchy = this.parseResponse(response);

      if (!hierarchy) {
        return {
          success: false,
          warnings: ['Failed to parse AI response into hierarchy structure'],
        };
      }

      const stats = this.calculateStats(hierarchy);
      const warnings = this.validateHierarchy(hierarchy, request);

      return {
        success: true,
        hierarchy,
        stats,
        warnings: warnings.length > 0 ? warnings : undefined,
        conversationId: this.generateConversationId(),
      };
    } catch (error) {
      this.logger.error('Hierarchy generation failed', error);
      return {
        success: false,
        warnings: [`Generation failed: ${error.message}`],
      };
    }
  }

  /**
   * Refine existing hierarchy with user feedback
   */
  async refineHierarchy(
    conversationId: string,
    currentHierarchy: GeneratedHierarchy,
    feedback: string,
  ): Promise<GenerationResult> {
    return this.generateHierarchy({
      description: feedback,
      refinementContext: {
        previousStructure: [currentHierarchy],
        userFeedback: feedback,
      },
    });
  }

  /**
   * Build prompt for AI
   */
  private buildPrompt(request: GenerationRequest): string {
    const parts = [
      `You are a financial hierarchy expert. Create a hierarchical structure based on the user's description.

OUTPUT FORMAT:
Return a JSON object with this structure:
{
  "name": "Root Name",
  "description": "Optional description",
  "children": [
    {
      "name": "Child 1",
      "children": [...],
      "suggestedFormula": { "type": "SUM", "text": "SUM(children)" }  // optional
    }
  ]
}

RULES:
1. Create realistic, industry-standard hierarchy names
2. Maintain logical parent-child relationships
3. Financial calculations should roll up correctly
4. Leaf nodes should be specific and mappable to data sources
5. Parent nodes should have aggregation formulas (SUM, SUBTRACT, etc.)`,
    ];

    if (request.industry) {
      parts.push(`\nINDUSTRY: ${request.industry}`);
      parts.push(this.getIndustryContext(request.industry));
    }

    if (request.hierarchyType) {
      parts.push(`\nTYPE: ${request.hierarchyType}`);
      parts.push(this.getTypeContext(request.hierarchyType));
    }

    if (request.maxLevels) {
      parts.push(`\nMAX LEVELS: ${request.maxLevels}`);
    }

    if (request.includeFormulas) {
      parts.push(`\nInclude formula suggestions for calculated nodes.`);
    }

    if (request.includeMappingHints) {
      parts.push(`\nInclude mapping hints (patterns for source data columns).`);
    }

    if (request.refinementContext?.previousStructure) {
      parts.push(`\nPREVIOUS STRUCTURE (modify this based on feedback):`);
      parts.push(JSON.stringify(request.refinementContext.previousStructure, null, 2));
    }

    parts.push(`\nUSER REQUEST: ${request.description}`);

    return parts.join('\n');
  }

  /**
   * Get industry-specific context
   */
  private getIndustryContext(industry: string): string {
    const contexts: Record<string, string> = {
      'oil_gas': `
Oil & Gas industry considerations:
- Revenue: Crude Oil Sales, Natural Gas Sales, NGL Sales, Gathering/Processing Fees
- Production costs: Lease Operating Expenses (LOE), Production Taxes, Gathering costs
- Exploration: Dry hole costs, G&G, Seismic
- DD&A: Depletion, Depreciation, ARO accretion
- Key metrics: BOE, EBITDAX, PV-10`,

      'manufacturing': `
Manufacturing industry considerations:
- Revenue by product line, channel, geography
- COGS: Direct materials, Direct labor, Manufacturing overhead
- Variances: Purchase price, Labor rate, Volume, Mix
- Inventory: Raw materials, WIP, Finished goods
- Key metrics: Gross margin by product, Capacity utilization`,

      'saas': `
SaaS industry considerations:
- Revenue: Subscription (MRR/ARR), Professional Services, Usage-based
- Bookings: New, Expansion, Churn
- CAC, LTV, CAC Payback Period
- Cohort analysis structure
- Rule of 40 components`,

      'transportation': `
Transportation industry considerations:
- Revenue: Freight, Intermodal, Logistics, Brokerage
- Operating costs: Fuel, Driver wages, Equipment, Maintenance
- Operating Ratio components
- Revenue per mile, Cost per mile metrics`,
    };

    return contexts[industry.toLowerCase()] || '';
  }

  /**
   * Get hierarchy type context
   */
  private getTypeContext(type: string): string {
    const contexts: Record<string, string> = {
      'income_statement': `
Income Statement structure:
- Revenue (by type, geography, segment)
- Cost of Goods Sold / Cost of Revenue
- Gross Profit (Revenue - COGS)
- Operating Expenses (S&M, R&D, G&A)
- Operating Income (Gross Profit - OpEx)
- Other Income/Expense
- Pre-tax Income
- Tax Expense
- Net Income`,

      'balance_sheet': `
Balance Sheet structure:
- Assets
  - Current Assets (Cash, AR, Inventory, Prepaid)
  - Non-Current Assets (PP&E, Intangibles, Investments)
  - Total Assets
- Liabilities
  - Current Liabilities (AP, Accrued, Short-term debt)
  - Non-Current Liabilities (Long-term debt, Deferred)
  - Total Liabilities
- Equity (Common stock, Retained earnings, AOCI)
- Total Liabilities & Equity`,

      'cash_flow': `
Cash Flow Statement structure:
- Operating Activities
  - Net Income
  - Adjustments (D&A, Stock comp, Working capital changes)
  - Net Cash from Operations
- Investing Activities
  - CapEx, Acquisitions, Investments
  - Net Cash from Investing
- Financing Activities
  - Debt, Equity, Dividends
  - Net Cash from Financing
- Net Change in Cash`,
    };

    return contexts[type] || '';
  }

  /**
   * Call Claude API
   */
  private async callClaude(prompt: string, refinement?: string): Promise<string> {
    const messages = [{ role: 'user', content: prompt }];

    if (refinement) {
      messages.push({
        role: 'user',
        content: `Based on the structure above, please modify it according to this feedback: ${refinement}`,
      });
    }

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': this.anthropicApiKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-3-haiku-20240307',
        max_tokens: 4096,
        messages,
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
  private async callOpenAI(prompt: string, refinement?: string): Promise<string> {
    const messages: Array<{ role: string; content: string }> = [
      { role: 'system', content: 'You are a financial hierarchy expert. Always respond with valid JSON.' },
      { role: 'user', content: prompt },
    ];

    if (refinement) {
      messages.push({
        role: 'user',
        content: `Please modify the structure based on this feedback: ${refinement}`,
      });
    }

    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${this.openaiApiKey}`,
      },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages,
        temperature: 0.7,
        max_tokens: 4096,
        response_format: { type: 'json_object' },
      }),
    });

    if (!response.ok) {
      throw new Error(`OpenAI API error: ${response.statusText}`);
    }

    const data = await response.json();
    return data.choices[0].message.content;
  }

  /**
   * Parse AI response into hierarchy
   */
  private parseResponse(response: string): GeneratedHierarchy | null {
    try {
      // Extract JSON from response
      const jsonMatch = response.match(/\{[\s\S]*\}/);
      if (!jsonMatch) return null;

      const parsed = JSON.parse(jsonMatch[0]);
      return this.normalizeHierarchy(parsed, 0);
    } catch (error) {
      this.logger.error('Failed to parse AI response', error);
      return null;
    }
  }

  /**
   * Normalize hierarchy structure
   */
  private normalizeHierarchy(node: any, level: number): GeneratedHierarchy {
    const id = `gen-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

    return {
      id,
      name: node.name || 'Unnamed',
      description: node.description,
      level,
      children: node.children?.map((child: any) => this.normalizeHierarchy(child, level + 1)),
      suggestedFormula: node.suggestedFormula,
      suggestedMapping: node.suggestedMapping,
    };
  }

  /**
   * Calculate stats for hierarchy
   */
  private calculateStats(hierarchy: GeneratedHierarchy): GenerationResult['stats'] {
    let totalNodes = 0;
    let maxDepth = 0;
    let leafNodes = 0;
    let formulaNodes = 0;

    const traverse = (node: GeneratedHierarchy, depth: number) => {
      totalNodes++;
      maxDepth = Math.max(maxDepth, depth);

      if (!node.children || node.children.length === 0) {
        leafNodes++;
      }

      if (node.suggestedFormula) {
        formulaNodes++;
      }

      node.children?.forEach(child => traverse(child, depth + 1));
    };

    traverse(hierarchy, 1);

    return { totalNodes, maxDepth, leafNodes, formulaNodes };
  }

  /**
   * Validate generated hierarchy
   */
  private validateHierarchy(hierarchy: GeneratedHierarchy, request: GenerationRequest): string[] {
    const warnings: string[] = [];

    // Check max levels
    if (request.maxLevels) {
      const stats = this.calculateStats(hierarchy);
      if (stats.maxDepth > request.maxLevels) {
        warnings.push(`Hierarchy exceeds requested max levels (${stats.maxDepth} > ${request.maxLevels})`);
      }
    }

    // Check for empty names
    const checkNames = (node: GeneratedHierarchy) => {
      if (!node.name || node.name.trim() === '') {
        warnings.push('Found node with empty name');
      }
      node.children?.forEach(checkNames);
    };
    checkNames(hierarchy);

    // Check for duplicate names at same level
    const checkDuplicates = (nodes: GeneratedHierarchy[]) => {
      const names = nodes.map(n => n.name.toLowerCase());
      const duplicates = names.filter((name, index) => names.indexOf(name) !== index);
      if (duplicates.length > 0) {
        warnings.push(`Duplicate names found: ${[...new Set(duplicates)].join(', ')}`);
      }
      nodes.forEach(node => {
        if (node.children) checkDuplicates(node.children);
      });
    };
    checkDuplicates([hierarchy]);

    return warnings;
  }

  /**
   * Generate fallback hierarchy (rule-based)
   */
  private generateFallbackHierarchy(request: GenerationRequest): GenerationResult {
    let hierarchy: GeneratedHierarchy;

    if (request.hierarchyType === 'income_statement' || request.description.toLowerCase().includes('income') || request.description.toLowerCase().includes('p&l')) {
      hierarchy = this.getStandardPL();
    } else if (request.hierarchyType === 'balance_sheet' || request.description.toLowerCase().includes('balance')) {
      hierarchy = this.getStandardBS();
    } else if (request.hierarchyType === 'cash_flow' || request.description.toLowerCase().includes('cash flow')) {
      hierarchy = this.getStandardCF();
    } else {
      hierarchy = this.getGenericHierarchy(request.description);
    }

    return {
      success: true,
      hierarchy,
      stats: this.calculateStats(hierarchy),
      warnings: ['Generated using fallback templates (AI not available)'],
    };
  }

  /**
   * Standard P&L template
   */
  private getStandardPL(): GeneratedHierarchy {
    return {
      id: 'gen-pl-root',
      name: 'Income Statement',
      level: 0,
      children: [
        {
          id: 'gen-revenue',
          name: 'Revenue',
          level: 1,
          suggestedFormula: { type: 'SUM', text: 'SUM(children)' },
          children: [
            { id: 'gen-product-rev', name: 'Product Revenue', level: 2 },
            { id: 'gen-service-rev', name: 'Service Revenue', level: 2 },
            { id: 'gen-other-rev', name: 'Other Revenue', level: 2 },
          ],
        },
        {
          id: 'gen-cogs',
          name: 'Cost of Goods Sold',
          level: 1,
          suggestedFormula: { type: 'SUM', text: 'SUM(children)' },
          children: [
            { id: 'gen-materials', name: 'Materials', level: 2 },
            { id: 'gen-labor', name: 'Direct Labor', level: 2 },
            { id: 'gen-overhead', name: 'Overhead', level: 2 },
          ],
        },
        {
          id: 'gen-gross-profit',
          name: 'Gross Profit',
          level: 1,
          suggestedFormula: { type: 'SUBTRACT', text: 'Revenue - COGS' },
        },
        {
          id: 'gen-opex',
          name: 'Operating Expenses',
          level: 1,
          suggestedFormula: { type: 'SUM', text: 'SUM(children)' },
          children: [
            { id: 'gen-sales', name: 'Sales & Marketing', level: 2 },
            { id: 'gen-rd', name: 'Research & Development', level: 2 },
            { id: 'gen-ga', name: 'General & Administrative', level: 2 },
          ],
        },
        {
          id: 'gen-op-income',
          name: 'Operating Income',
          level: 1,
          suggestedFormula: { type: 'SUBTRACT', text: 'Gross Profit - Operating Expenses' },
        },
        {
          id: 'gen-net-income',
          name: 'Net Income',
          level: 1,
          suggestedFormula: { type: 'SUBTRACT', text: 'Operating Income - Taxes' },
        },
      ],
    };
  }

  /**
   * Standard Balance Sheet template
   */
  private getStandardBS(): GeneratedHierarchy {
    return {
      id: 'gen-bs-root',
      name: 'Balance Sheet',
      level: 0,
      children: [
        {
          id: 'gen-assets',
          name: 'Assets',
          level: 1,
          suggestedFormula: { type: 'SUM', text: 'SUM(children)' },
          children: [
            {
              id: 'gen-current-assets',
              name: 'Current Assets',
              level: 2,
              suggestedFormula: { type: 'SUM', text: 'SUM(children)' },
              children: [
                { id: 'gen-cash', name: 'Cash & Equivalents', level: 3 },
                { id: 'gen-ar', name: 'Accounts Receivable', level: 3 },
                { id: 'gen-inventory', name: 'Inventory', level: 3 },
              ],
            },
            {
              id: 'gen-noncurrent-assets',
              name: 'Non-Current Assets',
              level: 2,
              suggestedFormula: { type: 'SUM', text: 'SUM(children)' },
              children: [
                { id: 'gen-ppe', name: 'Property, Plant & Equipment', level: 3 },
                { id: 'gen-intangibles', name: 'Intangible Assets', level: 3 },
              ],
            },
          ],
        },
        {
          id: 'gen-liabilities',
          name: 'Liabilities',
          level: 1,
          suggestedFormula: { type: 'SUM', text: 'SUM(children)' },
          children: [
            {
              id: 'gen-current-liabilities',
              name: 'Current Liabilities',
              level: 2,
              children: [
                { id: 'gen-ap', name: 'Accounts Payable', level: 3 },
                { id: 'gen-accrued', name: 'Accrued Expenses', level: 3 },
              ],
            },
            {
              id: 'gen-lt-liabilities',
              name: 'Long-Term Liabilities',
              level: 2,
              children: [
                { id: 'gen-lt-debt', name: 'Long-Term Debt', level: 3 },
              ],
            },
          ],
        },
        {
          id: 'gen-equity',
          name: 'Shareholders Equity',
          level: 1,
          suggestedFormula: { type: 'SUBTRACT', text: 'Assets - Liabilities' },
          children: [
            { id: 'gen-common-stock', name: 'Common Stock', level: 2 },
            { id: 'gen-retained', name: 'Retained Earnings', level: 2 },
          ],
        },
      ],
    };
  }

  /**
   * Standard Cash Flow template
   */
  private getStandardCF(): GeneratedHierarchy {
    return {
      id: 'gen-cf-root',
      name: 'Cash Flow Statement',
      level: 0,
      children: [
        {
          id: 'gen-cfo',
          name: 'Operating Activities',
          level: 1,
          suggestedFormula: { type: 'SUM', text: 'SUM(children)' },
          children: [
            { id: 'gen-net-income-cf', name: 'Net Income', level: 2 },
            { id: 'gen-da', name: 'Depreciation & Amortization', level: 2 },
            { id: 'gen-wc-changes', name: 'Working Capital Changes', level: 2 },
          ],
        },
        {
          id: 'gen-cfi',
          name: 'Investing Activities',
          level: 1,
          suggestedFormula: { type: 'SUM', text: 'SUM(children)' },
          children: [
            { id: 'gen-capex', name: 'Capital Expenditures', level: 2 },
            { id: 'gen-acquisitions', name: 'Acquisitions', level: 2 },
          ],
        },
        {
          id: 'gen-cff',
          name: 'Financing Activities',
          level: 1,
          suggestedFormula: { type: 'SUM', text: 'SUM(children)' },
          children: [
            { id: 'gen-debt-changes', name: 'Debt Proceeds/Repayments', level: 2 },
            { id: 'gen-dividends', name: 'Dividends Paid', level: 2 },
          ],
        },
        {
          id: 'gen-net-change',
          name: 'Net Change in Cash',
          level: 1,
          suggestedFormula: { type: 'SUM', text: 'Operating + Investing + Financing' },
        },
      ],
    };
  }

  /**
   * Generic hierarchy template
   */
  private getGenericHierarchy(description: string): GeneratedHierarchy {
    return {
      id: 'gen-root',
      name: description.split(' ').slice(0, 3).join(' '),
      level: 0,
      children: [
        {
          id: 'gen-cat1',
          name: 'Category 1',
          level: 1,
          children: [
            { id: 'gen-sub1', name: 'Subcategory 1.1', level: 2 },
            { id: 'gen-sub2', name: 'Subcategory 1.2', level: 2 },
          ],
        },
        {
          id: 'gen-cat2',
          name: 'Category 2',
          level: 1,
          children: [
            { id: 'gen-sub3', name: 'Subcategory 2.1', level: 2 },
            { id: 'gen-sub4', name: 'Subcategory 2.2', level: 2 },
          ],
        },
        {
          id: 'gen-total',
          name: 'Total',
          level: 1,
          suggestedFormula: { type: 'SUM', text: 'SUM(Category 1, Category 2)' },
        },
      ],
    };
  }

  /**
   * Generate conversation ID
   */
  private generateConversationId(): string {
    return `conv-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  }
}
