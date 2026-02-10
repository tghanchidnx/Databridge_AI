/**
 * Formula Suggester Service
 * AI-powered formula suggestions for hierarchy calculations
 */
import { Injectable, Logger } from '@nestjs/common';

export interface FormulaSuggestion {
  formulaType: 'SUM' | 'SUBTRACT' | 'MULTIPLY' | 'DIVIDE' | 'CUSTOM';
  formulaText: string;
  variables: string[];
  confidence: number;
  reasoning: string;
  category: string;
}

export interface HierarchyFormulaContext {
  name: string;
  hierarchyId: string;
  parentName?: string;
  siblingNames: string[];
  childNames: string[];
  level: number;
}

// Common financial formula patterns
const FORMULA_PATTERNS: Array<{
  keywords: string[];
  pattern: Omit<FormulaSuggestion, 'confidence'>;
}> = [
  {
    keywords: ['gross', 'profit'],
    pattern: {
      formulaType: 'SUBTRACT',
      formulaText: 'Revenue - Cost of Goods Sold',
      variables: ['Revenue', 'COGS'],
      reasoning: 'Gross Profit = Revenue - COGS',
      category: 'Income Statement',
    },
  },
  {
    keywords: ['operating', 'income'],
    pattern: {
      formulaType: 'SUBTRACT',
      formulaText: 'Gross Profit - Operating Expenses',
      variables: ['Gross Profit', 'Operating Expenses'],
      reasoning: 'Operating Income = Gross Profit - Operating Expenses',
      category: 'Income Statement',
    },
  },
  {
    keywords: ['net', 'income'],
    pattern: {
      formulaType: 'SUBTRACT',
      formulaText: 'Operating Income + Other Income - Taxes',
      variables: ['Operating Income', 'Other Income', 'Taxes'],
      reasoning: 'Net Income = Operating Income + Other - Taxes',
      category: 'Income Statement',
    },
  },
  {
    keywords: ['ebitda'],
    pattern: {
      formulaType: 'SUM',
      formulaText: 'Operating Income + Depreciation + Amortization',
      variables: ['Operating Income', 'D&A'],
      reasoning: 'EBITDA = Operating Income + D&A',
      category: 'Income Statement',
    },
  },
  {
    keywords: ['total', 'assets'],
    pattern: {
      formulaType: 'SUM',
      formulaText: 'Current Assets + Non-Current Assets',
      variables: ['Current Assets', 'Non-Current Assets'],
      reasoning: 'Total Assets = Current + Non-Current',
      category: 'Balance Sheet',
    },
  },
  {
    keywords: ['total', 'liabilities'],
    pattern: {
      formulaType: 'SUM',
      formulaText: 'Current Liabilities + Long-Term Liabilities',
      variables: ['Current Liabilities', 'Long-Term Liabilities'],
      reasoning: 'Total Liabilities = Current + Long-Term',
      category: 'Balance Sheet',
    },
  },
  {
    keywords: ['total', 'equity'],
    pattern: {
      formulaType: 'SUBTRACT',
      formulaText: 'Total Assets - Total Liabilities',
      variables: ['Total Assets', 'Total Liabilities'],
      reasoning: 'Equity = Assets - Liabilities',
      category: 'Balance Sheet',
    },
  },
  {
    keywords: ['working', 'capital'],
    pattern: {
      formulaType: 'SUBTRACT',
      formulaText: 'Current Assets - Current Liabilities',
      variables: ['Current Assets', 'Current Liabilities'],
      reasoning: 'Working Capital = Current Assets - Current Liabilities',
      category: 'Balance Sheet',
    },
  },
  {
    keywords: ['operating', 'ratio'],
    pattern: {
      formulaType: 'DIVIDE',
      formulaText: 'Operating Expenses / Revenue * 100',
      variables: ['Operating Expenses', 'Revenue'],
      reasoning: 'Operating Ratio = OpEx / Revenue',
      category: 'Ratios',
    },
  },
  {
    keywords: ['margin', 'gross'],
    pattern: {
      formulaType: 'DIVIDE',
      formulaText: 'Gross Profit / Revenue * 100',
      variables: ['Gross Profit', 'Revenue'],
      reasoning: 'Gross Margin = Gross Profit / Revenue',
      category: 'Ratios',
    },
  },
  {
    keywords: ['variance'],
    pattern: {
      formulaType: 'SUBTRACT',
      formulaText: 'Actual - Budget',
      variables: ['Actual', 'Budget'],
      reasoning: 'Variance = Actual - Budget',
      category: 'Variance Analysis',
    },
  },
  {
    keywords: ['yoy', 'year', 'over'],
    pattern: {
      formulaType: 'DIVIDE',
      formulaText: '(Current Period - Prior Period) / Prior Period * 100',
      variables: ['Current Period', 'Prior Period'],
      reasoning: 'YoY Growth = (Current - Prior) / Prior',
      category: 'Growth Metrics',
    },
  },
];

@Injectable()
export class FormulaSuggesterService {
  private readonly logger = new Logger(FormulaSuggesterService.name);

  /**
   * Suggest formulas for a hierarchy node
   */
  suggestFormulas(context: HierarchyFormulaContext): FormulaSuggestion[] {
    const suggestions: FormulaSuggestion[] = [];
    const normalizedName = context.name.toLowerCase();

    // 1. Match against known patterns
    for (const pattern of FORMULA_PATTERNS) {
      const matchScore = this.calculatePatternMatch(normalizedName, pattern.keywords);
      if (matchScore > 0.5) {
        suggestions.push({
          ...pattern.pattern,
          confidence: matchScore,
        });
      }
    }

    // 2. Check if this is a "Total" row that should sum children
    if (
      (normalizedName.includes('total') || normalizedName.includes('sum')) &&
      context.childNames.length > 0
    ) {
      suggestions.push({
        formulaType: 'SUM',
        formulaText: context.childNames.join(' + '),
        variables: context.childNames,
        confidence: 0.9,
        reasoning: `Sum of ${context.childNames.length} child hierarchies`,
        category: 'Aggregation',
      });
    }

    // 3. Check if siblings suggest a pattern
    if (context.siblingNames.length > 0) {
      const siblingPattern = this.inferFromSiblings(context.name, context.siblingNames);
      if (siblingPattern) {
        suggestions.push(siblingPattern);
      }
    }

    // 4. Suggest child sum for parent nodes
    if (context.childNames.length > 1 && !suggestions.some(s => s.formulaType === 'SUM')) {
      suggestions.push({
        formulaType: 'SUM',
        formulaText: `SUM of child hierarchies`,
        variables: context.childNames.slice(0, 5),
        confidence: 0.6,
        reasoning: 'Parent node typically sums children',
        category: 'Aggregation',
      });
    }

    // Sort by confidence and return top suggestions
    return suggestions
      .sort((a, b) => b.confidence - a.confidence)
      .slice(0, 5);
  }

  /**
   * Validate formula syntax
   */
  validateFormula(formula: string, availableVariables: string[]): {
    valid: boolean;
    errors: string[];
    warnings: string[];
  } {
    const errors: string[] = [];
    const warnings: string[] = [];

    // Check for referenced variables
    const referencedVars = formula.match(/\{([^}]+)\}/g) || [];
    for (const ref of referencedVars) {
      const varName = ref.slice(1, -1);
      if (!availableVariables.includes(varName)) {
        errors.push(`Unknown variable: ${varName}`);
      }
    }

    // Check for balanced parentheses
    let depth = 0;
    for (const char of formula) {
      if (char === '(') depth++;
      if (char === ')') depth--;
      if (depth < 0) {
        errors.push('Unbalanced parentheses');
        break;
      }
    }
    if (depth !== 0) {
      errors.push('Unbalanced parentheses');
    }

    // Check for division by zero potential
    if (formula.includes('/ 0') || formula.includes('/0')) {
      warnings.push('Potential division by zero');
    }

    return {
      valid: errors.length === 0,
      errors,
      warnings,
    };
  }

  /**
   * Generate SQL from formula
   */
  generateFormulaSQL(
    formula: string,
    variableToColumnMap: Record<string, string>,
  ): string {
    let sql = formula;

    // Replace variable references with column names
    for (const [variable, column] of Object.entries(variableToColumnMap)) {
      const regex = new RegExp(`\\{${variable}\\}`, 'g');
      sql = sql.replace(regex, `COALESCE(${column}, 0)`);
    }

    return sql;
  }

  /**
   * Calculate pattern match score
   */
  private calculatePatternMatch(name: string, keywords: string[]): number {
    let matchCount = 0;
    for (const keyword of keywords) {
      if (name.includes(keyword)) {
        matchCount++;
      }
    }
    return matchCount / keywords.length;
  }

  /**
   * Infer formula from sibling naming patterns
   */
  private inferFromSiblings(
    name: string,
    siblingNames: string[],
  ): FormulaSuggestion | null {
    // Check if this looks like a difference/subtraction scenario
    // e.g., if siblings are "Revenue" and "Cost", this might be their difference
    const normalizedName = name.toLowerCase();

    if (normalizedName.includes('net') || normalizedName.includes('margin')) {
      const potentialMinuend = siblingNames.find(s =>
        s.toLowerCase().includes('gross') || s.toLowerCase().includes('total'),
      );
      const potentialSubtrahend = siblingNames.find(s =>
        s.toLowerCase().includes('expense') || s.toLowerCase().includes('cost'),
      );

      if (potentialMinuend && potentialSubtrahend) {
        return {
          formulaType: 'SUBTRACT',
          formulaText: `{${potentialMinuend}} - {${potentialSubtrahend}}`,
          variables: [potentialMinuend, potentialSubtrahend],
          confidence: 0.7,
          reasoning: `Inferred from sibling pattern`,
          category: 'Calculated',
        };
      }
    }

    return null;
  }
}
