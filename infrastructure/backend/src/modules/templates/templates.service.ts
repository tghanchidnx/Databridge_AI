/**
 * Templates Service
 * Manages hierarchy templates for reuse
 */
import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { v4 as uuidv4 } from 'uuid';

export interface HierarchyTemplate {
  id: string;
  name: string;
  description: string;
  category: 'accounting' | 'finance' | 'operations';
  industry: string;
  structure: any;
  nodeCount: number;
  maxDepth: number;
  tags: string[];
  createdBy?: string;
  createdAt: Date;
  updatedAt: Date;
  usageCount: number;
  isSystem: boolean;
}

export interface CreateTemplateDto {
  name: string;
  description?: string;
  category: 'accounting' | 'finance' | 'operations';
  industry?: string;
  structure: any;
  tags?: string[];
}

@Injectable()
export class TemplatesService {
  private readonly logger = new Logger(TemplatesService.name);
  private templates = new Map<string, HierarchyTemplate>();

  constructor() {
    // Initialize with system templates
    this.initializeSystemTemplates();
  }

  /**
   * Get all templates
   */
  async getAllTemplates(): Promise<HierarchyTemplate[]> {
    return Array.from(this.templates.values()).sort((a, b) =>
      b.usageCount - a.usageCount
    );
  }

  /**
   * Get templates by category
   */
  async getTemplatesByCategory(category: string): Promise<HierarchyTemplate[]> {
    return Array.from(this.templates.values())
      .filter(t => t.category === category)
      .sort((a, b) => b.usageCount - a.usageCount);
  }

  /**
   * Get templates by industry
   */
  async getTemplatesByIndustry(industry: string): Promise<HierarchyTemplate[]> {
    return Array.from(this.templates.values())
      .filter(t => t.industry.toLowerCase().includes(industry.toLowerCase()))
      .sort((a, b) => b.usageCount - a.usageCount);
  }

  /**
   * Get template by ID
   */
  async getTemplateById(id: string): Promise<HierarchyTemplate> {
    const template = this.templates.get(id);
    if (!template) {
      throw new NotFoundException(`Template with ID ${id} not found`);
    }
    return template;
  }

  /**
   * Create a new template
   */
  async createTemplate(dto: CreateTemplateDto, userId?: string): Promise<HierarchyTemplate> {
    const { nodeCount, maxDepth } = this.calculateStructureStats(dto.structure);

    const template: HierarchyTemplate = {
      id: uuidv4(),
      name: dto.name,
      description: dto.description || '',
      category: dto.category,
      industry: dto.industry || 'General',
      structure: dto.structure,
      nodeCount,
      maxDepth,
      tags: dto.tags || [],
      createdBy: userId,
      createdAt: new Date(),
      updatedAt: new Date(),
      usageCount: 0,
      isSystem: false,
    };

    this.templates.set(template.id, template);
    this.logger.log(`Created template: ${template.name}`);

    return template;
  }

  /**
   * Update a template
   */
  async updateTemplate(id: string, updates: Partial<CreateTemplateDto>): Promise<HierarchyTemplate> {
    const template = await this.getTemplateById(id);

    if (template.isSystem) {
      throw new Error('Cannot modify system templates');
    }

    const updated: HierarchyTemplate = {
      ...template,
      ...updates,
      updatedAt: new Date(),
    };

    if (updates.structure) {
      const stats = this.calculateStructureStats(updates.structure);
      updated.nodeCount = stats.nodeCount;
      updated.maxDepth = stats.maxDepth;
    }

    this.templates.set(id, updated);
    return updated;
  }

  /**
   * Delete a template
   */
  async deleteTemplate(id: string): Promise<void> {
    const template = await this.getTemplateById(id);

    if (template.isSystem) {
      throw new Error('Cannot delete system templates');
    }

    this.templates.delete(id);
    this.logger.log(`Deleted template: ${template.name}`);
  }

  /**
   * Record template usage
   */
  async recordUsage(id: string): Promise<void> {
    const template = this.templates.get(id);
    if (template) {
      template.usageCount++;
      this.templates.set(id, template);
    }
  }

  /**
   * Search templates
   */
  async searchTemplates(query: string): Promise<HierarchyTemplate[]> {
    const lowercaseQuery = query.toLowerCase();
    return Array.from(this.templates.values())
      .filter(t =>
        t.name.toLowerCase().includes(lowercaseQuery) ||
        t.description.toLowerCase().includes(lowercaseQuery) ||
        t.tags.some(tag => tag.toLowerCase().includes(lowercaseQuery))
      )
      .sort((a, b) => b.usageCount - a.usageCount);
  }

  /**
   * Get recommended templates based on context
   */
  async getRecommendedTemplates(context: {
    industry?: string;
    category?: string;
    projectType?: string;
  }): Promise<HierarchyTemplate[]> {
    let templates = Array.from(this.templates.values());

    // Filter by industry if provided
    if (context.industry) {
      const industryMatches = templates.filter(t =>
        t.industry.toLowerCase().includes(context.industry!.toLowerCase())
      );
      if (industryMatches.length > 0) {
        templates = industryMatches;
      }
    }

    // Filter by category if provided
    if (context.category) {
      const categoryMatches = templates.filter(t => t.category === context.category);
      if (categoryMatches.length > 0) {
        templates = categoryMatches;
      }
    }

    // Sort by usage and return top 5
    return templates
      .sort((a, b) => b.usageCount - a.usageCount)
      .slice(0, 5);
  }

  /**
   * Calculate structure statistics
   */
  private calculateStructureStats(structure: any): { nodeCount: number; maxDepth: number } {
    let nodeCount = 0;
    let maxDepth = 0;

    const traverse = (node: any, depth: number) => {
      nodeCount++;
      maxDepth = Math.max(maxDepth, depth);
      if (node.children) {
        node.children.forEach((child: any) => traverse(child, depth + 1));
      }
    };

    traverse(structure, 1);
    return { nodeCount, maxDepth };
  }

  /**
   * Initialize system templates
   */
  private initializeSystemTemplates(): void {
    const systemTemplates: Omit<HierarchyTemplate, 'id' | 'createdAt' | 'updatedAt'>[] = [
      {
        name: 'Standard P&L',
        description: 'Standard income statement for most businesses',
        category: 'accounting',
        industry: 'General',
        structure: {
          name: 'Income Statement',
          children: [
            { name: 'Revenue', children: [{ name: 'Product Revenue' }, { name: 'Service Revenue' }] },
            { name: 'Cost of Goods Sold', children: [{ name: 'Materials' }, { name: 'Labor' }] },
            { name: 'Gross Profit', suggestedFormula: { type: 'SUBTRACT', text: 'Revenue - COGS' } },
            { name: 'Operating Expenses', children: [{ name: 'S&M' }, { name: 'R&D' }, { name: 'G&A' }] },
            { name: 'Operating Income', suggestedFormula: { type: 'SUBTRACT', text: 'Gross Profit - OpEx' } },
            { name: 'Net Income', suggestedFormula: { type: 'SUBTRACT', text: 'Operating Income - Tax' } },
          ],
        },
        nodeCount: 14,
        maxDepth: 3,
        tags: ['income statement', 'P&L', 'profit and loss'],
        usageCount: 100,
        isSystem: true,
      },
      {
        name: 'Standard Balance Sheet',
        description: 'Assets, liabilities, and equity structure',
        category: 'accounting',
        industry: 'General',
        structure: {
          name: 'Balance Sheet',
          children: [
            { name: 'Assets', children: [{ name: 'Current Assets' }, { name: 'Non-Current Assets' }] },
            { name: 'Liabilities', children: [{ name: 'Current Liabilities' }, { name: 'Long-Term Liabilities' }] },
            { name: 'Equity', suggestedFormula: { type: 'SUBTRACT', text: 'Assets - Liabilities' } },
          ],
        },
        nodeCount: 10,
        maxDepth: 3,
        tags: ['balance sheet', 'assets', 'liabilities', 'equity'],
        usageCount: 90,
        isSystem: true,
      },
      {
        name: 'Oil & Gas LOS',
        description: 'Lease Operating Statement for upstream operations',
        category: 'accounting',
        industry: 'Oil & Gas',
        structure: {
          name: 'Lease Operating Statement',
          children: [
            { name: 'Oil Revenue' },
            { name: 'Gas Revenue' },
            { name: 'NGL Revenue' },
            { name: 'Total Revenue', suggestedFormula: { type: 'SUM', text: 'SUM(Oil, Gas, NGL)' } },
            { name: 'Lease Operating Expenses', children: [{ name: 'Labor' }, { name: 'Chemicals' }, { name: 'Utilities' }] },
            { name: 'Net Operating Income', suggestedFormula: { type: 'SUBTRACT', text: 'Revenue - LOE' } },
          ],
        },
        nodeCount: 12,
        maxDepth: 3,
        tags: ['oil & gas', 'LOS', 'upstream', 'lease operating'],
        usageCount: 75,
        isSystem: true,
      },
      {
        name: 'SaaS P&L',
        description: 'Income statement with ARR/MRR tracking',
        category: 'accounting',
        industry: 'SaaS',
        structure: {
          name: 'SaaS Income Statement',
          children: [
            { name: 'Subscription Revenue', children: [{ name: 'MRR' }, { name: 'Annual Contracts' }] },
            { name: 'Professional Services' },
            { name: 'Total Revenue', suggestedFormula: { type: 'SUM', text: 'SUM(Subscriptions, Services)' } },
            { name: 'Cost of Revenue', children: [{ name: 'Hosting' }, { name: 'Support' }] },
            { name: 'Gross Profit', suggestedFormula: { type: 'SUBTRACT', text: 'Revenue - COR' } },
          ],
        },
        nodeCount: 15,
        maxDepth: 3,
        tags: ['SaaS', 'subscription', 'ARR', 'MRR'],
        usageCount: 80,
        isSystem: true,
      },
      {
        name: 'Cost Center Hierarchy',
        description: 'Expense allocation and responsibility structure',
        category: 'finance',
        industry: 'General',
        structure: {
          name: 'Cost Centers',
          children: [
            { name: 'Corporate', children: [{ name: 'Executive' }, { name: 'Finance' }, { name: 'Legal' }] },
            { name: 'Operations', children: [{ name: 'Production' }, { name: 'Quality' }, { name: 'Logistics' }] },
            { name: 'Commercial', children: [{ name: 'Sales' }, { name: 'Marketing' }] },
          ],
        },
        nodeCount: 13,
        maxDepth: 3,
        tags: ['cost center', 'budget', 'expense allocation'],
        usageCount: 70,
        isSystem: true,
      },
      {
        name: 'Geographic Hierarchy',
        description: 'Global regions, countries, states',
        category: 'operations',
        industry: 'General',
        structure: {
          name: 'Global',
          children: [
            { name: 'Americas', children: [{ name: 'North America' }, { name: 'Latin America' }] },
            { name: 'EMEA', children: [{ name: 'Western Europe' }, { name: 'Middle East' }] },
            { name: 'APAC', children: [{ name: 'Greater China' }, { name: 'Southeast Asia' }] },
          ],
        },
        nodeCount: 10,
        maxDepth: 3,
        tags: ['geography', 'regions', 'international'],
        usageCount: 65,
        isSystem: true,
      },
    ];

    systemTemplates.forEach(template => {
      const id = `system-${template.name.toLowerCase().replace(/\s+/g, '-')}`;
      this.templates.set(id, {
        ...template,
        id,
        createdAt: new Date('2024-01-01'),
        updatedAt: new Date('2024-01-01'),
      });
    });

    this.logger.log(`Initialized ${systemTemplates.length} system templates`);
  }
}
