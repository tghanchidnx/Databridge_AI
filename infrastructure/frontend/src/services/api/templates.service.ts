import { BaseApiService } from "./base.service";

// Types for Templates, Skills, and Knowledge Base
export interface TemplateMetadata {
  id: string;
  name: string;
  domain: string;
  hierarchy_type: string;
  industry: string;
  description: string;
  hierarchy_count: number;
}

export interface TemplateHierarchy {
  hierarchy_id: string;
  hierarchy_name: string;
  parent_id: string | null;
  level: number;
  sort_order: number;
  is_calculated: boolean;
  formula_hint?: string;
  node_type?: string;
  description?: string;
}

export interface FinancialTemplate extends TemplateMetadata {
  hierarchies: TemplateHierarchy[];
  recommended_mappings: any[];
  tags: string[];
  version: string;
}

export interface SkillDefinition {
  id: string;
  name: string;
  description: string;
  domain: string;
  industries: string[];
  prompt_file: string;
  documentation_file: string;
  capabilities: string[];
  hierarchy_types: string[];
  tags: string[];
  version: string;
}

export interface CustomPrompt {
  id: string;
  name: string;
  trigger: string;
  content: string;
  domain: string;
  category: string;
}

export interface ClientKnowledge {
  client_id: string;
  client_name: string;
  industry: string;
  chart_of_accounts_pattern?: string;
  gl_patterns: Record<string, string>;
  cost_center_pattern?: string;
  profit_center_pattern?: string;
  erp_system?: string;
  reporting_system?: string;
  custom_prompts: CustomPrompt[];
  preferred_templates: Record<string, string>;
  preferred_skills: Record<string, string>;
  notes?: string;
}

export interface TemplateRecommendation {
  template_id: string;
  template_name: string;
  domain: string;
  hierarchy_type: string;
  score: number;
  reason: string;
  industry_match: boolean;
}

class TemplatesService extends BaseApiService {
  // ========== TEMPLATES ==========

  /**
   * List all available templates with optional filtering
   */
  async listTemplates(filters?: {
    domain?: string;
    industry?: string;
    hierarchy_type?: string;
  }): Promise<TemplateMetadata[]> {
    const params = new URLSearchParams();
    if (filters?.domain) params.append("domain", filters.domain);
    if (filters?.industry) params.append("industry", filters.industry);
    if (filters?.hierarchy_type) params.append("hierarchy_type", filters.hierarchy_type);

    const response = await this.api.get(`/templates?${params.toString()}`);
    return this.extractData<TemplateMetadata[]>(response);
  }

  /**
   * Get template details by ID
   */
  async getTemplate(templateId: string): Promise<FinancialTemplate> {
    const response = await this.api.get(`/templates/${templateId}`);
    return this.extractData<FinancialTemplate>(response);
  }

  /**
   * Get template recommendations based on context
   */
  async getTemplateRecommendations(context: {
    industry?: string;
    hierarchy_type?: string;
    domain?: string;
  }): Promise<TemplateRecommendation[]> {
    const response = await this.api.post("/templates/recommendations", context);
    return this.extractData<TemplateRecommendation[]>(response);
  }

  /**
   * Create project from template
   */
  async createProjectFromTemplate(
    templateId: string,
    projectName: string,
    description?: string
  ): Promise<any> {
    const response = await this.api.post("/templates/create-project", {
      template_id: templateId,
      project_name: projectName,
      description,
    });
    return this.extractData<any>(response);
  }

  /**
   * Save current project as a new template
   */
  async saveAsTemplate(
    projectId: string,
    templateName: string,
    description: string,
    domain: string,
    industry: string
  ): Promise<TemplateMetadata> {
    const response = await this.api.post("/templates/save-as-template", {
      project_id: projectId,
      template_name: templateName,
      description,
      domain,
      industry,
    });
    return this.extractData<TemplateMetadata>(response);
  }

  // ========== SKILLS ==========

  /**
   * List all available AI skills
   */
  async listSkills(filters?: {
    domain?: string;
    industry?: string;
  }): Promise<SkillDefinition[]> {
    const params = new URLSearchParams();
    if (filters?.domain) params.append("domain", filters.domain);
    if (filters?.industry) params.append("industry", filters.industry);

    const response = await this.api.get(`/skills?${params.toString()}`);
    return this.extractData<SkillDefinition[]>(response);
  }

  /**
   * Get skill details by ID
   */
  async getSkill(skillId: string): Promise<SkillDefinition> {
    const response = await this.api.get(`/skills/${skillId}`);
    return this.extractData<SkillDefinition>(response);
  }

  /**
   * Get the system prompt for a skill
   */
  async getSkillPrompt(skillId: string): Promise<string> {
    const response = await this.api.get(`/skills/${skillId}/prompt`);
    return this.extractData<string>(response);
  }

  /**
   * Get recommended skills for an industry
   */
  async getSkillsForIndustry(industry: string): Promise<SkillDefinition[]> {
    const response = await this.api.get(`/skills/industry/${industry}`);
    return this.extractData<SkillDefinition[]>(response);
  }

  // ========== KNOWLEDGE BASE ==========

  /**
   * List all client profiles
   */
  async listClients(): Promise<ClientKnowledge[]> {
    const response = await this.api.get("/knowledge-base/clients");
    return this.extractData<ClientKnowledge[]>(response);
  }

  /**
   * Get client knowledge by ID
   */
  async getClientKnowledge(clientId: string): Promise<ClientKnowledge> {
    const response = await this.api.get(`/knowledge-base/clients/${clientId}`);
    return this.extractData<ClientKnowledge>(response);
  }

  /**
   * Create a new client profile
   */
  async createClient(client: Partial<ClientKnowledge>): Promise<ClientKnowledge> {
    const response = await this.api.post("/knowledge-base/clients", client);
    return this.extractData<ClientKnowledge>(response);
  }

  /**
   * Update client knowledge
   */
  async updateClient(
    clientId: string,
    updates: Partial<ClientKnowledge>
  ): Promise<ClientKnowledge> {
    const response = await this.api.put(`/knowledge-base/clients/${clientId}`, updates);
    return this.extractData<ClientKnowledge>(response);
  }

  /**
   * Delete a client profile
   */
  async deleteClient(clientId: string): Promise<void> {
    await this.api.delete(`/knowledge-base/clients/${clientId}`);
  }

  /**
   * Add a custom prompt to a client
   */
  async addClientPrompt(clientId: string, prompt: CustomPrompt): Promise<ClientKnowledge> {
    const response = await this.api.post(
      `/knowledge-base/clients/${clientId}/prompts`,
      prompt
    );
    return this.extractData<ClientKnowledge>(response);
  }

  /**
   * Remove a custom prompt from a client
   */
  async removeClientPrompt(clientId: string, promptId: string): Promise<ClientKnowledge> {
    const response = await this.api.delete(
      `/knowledge-base/clients/${clientId}/prompts/${promptId}`
    );
    return this.extractData<ClientKnowledge>(response);
  }

  // ========== DOMAINS & INDUSTRIES ==========

  /**
   * Get available domains
   */
  async getDomains(): Promise<Record<string, { name: string; description: string }>> {
    const response = await this.api.get("/templates/domains");
    return this.extractData<Record<string, { name: string; description: string }>>(response);
  }

  /**
   * Get available industries
   */
  async getIndustries(): Promise<Record<string, { name: string; description: string }>> {
    const response = await this.api.get("/templates/industries");
    return this.extractData<Record<string, { name: string; description: string }>>(response);
  }
}

export const templatesService = new TemplatesService();
