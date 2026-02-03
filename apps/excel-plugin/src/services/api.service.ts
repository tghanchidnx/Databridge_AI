/**
 * DataBridge API Service
 *
 * HTTP client for communicating with the DataBridge backend.
 * Handles all API calls for hierarchy management, data reconciliation,
 * and orchestrator integration.
 */

import axios, { AxiosInstance, AxiosRequestConfig, AxiosError } from 'axios';

// Types
export interface ApiResponse<T = any> {
  success: boolean;
  data?: T;
  error?: string;
  message?: string;
}

export interface ConnectionConfig {
  id?: string;
  connectionName: string;
  serverType: string;
  host?: string;
  port?: number;
  databaseName?: string;
  schemaName?: string;
}

export interface HierarchyProject {
  id: string;
  name: string;
  description?: string;
  createdAt: string;
  updatedAt: string;
}

export interface Hierarchy {
  id: string;
  hierarchyId: string;
  hierarchyName: string;
  parentId?: string;
  description?: string;
  hierarchyLevel: Record<string, string>;
  flags: HierarchyFlags;
  mapping: SourceMapping[];
}

export interface HierarchyFlags {
  include_flag: boolean;
  exclude_flag: boolean;
  transform_flag: boolean;
  active_flag: boolean;
}

export interface SourceMapping {
  mapping_index: number;
  source_database?: string;
  source_schema?: string;
  source_table?: string;
  source_column?: string;
  source_uid?: string;
}

export interface QueryResult {
  columns: string[];
  rows: any[][];
  rowCount: number;
  executionTime: number;
}

export interface DataProfile {
  rowCount: number;
  columnCount: number;
  columns: ColumnProfile[];
  duplicateRows: number;
  nullPercentage: number;
}

export interface ColumnProfile {
  name: string;
  type: string;
  nullCount: number;
  nullPercentage: number;
  uniqueCount: number;
  cardinality: number;
  sampleValues: any[];
}

export interface ReconciliationResult {
  matchedRows: number;
  orphansInSource: number;
  orphansInTarget: number;
  conflictingRows: number;
  details: ReconciliationDetail[];
}

export interface ReconciliationDetail {
  key: string;
  status: 'matched' | 'orphan_source' | 'orphan_target' | 'conflict';
  sourceValues?: Record<string, any>;
  targetValues?: Record<string, any>;
  conflicts?: string[];
}

export interface MappingSuggestion {
  sourceValue: string;
  hierarchyId: string;
  hierarchyName: string;
  confidence: number;
  matchType: 'exact' | 'fuzzy' | 'partial';
}

class DataBridgeApiService {
  private client: AxiosInstance;
  private baseUrl: string;
  private token: string | null = null;
  private apiKey: string | null = null;

  constructor() {
    this.baseUrl = this.getBaseUrl();
    this.client = axios.create({
      baseURL: this.baseUrl,
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json',
      },
    });

    // Add request interceptor for auth
    this.client.interceptors.request.use((config) => {
      if (this.token) {
        config.headers.Authorization = `Bearer ${this.token}`;
      }
      if (this.apiKey) {
        config.headers['X-API-Key'] = this.apiKey;
      }
      return config;
    });

    // Add response interceptor for error handling
    this.client.interceptors.response.use(
      (response) => response,
      (error: AxiosError) => {
        // Don't log 401 errors prominently - they're expected when not authenticated
        if (error.response?.status === 401) {
          console.debug('Authentication required for:', error.config?.url);
        } else {
          console.error('API Error:', error.response?.data || error.message);
        }
        return Promise.reject(error);
      }
    );
  }

  private getBaseUrl(): string {
    // Check for configured URL in storage
    const storedUrl = localStorage.getItem('databridge_api_url');
    if (storedUrl) return storedUrl;

    // Default to localhost for development (NestJS backend on port 3002)
    return 'http://localhost:3002/api';
  }

  // =========================================================================
  // Configuration
  // =========================================================================

  setBaseUrl(url: string): void {
    this.baseUrl = url;
    this.client.defaults.baseURL = url;
    localStorage.setItem('databridge_api_url', url);
  }

  setToken(token: string): void {
    this.token = token;
    localStorage.setItem('databridge_token', token);
  }

  setApiKey(apiKey: string): void {
    this.apiKey = apiKey;
    localStorage.setItem('databridge_api_key', apiKey);
  }

  loadStoredCredentials(): void {
    this.token = localStorage.getItem('databridge_token');
    this.apiKey = localStorage.getItem('databridge_api_key');
  }

  clearCredentials(): void {
    this.token = null;
    this.apiKey = null;
    localStorage.removeItem('databridge_token');
    localStorage.removeItem('databridge_api_key');
  }

  // =========================================================================
  // Health & Connection
  // =========================================================================

  async checkHealth(): Promise<ApiResponse<{ status: string }>> {
    try {
      // Use the health endpoint to verify connectivity
      const response = await this.client.get('/health');
      return { success: true, data: response.data?.data || { status: 'ok' } };
    } catch (error: any) {
      console.error('Health check failed:', error);
      return { success: false, error: 'Backend not reachable' };
    }
  }

  // =========================================================================
  // Projects
  // =========================================================================

  async listProjects(): Promise<ApiResponse<HierarchyProject[]>> {
    try {
      const response = await this.client.get('/smart-hierarchy/projects');
      return { success: true, data: response.data.data || response.data };
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  async getProject(projectId: string): Promise<ApiResponse<HierarchyProject>> {
    try {
      const response = await this.client.get(`/smart-hierarchy/projects/${projectId}`);
      return { success: true, data: response.data.data || response.data };
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  // =========================================================================
  // Hierarchies
  // =========================================================================

  async listHierarchies(projectId: string): Promise<ApiResponse<Hierarchy[]>> {
    try {
      const response = await this.client.get(`/smart-hierarchy/project/${projectId}`);
      return { success: true, data: response.data.data || response.data };
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  async getHierarchyTree(projectId: string): Promise<ApiResponse<Hierarchy[]>> {
    try {
      const response = await this.client.get(`/smart-hierarchy/project/${projectId}/tree`);
      return { success: true, data: response.data.data || response.data };
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  // =========================================================================
  // Connections
  // =========================================================================

  async listConnections(): Promise<ApiResponse<ConnectionConfig[]>> {
    try {
      const response = await this.client.get('/connections');
      return { success: true, data: response.data.data || response.data };
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  async testConnection(connectionId: string): Promise<ApiResponse<{ connected: boolean }>> {
    try {
      const response = await this.client.post(`/connections/${connectionId}/test`);
      return { success: true, data: response.data };
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  async executeQuery(connectionId: string, query: string): Promise<ApiResponse<QueryResult>> {
    try {
      const response = await this.client.post('/connections/query', {
        connectionId,
        query,
      });
      return { success: true, data: response.data };
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  // =========================================================================
  // Data Operations (via MCP proxy)
  // =========================================================================

  async profileData(data: any[][]): Promise<ApiResponse<DataProfile>> {
    try {
      const response = await this.client.post('/mcp/profile_data', { data });
      return { success: true, data: response.data };
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  async fuzzyMatch(
    sourceValues: string[],
    targetValues: string[],
    threshold: number = 80
  ): Promise<ApiResponse<MappingSuggestion[]>> {
    try {
      const response = await this.client.post('/mcp/fuzzy_match', {
        sourceValues,
        targetValues,
        threshold,
      });
      return { success: true, data: response.data };
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  async compareData(
    sourceData: any[][],
    targetData: any[][],
    keyColumns: string[]
  ): Promise<ApiResponse<ReconciliationResult>> {
    try {
      const response = await this.client.post('/mcp/compare_hashes', {
        sourceData,
        targetData,
        keyColumns,
      });
      return { success: true, data: response.data };
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  // =========================================================================
  // Mapping Suggestions
  // =========================================================================

  async suggestMappings(
    projectId: string,
    sourceValues: string[],
    column: string
  ): Promise<ApiResponse<MappingSuggestion[]>> {
    try {
      // Get hierarchies for the project
      const hierarchiesResult = await this.listHierarchies(projectId);
      if (!hierarchiesResult.success || !hierarchiesResult.data) {
        return { success: false, error: 'Failed to fetch hierarchies' };
      }

      // Extract hierarchy names for matching
      const hierarchyNames = hierarchiesResult.data.map((h) => ({
        id: h.hierarchyId,
        name: h.hierarchyName,
      }));

      // Use fuzzy matching to suggest mappings
      const response = await this.client.post('/mcp/suggest_mappings', {
        sourceValues,
        hierarchyNames,
        column,
      });

      return { success: true, data: response.data };
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  // =========================================================================
  // Orchestrator Integration
  // =========================================================================

  async submitTask(
    taskType: string,
    payload: Record<string, any>,
    priority: string = 'normal'
  ): Promise<ApiResponse<{ taskId: string; status: string }>> {
    try {
      const response = await this.client.post('/orchestrator/tasks', {
        type: taskType,
        payload,
        priority,
      });
      return { success: true, data: response.data };
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  async getTaskStatus(taskId: string): Promise<ApiResponse<{ status: string; progress: number }>> {
    try {
      const response = await this.client.get(`/orchestrator/tasks/${taskId}`);
      return { success: true, data: response.data };
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  async registerAsAgent(
    capabilities: Array<{ tool: string; proficiency: number }>
  ): Promise<ApiResponse<{ agentId: string }>> {
    try {
      const agentId = `excel-plugin-${Date.now()}`;
      const response = await this.client.post('/orchestrator/agents/register', {
        id: agentId,
        name: 'Excel Plugin',
        type: 'excel_plugin',
        capabilities,
        maxConcurrentTasks: 3,
      });
      return { success: true, data: { agentId, ...response.data } };
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  async sendHeartbeat(agentId: string): Promise<ApiResponse<void>> {
    try {
      await this.client.post(`/orchestrator/agents/${agentId}/heartbeat`, {
        healthStatus: 'healthy',
      });
      return { success: true };
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }
}

// Export singleton instance
export const apiService = new DataBridgeApiService();
export default apiService;
