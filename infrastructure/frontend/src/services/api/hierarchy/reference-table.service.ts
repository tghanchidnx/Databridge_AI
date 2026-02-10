import axiosInstance from '@/lib/axios';

export interface ReferenceTableColumn {
  name: string;
  type: 'string' | 'number' | 'boolean' | 'date';
  isPrimaryKey?: boolean;
  sampleValues?: string[];
}

export interface ReferenceTableSummary {
  id: string;
  name: string;
  displayName: string;
  description?: string;
  sourceFile?: string;
  columns: ReferenceTableColumn[];
  rowCount: number;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface CreateReferenceTableDto {
  name: string;
  displayName?: string;
  description?: string;
  csvContent: string;
  sourceFile?: string;
}

export interface QueryReferenceTableDto {
  tableName: string;
  columns?: string[];
  filterColumn?: string;
  filterValues?: string[];
  limit?: number;
  offset?: number;
  distinct?: boolean;
}

export interface QueryResult {
  columns: string[];
  rows: Record<string, any>[];
  totalRows: number;
}

export interface SaveViewerSelectionDto {
  projectId: string;
  hierarchyId: string;
  tableName: string;
  columnName: string;
  selectedValues: string[];
  applyToAll?: boolean;
  displayColumns?: string[];
}

export interface ViewerSelectionData {
  selectedValues: string[];
  displayColumns?: string[];
}

class ReferenceTableService {
  private basePath = '/smart-hierarchy';

  /**
   * Create or update a reference table from CSV content
   */
  async createFromCSV(dto: CreateReferenceTableDto): Promise<ReferenceTableSummary> {
    const response = await axiosInstance.post(`${this.basePath}/reference-tables`, dto);
    return response.data;
  }

  /**
   * List all reference tables
   */
  async listTables(): Promise<ReferenceTableSummary[]> {
    const response = await axiosInstance.get(`${this.basePath}/reference-tables`);
    return response.data;
  }

  /**
   * Get a specific reference table
   */
  async getTable(tableName: string): Promise<ReferenceTableSummary> {
    const response = await axiosInstance.get(`${this.basePath}/reference-tables/${tableName}`);
    return response.data;
  }

  /**
   * Query table data with optional filtering
   */
  async queryTable(query: QueryReferenceTableDto): Promise<QueryResult> {
    const response = await axiosInstance.post(
      `${this.basePath}/reference-tables/${query.tableName}/query`,
      query
    );
    return response.data;
  }

  /**
   * Get distinct values for a column
   */
  async getDistinctValues(
    tableName: string,
    columnName: string,
    filterColumn?: string,
    filterValue?: string
  ): Promise<string[]> {
    const params = new URLSearchParams();
    if (filterColumn) params.append('filterColumn', filterColumn);
    if (filterValue) params.append('filterValue', filterValue);

    const queryString = params.toString();
    const url = `${this.basePath}/reference-tables/${tableName}/distinct/${columnName}${queryString ? `?${queryString}` : ''}`;

    const response = await axiosInstance.get(url);
    return response.data;
  }

  /**
   * Delete a reference table
   */
  async deleteTable(tableName: string): Promise<void> {
    await axiosInstance.delete(`${this.basePath}/reference-tables/${tableName}`);
  }

  /**
   * Save viewer selection for a hierarchy
   */
  async saveViewerSelection(dto: SaveViewerSelectionDto): Promise<void> {
    await axiosInstance.post(`${this.basePath}/viewer-selections`, dto);
  }

  /**
   * Get viewer selections for a project
   */
  async getViewerSelections(
    projectId: string,
    hierarchyId?: string
  ): Promise<Record<string, Record<string, ViewerSelectionData>>> {
    const params = hierarchyId ? `?hierarchyId=${hierarchyId}` : '';
    const response = await axiosInstance.get(`${this.basePath}/viewer-selections/${projectId}${params}`);
    return response.data;
  }

  /**
   * Delete viewer selection
   */
  async deleteViewerSelection(
    projectId: string,
    hierarchyId: string,
    tableName: string,
    columnName: string
  ): Promise<void> {
    await axiosInstance.delete(
      `${this.basePath}/viewer-selections/${projectId}/${hierarchyId}/${tableName}/${columnName}`
    );
  }

  /**
   * Load a CSV file as a reference table
   */
  async loadFromFile(file: File, options?: { displayName?: string; description?: string }): Promise<ReferenceTableSummary> {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = async (e) => {
        try {
          const csvContent = e.target?.result as string;
          const tableName = file.name.replace(/\.csv$/i, '').toLowerCase();

          const result = await this.createFromCSV({
            name: tableName,
            displayName: options?.displayName || file.name.replace(/\.csv$/i, ''),
            description: options?.description,
            csvContent,
            sourceFile: file.name,
          });

          resolve(result);
        } catch (error) {
          reject(error);
        }
      };
      reader.onerror = () => reject(new Error('Failed to read file'));
      reader.readAsText(file);
    });
  }
}

export const referenceTableService = new ReferenceTableService();
