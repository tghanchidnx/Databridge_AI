import { Injectable, NotFoundException, BadRequestException } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma/prisma.service';
import * as Papa from 'papaparse';

export interface ReferenceTableColumn {
  name: string;
  type: 'string' | 'number' | 'boolean' | 'date';
  isPrimaryKey?: boolean;
  sampleValues?: string[];
}

export interface CreateReferenceTableDto {
  name: string;
  displayName?: string;
  description?: string;
  csvContent: string;
  sourceFile?: string;
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
  createdAt: Date;
  updatedAt: Date;
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

@Injectable()
export class ReferenceTableService {
  constructor(private prisma: PrismaService) {}

  /**
   * Create a reference table from CSV content
   */
  async createFromCSV(userId: string, dto: CreateReferenceTableDto): Promise<ReferenceTableSummary> {
    // Parse CSV
    const parseResult = Papa.parse(dto.csvContent, {
      header: true,
      skipEmptyLines: true,
      transformHeader: (header) => header.trim(),
    });

    if (parseResult.errors.length > 0) {
      throw new BadRequestException(`CSV parsing errors: ${parseResult.errors.map(e => e.message).join(', ')}`);
    }

    const rows = parseResult.data as Record<string, any>[];
    if (rows.length === 0) {
      throw new BadRequestException('CSV file is empty');
    }

    // Detect columns and types
    const columns = this.detectColumns(rows);

    // Normalize table name
    const tableName = dto.name.toLowerCase().replace(/[^a-z0-9_]/g, '_');

    // Check if table already exists
    const existing = await this.prisma.referenceTable.findUnique({
      where: { userId_name: { userId, name: tableName } },
    });

    if (existing) {
      // Update existing table
      const updated = await this.prisma.referenceTable.update({
        where: { id: existing.id },
        data: {
          displayName: dto.displayName || dto.name,
          description: dto.description,
          sourceFile: dto.sourceFile,
          columns: columns as any,
          rowCount: rows.length,
          data: JSON.stringify(rows),
          updatedAt: new Date(),
        },
      });

      return this.toSummary(updated);
    }

    // Create new table
    const created = await this.prisma.referenceTable.create({
      data: {
        userId,
        name: tableName,
        displayName: dto.displayName || dto.name,
        description: dto.description,
        sourceFile: dto.sourceFile,
        columns: columns as any,
        rowCount: rows.length,
        data: JSON.stringify(rows),
        isActive: true,
      },
    });

    return this.toSummary(created);
  }

  /**
   * List all reference tables for a user
   */
  async listTables(userId: string): Promise<ReferenceTableSummary[]> {
    const tables = await this.prisma.referenceTable.findMany({
      where: { userId, isActive: true },
      orderBy: { displayName: 'asc' },
    });

    return tables.map(t => this.toSummary(t));
  }

  /**
   * Get a specific reference table
   */
  async getTable(userId: string, tableName: string): Promise<ReferenceTableSummary> {
    const table = await this.prisma.referenceTable.findFirst({
      where: { userId, name: tableName.toLowerCase(), isActive: true },
    });

    if (!table) {
      throw new NotFoundException(`Reference table '${tableName}' not found`);
    }

    return this.toSummary(table);
  }

  /**
   * Get table data with optional filtering
   */
  async queryTable(userId: string, query: QueryReferenceTableDto): Promise<{
    columns: string[];
    rows: Record<string, any>[];
    totalRows: number;
  }> {
    const table = await this.prisma.referenceTable.findFirst({
      where: { userId, name: query.tableName.toLowerCase(), isActive: true },
    });

    if (!table) {
      throw new NotFoundException(`Reference table '${query.tableName}' not found`);
    }

    let rows: Record<string, any>[] = JSON.parse(table.data);
    const totalRows = rows.length;

    // Apply filter
    if (query.filterColumn && query.filterValues && query.filterValues.length > 0) {
      const filterSet = new Set(query.filterValues.map(v => String(v).toLowerCase()));
      rows = rows.filter(row => {
        const value = String(row[query.filterColumn] || '').toLowerCase();
        return filterSet.has(value);
      });
    }

    // Select specific columns
    let columns = Object.keys(rows[0] || {});
    if (query.columns && query.columns.length > 0) {
      columns = query.columns;
      rows = rows.map(row => {
        const newRow: Record<string, any> = {};
        columns.forEach(col => {
          newRow[col] = row[col];
        });
        return newRow;
      });
    }

    // Get distinct values
    if (query.distinct && columns.length === 1) {
      const distinctValues = new Set<string>();
      rows.forEach(row => {
        const value = row[columns[0]];
        if (value !== null && value !== undefined && value !== '') {
          distinctValues.add(String(value));
        }
      });
      rows = Array.from(distinctValues).sort().map(v => ({ [columns[0]]: v }));
    }

    // Apply pagination
    const offset = query.offset || 0;
    const limit = query.limit || rows.length;
    rows = rows.slice(offset, offset + limit);

    return { columns, rows, totalRows };
  }

  /**
   * Get distinct values for a column, optionally filtered by another column's value
   */
  async getDistinctValues(
    userId: string,
    tableName: string,
    columnName: string,
    filterColumn?: string,
    filterValue?: string,
  ): Promise<string[]> {
    const table = await this.prisma.referenceTable.findFirst({
      where: { userId, name: tableName.toLowerCase(), isActive: true },
    });

    if (!table) {
      throw new NotFoundException(`Reference table '${tableName}' not found`);
    }

    let rows: Record<string, any>[] = JSON.parse(table.data);

    // Apply filter if provided
    if (filterColumn && filterValue) {
      rows = rows.filter(row => {
        const value = String(row[filterColumn] || '').toLowerCase();
        return value === filterValue.toLowerCase();
      });
    }

    // Get distinct values
    const distinctValues = new Set<string>();
    rows.forEach(row => {
      const value = row[columnName];
      if (value !== null && value !== undefined && value !== '') {
        distinctValues.add(String(value));
      }
    });

    return Array.from(distinctValues).sort();
  }

  /**
   * Delete a reference table
   */
  async deleteTable(userId: string, tableName: string): Promise<void> {
    const table = await this.prisma.referenceTable.findFirst({
      where: { userId, name: tableName.toLowerCase() },
    });

    if (!table) {
      throw new NotFoundException(`Reference table '${tableName}' not found`);
    }

    await this.prisma.referenceTable.delete({ where: { id: table.id } });
  }

  /**
   * Detect column types from data
   */
  private detectColumns(rows: Record<string, any>[]): ReferenceTableColumn[] {
    if (rows.length === 0) return [];

    const columns: ReferenceTableColumn[] = [];
    const sampleSize = Math.min(rows.length, 100);

    for (const colName of Object.keys(rows[0])) {
      let type: 'string' | 'number' | 'boolean' | 'date' = 'string';
      const sampleValues: string[] = [];

      // Check sample values to determine type
      let numberCount = 0;
      let boolCount = 0;
      let dateCount = 0;

      for (let i = 0; i < sampleSize; i++) {
        const value = rows[i][colName];
        if (value === null || value === undefined || value === '') continue;

        const strValue = String(value);
        if (sampleValues.length < 5 && !sampleValues.includes(strValue)) {
          sampleValues.push(strValue);
        }

        // Check if number
        if (!isNaN(Number(strValue)) && strValue.trim() !== '') {
          numberCount++;
        }

        // Check if boolean
        if (['true', 'false', '1', '0', 'yes', 'no'].includes(strValue.toLowerCase())) {
          boolCount++;
        }

        // Check if date (simple check)
        if (/^\d{4}-\d{2}-\d{2}/.test(strValue)) {
          dateCount++;
        }
      }

      // Determine type based on majority
      const threshold = sampleSize * 0.8;
      if (numberCount > threshold) {
        type = 'number';
      } else if (boolCount > threshold) {
        type = 'boolean';
      } else if (dateCount > threshold) {
        type = 'date';
      }

      columns.push({
        name: colName,
        type,
        isPrimaryKey: colName.toLowerCase().includes('id') || colName.toLowerCase().includes('code'),
        sampleValues,
      });
    }

    return columns;
  }

  private toSummary(table: any): ReferenceTableSummary {
    return {
      id: table.id,
      name: table.name,
      displayName: table.displayName,
      description: table.description,
      sourceFile: table.sourceFile,
      columns: table.columns as ReferenceTableColumn[],
      rowCount: table.rowCount,
      isActive: table.isActive,
      createdAt: table.createdAt,
      updatedAt: table.updatedAt,
    };
  }

  // ============================================================================
  // Hierarchy Viewer Selections
  // ============================================================================

  /**
   * Save viewer selection for a hierarchy
   */
  async saveViewerSelection(
    userId: string,
    projectId: string,
    hierarchyId: string,
    tableName: string,
    columnName: string,
    selectedValues: string[],
    applyToAll: boolean = false,
    displayColumns: string[] = [],
  ): Promise<void> {
    await this.prisma.hierarchyViewerSelection.upsert({
      where: {
        userId_projectId_hierarchyId_tableName_columnName: {
          userId,
          projectId,
          hierarchyId,
          tableName: tableName.toLowerCase(),
          columnName,
        },
      },
      create: {
        userId,
        projectId,
        hierarchyId,
        tableName: tableName.toLowerCase(),
        columnName,
        selectedValues: selectedValues as any,
        displayColumns: displayColumns as any,
        applyToAll,
      },
      update: {
        selectedValues: selectedValues as any,
        displayColumns: displayColumns as any,
        applyToAll,
        updatedAt: new Date(),
      },
    });

    // If applyToAll is true, apply to all hierarchies with same table in project
    if (applyToAll) {
      // Get all hierarchies in project with this table in mapping
      const hierarchies = await this.prisma.smartHierarchyMaster.findMany({
        where: { projectId },
        select: { hierarchyId: true, mapping: true },
      });

      for (const h of hierarchies) {
        if (h.hierarchyId === hierarchyId) continue;

        const mapping = h.mapping as any[];
        const hasTable = mapping?.some(m =>
          m.source_table?.toLowerCase() === tableName.toLowerCase() ||
          m.id_table?.toLowerCase() === tableName.toLowerCase()
        );

        if (hasTable) {
          await this.prisma.hierarchyViewerSelection.upsert({
            where: {
              userId_projectId_hierarchyId_tableName_columnName: {
                userId,
                projectId,
                hierarchyId: h.hierarchyId,
                tableName: tableName.toLowerCase(),
                columnName,
              },
            },
            create: {
              userId,
              projectId,
              hierarchyId: h.hierarchyId,
              tableName: tableName.toLowerCase(),
              columnName,
              selectedValues: selectedValues as any,
              displayColumns: displayColumns as any,
              applyToAll: false,
            },
            update: {
              selectedValues: selectedValues as any,
              displayColumns: displayColumns as any,
              updatedAt: new Date(),
            },
          });
        }
      }
    }
  }

  /**
   * Get viewer selections for a project
   */
  async getViewerSelections(
    userId: string,
    projectId: string,
    hierarchyId?: string,
  ): Promise<Record<string, Record<string, { selectedValues: string[]; displayColumns: string[] }>>> {
    const where: any = { userId, projectId };
    if (hierarchyId) {
      where.hierarchyId = hierarchyId;
    }

    const selections = await this.prisma.hierarchyViewerSelection.findMany({ where });

    // Group by hierarchyId -> tableName.columnName -> { selectedValues, displayColumns }
    const result: Record<string, Record<string, { selectedValues: string[]; displayColumns: string[] }>> = {};

    for (const sel of selections) {
      if (!result[sel.hierarchyId]) {
        result[sel.hierarchyId] = {};
      }
      const key = `${sel.tableName}.${sel.columnName}`;
      result[sel.hierarchyId][key] = {
        selectedValues: sel.selectedValues as string[],
        displayColumns: (sel.displayColumns as string[]) || [],
      };
    }

    return result;
  }

  /**
   * Delete viewer selection
   */
  async deleteViewerSelection(
    userId: string,
    projectId: string,
    hierarchyId: string,
    tableName: string,
    columnName: string,
  ): Promise<void> {
    await this.prisma.hierarchyViewerSelection.deleteMany({
      where: {
        userId,
        projectId,
        hierarchyId,
        tableName: tableName.toLowerCase(),
        columnName,
      },
    });
  }
}
