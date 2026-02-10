import {
  Injectable,
  NotFoundException,
  BadRequestException,
  ForbiddenException,
  ConflictException,
} from '@nestjs/common';
import { PrismaService } from '../../../database/prisma/prisma.service';
import {
  CreateSmartHierarchyDto,
  UpdateSmartHierarchyDto,
  ExportProjectDto,
  ImportProjectDto,
} from '../dto/smart-hierarchy.dto';
import type { SmartHierarchyMaster, ProjectExportData } from '../types/smart-hierarchy.types';

@Injectable()
export class SmartHierarchyService {
  constructor(private readonly prisma: PrismaService) {}

  // ============================================================================
  // Hierarchy Level Calculation
  // ============================================================================

  /**
   * Calculate hierarchyLevel JSON based on parent-child relationships
   * Fills level_1 through level_15 based on tree depth
   */
  private async calculateHierarchyLevels(
    projectId: string,
    hierarchyName: string,
    parentId: string | null,
  ): Promise<any> {
    const levels: any = {
      level_1: '',
      level_2: '',
      level_3: '',
      level_4: '',
      level_5: '',
      level_6: '',
      level_7: '',
      level_8: '',
      level_9: '',
      level_10: '',
      level_11: '',
      level_12: '',
      level_13: '',
      level_14: '',
      level_15: '',
    };

    if (!parentId) {
      // Root level - set only level_1
      levels.level_1 = hierarchyName;
      return levels;
    }

    // Get parent hierarchy to inherit its levels
    const parent = await this.prisma.smartHierarchyMaster.findUnique({
      where: { id: parentId },
    });

    if (!parent) {
      // If parent not found, treat as root
      levels.level_1 = hierarchyName;
      return levels;
    }

    const parentLevels = (parent.hierarchyLevel as any) || {};

    // Copy parent's levels and find the next empty level
    let currentLevel = 1;
    for (let i = 1; i <= 15; i++) {
      const levelKey = `level_${i}`;
      if (parentLevels[levelKey]) {
        levels[levelKey] = parentLevels[levelKey];
        currentLevel = i + 1;
      } else {
        break;
      }
    }

    // Add current hierarchy name at the next level
    if (currentLevel <= 15) {
      levels[`level_${currentLevel}`] = hierarchyName;
    } else {
      // Max depth reached - log warning but don't fail
      console.warn(`Hierarchy depth limit (15 levels) reached for hierarchy: ${hierarchyName}`);
    }

    return levels;
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  /**
   * Generate a unique hierarchy ID from hierarchy name
   * Format: SLUG_COUNTER (e.g., TOTAL_REVENUE_1, OPERATING_EXPENSES_2)
   */
  private async generateUniqueHierarchyId(
    projectId: string,
    hierarchyName: string,
  ): Promise<string> {
    // Create slug from hierarchy name (uppercase, replace spaces/special chars with underscore)
    const slug = hierarchyName
      .toUpperCase()
      .replace(/[^A-Z0-9]+/g, '_')
      .replace(/^_+|_+$/g, '') // Remove leading/trailing underscores
      .substring(0, 50); // Limit length

    // Find existing hierarchies with similar IDs to determine next counter
    const existingHierarchies = await this.prisma.smartHierarchyMaster.findMany({
      where: {
        projectId: projectId,
        hierarchyId: {
          startsWith: slug,
        },
      },
      select: { hierarchyId: true },
      orderBy: { hierarchyId: 'desc' },
    });

    let counter = 1;
    if (existingHierarchies.length > 0) {
      // Extract counters from existing IDs and find max
      const counters = existingHierarchies
        .map((h) => {
          const match = h.hierarchyId.match(new RegExp(`^${slug}_(\\d+)$`));
          return match ? parseInt(match[1], 10) : 0;
        })
        .filter((c) => c > 0);

      if (counters.length > 0) {
        counter = Math.max(...counters) + 1;
      }
    }

    const newId = `${slug}_${counter}`;

    // Double-check uniqueness (handle race conditions)
    const exists = await this.prisma.smartHierarchyMaster.findUnique({
      where: {
        projectId_hierarchyId: {
          projectId: projectId,
          hierarchyId: newId,
        },
      },
    });

    if (exists) {
      // Rare race condition - try next counter
      return `${slug}_${counter + 1}`;
    }

    return newId;
  }

  // ============================================================================
  // CRUD Operations
  // ============================================================================

  async create(dto: CreateSmartHierarchyDto): Promise<SmartHierarchyMaster> {
    // Generate unique hierarchy ID if not provided or if temporary frontend ID
    let hierarchyId = dto.hierarchyId;
    if (!hierarchyId || hierarchyId.startsWith('HIER_')) {
      hierarchyId = await this.generateUniqueHierarchyId(dto.projectId, dto.hierarchyName);
    } else {
      // Check if provided hierarchy ID already exists in project
      const existing = await this.prisma.smartHierarchyMaster.findUnique({
        where: {
          projectId_hierarchyId: {
            projectId: dto.projectId,
            hierarchyId: hierarchyId,
          },
        },
      });

      if (existing) {
        throw new BadRequestException(
          `Hierarchy with ID '${hierarchyId}' already exists in this project`,
        );
      }
    }

    // Validate project exists
    const project = await this.prisma.hierarchyProject.findUnique({
      where: { id: dto.projectId },
    });

    if (!project) {
      throw new NotFoundException(`Project '${dto.projectId}' not found`);
    }

    // Validate parent exists if parentId is provided and get the UUID
    let parentUuid: string | null = null;
    if (dto.parentId) {
      const parent = await this.prisma.smartHierarchyMaster.findUnique({
        where: {
          projectId_hierarchyId: {
            projectId: dto.projectId,
            hierarchyId: dto.parentId,
          },
        },
      });

      if (!parent) {
        throw new NotFoundException(`Parent hierarchy '${dto.parentId}' not found`);
      }

      // Store the parent's UUID (id) for the database relationship
      parentUuid = parent.id;
    }

    // Determine isRoot and sortOrder
    const isRoot = !dto.parentId;
    let sortOrder = dto.sortOrder ?? 0;

    if (sortOrder === 0) {
      // Auto-assign sortOrder based on existing hierarchies at same level
      const siblings = await this.prisma.smartHierarchyMaster.findMany({
        where: {
          projectId: dto.projectId,
          parentId: parentUuid || null,
        },
        orderBy: { sortOrder: 'desc' },
        take: 1,
      });
      sortOrder = siblings.length > 0 ? (siblings[0].sortOrder || 0) + 1 : 1;
    }

    // Ensure mapping indexes are sequential
    const sortedMapping = dto.mapping
      .sort((a, b) => a.mapping_index - b.mapping_index)
      .map((m, idx) => ({ ...m, mapping_index: idx + 1 }));

    // Calculate hierarchyLevel based on parent-child relationships
    // This overrides any level data provided in DTO to ensure consistency
    const calculatedHierarchyLevel = await this.calculateHierarchyLevels(
      dto.projectId,
      dto.hierarchyName,
      parentUuid,
    );

    const created = await this.prisma.smartHierarchyMaster.create({
      data: {
        projectId: dto.projectId,
        hierarchyId: hierarchyId,
        hierarchyName: dto.hierarchyName,
        description: dto.description,
        parentId: parentUuid || null,
        isRoot,
        sortOrder,
        hierarchyLevel: calculatedHierarchyLevel as any,
        flags: dto.flags as any,
        mapping: sortedMapping as any,
        formulaConfig: dto.formulaConfig as any,
        filterConfig: dto.filterConfig as any,
        pivotConfig: dto.pivotConfig as any,
        metadata: dto.metadata as any,
        createdBy: dto.createdBy,
        updatedBy: dto.updatedBy || dto.createdBy, // Use updatedBy if provided, otherwise same as createdBy
      },
    });

    return await this.toSmartHierarchyMaster(created);
  }

  async findAll(projectId: string): Promise<SmartHierarchyMaster[]> {
    const hierarchies = await this.prisma.smartHierarchyMaster.findMany({
      where: { projectId },
      orderBy: [{ sortOrder: 'asc' }, { createdAt: 'asc' }],
    });

    return Promise.all(hierarchies.map((h) => this.toSmartHierarchyMaster(h)));
  }

  async findAllMinimal(projectId: string): Promise<any[]> {
    const hierarchies = await this.prisma.smartHierarchyMaster.findMany({
      where: { projectId },
      orderBy: [{ sortOrder: 'asc' }, { createdAt: 'asc' }],
      select: {
        id: true,
        projectId: true,
        hierarchyId: true,
        hierarchyName: true,
        description: true,
        parentId: true,
        isRoot: true,
        sortOrder: true,
        flags: true,
        formulaConfig: true,
        hierarchyLevel: true,
        mapping: true,
        createdAt: true,
        updatedAt: true,
      },
    });

    // Get parent hierarchyIds mapping for all records
    const parentUuids = hierarchies
      .map((h) => h.parentId)
      .filter((id): id is string => id !== null);
    const parentRecords = await this.prisma.smartHierarchyMaster.findMany({
      where: { id: { in: parentUuids } },
      select: { id: true, hierarchyId: true },
    });
    const parentMap = new Map(parentRecords.map((p) => [p.id, p.hierarchyId]));

    // Build children count map
    const childrenCountMap = new Map<string, number>();
    hierarchies.forEach((h) => {
      if (h.parentId) {
        const parentHierarchyId = parentMap.get(h.parentId);
        if (parentHierarchyId) {
          childrenCountMap.set(
            parentHierarchyId,
            (childrenCountMap.get(parentHierarchyId) || 0) + 1,
          );
        }
      }
    });

    return hierarchies.map((h) => {
      // Count active levels
      const levelsCount = this.extractActiveLevels(h.hierarchyLevel).length;

      // Count mappings (sources)
      const sourcesCount = Array.isArray(h.mapping) ? h.mapping.length : 0;

      // Get children count
      const childrenCount = childrenCountMap.get(h.hierarchyId) || 0;

      return {
        id: h.id,
        projectId: h.projectId,
        hierarchyId: h.hierarchyId,
        hierarchyName: h.hierarchyName,
        description: h.description,
        parentId: h.parentId ? parentMap.get(h.parentId) || null : null,
        isRoot: h.isRoot ?? true,
        sortOrder: h.sortOrder ?? 0,
        flags: h.flags,
        formulaConfig: h.formulaConfig,
        hasFormula: !!h.formulaConfig,
        levelsCount,
        sourcesCount,
        childrenCount,
        createdAt: h.createdAt,
        updatedAt: h.updatedAt,
      };
    });
  }

  async findOne(projectId: string, hierarchyId: string): Promise<SmartHierarchyMaster> {
    const hierarchy = await this.prisma.smartHierarchyMaster.findUnique({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId,
        },
      },
    });

    if (!hierarchy) {
      throw new NotFoundException(`Hierarchy '${hierarchyId}' not found in project '${projectId}'`);
    }

    return await this.toSmartHierarchyMaster(hierarchy);
  }

  async getHierarchyDependencies(projectId: string, hierarchyId: string): Promise<any> {
    const hierarchy = await this.findOne(projectId, hierarchyId);

    // Get all hierarchies in project to find relationships
    const allHierarchies = await this.prisma.smartHierarchyMaster.findMany({
      where: { projectId },
      select: {
        hierarchyId: true,
        hierarchyName: true,
        parentId: true,
        mapping: true,
      },
    });

    // Find parent
    const parent = hierarchy.parentId
      ? allHierarchies.find((h) => h.hierarchyId === hierarchy.parentId)
      : null;

    // Find children
    const children = allHierarchies.filter((h) => h.parentId === hierarchy.hierarchyId);

    // Find dependencies based on mapping sources
    const dependencies: any[] = [];
    if (hierarchy.mapping && Array.isArray(hierarchy.mapping)) {
      for (const mapping of hierarchy.mapping as any[]) {
        const sourceKey = `${mapping.source_database}.${mapping.source_schema}.${mapping.source_table}`;

        // Find other hierarchies using same source
        const relatedHierarchies = allHierarchies.filter((h) => {
          if (h.hierarchyId === hierarchyId || !h.mapping) return false;
          return (h.mapping as any[]).some(
            (m) => `${m.source_database}.${m.source_schema}.${m.source_table}` === sourceKey,
          );
        });

        if (relatedHierarchies.length > 0) {
          dependencies.push({
            source: sourceKey,
            relatedHierarchies: relatedHierarchies.map((h) => ({
              hierarchyId: h.hierarchyId,
              hierarchyName: h.hierarchyName,
            })),
          });
        }
      }
    }

    return {
      hierarchyId: hierarchy.hierarchyId,
      hierarchyName: hierarchy.hierarchyName,
      parent: parent
        ? {
            hierarchyId: parent.hierarchyId,
            hierarchyName: parent.hierarchyName,
          }
        : null,
      children: children.map((c) => ({
        hierarchyId: c.hierarchyId,
        hierarchyName: c.hierarchyName,
      })),
      dependencies,
      dependencyCount: dependencies.length,
    };
  }

  async update(
    projectId: string,
    hierarchyId: string,
    dto: UpdateSmartHierarchyDto,
  ): Promise<SmartHierarchyMaster> {
    // Check if hierarchy exists
    const existing = await this.findOne(projectId, hierarchyId);

    // Validate parent exists if parentId is being changed and convert to UUID
    let parentUuid: string | null | undefined = undefined;
    if (dto.parentId !== undefined) {
      if (dto.parentId === null) {
        parentUuid = null;
      } else {
        if (dto.parentId === hierarchyId) {
          throw new BadRequestException('Hierarchy cannot be its own parent');
        }

        const parent = await this.prisma.smartHierarchyMaster.findUnique({
          where: {
            projectId_hierarchyId: {
              projectId,
              hierarchyId: dto.parentId,
            },
          },
        });

        if (!parent) {
          throw new NotFoundException(`Parent hierarchy '${dto.parentId}' not found`);
        }

        parentUuid = parent.id;

        // Check for circular dependency using UUID
        let currentParent = parent;
        while (currentParent.parentId) {
          if (currentParent.parentId === existing.id) {
            throw new BadRequestException('Circular dependency detected: Cannot set parent');
          }
          const nextParent = await this.prisma.smartHierarchyMaster.findUnique({
            where: { id: currentParent.parentId },
          });
          if (!nextParent) break;
          currentParent = nextParent;
        }
      }
    }

    // If mapping is being updated, ensure indexes are sequential
    let sortedMapping = undefined;
    if (dto.mapping) {
      sortedMapping = dto.mapping
        .sort((a, b) => a.mapping_index - b.mapping_index)
        .map((m, idx) => ({ ...m, mapping_index: idx + 1 }));
    }

    // Deep merge JSON fields
    const updateData: any = {};
    if (dto.hierarchyName) updateData.hierarchyName = dto.hierarchyName;
    if (dto.description !== undefined) updateData.description = dto.description;
    if (parentUuid !== undefined) {
      updateData.parentId = parentUuid;
      updateData.isRoot = !parentUuid;
    }
    if (dto.sortOrder !== undefined) updateData.sortOrder = dto.sortOrder;

    // Recalculate hierarchyLevel if parentId or hierarchyName changes
    // This ensures levels stay consistent with tree structure
    if (parentUuid !== undefined || dto.hierarchyName) {
      const newParentId = parentUuid !== undefined ? parentUuid : existing.parentId;
      const newHierarchyName = dto.hierarchyName || existing.hierarchyName;
      updateData.hierarchyLevel = (await this.calculateHierarchyLevels(
        projectId,
        newHierarchyName,
        newParentId,
      )) as any;
    } else if (dto.hierarchyLevel) {
      // Only use provided hierarchyLevel if no structural changes
      updateData.hierarchyLevel = dto.hierarchyLevel as any;
    }

    if (dto.flags) updateData.flags = dto.flags as any;
    if (sortedMapping) updateData.mapping = sortedMapping as any;
    if (dto.formulaConfig !== undefined) updateData.formulaConfig = dto.formulaConfig as any;
    if (dto.filterConfig !== undefined) updateData.filterConfig = dto.filterConfig as any;
    if (dto.pivotConfig !== undefined) updateData.pivotConfig = dto.pivotConfig as any;
    if (dto.metadata !== undefined) updateData.metadata = dto.metadata as any;
    if (dto.updatedBy) updateData.updatedBy = dto.updatedBy;

    const updated = await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId,
        },
      },
      data: updateData,
    });

    return await this.toSmartHierarchyMaster(updated);
  }

  async delete(projectId: string, hierarchyId: string): Promise<void> {
    await this.findOne(projectId, hierarchyId); // Ensure it exists

    await this.prisma.smartHierarchyMaster.delete({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId,
        },
      },
    });
  }

  async bulkDelete(
    projectId: string,
    hierarchyIds: string[],
  ): Promise<{
    deleted: number;
    failed: number;
    errors: Array<{ hierarchyId: string; error: string }>;
  }> {
    const result = {
      deleted: 0,
      failed: 0,
      errors: [] as Array<{ hierarchyId: string; error: string }>,
    };

    // Fetch all hierarchies in the project to understand parent-child relationships
    const allHierarchies = await this.prisma.smartHierarchyMaster.findMany({
      where: { projectId },
      select: { id: true, hierarchyId: true, parentId: true },
    });

    // Build a map for quick lookup
    const hierarchyMap = new Map(allHierarchies.map((h) => [h.hierarchyId, h]));

    // Filter to only the hierarchies we want to delete
    const toDelete = hierarchyIds.map((hId) => hierarchyMap.get(hId)).filter(Boolean);

    // Sort hierarchies bottom-up (children first, then parents)
    // This prevents cascade deletion errors when parent is deleted before children
    const sortedHierarchies = this.sortHierarchiesBottomUp(toDelete, hierarchyMap);

    // Process deletions in bottom-up order
    for (const hierarchy of sortedHierarchies) {
      try {
        await this.delete(projectId, hierarchy.hierarchyId);
        result.deleted++;
      } catch (error: any) {
        result.failed++;
        result.errors.push({
          hierarchyId: hierarchy.hierarchyId,
          error: error.message || 'Unknown error',
        });
      }
    }

    return result;
  }

  /**
   * Sort hierarchies in bottom-up order (children before parents)
   * This prevents cascade deletion errors
   */
  private sortHierarchiesBottomUp(hierarchies: any[], hierarchyMap: Map<string, any>): any[] {
    // Calculate depth for each hierarchy
    const depthMap = new Map<string, number>();

    const calculateDepth = (hierarchyId: string): number => {
      if (depthMap.has(hierarchyId)) {
        return depthMap.get(hierarchyId)!;
      }

      const hierarchy = hierarchyMap.get(hierarchyId);
      if (!hierarchy || !hierarchy.parentId) {
        depthMap.set(hierarchyId, 0);
        return 0;
      }

      // Find parent hierarchy
      const parent = Array.from(hierarchyMap.values()).find((h) => h.id === hierarchy.parentId);

      if (!parent) {
        depthMap.set(hierarchyId, 0);
        return 0;
      }

      const depth = calculateDepth(parent.hierarchyId) + 1;
      depthMap.set(hierarchyId, depth);
      return depth;
    };

    // Calculate depths for all hierarchies to delete
    hierarchies.forEach((h) => calculateDepth(h.hierarchyId));

    // Sort by depth descending (deepest first = children first)
    return hierarchies.sort((a, b) => {
      const depthA = depthMap.get(a.hierarchyId) || 0;
      const depthB = depthMap.get(b.hierarchyId) || 0;
      return depthB - depthA;
    });
  }

  async bulkUpdateOrder(
    projectId: string,
    updates: Array<{
      hierarchyId: string;
      parentId?: string | null;
      isRoot?: boolean;
      sortOrder: number;
    }>,
  ): Promise<{
    updated: number;
    failed: number;
    errors: Array<{ hierarchyId: string; error: string }>;
  }> {
    const result = {
      updated: 0,
      failed: 0,
      errors: [] as Array<{ hierarchyId: string; error: string }>,
    };

    // Process each update in a transaction
    try {
      await this.prisma.$transaction(async (prisma) => {
        for (const update of updates) {
          try {
            // Find the hierarchy UUID
            const hierarchy = await prisma.smartHierarchyMaster.findFirst({
              where: {
                projectId,
                hierarchyId: update.hierarchyId,
              },
            });

            if (!hierarchy) {
              result.failed++;
              result.errors.push({
                hierarchyId: update.hierarchyId,
                error: 'Hierarchy not found',
              });
              continue;
            }

            // Resolve parent UUID if parentId is provided
            let parentUuid: string | null = null;
            if (update.parentId) {
              const parentHierarchy = await prisma.smartHierarchyMaster.findFirst({
                where: {
                  projectId,
                  hierarchyId: update.parentId,
                },
                select: { id: true },
              });

              if (!parentHierarchy) {
                result.failed++;
                result.errors.push({
                  hierarchyId: update.hierarchyId,
                  error: `Parent hierarchy '${update.parentId}' not found`,
                });
                continue;
              }
              parentUuid = parentHierarchy.id;
            }

            // Update the hierarchy
            await prisma.smartHierarchyMaster.update({
              where: { id: hierarchy.id },
              data: {
                parentId: parentUuid,
                isRoot: update.isRoot ?? !update.parentId,
                sortOrder: update.sortOrder,
              },
            });

            result.updated++;
          } catch (error: any) {
            result.failed++;
            result.errors.push({
              hierarchyId: update.hierarchyId,
              error: error.message || 'Unknown error',
            });
          }
        }
      });
    } catch (error: any) {
      throw new BadRequestException(`Bulk update failed: ${error.message}`);
    }

    return result;
  }

  // ============================================================================
  // Export/Import Operations
  // ============================================================================

  async exportProject(dto: ExportProjectDto): Promise<any> {
    const project = await this.prisma.hierarchyProject.findUnique({
      where: { id: dto.projectId },
    });

    if (!project) {
      throw new NotFoundException(`Project '${dto.projectId}' not found`);
    }

    const hierarchies = await this.findAll(dto.projectId);

    // Add parentHierarchyId to each hierarchy for proper cross-project import
    // Note: h.parentId is already the hierarchyId string (converted by toSmartHierarchyMaster)
    const hierarchiesWithParentRef = hierarchies.map((h) => {
      console.log(`Hierarchy ${h.hierarchyId}: parentId=${h.parentId}`);
      return {
        ...h,
        parentHierarchyId: h.parentId, // parentId is already hierarchyId string
      };
    });

    const exportData: ProjectExportData = {
      project_info: {
        id: project.id,
        name: project.name,
        description: project.description || undefined,
        created_at: project.createdAt,
      },
      hierarchies: hierarchiesWithParentRef,
      metadata: project.metadata as any,
    };

    // Project configuration export is ALWAYS JSON
    // For CSV export, use separate endpoints: export-hierarchy-csv and export-mapping-csv
    const format = 'json';
    const contentType = 'application/json';

    // Exports are now returned directly (not saved to database)
    // Use DeploymentHistory for tracking deployed configurations
    return {
      exportId: `export_${Date.now()}`, // Generate a temporary ID
      exportData: exportData, // Always return JSON object
      format,
      contentType,
      createdAt: new Date(),
    };
  }

  private convertToCSV(hierarchies: SmartHierarchyMaster[], project: any): string {
    const rows: string[] = [];

    // Header row - simplified to avoid level complexity
    const headers = [
      'project_id',
      'project_name',
      'hierarchy_id',
      'hierarchy_name',
      'description',
      'parent_hierarchy_id',
      'sort_order',
      'is_root',
      'flags_json',
      'mapping_json',
      'formula_config_json',
      'filter_config_json',
      'pivot_config_json',
      'metadata_json',
      'created_by',
      'created_at',
    ];
    rows.push(headers.join(','));

    // Data rows
    for (const h of hierarchies) {
      const row = [
        this.escapeCSV(project.id),
        this.escapeCSV(project.name),
        this.escapeCSV(h.hierarchyId),
        this.escapeCSV(h.hierarchyName),
        this.escapeCSV(h.description || ''),
        this.escapeCSV((h as any).parentHierarchyId || ''),
        h.sortOrder?.toString() || '0',
        h.isRoot ? 'true' : 'false',
        this.escapeCSV(JSON.stringify(h.flags)),
        this.escapeCSV(JSON.stringify(h.mapping)),
        this.escapeCSV(h.formulaConfig ? JSON.stringify(h.formulaConfig) : ''),
        this.escapeCSV(h.filterConfig ? JSON.stringify(h.filterConfig) : ''),
        this.escapeCSV(h.pivotConfig ? JSON.stringify(h.pivotConfig) : ''),
        this.escapeCSV(h.metadata ? JSON.stringify(h.metadata) : ''),
        this.escapeCSV(h.createdBy || ''),
        this.escapeCSV(h.createdAt?.toISOString() || ''),
      ];
      rows.push(row.join(','));
    }

    return rows.join('\n');
  }
  private escapeCSV(value: string): string {
    if (!value) return '""';
    const str = value.toString();
    if (str.includes(',') || str.includes('"') || str.includes('\n')) {
      return `"${str.replace(/"/g, '""')}"`;
    }
    return `"${str}"`;
  }

  async importProject(dto: ImportProjectDto): Promise<{ imported: number; skipped: number }> {
    const { projectId, exportData, format } = dto;

    // Validate project exists
    const project = await this.prisma.hierarchyProject.findUnique({
      where: { id: projectId },
    });

    if (!project) {
      throw new NotFoundException(`Project '${projectId}' not found`);
    }

    let hierarchies: any[] = [];

    if (format === 'csv') {
      // Parse CSV format
      hierarchies = this.parseCSV(exportData);
      console.log(`Parsed CSV: ${hierarchies.length} hierarchies`);
      if (hierarchies.length > 0) {
        console.log('First hierarchy:', JSON.stringify(hierarchies[0], null, 2));
      }
    } else {
      // JSON format
      hierarchies = exportData.hierarchies || [];
      console.log(`JSON format: ${hierarchies.length} hierarchies`);
      if (hierarchies.length > 0) {
        console.log('First hierarchy:', JSON.stringify(hierarchies[0], null, 2));
      }
    }

    let imported = 0;
    let skipped = 0;

    console.log(
      `Starting import: ${hierarchies.length} hierarchies to import into project ${projectId}`,
    );

    // Two-pass import to handle parent-child relationships correctly
    // Pass 1: Create all hierarchies without parent relationships
    const hierarchyIdToUuidMap = new Map<string, string>();

    for (const hierarchy of hierarchies) {
      try {
        console.log(`Checking hierarchy: ${hierarchy.hierarchyId}`);

        // Check if hierarchy already exists
        const existing = await this.prisma.smartHierarchyMaster.findUnique({
          where: {
            projectId_hierarchyId: {
              projectId,
              hierarchyId: hierarchy.hierarchyId,
            },
          },
        });

        if (existing) {
          console.log(`Hierarchy ${hierarchy.hierarchyId} already exists, skipping`);
          skipped++;
          hierarchyIdToUuidMap.set(hierarchy.hierarchyId, existing.id);
          continue;
        }

        console.log(`Creating hierarchy: ${hierarchy.hierarchyId} (${hierarchy.hierarchyName})`);

        // Create hierarchy WITHOUT parent link (will be added in pass 2)
        const created = await this.prisma.smartHierarchyMaster.create({
          data: {
            projectId,
            hierarchyId: hierarchy.hierarchyId,
            hierarchyName: hierarchy.hierarchyName,
            description: hierarchy.description || null,
            parentId: null, // Will be set in pass 2
            sortOrder: hierarchy.sortOrder || 0,
            isRoot: true, // Temporarily true, will be updated in pass 2
            hierarchyLevel: hierarchy.hierarchyLevel as any,
            flags: (hierarchy.flags as any) || {},
            mapping: (hierarchy.mapping as any) || [],
            formulaConfig: hierarchy.formulaConfig as any,
            filterConfig: hierarchy.filterConfig as any,
            pivotConfig: hierarchy.pivotConfig as any,
            metadata: hierarchy.metadata as any,
            createdBy: hierarchy.createdBy || 'system',
          },
        });

        console.log(`Created hierarchy ${hierarchy.hierarchyId} with UUID ${created.id}`);
        hierarchyIdToUuidMap.set(hierarchy.hierarchyId, created.id);
        imported++;
      } catch (error) {
        console.error(
          `Failed to create hierarchy ${hierarchy.hierarchyId}:`,
          error.message || error,
        );
        skipped++;
      }
    }

    // Pass 2: Update parent relationships using hierarchyId references
    for (const hierarchy of hierarchies) {
      const parentHierarchyIdRef = hierarchy.parentHierarchyId || hierarchy.parentId;

      if (parentHierarchyIdRef) {
        try {
          const childUuid = hierarchyIdToUuidMap.get(hierarchy.hierarchyId);
          const parentUuid = hierarchyIdToUuidMap.get(parentHierarchyIdRef);

          if (childUuid && parentUuid) {
            await this.prisma.smartHierarchyMaster.update({
              where: { id: childUuid },
              data: {
                parentId: parentUuid,
                isRoot: false,
              },
            });
            console.log(`Linked ${hierarchy.hierarchyId} to parent ${parentHierarchyIdRef}`);
          } else {
            console.warn(
              `Could not link ${hierarchy.hierarchyId}: child=${childUuid}, parent=${parentUuid}`,
            );
          }
        } catch (error) {
          console.error(`Failed to link parent for hierarchy ${hierarchy.hierarchyId}:`, error);
        }
      }
    }

    return { imported, skipped };
  }

  private parseCSV(csvContent: string): any[] {
    const lines = csvContent.split('\n').filter((line) => line.trim());
    console.log(`parseCSV: Processing ${lines.length} lines`);
    if (lines.length < 2) return [];

    // Parse header
    const headers = this.parseCSVLine(lines[0]);
    console.log(`Headers: ${headers.join(', ')}`);
    const hierarchies: any[] = [];

    // Parse data rows
    for (let i = 1; i < lines.length; i++) {
      const values = this.parseCSVLine(lines[i]);
      if (values.length !== headers.length) {
        console.warn(
          `Row ${i}: Column count mismatch. Expected ${headers.length}, got ${values.length}`,
        );
        continue;
      }

      const row: any = {};
      headers.forEach((header, index) => {
        row[header] = values[index];
      });

      // Build hierarchy object - parentHierarchyId will contain hierarchyId string
      // Remove quotes and check for empty strings
      const cleanValue = (val: string) => {
        if (!val) return null;
        const cleaned = val.replace(/^"|"$/g, '').trim();
        return cleaned === '' ? null : cleaned;
      };

      // Extract parent from metadata if parent_hierarchy_id is empty (fallback for old exports)
      let parentHierarchyId = cleanValue(row.parent_hierarchy_id);
      if (!parentHierarchyId && row.metadata_json) {
        try {
          const metadata = JSON.parse(row.metadata_json);
          if (metadata.parentHierarchyId) {
            parentHierarchyId = metadata.parentHierarchyId;
            console.log(
              `Row ${i}: Extracted parentHierarchyId from metadata: ${parentHierarchyId} for hierarchy ${cleanValue(row.hierarchy_id)}`,
            );
          }
        } catch (e) {
          console.warn(`Row ${i}: Failed to parse metadata_json:`, e.message);
        }
      }

      const hierarchy: any = {
        hierarchyId: cleanValue(row.hierarchy_id),
        hierarchyName: cleanValue(row.hierarchy_name),
        description: cleanValue(row.description),
        parentHierarchyId: parentHierarchyId, // Use parentHierarchyId for consistency
        sortOrder: parseInt(row.sort_order) || 0,
        isRoot: row.is_root === 'true',
        hierarchyLevel: {}, // Add empty hierarchyLevel object
        flags: cleanValue(row.flags_json) ? JSON.parse(row.flags_json) : {},
        mapping: cleanValue(row.mapping_json) ? JSON.parse(row.mapping_json) : [],
        formulaConfig: cleanValue(row.formula_config_json)
          ? JSON.parse(row.formula_config_json)
          : null,
        filterConfig: cleanValue(row.filter_config_json)
          ? JSON.parse(row.filter_config_json)
          : null,
        pivotConfig: cleanValue(row.pivot_config_json) ? JSON.parse(row.pivot_config_json) : null,
        metadata: cleanValue(row.metadata_json) ? JSON.parse(row.metadata_json) : null,
        createdBy: cleanValue(row.created_by),
      };

      console.log(
        `Row ${i}: Parsed hierarchy ${hierarchy.hierarchyId}, parent=${hierarchy.parentHierarchyId}, isRoot=${hierarchy.isRoot}`,
      );
      hierarchies.push(hierarchy);
    }

    console.log(`parseCSV: Successfully parsed ${hierarchies.length} hierarchies`);
    return hierarchies;
  }

  private parseCSVLine(line: string): string[] {
    const values: string[] = [];
    let current = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      const nextChar = line[i + 1];

      if (char === '"') {
        if (inQuotes && nextChar === '"') {
          current += '"';
          i++; // Skip next quote
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === ',' && !inQuotes) {
        values.push(current);
        current = '';
      } else {
        current += char;
      }
    }

    values.push(current);
    return values;
  }

  // Export list and retrieval now use DeploymentHistory
  async listExports(projectId: string): Promise<any[]> {
    // Return deployment history as exports for backwards compatibility
    const deployments = await this.prisma.deploymentHistory.findMany({
      where: { projectId },
      orderBy: { deployedAt: 'desc' },
      select: {
        id: true,
        masterTableName: true,
        database: true,
        schema: true,
        deployedBy: true,
        deployedAt: true,
        status: true,
        hierarchyIds: true,
      },
      take: 50, // Limit to last 50 deployments
    });

    // Map to export-like format
    return deployments.map((d) => ({
      id: d.id,
      exportName: `${d.database}.${d.schema}.${d.masterTableName}`,
      description: `Deployed ${(d.hierarchyIds as string[]).length} hierarchies`,
      exportType: 'deployment',
      version: d.deployedAt.toISOString(),
      createdBy: d.deployedBy,
      createdAt: d.deployedAt,
      status: d.status,
    }));
  }

  async getExport(exportId: string): Promise<any> {
    // Try to get from deployment history
    const deployment = await this.prisma.deploymentHistory.findUnique({
      where: { id: exportId },
    });

    if (!deployment) {
      throw new NotFoundException(`Export '${exportId}' not found`);
    }

    return {
      id: deployment.id,
      projectId: deployment.projectId,
      exportName: `${deployment.database}.${deployment.schema}.${deployment.masterTableName}`,
      description: `Deployed ${(deployment.hierarchyIds as string[]).length} hierarchies`,
      exportData: {
        hierarchyIds: deployment.hierarchyIds,
        hierarchyNames: deployment.hierarchyNames,
        scripts: {
          insert: deployment.insertScript,
          view: deployment.viewScript,
          mapping: deployment.mappingScript,
          dynamicTable: deployment.dynamicTableScript,
        },
      },
      exportType: 'deployment',
      version: deployment.deployedAt.toISOString(),
      createdBy: deployment.deployedBy,
      createdAt: deployment.deployedAt,
    };
  }

  // ============================================================================
  // Tree Building for Frontend
  // ============================================================================

  async buildHierarchyTree(projectId: string): Promise<any[]> {
    const hierarchies = await this.findAll(projectId);

    return hierarchies.map((h) => {
      // Extract active levels
      const levels = this.extractActiveLevels(h.hierarchyLevel);

      return {
        id: h.hierarchyId,
        name: h.hierarchyName,
        description: h.description,
        levels,
        mappingCount: h.mapping.length,
        flags: h.flags,
        hasFormula: !!h.formulaConfig,
        hasFilter: !!h.filterConfig,
        hasPivot: !!h.pivotConfig,
      };
    });
  }

  // ============================================================================
  // Project Management Methods
  // ============================================================================

  async getProjects(userId: string): Promise<any[]> {
    // Get projects owned by user
    const ownedProjects = await this.prisma.hierarchyProject.findMany({
      where: {
        userId,
        isActive: true,
      },
      orderBy: { createdAt: 'desc' },
      select: {
        id: true,
        name: true,
        description: true,
        userId: true,
        organizationId: true,
        isActive: true,
        createdAt: true,
        updatedAt: true,
      },
    });

    // Get projects where user is a member (accepted invitations)
    const memberProjects = await this.prisma.hierarchyProject.findMany({
      where: {
        isActive: true,
        members: {
          some: {
            userId,
            invitationStatus: 'accepted',
            isActive: true,
          },
        },
      },
      orderBy: { createdAt: 'desc' },
      select: {
        id: true,
        name: true,
        description: true,
        userId: true,
        organizationId: true,
        isActive: true,
        createdAt: true,
        updatedAt: true,
      },
    });

    // Combine and deduplicate projects
    const projectMap = new Map();
    [...ownedProjects, ...memberProjects].forEach((p) => projectMap.set(p.id, p));
    return Array.from(projectMap.values());
  }

  // ============================================================================
  // Dashboard Endpoints
  // ============================================================================

  async getDashboardStats(userId: string): Promise<any> {
    // Get user's projects count
    const projectsCount = await this.prisma.hierarchyProject.count({
      where: {
        OR: [
          { userId },
          {
            members: {
              some: {
                userId,
                invitationStatus: 'accepted',
                isActive: true,
              },
            },
          },
        ],
        isActive: true,
      },
    });

    // Get active connections count
    const totalConnections = await this.prisma.connection.count({
      where: { userId },
    });

    const activeConnectionsCount = await this.prisma.connection.count({
      where: {
        userId,
        status: 'active',
      },
    });

    // Get schema comparisons count
    const schemaComparisonsCount = await this.prisma.schemaComparisonJob.count({
      where: { userId },
    });

    // Get this month's comparisons
    const startOfMonth = new Date();
    startOfMonth.setDate(1);
    startOfMonth.setHours(0, 0, 0, 0);

    const monthlyComparisons = await this.prisma.schemaComparisonJob.count({
      where: {
        userId,
        createdAt: { gte: startOfMonth },
      },
    });

    // Get this week's comparisons
    const startOfWeek = new Date();
    const dayOfWeek = startOfWeek.getDay();
    startOfWeek.setDate(startOfWeek.getDate() - dayOfWeek);
    startOfWeek.setHours(0, 0, 0, 0);

    const weeklyComparisons = await this.prisma.schemaComparisonJob.count({
      where: {
        userId,
        createdAt: { gte: startOfWeek },
      },
    });

    // Get team members count from projects (unique members across all user's projects)
    const userProjects = await this.prisma.hierarchyProject.findMany({
      where: {
        OR: [
          { userId },
          {
            members: {
              some: {
                userId,
                invitationStatus: 'accepted',
                isActive: true,
              },
            },
          },
        ],
        isActive: true,
      },
      include: {
        members: {
          where: {
            invitationStatus: 'accepted',
            isActive: true,
          },
          select: {
            userId: true,
          },
        },
      },
    });

    // Count unique team members across all projects
    const uniqueMembers = new Set<string>();
    userProjects.forEach((project) => {
      // Add project owner
      uniqueMembers.add(project.userId);
      // Add all members
      project.members.forEach((member) => {
        if (member.userId) {
          uniqueMembers.add(member.userId);
        }
      });
    });
    const teamMembersCount = uniqueMembers.size;

    return {
      activeConnections: {
        count: activeConnectionsCount,
        total: totalConnections,
      },
      hierarchyProjects: {
        count: projectsCount,
        change: `${projectsCount} total`,
      },
      schemaComparisons: {
        count: schemaComparisonsCount,
        change: `+${monthlyComparisons} this month`,
        monthlyIncrease: monthlyComparisons,
      },
      weeklyComparisons: {
        count: weeklyComparisons,
        change: `+${weeklyComparisons} this week`,
      },
      teamMembers: {
        count: teamMembersCount,
        change: '1 active now',
      },
    };
  }

  async getDashboardActivities(userId: string, limit: number = 10): Promise<any[]> {
    // Get recent audit logs for the user
    const auditLogs = await this.prisma.auditLog.findMany({
      where: { userId },
      orderBy: { createdAt: 'desc' },
      take: limit,
      include: {
        user: {
          select: {
            name: true,
            email: true,
          },
        },
      },
    });

    // Transform audit logs into activity format
    const activities = auditLogs.map((log) => {
      const userName = log.user.name || log.user.email;

      // Format action text
      let actionText = log.action;
      if (log.action.includes('CREATE')) {
        actionText = `${log.entity} created`;
      } else if (log.action.includes('UPDATE')) {
        actionText = `${log.entity} updated`;
      } else if (log.action.includes('DELETE')) {
        actionText = `${log.entity} deleted`;
      } else if (log.action.includes('SCHEMA_COMPARISON')) {
        actionText = 'Schema comparison initiated';
      }

      // Calculate time ago
      const now = new Date();
      const diffMs = now.getTime() - log.createdAt.getTime();
      const diffMins = Math.floor(diffMs / 60000);
      const diffHours = Math.floor(diffMs / 3600000);
      const diffDays = Math.floor(diffMs / 86400000);

      let timeAgo = '';
      if (diffMins < 60) {
        timeAgo = `${diffMins} minute${diffMins !== 1 ? 's' : ''} ago`;
      } else if (diffHours < 24) {
        timeAgo = `${diffHours} hour${diffHours !== 1 ? 's' : ''} ago`;
      } else {
        timeAgo = `${diffDays} day${diffDays !== 1 ? 's' : ''} ago`;
      }

      return {
        action: actionText,
        project: log.entityId || log.entity,
        user: userName,
        time: timeAgo,
        status: log.status === 'SUCCESS' ? 'success' : 'warning',
        createdAt: log.createdAt,
      };
    });

    return activities;
  }

  async getDashboardConnections(userId: string, limit: number = 4): Promise<any[]> {
    const connections = await this.prisma.connection.findMany({
      where: { userId },
      orderBy: { updatedAt: 'desc' },
      take: limit,
      select: {
        id: true,
        connectionName: true,
        serverType: true,
        status: true,
        host: true,
        snowflakeAccount: true,
        updatedAt: true,
      },
    });

    return connections.map((conn) => ({
      id: conn.id,
      name: conn.connectionName,
      type: conn.serverType,
      status: conn.status === 'active' ? 'connected' : 'disconnected',
      host: conn.host || conn.snowflakeAccount || 'N/A',
      lastUsed: conn.updatedAt.toISOString(),
    }));
  }

  async getProject(projectId: string, userId: string): Promise<any> {
    const project = await this.prisma.hierarchyProject.findFirst({
      where: {
        id: projectId,
        userId,
        isActive: true,
      },
      select: {
        id: true,
        name: true,
        description: true,
        userId: true,
        organizationId: true,
        isActive: true,
        metadata: true,
        createdAt: true,
        updatedAt: true,
      },
    });

    if (!project) {
      throw new NotFoundException(`Project '${projectId}' not found`);
    }

    return project;
  }

  async createProject(data: {
    name: string;
    description?: string;
    userId: string;
    organizationId?: string;
  }): Promise<any> {
    // If no organizationId provided, get it from user
    let orgId = data.organizationId;
    if (!orgId) {
      const user = await this.prisma.user.findUnique({
        where: { id: data.userId },
        select: { organizationId: true },
      });
      orgId = user?.organizationId;
    }

    const project = await this.prisma.hierarchyProject.create({
      data: {
        name: data.name,
        description: data.description,
        user: {
          connect: { id: data.userId },
        },
        organization: orgId ? { connect: { id: orgId } } : undefined,
        isActive: true,
        members: {
          create: {
            userId: data.userId,
            role: 'owner',
            accessType: 'direct',
            invitedBy: data.userId,
            invitationStatus: 'accepted',
            acceptedAt: new Date(),
            isActive: true,
          },
        },
      },
      select: {
        id: true,
        name: true,
        description: true,
        userId: true,
        organizationId: true,
        isActive: true,
        createdAt: true,
        updatedAt: true,
      },
    });

    return project;
  }

  async updateProject(
    projectId: string,
    userId: string,
    data: { name?: string; description?: string; isActive?: boolean },
  ): Promise<any> {
    // Check if project exists and belongs to user
    const existing = await this.prisma.hierarchyProject.findFirst({
      where: {
        id: projectId,
        userId,
      },
    });

    if (!existing) {
      throw new NotFoundException(`Project '${projectId}' not found`);
    }

    const updated = await this.prisma.hierarchyProject.update({
      where: { id: projectId },
      data,
      select: {
        id: true,
        name: true,
        description: true,
        userId: true,
        organizationId: true,
        isActive: true,
        createdAt: true,
        updatedAt: true,
      },
    });

    return updated;
  }

  async deleteProject(projectId: string, userId: string): Promise<void> {
    // Check if project exists and belongs to user
    const existing = await this.prisma.hierarchyProject.findFirst({
      where: {
        id: projectId,
        userId,
      },
    });

    if (!existing) {
      throw new NotFoundException(`Project '${projectId}' not found`);
    }

    // Soft delete by setting isActive to false
    await this.prisma.hierarchyProject.delete({
      where: { id: projectId },
      include: {
        members: true,
        smartHierarchies: true,
      },
    });
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  private async toSmartHierarchyMaster(record: any): Promise<SmartHierarchyMaster> {
    // Convert parent UUID to hierarchyId for API responses
    let parentHierarchyId: string | null = null;
    if (record.parentId) {
      const parent = await this.prisma.smartHierarchyMaster.findUnique({
        where: { id: record.parentId },
        select: { hierarchyId: true },
      });
      parentHierarchyId = parent?.hierarchyId || null;
    }

    return {
      id: record.id,
      projectId: record.projectId,
      hierarchyId: record.hierarchyId,
      hierarchyName: record.hierarchyName,
      description: record.description,
      // Tree structure fields
      parentId: parentHierarchyId,
      isRoot: record.isRoot,
      sortOrder: record.sortOrder,
      // JSON fields
      hierarchyLevel: record.hierarchyLevel,
      flags: record.flags,
      mapping: record.mapping,
      formulaConfig: record.formulaConfig,
      filterConfig: record.filterConfig,
      pivotConfig: record.pivotConfig,
      metadata: record.metadata,
      createdBy: record.createdBy,
      createdAt: record.createdAt,
      updatedAt: record.updatedAt,
    };
  }

  private extractActiveLevels(hierarchyLevel: any): string[] {
    const levels: string[] = [];
    for (let i = 1; i <= 15; i++) {
      const level = hierarchyLevel[`level_${i}`];
      if (level) {
        levels.push(level);
      }
    }
    return levels;
  }

  // ===========================
  // Project Member Management
  // ===========================

  async getProjectMembers(projectId: string, userId: string): Promise<any[]> {
    // Check if user has access to project
    await this.checkProjectAccess(projectId, userId, ['owner', 'editor', 'viewer']);

    const members = await this.prisma.hierarchyProjectMember.findMany({
      where: {
        projectId,
        isActive: true,
      },
      include: {
        user: {
          select: {
            id: true,
            name: true,
            email: true,
            avatarUrl: true,
          },
        },
        inviter: {
          select: {
            id: true,
            name: true,
            email: true,
          },
        },
      },
      orderBy: [
        { role: 'asc' }, // owner first
        { createdAt: 'asc' },
      ],
    });

    return members;
  }

  async inviteProjectMember(
    projectId: string,
    userId: string,
    data: {
      userEmail?: string;
      inviteUserId?: string;
      role: 'editor' | 'viewer';
      accessType?: 'direct' | 'organization';
    },
  ): Promise<any> {
    // Check if user has owner or editor access
    await this.checkProjectAccess(projectId, userId, ['owner', 'editor']);

    // Check if member already exists with pending or accepted status
    const existingMember = data.inviteUserId
      ? await this.prisma.hierarchyProjectMember.findFirst({
          where: {
            projectId,
            userId: data.inviteUserId,
            invitationStatus: { in: ['pending', 'accepted'] },
          },
        })
      : await this.prisma.hierarchyProjectMember.findFirst({
          where: {
            projectId,
            userEmail: data.userEmail,
            invitationStatus: { in: ['pending', 'accepted'] },
          },
        });

    if (existingMember) {
      throw new ConflictException(
        'User is already a member of this project or has a pending invitation',
      );
    }

    // If inviting by userId, get user email
    let targetUserId = data.inviteUserId;
    let targetEmail = data.userEmail;

    if (data.inviteUserId) {
      const user = await this.prisma.user.findUnique({
        where: { id: data.inviteUserId },
        select: { email: true },
      });
      if (!user) {
        throw new NotFoundException('User not found');
      }
      targetEmail = user.email;
    } else if (data.userEmail) {
      // Check if user exists by email
      const user = await this.prisma.user.findUnique({
        where: { email: data.userEmail },
        select: { id: true },
      });
      if (user) {
        targetUserId = user.id;
      }
    }

    const member = await this.prisma.hierarchyProjectMember.create({
      data: {
        projectId,
        userId: targetUserId,
        userEmail: targetEmail,
        role: data.role,
        accessType: data.accessType || 'direct',
        invitedBy: userId,
        invitationStatus: 'pending',
        isActive: true,
      },
      include: {
        user: {
          select: {
            id: true,
            name: true,
            email: true,
            avatarUrl: true,
          },
        },
      },
    });

    return member;
  }

  async shareProjectWithOrganization(
    projectId: string,
    userId: string,
    role: 'editor' | 'viewer',
  ): Promise<any> {
    // Check if user is owner
    await this.checkProjectAccess(projectId, userId, ['owner']);

    // Get project with organization
    const project = await this.prisma.hierarchyProject.findUnique({
      where: { id: projectId },
      select: { organizationId: true },
    });

    if (!project?.organizationId) {
      throw new BadRequestException('Project is not associated with an organization');
    }

    // Get all organization members
    const orgMembers = await this.prisma.organizationMember.findMany({
      where: {
        organizationId: project.organizationId,
      },
      select: {
        userId: true,
        user: {
          select: {
            email: true,
          },
        },
      },
    });

    // Add all org members who aren't already members
    const results = [];
    for (const orgMember of orgMembers) {
      const existingMember = await this.prisma.hierarchyProjectMember.findFirst({
        where: {
          projectId,
          userId: orgMember.userId,
        },
      });

      if (!existingMember) {
        const member = await this.prisma.hierarchyProjectMember.create({
          data: {
            projectId,
            userId: orgMember.userId,
            userEmail: orgMember.user.email,
            role,
            accessType: 'organization',
            invitedBy: userId,
            invitationStatus: 'accepted',
            acceptedAt: new Date(),
            isActive: true,
          },
        });
        results.push(member);
      }
    }

    return {
      added: results.length,
      members: results,
    };
  }

  async updateProjectMember(
    projectId: string,
    memberId: string,
    userId: string,
    data: {
      role?: 'editor' | 'viewer';
      isActive?: boolean;
    },
  ): Promise<any> {
    // Check if user is owner
    await this.checkProjectAccess(projectId, userId, ['owner']);

    // Cannot change owner role
    const member = await this.prisma.hierarchyProjectMember.findUnique({
      where: { id: memberId },
    });

    if (member?.role === 'owner') {
      throw new BadRequestException('Cannot modify owner role');
    }

    const updated = await this.prisma.hierarchyProjectMember.update({
      where: { id: memberId },
      data: {
        role: data.role,
        isActive: data.isActive,
      },
      include: {
        user: {
          select: {
            id: true,
            name: true,
            email: true,
            avatarUrl: true,
          },
        },
      },
    });

    return updated;
  }

  async removeProjectMember(projectId: string, memberId: string, userId: string): Promise<void> {
    // Check if user is owner
    await this.checkProjectAccess(projectId, userId, ['owner']);

    // Cannot remove owner
    const member = await this.prisma.hierarchyProjectMember.findUnique({
      where: { id: memberId },
    });

    if (member?.role === 'owner') {
      throw new BadRequestException('Cannot remove project owner');
    }

    await this.prisma.hierarchyProjectMember.delete({
      where: { id: memberId },
    });
  }

  async acceptProjectInvitation(projectId: string, memberId: string, userId: string) {
    // Verify the member belongs to the current user
    const member = await this.prisma.hierarchyProjectMember.findUnique({
      where: { id: memberId },
      include: { project: true },
    });

    if (!member || member.userId !== userId) {
      throw new ForbiddenException('You can only accept your own invitations');
    }

    if (member.invitationStatus === 'accepted') {
      throw new BadRequestException('Invitation already accepted');
    }

    // Update invitation status
    const updated = await this.prisma.hierarchyProjectMember.update({
      where: { id: memberId },
      data: {
        invitationStatus: 'accepted',
        acceptedAt: new Date(),
        isActive: true,
      },
      include: {
        project: true,
        user: true,
      },
    });

    return {
      message: 'Project invitation accepted',
      membership: updated,
    };
  }

  async declineProjectInvitation(projectId: string, memberId: string, userId: string) {
    // Verify the member belongs to the current user
    const member = await this.prisma.hierarchyProjectMember.findUnique({
      where: { id: memberId },
    });

    if (!member || member.userId !== userId) {
      throw new ForbiddenException('You can only decline your own invitations');
    }

    if (member.invitationStatus === 'accepted') {
      throw new BadRequestException('Cannot decline an already accepted invitation');
    }

    // Update invitation status or delete
    await this.prisma.hierarchyProjectMember.update({
      where: { id: memberId },
      data: {
        invitationStatus: 'declined',
        isActive: false,
      },
    });

    return {
      message: 'Project invitation declined',
    };
  }

  async getPendingInvitations(userId: string) {
    const memberRecords = await this.prisma.hierarchyProjectMember.findMany({
      where: {
        userId,
        invitationStatus: 'pending',
        isActive: true,
      },
      include: {
        project: {
          select: {
            id: true,
            name: true,
            description: true,
            userId: true,
            organizationId: true,
            isActive: true,
            createdAt: true,
            updatedAt: true,
          },
        },
        inviter: {
          select: {
            id: true,
            name: true,
            email: true,
            avatarUrl: true,
          },
        },
      },
      orderBy: {
        invitedAt: 'desc',
      },
    });

    // Transform to match frontend expected structure
    const invitations = memberRecords.map((record) => ({
      member: {
        id: record.id,
        projectId: record.projectId,
        userId: record.userId,
        userEmail: record.userEmail,
        role: record.role,
        accessType: record.accessType,
        invitationStatus: record.invitationStatus,
        invitedBy: record.invitedBy,
        invitedAt: record.invitedAt.toISOString(),
        acceptedAt: record.acceptedAt?.toISOString(),
        isActive: record.isActive,
      },
      project: record.project,
      inviter: record.inviter,
    }));

    return invitations;
  }

  private async checkProjectAccess(
    projectId: string,
    userId: string,
    allowedRoles: string[],
  ): Promise<void> {
    const member = await this.prisma.hierarchyProjectMember.findFirst({
      where: {
        projectId,
        userId,
        isActive: true,
        invitationStatus: 'accepted',
        role: { in: allowedRoles },
      },
    });

    if (!member) {
      throw new ForbiddenException('Insufficient permissions to access this project');
    }
  }

  // ============================================================================
  // Formula Group Management
  // ============================================================================

  // Old legacy methods removed - use saveFormulaToHierarchy and linkHierarchyToFormulaGroup instead

  /**
   * Get hierarchies by project with optional search
   */
  async searchHierarchies(
    projectId: string,
    searchTerm?: string,
    excludeIds?: string[],
  ): Promise<SmartHierarchyMaster[]> {
    const where: any = {
      projectId,
    };

    if (searchTerm) {
      where.OR = [
        { hierarchyId: { contains: searchTerm } },
        { hierarchyName: { contains: searchTerm } },
        { description: { contains: searchTerm } },
      ];
    }

    if (excludeIds && excludeIds.length > 0) {
      where.hierarchyId = {
        notIn: excludeIds,
      };
    }

    const hierarchies = await this.prisma.smartHierarchyMaster.findMany({
      where,
      orderBy: [{ sortOrder: 'asc' }, { hierarchyName: 'asc' }],
    });

    return Promise.all(hierarchies.map((h) => this.toSmartHierarchyMaster(h)));
  }

  /**
   * Unlink hierarchy from formula group
   * Removes formula_group_ref from child hierarchy and removes rule from main hierarchy
   */
  async unlinkFromFormulaGroup(
    projectId: string,
    hierarchyId: string,
  ): Promise<SmartHierarchyMaster> {
    // Get child hierarchy
    const childHierarchy = await this.findOne(projectId, hierarchyId);
    if (!childHierarchy) {
      throw new Error('Child hierarchy not found');
    }

    const formulaConfig = childHierarchy.formulaConfig as any;
    const formulaGroupRef = formulaConfig?.formula_group_ref;

    let mainHierarchyId: string | null = null;

    // Case 1: New structure with formula_group_ref
    if (formulaGroupRef) {
      mainHierarchyId = formulaGroupRef.mainHierarchyId;
    } else {
      // Case 2: Old structure - search for this hierarchy in all formula groups
      const allHierarchies = await this.prisma.smartHierarchyMaster.findMany({
        where: { projectId },
      });

      for (const h of allHierarchies) {
        const config = h.formulaConfig as any;
        if (config?.formula_group?.rules) {
          const hasRule = config.formula_group.rules.some(
            (rule: any) => rule.hierarchyId === hierarchyId,
          );
          if (hasRule) {
            mainHierarchyId = h.hierarchyId;
            break;
          }
        }
      }
    }

    if (!mainHierarchyId) {
      throw new Error('Hierarchy is not linked to any formula group');
    }

    // Get main hierarchy
    const mainHierarchy = await this.findOne(projectId, mainHierarchyId);
    if (!mainHierarchy) {
      throw new Error('Main hierarchy not found');
    }

    const mainFormulaConfig = mainHierarchy.formulaConfig as any;
    const formulaGroup = mainFormulaConfig?.formula_group;

    if (!formulaGroup) {
      throw new Error('Formula group not found in main hierarchy');
    }

    // Remove rule from main hierarchy's rules array
    const updatedRules = formulaGroup.rules.filter((rule: any) => rule.hierarchyId !== hierarchyId);

    const updatedFormulaGroup = {
      ...formulaGroup,
      rules: updatedRules,
    };

    // Update main hierarchy
    await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId: mainHierarchyId,
        },
      },
      data: {
        formulaConfig: {
          ...mainFormulaConfig,
          formula_group: updatedFormulaGroup,
        } as any,
      },
    });

    // Remove formula_group_ref from child hierarchy (if it exists)
    const updatedChildFormulaConfig = { ...formulaConfig };
    delete updatedChildFormulaConfig.formula_group_ref;

    const updated = await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId,
        },
      },
      data: {
        formulaConfig: updatedChildFormulaConfig as any,
      },
    });

    return this.toSmartHierarchyMaster(updated);
  }

  /**
   * Get all formula groups in a project (lightweight - only essential data)
   * Supports optional search by group name
   */
  async getFormulaGroups(projectId: string, searchTerm?: string): Promise<any[]> {
    console.log('[getFormulaGroups] Fetching for projectId:', projectId, 'searchTerm:', searchTerm);

    const hierarchies = await this.prisma.smartHierarchyMaster.findMany({
      where: { projectId },
      select: {
        hierarchyId: true,
        hierarchyName: true,
        formulaConfig: true,
      },
    });

    console.log('[getFormulaGroups] Found hierarchies:', hierarchies.length);

    const formulaGroups = hierarchies
      .filter((h) => {
        // Parse formulaConfig if it's a string
        let config = h.formulaConfig;
        if (typeof config === 'string') {
          try {
            config = JSON.parse(config);
          } catch (e) {
            console.error(`[getFormulaGroups] Failed to parse formulaConfig for ${h.hierarchyId}`);
            return false;
          }
        }

        if (!config) {
          console.log(`[getFormulaGroups] Hierarchy ${h.hierarchyId}: No formulaConfig`);
          return false;
        }

        const configObj = config as any;

        // Check for formula_group property
        const hasFormulaGroup =
          configObj.formula_group && typeof configObj.formula_group === 'object';

        if (!hasFormulaGroup) {
          console.log(
            `[getFormulaGroups] Hierarchy ${h.hierarchyId}: formula_group missing or invalid. Config:`,
            JSON.stringify(config),
          );
          return false;
        }

        console.log(
          `[getFormulaGroups] Hierarchy ${h.hierarchyId} has valid formula_group:`,
          configObj.formula_group.groupName,
        );
        return true;
      })
      .map((h) => {
        // Parse formulaConfig if it's a string
        let config = h.formulaConfig;
        if (typeof config === 'string') {
          try {
            config = JSON.parse(config);
          } catch (e) {
            console.error(`[getFormulaGroups] Failed to parse formulaConfig for ${h.hierarchyId}`);
            config = {};
          }
        }

        return {
          hierarchyId: h.hierarchyId,
          hierarchyName: h.hierarchyName,
          formulaGroup: (config as any).formula_group,
        };
      })
      .filter((item) => {
        // Apply search filter if provided
        if (searchTerm && searchTerm.trim()) {
          const search = searchTerm.toLowerCase();
          const groupName = item.formulaGroup?.groupName?.toLowerCase() || '';
          const mainHierarchy = item.formulaGroup?.mainHierarchy?.toLowerCase() || '';
          const hierarchyName = item.hierarchyName?.toLowerCase() || '';

          return (
            groupName.includes(search) ||
            mainHierarchy.includes(search) ||
            hierarchyName.includes(search)
          );
        }
        return true;
      });

    console.log('[getFormulaGroups] Returning formula groups:', formulaGroups.length);
    if (formulaGroups.length > 0) {
      console.log(
        '[getFormulaGroups] Sample formula group:',
        JSON.stringify(formulaGroups[0], null, 2),
      );
    }
    return formulaGroups;
  }

  /**
   * Get hierarchies that use this hierarchy in their formula groups
   */
  async getFormulaGroupUsages(
    projectId: string,
    hierarchyId: string,
  ): Promise<SmartHierarchyMaster[]> {
    const allHierarchies = await this.prisma.smartHierarchyMaster.findMany({
      where: { projectId },
    });

    const usages = allHierarchies.filter((h) => {
      const formulaConfig = h.formulaConfig as any;
      if (!formulaConfig?.formula_group?.rules) return false;

      return formulaConfig.formula_group.rules.some(
        (rule: any) => rule.hierarchyId === hierarchyId,
      );
    });

    return Promise.all(usages.map((h) => this.toSmartHierarchyMaster(h)));
  }

  /**
   * Get formula group details for a hierarchy
   */
  async getFormulaGroup(projectId: string, hierarchyId: string): Promise<any | null> {
    const hierarchy = await this.findOne(projectId, hierarchyId);
    const formulaConfig = hierarchy.formulaConfig as any;
    return formulaConfig?.formula_group || null;
  }

  /**
   * Remove formula group from a hierarchy
   */
  async removeFormulaGroup(projectId: string, hierarchyId: string): Promise<SmartHierarchyMaster> {
    const existing = await this.findOne(projectId, hierarchyId);

    const updatedFormulaConfig = {
      ...((existing.formulaConfig as any) || {}),
      formula_group: undefined,
      formula_type: undefined,
      formula_text: '',
    };

    const updated = await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId,
        },
      },
      data: {
        formulaConfig: updatedFormulaConfig as any,
      },
    });

    return await this.toSmartHierarchyMaster(updated);
  }

  // ============================================================================
  // Formula Groups - Stored in smart_hierarchy_master.formula_config ONLY
  // ============================================================================

  /**
   * Get all hierarchies with formulas for a project
   * Efficiently queries only hierarchies that have formulaConfig set
   */
  async getProjectFormulas(projectId: string): Promise<SmartHierarchyMaster[]> {
    const hierarchies = await this.prisma.smartHierarchyMaster.findMany({
      where: {
        projectId,
        formulaConfig: { not: null },
      },
      orderBy: [{ sortOrder: 'asc' }, { hierarchyName: 'asc' }],
    });

    return Promise.all(hierarchies.map((h) => this.toSmartHierarchyMaster(h)));
  }

  /**
   * Get formula from a specific hierarchy
   * Returns the formula_group object if this is a main hierarchy,
   * or formula_group_ref if this is a contributor hierarchy
   */
  async getFormulaFromHierarchy(projectId: string, hierarchyId: string): Promise<any | null> {
    const hierarchy = await this.findOne(projectId, hierarchyId);

    if (!hierarchy || !hierarchy.formulaConfig) {
      return null;
    }

    const config = hierarchy.formulaConfig as any;

    // Return formula_group (main hierarchy) or formula_group_ref (contributor)
    return {
      hierarchyId: hierarchy.hierarchyId,
      hierarchyName: hierarchy.hierarchyName,
      formula_type: config.formula_type,
      formula_text: config.formula_text,
      formula_group: config.formula_group || null,
      formula_group_ref: config.formula_group_ref || null,
      isMainHierarchy: !!config.formula_group,
      isContributor: !!config.formula_group_ref,
    };
  }

  /**
   * Save formula to main hierarchy (stores full formula_group)
   * This creates/updates a formula group stored directly in the hierarchy's formulaConfig
   */
  async saveFormulaToHierarchy(
    projectId: string,
    hierarchyId: string,
    formulaGroup: any,
  ): Promise<SmartHierarchyMaster> {
    const existing = await this.findOne(projectId, hierarchyId);

    // Validate that all referenced hierarchies exist
    if (formulaGroup?.rules) {
      for (const rule of formulaGroup.rules) {
        const refHierarchy = await this.prisma.smartHierarchyMaster.findUnique({
          where: {
            projectId_hierarchyId: {
              projectId,
              hierarchyId: rule.hierarchyId,
            },
          },
        });

        if (!refHierarchy) {
          throw new NotFoundException(
            `Referenced hierarchy '${rule.hierarchyId}' not found in formula group`,
          );
        }
      }
    }

    // Store formula in formulaConfig
    const updatedFormulaConfig = {
      formula_type: 'AGGREGATE',
      formula_text: formulaGroup.groupName || '',
      formula_group: {
        groupName: formulaGroup.groupName,
        mainHierarchyId: hierarchyId,
        rules: formulaGroup.rules || [],
        formula_params: formulaGroup.formula_params || {},
      },
    };

    const updated = await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId,
        },
      },
      data: {
        formulaConfig: updatedFormulaConfig as any,
      },
    });

    return await this.toSmartHierarchyMaster(updated);
  }

  /**
   * Remove formula from hierarchy
   */
  async removeFormulaFromHierarchy(
    projectId: string,
    hierarchyId: string,
  ): Promise<SmartHierarchyMaster> {
    const updated = await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId,
        },
      },
      data: {
        formulaConfig: null,
      },
    });

    return await this.toSmartHierarchyMaster(updated);
  }

  /**
   * Link hierarchy to existing formula group (stores reference only)
   * The target hierarchy must already have a formula_group in its formulaConfig
   */
  async linkHierarchyToFormulaGroup(
    projectId: string,
    hierarchyId: string,
    data: any,
  ): Promise<SmartHierarchyMaster> {
    const {
      mainHierarchyId,
      role,
      FORMULA_PRECEDENCE,
      FORMULA_PARAM_REF,
      FORMULA_PARAM2_CONST_NUMBER,
    } = data;

    const existing = await this.findOne(projectId, hierarchyId);

    // Get the main hierarchy to verify it has a formula group
    const mainHierarchy = await this.findOne(projectId, mainHierarchyId);
    const mainFormulaConfig = mainHierarchy.formulaConfig as any;

    // If main hierarchy doesn't have formula_group but has AGGREGATE type, initialize it
    if (!mainFormulaConfig?.formula_group) {
      if (mainFormulaConfig?.formula_type === 'AGGREGATE') {
        // Initialize formula_group structure from existing data
        const initializedConfig = {
          formula_type: 'AGGREGATE',
          formula_text: mainFormulaConfig.formula_text || mainHierarchy.hierarchyName,
          formula_group: {
            groupName: mainFormulaConfig.formula_text || mainHierarchy.hierarchyName,
            mainHierarchyId: mainHierarchyId,
            rules: [],
            formula_params: mainFormulaConfig.formula_params || {},
          },
        };

        await this.prisma.smartHierarchyMaster.update({
          where: {
            projectId_hierarchyId: {
              projectId,
              hierarchyId: mainHierarchyId,
            },
          },
          data: {
            formulaConfig: initializedConfig as any,
          },
        });

        // Reload main hierarchy with updated config
        const updatedMainHierarchy = await this.findOne(projectId, mainHierarchyId);
        mainFormulaConfig.formula_group = (updatedMainHierarchy.formulaConfig as any).formula_group;
      } else {
        throw new NotFoundException(
          `Main hierarchy '${mainHierarchyId}' does not have a formula group`,
        );
      }
    }

    // Store reference to the main hierarchy
    const formulaConfig = {
      formula_type: 'AGGREGATE',
      formula_text: mainFormulaConfig.formula_group.groupName,
      formula_group_ref: {
        mainHierarchyId,
        groupName: mainFormulaConfig.formula_group.groupName,
        role,
        FORMULA_PRECEDENCE,
        FORMULA_PARAM_REF,
        FORMULA_PARAM2_CONST_NUMBER,
      },
    };

    const updated = await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId,
        },
      },
      data: {
        formulaConfig: formulaConfig as any,
      },
    });

    // Also add this hierarchy to the main hierarchy's formula_group.rules if not already there
    const updatedRules = [...(mainFormulaConfig.formula_group.rules || [])];
    const existingRuleIndex = updatedRules.findIndex((r) => r.hierarchyId === hierarchyId);

    const newRule = {
      operation: role || 'SUM',
      hierarchyId: hierarchyId,
      hierarchyName: existing.hierarchyName,
      FORMULA_PRECEDENCE,
      FORMULA_PARAM_REF,
      FORMULA_PARAM2_CONST_NUMBER,
    };

    if (existingRuleIndex >= 0) {
      updatedRules[existingRuleIndex] = newRule;
    } else {
      updatedRules.push(newRule);
    }

    // Update main hierarchy
    await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId: mainHierarchyId,
        },
      },
      data: {
        formulaConfig: {
          ...mainFormulaConfig,
          formula_group: {
            ...mainFormulaConfig.formula_group,
            rules: updatedRules,
          },
        } as any,
      },
    });

    return await this.toSmartHierarchyMaster(updated);
  }

  /**
   * Unlink hierarchy from formula group (remove reference)
   */
  async unlinkHierarchyFromFormulaGroup(
    projectId: string,
    hierarchyId: string,
  ): Promise<SmartHierarchyMaster> {
    const existing = await this.findOne(projectId, hierarchyId);
    const formulaConfig = existing.formulaConfig as any;

    // If this has a formula_group_ref, remove from main hierarchy's rules
    if (formulaConfig?.formula_group_ref?.mainHierarchyId) {
      const mainHierarchyId = formulaConfig.formula_group_ref.mainHierarchyId;
      const mainHierarchy = await this.findOne(projectId, mainHierarchyId);
      const mainFormulaConfig = mainHierarchy.formulaConfig as any;

      if (mainFormulaConfig?.formula_group?.rules) {
        const updatedRules = mainFormulaConfig.formula_group.rules.filter(
          (r: any) => r.hierarchyId !== hierarchyId,
        );

        await this.prisma.smartHierarchyMaster.update({
          where: {
            projectId_hierarchyId: {
              projectId,
              hierarchyId: mainHierarchyId,
            },
          },
          data: {
            formulaConfig: {
              ...mainFormulaConfig,
              formula_group: {
                ...mainFormulaConfig.formula_group,
                rules: updatedRules,
              },
            } as any,
          },
        });
      }
    }

    // Remove formula config from this hierarchy
    const updated = await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId,
        },
      },
      data: {
        formulaConfig: null,
      },
    });

    return await this.toSmartHierarchyMaster(updated);
  }

  // ============================================================================
  // Filter Groups (NEW - Standardized with separate storage)
  // ============================================================================

  /**
   * Create a new filter group (stored separately)
   */
  async createFilterGroup(data: any): Promise<any> {
    const {
      projectId,
      groupName,
      filter_group_1,
      filter_group_2,
      filter_group_3,
      filter_group_4,
      filter_conditions,
      custom_sql,
      assignedHierarchies,
    } = data;

    const id = `FILTERG_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

    const project = await this.prisma.hierarchyProject.findUnique({
      where: { id: projectId },
    });

    if (!project) {
      throw new NotFoundException(`Project ${projectId} not found`);
    }

    const metadata = (project.metadata as any) || {};
    const filterGroups = metadata.filterGroups || [];

    const newFilterGroup = {
      id,
      groupName,
      filter_group_1,
      filter_group_2,
      filter_group_3,
      filter_group_4,
      filter_conditions,
      custom_sql,
      assignedHierarchies: assignedHierarchies || [],
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };

    filterGroups.push(newFilterGroup);

    await this.prisma.hierarchyProject.update({
      where: { id: projectId },
      data: {
        metadata: {
          ...metadata,
          filterGroups,
        } as any,
      },
    });

    return newFilterGroup;
  }

  /**
   * Get filter group by ID
   */
  async getFilterGroupById(id: string): Promise<any> {
    const projects = await this.prisma.hierarchyProject.findMany();

    for (const project of projects) {
      const metadata = (project.metadata as any) || {};
      const filterGroups = metadata.filterGroups || [];
      const filterGroup = filterGroups.find((fg: any) => fg.id === id);

      if (filterGroup) {
        return filterGroup;
      }
    }

    throw new NotFoundException(`Filter group ${id} not found`);
  }

  /**
   * Update filter group by ID
   */
  async updateFilterGroupById(id: string, updates: any): Promise<any> {
    const projects = await this.prisma.hierarchyProject.findMany();

    for (const project of projects) {
      const metadata = (project.metadata as any) || {};
      const filterGroups = metadata.filterGroups || [];
      const index = filterGroups.findIndex((fg: any) => fg.id === id);

      if (index !== -1) {
        filterGroups[index] = {
          ...filterGroups[index],
          ...updates,
          updatedAt: new Date().toISOString(),
        };

        await this.prisma.hierarchyProject.update({
          where: { id: project.id },
          data: {
            metadata: {
              ...metadata,
              filterGroups,
            } as any,
          },
        });

        return filterGroups[index];
      }
    }

    throw new NotFoundException(`Filter group ${id} not found`);
  }

  /**
   * Delete filter group by ID
   */
  async deleteFilterGroup(id: string): Promise<void> {
    const projects = await this.prisma.hierarchyProject.findMany();

    for (const project of projects) {
      const metadata = (project.metadata as any) || {};
      const filterGroups = metadata.filterGroups || [];
      const index = filterGroups.findIndex((fg: any) => fg.id === id);

      if (index !== -1) {
        filterGroups.splice(index, 1);

        await this.prisma.hierarchyProject.update({
          where: { id: project.id },
          data: {
            metadata: {
              ...metadata,
              filterGroups,
            } as any,
          },
        });

        return;
      }
    }

    throw new NotFoundException(`Filter group ${id} not found`);
  }

  /**
   * List all filter groups for a project
   */
  async listFilterGroups(projectId: string): Promise<any[]> {
    const project = await this.prisma.hierarchyProject.findUnique({
      where: { id: projectId },
    });

    if (!project) {
      throw new NotFoundException(`Project ${projectId} not found`);
    }

    const metadata = (project.metadata as any) || {};
    return metadata.filterGroups || [];
  }

  /**
   * Assign hierarchy to filter group (create reference)
   */
  async assignHierarchyToFilterGroup(
    projectId: string,
    hierarchyId: string,
    data: any,
  ): Promise<SmartHierarchyMaster> {
    const { filterGroupId, filterGroupName } = data;

    const existing = await this.findOne(projectId, hierarchyId);

    const filterConfig = {
      ...((existing.filterConfig as any) || {}),
      filter_group_ref: {
        filterGroupId,
        filterGroupName,
      },
    };

    const updated = await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId,
        },
      },
      data: {
        filterConfig: filterConfig as any,
      },
    });

    // Update filter group's assignedHierarchies
    const filterGroup = await this.getFilterGroupById(filterGroupId);
    const assignedHierarchies = filterGroup.assignedHierarchies || [];
    if (!assignedHierarchies.includes(hierarchyId)) {
      assignedHierarchies.push(hierarchyId);
      await this.updateFilterGroupById(filterGroupId, { assignedHierarchies });
    }

    return await this.toSmartHierarchyMaster(updated);
  }

  /**
   * Unassign hierarchy from filter group (remove reference)
   */
  async unassignHierarchyFromFilterGroup(
    projectId: string,
    hierarchyId: string,
  ): Promise<SmartHierarchyMaster> {
    const existing = await this.findOne(projectId, hierarchyId);
    const filterGroupId = (existing.filterConfig as any)?.filter_group_ref?.filterGroupId;

    const filterConfig = {
      ...((existing.filterConfig as any) || {}),
      filter_group_ref: undefined,
    };

    const updated = await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId,
        },
      },
      data: {
        filterConfig: filterConfig as any,
      },
    });

    // Update filter group's assignedHierarchies
    if (filterGroupId) {
      try {
        const filterGroup = await this.getFilterGroupById(filterGroupId);
        const assignedHierarchies = (filterGroup.assignedHierarchies || []).filter(
          (id: string) => id !== hierarchyId,
        );
        await this.updateFilterGroupById(filterGroupId, { assignedHierarchies });
      } catch (error) {
        // Filter group might have been deleted
        console.warn(`Filter group ${filterGroupId} not found during unassign`);
      }
    }

    return await this.toSmartHierarchyMaster(updated);
  }

  /**
   * Get filter attributes from a specific hierarchy for copying
   * Returns only filter-related fields without the full hierarchy data
   */
  async getHierarchyFilterAttributes(projectId: string, hierarchyId: string): Promise<any> {
    const hierarchy = await this.findOne(projectId, hierarchyId);

    if (!hierarchy) {
      throw new NotFoundException(`Hierarchy ${hierarchyId} not found in project ${projectId}`);
    }

    // Extract only filter-related attributes
    const filterConfig = (hierarchy.filterConfig as any) || {};

    return {
      hierarchyId: hierarchy.hierarchyId,
      hierarchyName: hierarchy.hierarchyName,
      filter_group_1: filterConfig.filter_group_1,
      filter_group_2: filterConfig.filter_group_2,
      filter_group_3: filterConfig.filter_group_3,
      filter_group_4: filterConfig.filter_group_4,
      filter_conditions: filterConfig.filter_conditions || [],
      custom_sql: filterConfig.custom_sql,
      filter_group_ref: filterConfig.filter_group_ref,
    };
  }

  // ============================================================================
  // Total Formula Management
  // ============================================================================

  /**
   * Create or update total formula in the main hierarchy's filter_config
   */
  async createOrUpdateTotalFormula(
    projectId: string,
    mainHierarchyId: string,
    data: {
      mainHierarchyName: string;
      aggregation: 'SUM' | 'AVERAGE' | 'COUNT' | 'MIN' | 'MAX';
      children: Array<{ hierarchyId: string; hierarchyName: string; level?: number }>;
    },
  ): Promise<SmartHierarchyMaster> {
    // Validate main hierarchy exists
    const mainHierarchy = await this.findOne(projectId, mainHierarchyId);
    if (!mainHierarchy) {
      throw new NotFoundException(
        `Main hierarchy ${mainHierarchyId} not found in project ${projectId}`,
      );
    }

    // Validate all child hierarchies exist and calculate their levels
    const childrenWithLevels = [];
    for (const child of data.children) {
      const childHierarchy = await this.findOne(projectId, child.hierarchyId);
      if (!childHierarchy) {
        throw new NotFoundException(
          `Child hierarchy ${child.hierarchyId} not found in project ${projectId}`,
        );
      }

      // Calculate level from hierarchyLevel
      const level = this.calculateHierarchyLevelNumber(childHierarchy.hierarchyLevel as any);
      childrenWithLevels.push({
        hierarchyId: child.hierarchyId,
        hierarchyName: child.hierarchyName,
        level: level,
      });
    }

    // Update the main hierarchy's filter_config with total_formula
    const filterConfig = (mainHierarchy.filterConfig as any) || {};
    filterConfig.total_formula = {
      mainHierarchyId: mainHierarchyId,
      mainHierarchyName: data.mainHierarchyName,
      aggregation: data.aggregation,
      children: childrenWithLevels,
    };

    const updated = await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId: mainHierarchyId,
        },
      },
      data: {
        filterConfig: filterConfig as any,
      },
    });

    return updated as any as SmartHierarchyMaster;
  }

  /**
   * Helper method to calculate hierarchy level number from hierarchyLevel object
   */
  private calculateHierarchyLevelNumber(hierarchyLevel: any): number {
    if (!hierarchyLevel) return 1;

    let level = 0;
    for (let i = 1; i <= 15; i++) {
      const key = `level_${i}`;
      if (hierarchyLevel[key] && hierarchyLevel[key] !== '') {
        level = i;
      }
    }

    return level || 1;
  }

  /**
   * Get total formula from a hierarchy
   */
  async getTotalFormula(projectId: string, hierarchyId: string): Promise<any | null> {
    const hierarchy = await this.findOne(projectId, hierarchyId);
    if (!hierarchy) {
      throw new NotFoundException(`Hierarchy ${hierarchyId} not found in project ${projectId}`);
    }

    const filterConfig = (hierarchy.filterConfig as any) || {};
    return filterConfig.total_formula || null;
  }

  /**
   * List all hierarchies that have total formulas in a project
   */
  async listHierarchiesWithTotalFormulas(projectId: string): Promise<
    Array<{
      hierarchyId: string;
      hierarchyName: string;
      totalFormula: any;
    }>
  > {
    const hierarchies = await this.findAll(projectId);

    const withTotalFormulas = hierarchies
      .filter((h) => {
        const filterConfig = (h.filterConfig as any) || {};
        return filterConfig.total_formula;
      })
      .map((h) => {
        const filterConfig = (h.filterConfig as any) || {};
        return {
          hierarchyId: h.hierarchyId,
          hierarchyName: h.hierarchyName,
          totalFormula: filterConfig.total_formula,
        };
      });

    return withTotalFormulas;
  }

  /**
   * Delete total formula from a hierarchy
   */
  async deleteTotalFormula(projectId: string, hierarchyId: string): Promise<SmartHierarchyMaster> {
    const hierarchy = await this.findOne(projectId, hierarchyId);
    if (!hierarchy) {
      throw new NotFoundException(`Hierarchy ${hierarchyId} not found in project ${projectId}`);
    }

    const filterConfig = (hierarchy.filterConfig as any) || {};
    delete filterConfig.total_formula;

    const updated = await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId,
        },
      },
      data: {
        filterConfig: filterConfig as any,
      },
    });

    return updated as any as SmartHierarchyMaster;
  }

  /**
   * Add a child to an existing total formula
   */
  async addChildToTotalFormula(
    projectId: string,
    mainHierarchyId: string,
    child: { hierarchyId: string; hierarchyName: string; level?: number },
  ): Promise<SmartHierarchyMaster> {
    const hierarchy = await this.findOne(projectId, mainHierarchyId);
    if (!hierarchy) {
      throw new NotFoundException(`Hierarchy ${mainHierarchyId} not found in project ${projectId}`);
    }

    const filterConfig = (hierarchy.filterConfig as any) || {};
    if (!filterConfig.total_formula) {
      throw new NotFoundException(`Total formula not found in hierarchy ${mainHierarchyId}`);
    }

    // Validate child hierarchy exists
    const childHierarchy = await this.findOne(projectId, child.hierarchyId);
    if (!childHierarchy) {
      throw new NotFoundException(
        `Child hierarchy ${child.hierarchyId} not found in project ${projectId}`,
      );
    }

    // Check if child already exists
    const exists = filterConfig.total_formula.children.some(
      (c: any) => c.hierarchyId === child.hierarchyId,
    );
    if (exists) {
      throw new BadRequestException(
        `Child hierarchy ${child.hierarchyId} already exists in total formula`,
      );
    }

    // Calculate level and add child
    const level = this.calculateHierarchyLevelNumber(childHierarchy.hierarchyLevel as any);
    filterConfig.total_formula.children.push({
      hierarchyId: child.hierarchyId,
      hierarchyName: child.hierarchyName,
      level: level,
    });

    const updated = await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId: mainHierarchyId,
        },
      },
      data: {
        filterConfig: filterConfig as any,
      },
    });

    return updated as any as SmartHierarchyMaster;
  }

  /**
   * Remove a child from an existing total formula
   */
  async removeChildFromTotalFormula(
    projectId: string,
    mainHierarchyId: string,
    childHierarchyId: string,
  ): Promise<SmartHierarchyMaster> {
    const hierarchy = await this.findOne(projectId, mainHierarchyId);
    if (!hierarchy) {
      throw new NotFoundException(`Hierarchy ${mainHierarchyId} not found in project ${projectId}`);
    }

    const filterConfig = (hierarchy.filterConfig as any) || {};
    if (!filterConfig.total_formula) {
      throw new NotFoundException(`Total formula not found in hierarchy ${mainHierarchyId}`);
    }

    // Remove child
    filterConfig.total_formula.children = filterConfig.total_formula.children.filter(
      (c: any) => c.hierarchyId !== childHierarchyId,
    );

    const updated = await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId: mainHierarchyId,
        },
      },
      data: {
        filterConfig: filterConfig as any,
      },
    });

    return updated as any as SmartHierarchyMaster;
  }

  // ============================================================================
  // Formula Group Management (stores in formulaConfig, creates new CSV entries)
  // ============================================================================

  /**
   * Create or update formula group in the main hierarchy's formula_config
   */
  async createOrUpdateFormulaGroup(
    projectId: string,
    mainHierarchyId: string,
    data: {
      mainHierarchyName: string;
      rules: Array<{
        hierarchyId: string;
        hierarchyName: string;
        operation:
          | 'ADD'
          | 'SUBTRACT'
          | 'MULTIPLY'
          | 'DIVIDE'
          | 'SUM'
          | 'AVERAGE'
          | 'COUNT'
          | 'MIN'
          | 'MAX';
        precedence: number;
        parameterReference?: string;
        constantNumber?: number;
        formulaRefSource?: string;
        formulaRefTable?: string;
      }>;
    },
  ): Promise<SmartHierarchyMaster> {
    // Validate main hierarchy exists
    const mainHierarchy = await this.findOne(projectId, mainHierarchyId);
    if (!mainHierarchy) {
      throw new NotFoundException(
        `Main hierarchy ${mainHierarchyId} not found in project ${projectId}`,
      );
    }

    // Validate all rule hierarchies exist
    for (const rule of data.rules) {
      const ruleHierarchy = await this.findOne(projectId, rule.hierarchyId);
      if (!ruleHierarchy) {
        throw new NotFoundException(
          `Rule hierarchy ${rule.hierarchyId} not found in project ${projectId}`,
        );
      }
    }

    // Update the main hierarchy's formula_config with formula_group
    const formulaConfig = (mainHierarchy.formulaConfig as any) || {};
    formulaConfig.formula_group = {
      mainHierarchyId: mainHierarchyId,
      mainHierarchyName: data.mainHierarchyName,
      rules: data.rules.map((rule) => ({
        ...rule,
        formulaRefSource: rule.formulaRefSource || '',
        formulaRefTable: rule.formulaRefTable || '',
      })),
    };
    formulaConfig.formula_type = 'AGGREGATE';
    formulaConfig.formula_text = data.mainHierarchyName;

    // Store formula_ref_source and formula_ref_table in metadata for each rule
    const metadata = (mainHierarchy.metadata as any) || {};
    for (const rule of data.rules) {
      if (rule.formulaRefSource || rule.formulaRefTable) {
        if (!metadata.formula_refs) {
          metadata.formula_refs = {};
        }
        metadata.formula_refs[rule.hierarchyId] = {
          formula_ref_source: rule.formulaRefSource || '',
          formula_ref_table: rule.formulaRefTable || '',
        };
      }
    }

    // Set calculation_flag to true
    const flags = (mainHierarchy.flags as any) || {};
    flags.calculation_flag = true;

    const updated = await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId: mainHierarchyId,
        },
      },
      data: {
        formulaConfig: formulaConfig as any,
        metadata: metadata as any,
        flags: flags as any,
      },
    });

    return updated as any as SmartHierarchyMaster;
  }

  /**
   * List all hierarchies that have formula groups in a project
   */
  async listHierarchiesWithFormulaGroups(projectId: string): Promise<
    Array<{
      hierarchyId: string;
      hierarchyName: string;
      formulaGroup: any;
    }>
  > {
    const hierarchies = await this.findAll(projectId);

    const withFormulaGroups = hierarchies
      .filter((h) => {
        const formulaConfig = (h.formulaConfig as any) || {};
        return formulaConfig.formula_group;
      })
      .map((h) => {
        const formulaConfig = (h.formulaConfig as any) || {};
        const metadata = (h.metadata as any) || {};
        const formulaRefs = metadata.formula_refs || {};

        // Merge formula_refs from metadata into rules
        const formulaGroup = { ...formulaConfig.formula_group };
        if (formulaGroup.rules) {
          formulaGroup.rules = formulaGroup.rules.map((rule: any) => {
            const refData = formulaRefs[rule.hierarchyId] || {};
            // Also check if this is the main hierarchy and use its metadata directly
            const isMainRule = rule.hierarchyId === h.hierarchyId;
            return {
              ...rule,
              formulaRefSource:
                rule.formulaRefSource ||
                refData.formula_ref_source ||
                (isMainRule ? metadata.formula_ref_source : '') ||
                '',
              formulaRefTable:
                rule.formulaRefTable ||
                refData.formula_ref_table ||
                (isMainRule ? metadata.formula_ref_table : '') ||
                '',
            };
          });
        }

        return {
          hierarchyId: h.hierarchyId,
          hierarchyName: h.hierarchyName,
          formulaGroup: formulaGroup,
        };
      });

    return withFormulaGroups;
  }

  /**
   * Get formula information for a specific hierarchy
   * Returns:
   * - If hierarchy HAS a formula group (is a formula owner): show its formula details
   * - If hierarchy is PART OF a formula group (contributor): show parent formula info
   * - If hierarchy has no formula: return null
   */
  async getHierarchyFormulaInfo(
    projectId: string,
    hierarchyId: string,
  ): Promise<{
    isFormulaOwner: boolean;
    isContributor: boolean;
    ownFormula?: {
      groupName: string;
      mainHierarchyId: string;
      mainHierarchyName: string;
      rules: any[];
      formula_params?: any;
    };
    contributorOf?: {
      mainHierarchyId: string;
      mainHierarchyName: string;
      groupName: string;
      myRole: string;
      myPrecedence?: number;
      allRules: any[];
    };
  } | null> {
    const hierarchy = await this.findOne(projectId, hierarchyId);
    if (!hierarchy) {
      throw new NotFoundException(`Hierarchy ${hierarchyId} not found in project ${projectId}`);
    }

    const formulaConfig = (hierarchy.formulaConfig as any) || {};
    const filterConfig = (hierarchy.filterConfig as any) || {};
    const result: any = {
      isFormulaOwner: false,
      isContributor: false,
    };

    console.log(`[getHierarchyFormulaInfo] Checking hierarchy ${hierarchyId}:`, {
      hasFormulaGroup: !!formulaConfig.formula_group,
      hasTotalFormulaInFormulaConfig: !!formulaConfig.total_formula,
      hasTotalFormulaInFilterConfig: !!filterConfig.total_formula,
      hasFormulaGroupRef: !!formulaConfig.formula_group_ref,
    });

    // Check if this hierarchy OWNS a formula group
    if (formulaConfig.formula_group) {
      const metadata = (hierarchy.metadata as any) || {};
      const formulaRefs = metadata.formula_refs || {};

      // Merge formula_refs from metadata into rules
      const rules = (formulaConfig.formula_group.rules || []).map((rule: any) => {
        const refData = formulaRefs[rule.hierarchyId] || {};
        // Also check if this is the main hierarchy and use its metadata directly
        const isMainRule = rule.hierarchyId === hierarchyId;
        return {
          ...rule,
          formulaRefSource:
            rule.formulaRefSource ||
            refData.formula_ref_source ||
            (isMainRule ? metadata.formula_ref_source : '') ||
            '',
          formulaRefTable:
            rule.formulaRefTable ||
            refData.formula_ref_table ||
            (isMainRule ? metadata.formula_ref_table : '') ||
            '',
        };
      });

      result.isFormulaOwner = true;
      result.ownFormula = {
        groupName: formulaConfig.formula_group.mainHierarchyName || hierarchy.hierarchyName,
        mainHierarchyId: hierarchyId,
        mainHierarchyName: hierarchy.hierarchyName,
        rules: rules,
        formula_params: formulaConfig.formula_group.formula_params || {},
      };
    }

    // Check if this hierarchy OWNS a total_formula (check both formulaConfig and filterConfig)
    const totalFormula = formulaConfig.total_formula || filterConfig.total_formula;
    if (totalFormula) {
      result.isFormulaOwner = true;

      // Convert total_formula children to rules format for consistent display
      const rules = (totalFormula.children || []).map((child: any) => ({
        hierarchyId: child.hierarchyId,
        hierarchyName: child.hierarchyName,
        operation: totalFormula.aggregation || 'SUM',
      }));

      result.ownFormula = {
        groupName: totalFormula.mainHierarchyName || hierarchy.hierarchyName,
        mainHierarchyId: hierarchyId,
        mainHierarchyName: hierarchy.hierarchyName,
        rules: rules,
        formula_params: {},
        formulaType: 'TOTAL_FORMULA',
      };
    }

    // Check if this hierarchy is a CONTRIBUTOR to another formula group or total_formula
    // Method 1: Check if it has formula_group_ref
    let mainHierarchyId = formulaConfig.formula_group_ref;

    // Method 2: If no formula_group_ref, search all hierarchies to see if this one is in their rules
    if (!mainHierarchyId) {
      const allHierarchies = await this.findAll(projectId);
      console.log(
        `[getHierarchyFormulaInfo] Searching ${allHierarchies.length} hierarchies for ${hierarchyId}`,
      );

      for (const otherHierarchy of allHierarchies) {
        const otherFormulaConfig = (otherHierarchy.formulaConfig as any) || {};
        const otherFilterConfig = (otherHierarchy.filterConfig as any) || {};

        // Check formula_group rules
        if (otherFormulaConfig.formula_group && otherFormulaConfig.formula_group.rules) {
          const isInRules = otherFormulaConfig.formula_group.rules.some(
            (rule: any) => rule.hierarchyId === hierarchyId,
          );

          if (isInRules) {
            console.log(
              `[getHierarchyFormulaInfo] Found in formula_group of ${otherHierarchy.hierarchyId}`,
            );
            mainHierarchyId = otherHierarchy.hierarchyId;
            break;
          }
        }

        // Check total_formula children (in both formulaConfig and filterConfig)
        const otherTotalFormula =
          otherFormulaConfig.total_formula || otherFilterConfig.total_formula;
        if (otherTotalFormula) {
          console.log(
            `[getHierarchyFormulaInfo] Checking ${otherHierarchy.hierarchyId} total_formula:`,
            {
              hasTotalFormula: true,
              hasChildren: !!otherTotalFormula.children,
              childrenCount: otherTotalFormula.children?.length || 0,
              children: otherTotalFormula.children,
            },
          );

          if (otherTotalFormula.children) {
            const isInChildren = otherTotalFormula.children.some((child: any) => {
              console.log(
                `[getHierarchyFormulaInfo] Comparing child ${child.hierarchyId} with ${hierarchyId}`,
              );
              return child.hierarchyId === hierarchyId;
            });

            if (isInChildren) {
              console.log(
                `[getHierarchyFormulaInfo] Found in total_formula of ${otherHierarchy.hierarchyId}`,
                {
                  totalFormula: otherTotalFormula,
                },
              );
              mainHierarchyId = otherHierarchy.hierarchyId;
              break;
            }
          }
        }
      }

      if (!mainHierarchyId) {
        console.log(`[getHierarchyFormulaInfo] Not found as contributor in any formula`);
      }
    }

    // If we found a main hierarchy (either via ref or by searching rules/children)
    if (mainHierarchyId) {
      result.isContributor = true;

      // Fetch the main hierarchy to get full formula details
      const mainHierarchy = await this.findOne(projectId, mainHierarchyId);
      if (mainHierarchy) {
        const mainFormulaConfig = (mainHierarchy.formulaConfig as any) || {};
        const mainFilterConfig = (mainHierarchy.filterConfig as any) || {};

        // Check for formula_group first
        const mainFormulaGroup = mainFormulaConfig.formula_group;
        if (mainFormulaGroup) {
          // Verify this hierarchy is actually in the rules
          const myRule = mainFormulaGroup.rules?.find((r: any) => r.hierarchyId === hierarchyId);

          if (myRule) {
            result.contributorOf = {
              mainHierarchyId: mainHierarchy.hierarchyId,
              mainHierarchyName: mainHierarchy.hierarchyName,
              groupName: mainFormulaGroup.mainHierarchyName || mainHierarchy.hierarchyName,
              myRole: myRule.operation,
              myPrecedence: myRule.precedence,
              allRules: mainFormulaGroup.rules || [],
              formulaType: 'FORMULA_GROUP',
            };
          }
        }

        // Check for total_formula (if not already set from formula_group)
        if (!result.contributorOf) {
          const totalFormula = mainFormulaConfig.total_formula || mainFilterConfig.total_formula;
          if (totalFormula) {
            // Verify this hierarchy is actually in the children
            const myChild = totalFormula.children?.find((c: any) => c.hierarchyId === hierarchyId);

            if (myChild) {
              // Convert children to rules format
              const allRules = (totalFormula.children || []).map((child: any) => ({
                hierarchyId: child.hierarchyId,
                hierarchyName: child.hierarchyName,
                operation: totalFormula.aggregation || 'SUM',
              }));

              result.contributorOf = {
                mainHierarchyId: mainHierarchy.hierarchyId,
                mainHierarchyName: mainHierarchy.hierarchyName,
                groupName: mainHierarchy.hierarchyName,
                myRole: totalFormula.aggregation || 'SUM',
                myPrecedence: undefined,
                allRules: allRules,
                formulaType: 'TOTAL_FORMULA',
              };
            }
          }
        }
      }
    }

    // If neither owner nor contributor, return null
    if (!result.isFormulaOwner && !result.isContributor) {
      console.log(`[getHierarchyFormulaInfo] Returning null - no formula involvement`);
      return null;
    }

    console.log(`[getHierarchyFormulaInfo] Returning result:`, {
      isFormulaOwner: result.isFormulaOwner,
      isContributor: result.isContributor,
      hasOwnFormula: !!result.ownFormula,
      hasContributorOf: !!result.contributorOf,
    });

    return result;
  }

  /**
   * Delete formula group from a hierarchy (new approach)
   */
  async deleteFormulaGroupNew(
    projectId: string,
    hierarchyId: string,
  ): Promise<SmartHierarchyMaster> {
    const hierarchy = await this.findOne(projectId, hierarchyId);
    if (!hierarchy) {
      throw new NotFoundException(`Hierarchy ${hierarchyId} not found in project ${projectId}`);
    }

    const formulaConfig = (hierarchy.formulaConfig as any) || {};
    delete formulaConfig.formula_group;
    delete formulaConfig.formula_type;
    delete formulaConfig.formula_text;

    // Set calculation_flag to false
    const flags = (hierarchy.flags as any) || {};
    flags.calculation_flag = false;

    const updated = await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId,
        },
      },
      data: {
        formulaConfig: Object.keys(formulaConfig).length > 0 ? (formulaConfig as any) : null,
        flags: flags as any,
      },
    });

    return updated as any as SmartHierarchyMaster;
  }

  /**
   * Add a rule to an existing formula group
   */
  async addRuleToFormulaGroup(
    projectId: string,
    mainHierarchyId: string,
    rule: {
      hierarchyId: string;
      hierarchyName: string;
      operation:
        | 'ADD'
        | 'SUBTRACT'
        | 'MULTIPLY'
        | 'DIVIDE'
        | 'SUM'
        | 'AVERAGE'
        | 'COUNT'
        | 'MIN'
        | 'MAX';
      precedence: number;
      parameterReference?: string;
      constantNumber?: number;
      formulaRefSource?: string;
      formulaRefTable?: string;
    },
  ): Promise<SmartHierarchyMaster> {
    const hierarchy = await this.findOne(projectId, mainHierarchyId);
    if (!hierarchy) {
      throw new NotFoundException(`Hierarchy ${mainHierarchyId} not found in project ${projectId}`);
    }

    const formulaConfig = (hierarchy.formulaConfig as any) || {};
    if (!formulaConfig.formula_group) {
      throw new NotFoundException(`Formula group not found in hierarchy ${mainHierarchyId}`);
    }

    // Validate rule hierarchy exists
    const ruleHierarchy = await this.findOne(projectId, rule.hierarchyId);
    if (!ruleHierarchy) {
      throw new NotFoundException(
        `Rule hierarchy ${rule.hierarchyId} not found in project ${projectId}`,
      );
    }

    // Check if rule already exists
    const exists = formulaConfig.formula_group.rules.some(
      (r: any) => r.hierarchyId === rule.hierarchyId,
    );
    if (exists) {
      throw new BadRequestException(
        `Rule hierarchy ${rule.hierarchyId} already exists in formula group`,
      );
    }

    // Add rule with formula ref fields
    const newRule = {
      ...rule,
      formulaRefSource: rule.formulaRefSource || '',
      formulaRefTable: rule.formulaRefTable || '',
    };
    formulaConfig.formula_group.rules.push(newRule);

    // Store formula_ref_source and formula_ref_table in metadata
    const metadata = (hierarchy.metadata as any) || {};
    if (rule.formulaRefSource || rule.formulaRefTable) {
      if (!metadata.formula_refs) {
        metadata.formula_refs = {};
      }
      metadata.formula_refs[rule.hierarchyId] = {
        formula_ref_source: rule.formulaRefSource || '',
        formula_ref_table: rule.formulaRefTable || '',
      };
    }

    const updated = await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId: mainHierarchyId,
        },
      },
      data: {
        formulaConfig: formulaConfig as any,
        metadata: metadata as any,
      },
    });

    return updated as any as SmartHierarchyMaster;
  }

  /**
   * Remove a rule from an existing formula group
   */
  async removeRuleFromFormulaGroup(
    projectId: string,
    mainHierarchyId: string,
    ruleHierarchyId: string,
  ): Promise<SmartHierarchyMaster> {
    const hierarchy = await this.findOne(projectId, mainHierarchyId);
    if (!hierarchy) {
      throw new NotFoundException(`Hierarchy ${mainHierarchyId} not found in project ${projectId}`);
    }

    const formulaConfig = (hierarchy.formulaConfig as any) || {};
    if (!formulaConfig.formula_group) {
      throw new NotFoundException(`Formula group not found in hierarchy ${mainHierarchyId}`);
    }

    // Remove rule
    formulaConfig.formula_group.rules = formulaConfig.formula_group.rules.filter(
      (r: any) => r.hierarchyId !== ruleHierarchyId,
    );

    const updated = await this.prisma.smartHierarchyMaster.update({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId: mainHierarchyId,
        },
      },
      data: {
        formulaConfig: formulaConfig as any,
      },
    });

    return updated as any as SmartHierarchyMaster;
  }

  // ============================================================================
  // Deployment History Management
  // ============================================================================

  async getProjectById(projectId: string): Promise<any> {
    const project = await this.prisma.hierarchyProject.findUnique({
      where: { id: projectId },
    });

    if (!project) {
      throw new NotFoundException('Project not found');
    }

    return project;
  }

  async getDeploymentHistory(
    projectId: string,
    limit: number = 50,
    offset: number = 0,
  ): Promise<any> {
    const deployments = await this.prisma.deploymentHistory.findMany({
      where: { projectId },
      orderBy: { deployedAt: 'desc' },
      take: limit,
      skip: offset,
    });

    const total = await this.prisma.deploymentHistory.count({
      where: { projectId },
    });

    return {
      deployments,
      total,
      limit,
      offset,
    };
  }

  async getDeploymentById(id: string): Promise<any> {
    const deployment = await this.prisma.deploymentHistory.findUnique({
      where: { id },
    });

    if (!deployment) {
      throw new NotFoundException('Deployment not found');
    }

    return deployment;
  }
}
