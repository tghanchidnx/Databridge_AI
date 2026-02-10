import {
  Controller,
  Get,
  Post,
  Put,
  Delete,
  Body,
  Param,
  Query,
  UseGuards,
  Req,
  Logger,
} from '@nestjs/common';
import { ApiTags, ApiOperation, ApiBearerAuth } from '@nestjs/swagger';
import { JwtAuthGuard } from '../../../common/guards/jwt-auth.guard';
import { CurrentUser } from '../../../common/decorators/current-user.decorator';
import { SmartHierarchyService } from '../services/smart-hierarchy.service';
import { ScriptGeneratorService } from '../services/script-generator.service';
import { SnowflakeDeploymentService } from '../services/snowflake-deployment.service';
import { CsvExportImportService } from '../services/csv-export-import.service';
import { OldFormatMappingService } from '../services/old-format-mapping.service';
import { OldFormatImportV2Service } from '../services/old-format-import-v2.service';
import { CsvAnalyzerService } from '../services/csv-analyzer.service';
import { ChunkedUploadService } from '../services/chunked-upload.service';
import { UploadLogService } from '../services/upload-log.service';
import { ReferenceTableService } from '../services/reference-table.service';
import {
  CreateSmartHierarchyDto,
  UpdateSmartHierarchyDto,
  ExportProjectDto,
  ImportProjectDto,
  GenerateScriptDto,
  PushToSnowflakeDto,
  CreateProjectDto,
  UpdateProjectDto,
  CreateTotalFormulaDto,
  TotalFormulaChildDto,
  CreateFormulaGroupDto,
  UpdateFormulaGroupDto,
  FormulaGroupRuleDto,
  BulkDeleteHierarchiesDto,
  BulkUpdateOrderDto,
} from '../dto/smart-hierarchy.dto';

@ApiTags('Smart Hierarchy')
@Controller('smart-hierarchy')
@UseGuards(JwtAuthGuard)
@ApiBearerAuth()
export class SmartHierarchyController {
  private readonly logger = new Logger(SmartHierarchyController.name);

  constructor(
    private readonly hierarchyService: SmartHierarchyService,
    private readonly scriptService: ScriptGeneratorService,
    private readonly snowflakeService: SnowflakeDeploymentService,
    private readonly csvService: CsvExportImportService,
    private readonly mappingService: OldFormatMappingService,
    private readonly oldFormatImportV2Service: OldFormatImportV2Service,
    private readonly csvAnalyzerService: CsvAnalyzerService,
    private readonly chunkedUploadService: ChunkedUploadService,
    private readonly uploadLogService: UploadLogService,
    private readonly referenceTableService: ReferenceTableService,
  ) {}

  // ============================================================================
  // Dashboard Endpoints
  // ============================================================================

  @Get('dashboard/stats')
  @ApiOperation({ summary: 'Get dashboard statistics' })
  async getDashboardStats(@Req() req: any) {
    return await this.hierarchyService.getDashboardStats(req.user.id);
  }

  @Get('dashboard/activities')
  @ApiOperation({ summary: 'Get recent activities' })
  async getDashboardActivities(@Req() req: any, @Query('limit') limit?: string) {
    const limitNum = limit ? parseInt(limit, 10) : 10;
    return await this.hierarchyService.getDashboardActivities(req.user.id, limitNum);
  }

  @Get('dashboard/connections')
  @ApiOperation({ summary: 'Get recent connections for dashboard' })
  async getDashboardConnections(@Req() req: any, @Query('limit') limit?: string) {
    const limitNum = limit ? parseInt(limit, 10) : 4;
    return await this.hierarchyService.getDashboardConnections(req.user.id, limitNum);
  }

  // ============================================================================
  // Project Management Endpoints
  // ============================================================================

  @Get('projects')
  @ApiOperation({ summary: 'Get all projects for current user' })
  async getProjects(@Req() req: any) {
    return await this.hierarchyService.getProjects(req.user.id);
  }

  @Get('projects/pending-invitations')
  @ApiOperation({ summary: 'Get pending project invitations for current user' })
  async getPendingInvitations(@Req() req: any) {
    return await this.hierarchyService.getPendingInvitations(req.user.id);
  }

  @Get('projects/:projectId')
  @ApiOperation({ summary: 'Get a specific project' })
  async getProject(@Req() req: any, @Param('projectId') projectId: string) {
    return await this.hierarchyService.getProject(projectId, req.user.id);
  }

  @Post('projects')
  @ApiOperation({ summary: 'Create a new project' })
  async createProject(@Req() req: any, @Body() dto: CreateProjectDto) {
    return await this.hierarchyService.createProject({
      ...dto,
      userId: req.user.id,
    });
  }

  @Put('projects/:projectId')
  @ApiOperation({ summary: 'Update a project' })
  async updateProject(
    @Req() req: any,
    @Param('projectId') projectId: string,
    @Body() dto: UpdateProjectDto,
  ) {
    return await this.hierarchyService.updateProject(projectId, req.user.id, dto);
  }

  @Delete('projects/:projectId')
  @ApiOperation({ summary: 'Delete a project (soft delete)' })
  async deleteProject(@Req() req: any, @Param('projectId') projectId: string) {
    await this.hierarchyService.deleteProject(projectId, req.user.id);
    return { message: 'Project deleted successfully' };
  }

  // ============================================================================
  // Project Member Management Endpoints
  // ============================================================================

  @Get('projects/:projectId/members')
  @ApiOperation({ summary: 'Get all members of a project' })
  async getProjectMembers(@Req() req: any, @Param('projectId') projectId: string) {
    return await this.hierarchyService.getProjectMembers(projectId, req.user.id);
  }

  @Post('projects/:projectId/members')
  @ApiOperation({ summary: 'Invite a member to project' })
  async inviteProjectMember(
    @Req() req: any,
    @Param('projectId') projectId: string,
    @Body()
    dto: {
      userEmail?: string;
      inviteUserId?: string;
      role: 'editor' | 'viewer';
      accessType?: 'direct' | 'organization';
    },
  ) {
    return await this.hierarchyService.inviteProjectMember(projectId, req.user.id, dto);
  }

  @Post('projects/:projectId/share-organization')
  @ApiOperation({ summary: 'Share project with entire organization' })
  async shareWithOrganization(
    @Req() req: any,
    @Param('projectId') projectId: string,
    @Body() dto: { role: 'editor' | 'viewer' },
  ) {
    return await this.hierarchyService.shareProjectWithOrganization(
      projectId,
      req.user.id,
      dto.role,
    );
  }

  @Put('projects/:projectId/members/:memberId')
  @ApiOperation({ summary: 'Update member permissions' })
  async updateProjectMember(
    @Req() req: any,
    @Param('projectId') projectId: string,
    @Param('memberId') memberId: string,
    @Body() dto: { role?: 'editor' | 'viewer'; isActive?: boolean },
  ) {
    return await this.hierarchyService.updateProjectMember(projectId, memberId, req.user.id, dto);
  }

  @Delete('projects/:projectId/members/:memberId')
  @ApiOperation({ summary: 'Remove a member from project' })
  async removeProjectMember(
    @Req() req: any,
    @Param('projectId') projectId: string,
    @Param('memberId') memberId: string,
  ) {
    await this.hierarchyService.removeProjectMember(projectId, memberId, req.user.id);
    return { message: 'Member removed successfully' };
  }

  @Post('projects/:projectId/members/:memberId/accept')
  @ApiOperation({ summary: 'Accept project invitation' })
  async acceptProjectInvitation(
    @Req() req: any,
    @Param('projectId') projectId: string,
    @Param('memberId') memberId: string,
  ) {
    return await this.hierarchyService.acceptProjectInvitation(projectId, memberId, req.user.id);
  }

  @Post('projects/:projectId/members/:memberId/decline')
  @ApiOperation({ summary: 'Decline project invitation' })
  async declineProjectInvitation(
    @Req() req: any,
    @Param('projectId') projectId: string,
    @Param('memberId') memberId: string,
  ) {
    return await this.hierarchyService.declineProjectInvitation(projectId, memberId, req.user.id);
  }

  // ============================================================================
  // CRUD Endpoints
  // ============================================================================

  @Post()
  @ApiOperation({ summary: 'Create a new smart hierarchy' })
  async create(
    @CurrentUser('email') userEmail: string,
    @CurrentUser('id') userId: string,
    @Body() dto: CreateSmartHierarchyDto,
  ) {
    this.logger.log(`Creating hierarchy by user: ${userEmail} (${userId})`);
    return await this.hierarchyService.create({
      ...dto,
      createdBy: userEmail,
      updatedBy: userEmail,
    });
  }

  @Get('project/:projectId')
  @ApiOperation({ summary: 'Get all hierarchies for a project' })
  async findAll(@Param('projectId') projectId: string, @Query('minimal') minimal?: string) {
    if (minimal === 'true') {
      return await this.hierarchyService.findAllMinimal(projectId);
    }
    return await this.hierarchyService.findAll(projectId);
  }

  @Get('project/:projectId/tree')
  @ApiOperation({ summary: 'Get hierarchy tree for project' })
  async getTree(@Param('projectId') projectId: string) {
    return await this.hierarchyService.buildHierarchyTree(projectId);
  }

  @Get('project/:projectId/:hierarchyId')
  @ApiOperation({ summary: 'Get a specific hierarchy' })
  async findOne(@Param('projectId') projectId: string, @Param('hierarchyId') hierarchyId: string) {
    return await this.hierarchyService.findOne(projectId, hierarchyId);
  }

  @Get('project/:projectId/:hierarchyId/dependencies')
  @ApiOperation({ summary: 'Get hierarchy dependencies and relationships' })
  async getDependencies(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
  ) {
    return await this.hierarchyService.getHierarchyDependencies(projectId, hierarchyId);
  }

  @Put('project/:projectId/:hierarchyId')
  @ApiOperation({ summary: 'Update a hierarchy' })
  async update(
    @CurrentUser('email') userEmail: string,
    @CurrentUser('id') userId: string,
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
    @Body() dto: UpdateSmartHierarchyDto,
  ) {
    this.logger.log(`Updating hierarchy ${hierarchyId} by user: ${userEmail} (${userId})`);
    return await this.hierarchyService.update(projectId, hierarchyId, {
      ...dto,
      updatedBy: userEmail,
    });
  }

  @Delete('project/:projectId/:hierarchyId')
  @ApiOperation({ summary: 'Delete a hierarchy' })
  async delete(@Param('projectId') projectId: string, @Param('hierarchyId') hierarchyId: string) {
    await this.hierarchyService.delete(projectId, hierarchyId);
    return { message: 'Hierarchy deleted successfully' };
  }

  @Post('project/:projectId/bulk-delete')
  @ApiOperation({ summary: 'Delete multiple hierarchies' })
  async bulkDelete(@Param('projectId') projectId: string, @Body() dto: BulkDeleteHierarchiesDto) {
    const result = await this.hierarchyService.bulkDelete(projectId, dto.hierarchyIds);
    return result;
  }

  @Post('project/:projectId/bulk-update-order')
  @ApiOperation({ summary: 'Bulk update hierarchy order and parent relationships' })
  async bulkUpdateOrder(
    @CurrentUser('email') userEmail: string,
    @Param('projectId') projectId: string,
    @Body() dto: BulkUpdateOrderDto,
  ) {
    this.logger.log(`Bulk updating hierarchy order by user: ${userEmail}`);
    return await this.hierarchyService.bulkUpdateOrder(projectId, dto.updates);
  }

  // ============================================================================
  // Export/Import Endpoints
  // ============================================================================

  @Post('export')
  @ApiOperation({ summary: 'Export project to JSON backup' })
  async exportProject(@CurrentUser('email') userEmail: string, @Body() dto: ExportProjectDto) {
    this.logger.log(`Exporting project by user: ${userEmail}`);
    return await this.hierarchyService.exportProject({
      ...dto,
      createdBy: userEmail,
    });
  }

  @Post('import')
  @ApiOperation({ summary: 'Import project from JSON backup' })
  async importProject(@CurrentUser('email') userEmail: string, @Body() dto: ImportProjectDto) {
    this.logger.log(`Importing project by user: ${userEmail}`);
    return await this.hierarchyService.importProject(dto);
  }

  @Get('exports/:projectId')
  @ApiOperation({ summary: 'List all exports for a project' })
  async listExports(@Param('projectId') projectId: string) {
    return await this.hierarchyService.listExports(projectId);
  }

  @Get('export/:exportId')
  @ApiOperation({ summary: 'Get a specific export' })
  async getExport(@Param('exportId') exportId: string) {
    return await this.hierarchyService.getExport(exportId);
  }

  // ============================================================================
  // Script Generation Endpoints
  // ============================================================================

  @Post('generate-scripts')
  @ApiOperation({ summary: 'Generate SQL scripts (INSERT, VIEW, DT)' })
  async generateScripts(
    @CurrentUser('email') userEmail: string,
    @CurrentUser('id') userId: string,
    @Body() dto: GenerateScriptDto,
  ) {
    this.logger.log(`Generating scripts by user: ${userEmail} (${userId})`);
    return await this.scriptService.generateScripts({
      ...dto,
      deployedBy: userEmail,
    });
  }

  // ============================================================================
  // Snowflake Deployment Endpoints
  // ============================================================================

  @Post('push-to-snowflake')
  @ApiOperation({ summary: 'Push hierarchy to Snowflake' })
  async pushToSnowflake(
    @CurrentUser('email') userEmail: string,
    @CurrentUser('id') userId: string,
    @Body() dto: PushToSnowflakeDto,
  ) {
    this.logger.log(`Deploying to Snowflake by user: ${userEmail} (${userId})`);
    return await this.snowflakeService.pushToSnowflake({
      ...dto,
      deployedBy: userEmail,
    });
  }

  @Get('projects/:projectId/deployment-config')
  @ApiOperation({ summary: 'Get saved deployment configuration for project' })
  async getDeploymentConfig(@Param('projectId') projectId: string) {
    const project = await this.hierarchyService.getProjectById(projectId);
    return project.deploymentConfig || null;
  }

  @Get('projects/:projectId/deployment-history')
  @ApiOperation({ summary: 'Get deployment history for project' })
  async getDeploymentHistory(
    @Param('projectId') projectId: string,
    @Query('limit') limit?: number,
    @Query('offset') offset?: number,
  ) {
    return await this.hierarchyService.getDeploymentHistory(
      projectId,
      limit ? parseInt(limit.toString()) : 50,
      offset ? parseInt(offset.toString()) : 0,
    );
  }

  @Get('deployment-history/:id')
  @ApiOperation({ summary: 'Get single deployment record with scripts' })
  async getDeploymentById(@Param('id') id: string) {
    return await this.hierarchyService.getDeploymentById(id);
  }

  // ============================================================================
  // Formula Group Management Endpoints
  // ============================================================================

  // Old endpoints removed - use /formula PUT and /link-formula-group POST instead
  // Kept for backward compatibility - redirects to new methods
  @Put('projects/:projectId/hierarchies/:hierarchyId/formula-group')
  @ApiOperation({ summary: 'Update formula (legacy endpoint - use PUT /formula instead)' })
  async updateFormulaGroup(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
    @Body() formulaGroup: any,
  ) {
    return await this.hierarchyService.saveFormulaToHierarchy(projectId, hierarchyId, formulaGroup);
  }

  @Post('projects/:projectId/hierarchies/:hierarchyId/assign-to-formula-group')
  @ApiOperation({
    summary: 'Assign to formula (legacy endpoint - use POST /link-formula-group instead)',
  })
  async assignToFormulaGroup(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
    @Body()
    dto: {
      mainHierarchyId: string;
      role: string;
      FORMULA_PRECEDENCE?: number;
      FORMULA_PARAM_REF?: string;
      FORMULA_PARAM2_CONST_NUMBER?: number;
    },
  ) {
    return await this.hierarchyService.linkHierarchyToFormulaGroup(projectId, hierarchyId, dto);
  }

  @Delete('projects/:projectId/hierarchies/:hierarchyId/unlink-from-formula-group')
  @ApiOperation({ summary: 'Unlink hierarchy from formula group' })
  async unlinkFromFormulaGroup(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
  ) {
    return await this.hierarchyService.unlinkFromFormulaGroup(projectId, hierarchyId);
  }

  @Get('projects/:projectId/hierarchies/:hierarchyId/formula-group')
  @ApiOperation({ summary: 'Get formula group for a hierarchy' })
  async getFormulaGroup(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
  ) {
    return await this.hierarchyService.getFormulaGroup(projectId, hierarchyId);
  }

  @Delete('projects/:projectId/hierarchies/:hierarchyId/formula-group')
  @ApiOperation({ summary: 'Remove formula group from a hierarchy' })
  async removeFormulaGroup(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
  ) {
    return await this.hierarchyService.removeFormulaGroup(projectId, hierarchyId);
  }

  @Get('projects/:projectId/hierarchies/search')
  @ApiOperation({ summary: 'Search hierarchies in a project' })
  async searchHierarchies(
    @Param('projectId') projectId: string,
    @Query('searchTerm') searchTerm?: string,
    @Query('excludeIds') excludeIds?: string,
  ) {
    const excludeArray = excludeIds ? excludeIds.split(',') : [];
    return await this.hierarchyService.searchHierarchies(projectId, searchTerm, excludeArray);
  }

  @Get('projects/:projectId/formula-groups')
  @ApiOperation({ summary: 'Get all formula groups in a project (lightweight)' })
  async getFormulaGroups(@Param('projectId') projectId: string, @Query('search') search?: string) {
    return await this.hierarchyService.getFormulaGroups(projectId, search);
  }

  @Get('projects/:projectId/hierarchies/:hierarchyId/formula-usages')
  @ApiOperation({ summary: 'Get hierarchies that use this hierarchy in their formula groups' })
  async getFormulaGroupUsages(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
  ) {
    return await this.hierarchyService.getFormulaGroupUsages(projectId, hierarchyId);
  }

  // ============================================================================
  // Formula Groups (NEW - Standardized)
  // ============================================================================

  // Get all hierarchies with formulas for a project
  @Get('projects/:projectId/formulas')
  @ApiOperation({ summary: 'Get all hierarchies with formulas for a project' })
  async getProjectFormulas(@Param('projectId') projectId: string) {
    return await this.hierarchyService.getProjectFormulas(projectId);
  }

  // Get formula from specific hierarchy
  @Get('projects/:projectId/hierarchies/:hierarchyId/formula')
  @ApiOperation({ summary: 'Get formula configuration from a specific hierarchy' })
  async getFormulaFromHierarchy(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
  ) {
    return await this.hierarchyService.getFormulaFromHierarchy(projectId, hierarchyId);
  }

  // Save/Create formula in hierarchy
  @Put('projects/:projectId/hierarchies/:hierarchyId/formula')
  @ApiOperation({
    summary: 'Save formula to hierarchy (creates/updates formula_group in formulaConfig)',
  })
  async saveFormulaToHierarchy(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
    @Body() formulaGroup: any,
  ) {
    return await this.hierarchyService.saveFormulaToHierarchy(projectId, hierarchyId, formulaGroup);
  }

  // Remove formula from hierarchy
  @Delete('projects/:projectId/hierarchies/:hierarchyId/formula')
  @ApiOperation({ summary: 'Remove formula from hierarchy' })
  async removeFormulaFromHierarchy(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
  ) {
    return await this.hierarchyService.removeFormulaFromHierarchy(projectId, hierarchyId);
  }

  // Link hierarchy to existing formula (creates reference to main hierarchy)
  @Post('projects/:projectId/hierarchies/:hierarchyId/link-formula-group')
  @ApiOperation({
    summary: 'Link hierarchy to existing formula group (create reference to main hierarchy)',
  })
  async linkHierarchyToFormulaGroup(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
    @Body() body: any,
  ) {
    return await this.hierarchyService.linkHierarchyToFormulaGroup(projectId, hierarchyId, body);
  }

  // Unlink hierarchy from formula group
  @Delete('projects/:projectId/hierarchies/:hierarchyId/unlink-formula-group')
  @ApiOperation({ summary: 'Unlink hierarchy from formula group (remove reference and formula)' })
  async unlinkHierarchyFromFormulaGroup(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
  ) {
    return await this.hierarchyService.unlinkHierarchyFromFormulaGroup(projectId, hierarchyId);
  }

  // ============================================================================
  // Filter Groups (NEW - Standardized)
  // ============================================================================

  @Post('filter-groups')
  @ApiOperation({ summary: 'Create a new filter group (stored separately)' })
  async createFilterGroup(@Body() body: any) {
    return await this.hierarchyService.createFilterGroup(body);
  }

  @Get('filter-groups/:id')
  @ApiOperation({ summary: 'Get filter group by ID' })
  async getFilterGroupById(@Param('id') id: string) {
    return await this.hierarchyService.getFilterGroupById(id);
  }

  @Put('filter-groups/:id')
  @ApiOperation({ summary: 'Update filter group by ID' })
  async updateFilterGroupById(@Param('id') id: string, @Body() body: any) {
    return await this.hierarchyService.updateFilterGroupById(id, body);
  }

  @Delete('filter-groups/:id')
  @ApiOperation({ summary: 'Delete filter group by ID' })
  async deleteFilterGroup(@Param('id') id: string) {
    return await this.hierarchyService.deleteFilterGroup(id);
  }

  @Get('projects/:projectId/filter-groups-list')
  @ApiOperation({ summary: 'List all filter groups for a project' })
  async listFilterGroups(@Param('projectId') projectId: string) {
    return await this.hierarchyService.listFilterGroups(projectId);
  }

  @Post('projects/:projectId/hierarchies/:hierarchyId/assign-filter-group')
  @ApiOperation({ summary: 'Assign hierarchy to filter group (create reference)' })
  async assignHierarchyToFilterGroup(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
    @Body() body: any,
  ) {
    return await this.hierarchyService.assignHierarchyToFilterGroup(projectId, hierarchyId, body);
  }

  @Delete('projects/:projectId/hierarchies/:hierarchyId/unassign-filter-group')
  @ApiOperation({ summary: 'Unassign hierarchy from filter group (remove reference)' })
  async unassignHierarchyFromFilterGroup(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
  ) {
    return await this.hierarchyService.unassignHierarchyFromFilterGroup(projectId, hierarchyId);
  }

  @Get('projects/:projectId/hierarchies/:hierarchyId/filter-attributes')
  @ApiOperation({ summary: 'Get filter attributes from a hierarchy for copying' })
  async getHierarchyFilterAttributes(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
  ) {
    return await this.hierarchyService.getHierarchyFilterAttributes(projectId, hierarchyId);
  }

  // ============================================================================
  // CSV Export/Import Endpoints (Separate Hierarchy & Mapping)
  // ============================================================================

  @Get('projects/:projectId/export-hierarchy-csv')
  @ApiOperation({ summary: 'Export hierarchies to CSV' })
  async exportHierarchyCSV(@Param('projectId') projectId: string, @Req() req: any) {
    const project = await this.hierarchyService.getProject(projectId, req.user.id);
    const csvContent = await this.csvService.exportHierarchyCSV(projectId);
    const projectName = project.name.toUpperCase().replace(/[^A-Z0-9_-]/g, '_');
    const dateStr = new Date().toISOString().split('T')[0];
    return {
      content: csvContent,
      filename: `HIERARCHY_${projectName}_${dateStr}.csv`,
      contentType: 'text/csv',
    };
  }

  @Get('projects/:projectId/export-mapping-csv')
  @ApiOperation({ summary: 'Export mappings to CSV' })
  async exportMappingCSV(@Param('projectId') projectId: string, @Req() req: any) {
    const project = await this.hierarchyService.getProject(projectId, req.user.id);
    const csvContent = await this.csvService.exportMappingCSV(projectId);
    const projectName = project.name.toUpperCase().replace(/[^A-Z0-9_-]/g, '_');
    const dateStr = new Date().toISOString().split('T')[0];
    return {
      content: csvContent,
      filename: `MAPPING_${projectName}_${dateStr}.csv`,
      contentType: 'text/csv',
    };
  }

  @Post('projects/:projectId/import-hierarchy-csv')
  @ApiOperation({ summary: 'Import hierarchies from CSV (auto-detects format)' })
  async importHierarchyCSV(
    @Param('projectId') projectId: string,
    @Body() body: { csvContent: string },
  ) {
    return await this.oldFormatImportV2Service.importHierarchyCSV(projectId, body.csvContent);
  }

  @Post('projects/:projectId/import-mapping-csv')
  @ApiOperation({ summary: 'Import mappings from CSV' })
  async importMappingCSV(
    @Param('projectId') projectId: string,
    @Body() body: { csvContent: string },
  ) {
    return await this.mappingService.importMappingCSV(projectId, body.csvContent);
  }

  @Post('projects/:projectId/import-both-csv')
  @ApiOperation({ summary: 'Import both hierarchy and mapping CSVs in one operation' })
  async importBothCSVs(
    @Param('projectId') projectId: string,
    @Body() body: { hierarchyCSV: string; mappingCSV: string },
  ) {
    // Import hierarchy first using V2 service
    const hierarchyResult = await this.oldFormatImportV2Service.importHierarchyCSV(
      projectId,
      body.hierarchyCSV,
    );
    // Then import mappings
    const mappingResult = await this.mappingService.importMappingCSV(projectId, body.mappingCSV);

    return {
      hierarchy: hierarchyResult,
      mapping: mappingResult,
    };
  }

  @Post('projects/:projectId/analyze-hierarchy-csv')
  @ApiOperation({ summary: 'Analyze hierarchy CSV and provide insights (DEPRECATED)' })
  async analyzeHierarchyCSV(
    @Param('projectId') projectId: string,
    @Body() body: { csvContent: string },
  ) {
    // This endpoint is deprecated - CSV import now handles all formats automatically
    return {
      message: 'CSV analysis is no longer needed - import endpoint auto-detects format',
      deprecated: true,
    };
  }

  // ============================================================================
  // Enhanced CSV Import with Analysis & Auto-Fix (NEW)
  // ============================================================================

  @Post('projects/:projectId/analyze-csv')
  @ApiOperation({ summary: 'Analyze CSV file before import - detect issues and suggest fixes' })
  async analyzeCSV(
    @Param('projectId') projectId: string,
    @Body() body: { csvContent: string; fileName?: string },
  ) {
    return await this.csvAnalyzerService.analyzeCSV(body.csvContent, body.fileName);
  }

  @Post('projects/:projectId/smart-import-hierarchy')
  @ApiOperation({ summary: 'Smart import hierarchy with analysis, auto-fix, and detailed logging' })
  async smartImportHierarchy(
    @Param('projectId') projectId: string,
    @Body() body: {
      csvContent: string;
      fileName?: string;
      options?: {
        autoFix?: boolean;
        validateBeforeImport?: boolean;
        dryRun?: boolean;
      };
    },
  ) {
    return await this.chunkedUploadService.directImport(
      projectId,
      body.csvContent,
      body.fileName || 'hierarchy.csv',
      'hierarchy',
      body.options,
    );
  }

  @Post('projects/:projectId/smart-import-mapping')
  @ApiOperation({ summary: 'Smart import mapping with analysis, auto-fix, and detailed logging' })
  async smartImportMapping(
    @Param('projectId') projectId: string,
    @Body() body: {
      csvContent: string;
      fileName?: string;
      options?: {
        autoFix?: boolean;
        validateBeforeImport?: boolean;
        dryRun?: boolean;
      };
    },
  ) {
    return await this.chunkedUploadService.directImport(
      projectId,
      body.csvContent,
      body.fileName || 'mapping.csv',
      'mapping',
      body.options,
    );
  }

  @Post('projects/:projectId/smart-import-both')
  @ApiOperation({ summary: 'Smart import both hierarchy and mapping with full analysis' })
  async smartImportBoth(
    @Param('projectId') projectId: string,
    @Body() body: {
      hierarchyCSV: string;
      mappingCSV: string;
      hierarchyFileName?: string;
      mappingFileName?: string;
      options?: {
        autoFix?: boolean;
        validateBeforeImport?: boolean;
        dryRun?: boolean;
      };
    },
  ) {
    // Import hierarchy first
    const hierarchyResult = await this.chunkedUploadService.directImport(
      projectId,
      body.hierarchyCSV,
      body.hierarchyFileName || 'hierarchy.csv',
      'hierarchy',
      body.options,
    );

    // Only import mapping if hierarchy succeeded
    if (hierarchyResult.success || hierarchyResult.rowsImported > 0) {
      const mappingResult = await this.chunkedUploadService.directImport(
        projectId,
        body.mappingCSV,
        body.mappingFileName || 'mapping.csv',
        'mapping',
        body.options,
      );

      return {
        hierarchy: hierarchyResult,
        mapping: mappingResult,
        overallSuccess: hierarchyResult.success && mappingResult.success,
      };
    }

    return {
      hierarchy: hierarchyResult,
      mapping: null,
      overallSuccess: false,
      message: 'Mapping import skipped due to hierarchy import failure',
    };
  }

  // ============================================================================
  // Chunked Upload Endpoints (for large files)
  // ============================================================================

  @Post('upload/initialize')
  @ApiOperation({ summary: 'Initialize a chunked upload session for large files' })
  async initializeChunkedUpload(
    @Req() req: any,
    @Body() body: {
      fileName: string;
      fileSize: number;
      totalChunks: number;
      projectId?: string;
    },
  ) {
    return this.chunkedUploadService.initializeUpload({
      ...body,
      userId: req.user?.id,
    });
  }

  @Post('upload/:uploadId/chunk/:chunkIndex')
  @ApiOperation({ summary: 'Upload a chunk of data' })
  async uploadChunk(
    @Param('uploadId') uploadId: string,
    @Param('chunkIndex') chunkIndex: string,
    @Body() body: { data: string },
  ) {
    return await this.chunkedUploadService.receiveChunk(
      uploadId,
      parseInt(chunkIndex, 10),
      body.data,
    );
  }

  @Post('upload/:uploadId/process')
  @ApiOperation({ summary: 'Process completed chunked upload' })
  async processChunkedUpload(
    @Param('uploadId') uploadId: string,
    @Body() body: {
      projectId: string;
      fileType: 'hierarchy' | 'mapping';
      options?: {
        autoFix?: boolean;
        validateBeforeImport?: boolean;
        dryRun?: boolean;
      };
    },
  ) {
    return await this.chunkedUploadService.processUpload(
      uploadId,
      body.projectId,
      body.fileType,
      body.options,
    );
  }

  @Get('upload/:uploadId/status')
  @ApiOperation({ summary: 'Get status of chunked upload' })
  async getUploadStatus(@Param('uploadId') uploadId: string) {
    const status = this.chunkedUploadService.getSessionStatus(uploadId);
    if (!status) {
      return { error: 'Upload session not found' };
    }
    return {
      uploadId: status.uploadId,
      fileName: status.fileName,
      fileSize: status.fileSize,
      progress: (status.receivedChunks.length / status.totalChunks) * 100,
      receivedChunks: status.receivedChunks.length,
      totalChunks: status.totalChunks,
      expiresAt: status.expiresAt,
    };
  }

  @Delete('upload/:uploadId')
  @ApiOperation({ summary: 'Cancel a chunked upload' })
  async cancelUpload(@Param('uploadId') uploadId: string) {
    const cancelled = this.chunkedUploadService.cancelUpload(uploadId);
    return { cancelled };
  }

  // ============================================================================
  // Upload Logs
  // ============================================================================

  @Get('projects/:projectId/upload-logs')
  @ApiOperation({ summary: 'Get upload logs for a project' })
  async getProjectUploadLogs(
    @Param('projectId') projectId: string,
    @Query('limit') limit?: string,
  ) {
    const limitNum = limit ? parseInt(limit, 10) : 50;
    return await this.uploadLogService.getProjectLogs(projectId, limitNum);
  }

  @Get('upload-logs/:logId')
  @ApiOperation({ summary: 'Get a specific upload log' })
  async getUploadLog(@Param('logId') logId: string) {
    const log = this.uploadLogService.getLog(logId);
    if (!log) {
      return { error: 'Log not found' };
    }
    return log;
  }

  @Get('upload-logs/:logId/report')
  @ApiOperation({ summary: 'Get formatted report for an upload log' })
  async getUploadLogReport(@Param('logId') logId: string) {
    return {
      report: this.uploadLogService.generateReport(logId),
    };
  }

  // ============================================================================
  // Total Formula Management Endpoints
  // ============================================================================

  @Post('projects/:projectId/hierarchies/:hierarchyId/total-formula')
  @ApiOperation({ summary: 'Create or update total formula in hierarchy' })
  async createOrUpdateTotalFormula(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
    @Body() data: CreateTotalFormulaDto,
  ) {
    return await this.hierarchyService.createOrUpdateTotalFormula(projectId, hierarchyId, data);
  }

  @Get('projects/:projectId/hierarchies/:hierarchyId/total-formula')
  @ApiOperation({ summary: 'Get total formula from hierarchy' })
  async getTotalFormula(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
  ) {
    return await this.hierarchyService.getTotalFormula(projectId, hierarchyId);
  }

  @Get('projects/:projectId/total-formulas')
  @ApiOperation({ summary: 'List all hierarchies with total formulas' })
  async listHierarchiesWithTotalFormulas(@Param('projectId') projectId: string) {
    return await this.hierarchyService.listHierarchiesWithTotalFormulas(projectId);
  }

  @Delete('projects/:projectId/hierarchies/:hierarchyId/total-formula')
  @ApiOperation({ summary: 'Delete total formula from hierarchy' })
  async deleteTotalFormula(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
  ) {
    return await this.hierarchyService.deleteTotalFormula(projectId, hierarchyId);
  }

  @Post('projects/:projectId/hierarchies/:hierarchyId/total-formula/children')
  @ApiOperation({ summary: 'Add child to total formula' })
  async addChildToTotalFormula(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
    @Body() child: TotalFormulaChildDto,
  ) {
    return await this.hierarchyService.addChildToTotalFormula(projectId, hierarchyId, child);
  }

  @Delete('projects/:projectId/hierarchies/:hierarchyId/total-formula/children/:childHierarchyId')
  @ApiOperation({ summary: 'Remove child from total formula' })
  async removeChildFromTotalFormula(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
    @Param('childHierarchyId') childHierarchyId: string,
  ) {
    return await this.hierarchyService.removeChildFromTotalFormula(
      projectId,
      hierarchyId,
      childHierarchyId,
    );
  }

  // ============================================================================
  // Formula Group Endpoints
  // ============================================================================

  @Post('projects/:projectId/hierarchies/:hierarchyId/formula-group')
  @ApiOperation({ summary: 'Create or update formula group for a hierarchy' })
  async createOrUpdateFormulaGroup(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
    @Body() dto: CreateFormulaGroupDto,
  ) {
    return await this.hierarchyService.createOrUpdateFormulaGroup(projectId, hierarchyId, {
      mainHierarchyName: dto.mainHierarchyName,
      rules: dto.rules,
    });
  }

  @Get('projects/:projectId/hierarchies/:hierarchyId/formula-group')
  @ApiOperation({ summary: 'Get formula group from a hierarchy' })
  async getFormulaGroupFromHierarchy(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
  ) {
    return await this.hierarchyService.getFormulaGroup(projectId, hierarchyId);
  }

  @Get('projects/:projectId/formula-groups')
  @ApiOperation({ summary: 'List all hierarchies with formula groups in a project' })
  async listHierarchiesWithFormulaGroups(@Param('projectId') projectId: string) {
    return await this.hierarchyService.listHierarchiesWithFormulaGroups(projectId);
  }

  @Get('projects/:projectId/hierarchies/:hierarchyId/formula-info')
  @ApiOperation({ summary: 'Get formula information for a specific hierarchy' })
  async getHierarchyFormulaInfo(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
  ) {
    return await this.hierarchyService.getHierarchyFormulaInfo(projectId, hierarchyId);
  }

  @Delete('projects/:projectId/hierarchies/:hierarchyId/formula-group')
  @ApiOperation({ summary: 'Delete formula group from a hierarchy' })
  async deleteFormulaGroupFromHierarchy(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
  ) {
    return await this.hierarchyService.deleteFormulaGroupNew(projectId, hierarchyId);
  }

  @Post('projects/:projectId/hierarchies/:hierarchyId/formula-group/rules')
  @ApiOperation({ summary: 'Add rule to formula group' })
  async addRuleToFormulaGroup(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
    @Body() rule: FormulaGroupRuleDto,
  ) {
    return await this.hierarchyService.addRuleToFormulaGroup(projectId, hierarchyId, rule);
  }

  @Delete('projects/:projectId/hierarchies/:hierarchyId/formula-group/rules/:ruleHierarchyId')
  @ApiOperation({ summary: 'Remove rule from formula group' })
  async removeRuleFromFormulaGroup(
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
    @Param('ruleHierarchyId') ruleHierarchyId: string,
  ) {
    return await this.hierarchyService.removeRuleFromFormulaGroup(
      projectId,
      hierarchyId,
      ruleHierarchyId,
    );
  }

  // ============================================================================
  // Reference Table Endpoints (Virtual Tables from CSV)
  // ============================================================================

  @Post('reference-tables')
  @ApiOperation({ summary: 'Create or update a reference table from CSV' })
  async createReferenceTable(
    @Req() req: any,
    @Body() body: { name: string; displayName?: string; description?: string; csvContent: string; sourceFile?: string },
  ) {
    return await this.referenceTableService.createFromCSV(req.user.id, body);
  }

  @Get('reference-tables')
  @ApiOperation({ summary: 'List all reference tables for current user' })
  async listReferenceTables(@Req() req: any) {
    return await this.referenceTableService.listTables(req.user.id);
  }

  @Get('reference-tables/:tableName')
  @ApiOperation({ summary: 'Get a specific reference table' })
  async getReferenceTable(@Req() req: any, @Param('tableName') tableName: string) {
    return await this.referenceTableService.getTable(req.user.id, tableName);
  }

  @Post('reference-tables/:tableName/query')
  @ApiOperation({ summary: 'Query reference table data with optional filtering' })
  async queryReferenceTable(
    @Req() req: any,
    @Param('tableName') tableName: string,
    @Body() query: {
      columns?: string[];
      filterColumn?: string;
      filterValues?: string[];
      limit?: number;
      offset?: number;
      distinct?: boolean;
    },
  ) {
    return await this.referenceTableService.queryTable(req.user.id, {
      tableName,
      ...query,
    });
  }

  @Get('reference-tables/:tableName/distinct/:columnName')
  @ApiOperation({ summary: 'Get distinct values for a column' })
  async getDistinctValues(
    @Req() req: any,
    @Param('tableName') tableName: string,
    @Param('columnName') columnName: string,
    @Query('filterColumn') filterColumn?: string,
    @Query('filterValue') filterValue?: string,
  ) {
    return await this.referenceTableService.getDistinctValues(
      req.user.id,
      tableName,
      columnName,
      filterColumn,
      filterValue,
    );
  }

  @Delete('reference-tables/:tableName')
  @ApiOperation({ summary: 'Delete a reference table' })
  async deleteReferenceTable(@Req() req: any, @Param('tableName') tableName: string) {
    await this.referenceTableService.deleteTable(req.user.id, tableName);
    return { deleted: true };
  }

  // ============================================================================
  // Hierarchy Viewer Selection Endpoints
  // ============================================================================

  @Post('viewer-selections')
  @ApiOperation({ summary: 'Save viewer selection for a hierarchy' })
  async saveViewerSelection(
    @Req() req: any,
    @Body() body: {
      projectId: string;
      hierarchyId: string;
      tableName: string;
      columnName: string;
      selectedValues: string[];
      applyToAll?: boolean;
      displayColumns?: string[];
    },
  ) {
    await this.referenceTableService.saveViewerSelection(
      req.user.id,
      body.projectId,
      body.hierarchyId,
      body.tableName,
      body.columnName,
      body.selectedValues,
      body.applyToAll || false,
      body.displayColumns || [],
    );
    return { saved: true };
  }

  @Get('viewer-selections/:projectId')
  @ApiOperation({ summary: 'Get viewer selections for a project' })
  async getViewerSelections(
    @Req() req: any,
    @Param('projectId') projectId: string,
    @Query('hierarchyId') hierarchyId?: string,
  ) {
    return await this.referenceTableService.getViewerSelections(
      req.user.id,
      projectId,
      hierarchyId,
    );
  }

  @Delete('viewer-selections/:projectId/:hierarchyId/:tableName/:columnName')
  @ApiOperation({ summary: 'Delete a viewer selection' })
  async deleteViewerSelection(
    @Req() req: any,
    @Param('projectId') projectId: string,
    @Param('hierarchyId') hierarchyId: string,
    @Param('tableName') tableName: string,
    @Param('columnName') columnName: string,
  ) {
    await this.referenceTableService.deleteViewerSelection(
      req.user.id,
      projectId,
      hierarchyId,
      tableName,
      columnName,
    );
    return { deleted: true };
  }
}
