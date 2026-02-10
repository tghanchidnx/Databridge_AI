import { Controller, Get, Post, Body, UseGuards } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiBearerAuth } from '@nestjs/swagger';
import { ReportsService } from './reports.service';
import { JwtAuthGuard } from '../../common/guards/jwt-auth.guard';
import { CurrentUser } from '../../common/decorators/current-user.decorator';
import { GenerateReportDto } from './dto/generate-report.dto';

@ApiTags('Reports')
@ApiBearerAuth()
@UseGuards(JwtAuthGuard)
@Controller('reports')
export class ReportsController {
  constructor(private readonly reportsService: ReportsService) {}

  @Get('hierarchy')
  @ApiOperation({
    summary: 'Get hierarchy mapping for current user',
    description: 'Retrieves all column aliases and mappings created by the user',
  })
  @ApiResponse({ status: 200, description: 'Hierarchy mapping retrieved successfully' })
  getHierarchyMapping(@CurrentUser('id') userId: string) {
    return this.reportsService.getHierarchyMapping(userId);
  }

  @Post('generate')
  @ApiOperation({
    summary: 'Generate comprehensive database hierarchy report',
    description: 'Generates detailed report of schemas, tables, columns with optional row counts',
  })
  @ApiResponse({ status: 200, description: 'Report generated successfully' })
  @ApiResponse({ status: 404, description: 'Connection not found' })
  generateReport(@CurrentUser('id') userId: string, @Body() generateReportDto: GenerateReportDto) {
    return this.reportsService.generateReport(userId, generateReportDto);
  }

  @Post('generate/json')
  @ApiOperation({ summary: 'Export report as JSON string' })
  @ApiResponse({ status: 200, description: 'JSON report exported' })
  exportReportAsJson(
    @CurrentUser('id') userId: string,
    @Body() generateReportDto: GenerateReportDto,
  ) {
    return this.reportsService.exportReportAsJson(userId, generateReportDto);
  }

  @Post('generate/csv')
  @ApiOperation({ summary: 'Export report as CSV' })
  @ApiResponse({ status: 200, description: 'CSV report exported' })
  async exportReportAsCsv(
    @CurrentUser('id') userId: string,
    @Body() generateReportDto: GenerateReportDto,
  ) {
    const csv = await this.reportsService.exportReportAsCsv(userId, generateReportDto);
    return { csv };
  }
}
