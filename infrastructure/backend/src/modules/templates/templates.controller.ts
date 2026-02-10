/**
 * Templates Controller
 * REST API for hierarchy templates
 */
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
} from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiBearerAuth } from '@nestjs/swagger';
import { TemplatesService, CreateTemplateDto } from './templates.service';
import { JwtAuthGuard } from '../../common/guards/jwt-auth.guard';

@ApiTags('templates')
@Controller('templates')
@UseGuards(JwtAuthGuard)
@ApiBearerAuth()
export class TemplatesController {
  constructor(private readonly templatesService: TemplatesService) {}

  @Get()
  @ApiOperation({ summary: 'Get all templates' })
  @ApiResponse({ status: 200, description: 'Templates retrieved' })
  async getAllTemplates(
    @Query('category') category?: string,
    @Query('industry') industry?: string,
  ) {
    if (category) {
      return this.templatesService.getTemplatesByCategory(category);
    }
    if (industry) {
      return this.templatesService.getTemplatesByIndustry(industry);
    }
    return this.templatesService.getAllTemplates();
  }

  @Get('search')
  @ApiOperation({ summary: 'Search templates' })
  @ApiResponse({ status: 200, description: 'Search results' })
  async searchTemplates(@Query('q') query: string) {
    return this.templatesService.searchTemplates(query || '');
  }

  @Get('recommended')
  @ApiOperation({ summary: 'Get recommended templates' })
  @ApiResponse({ status: 200, description: 'Recommended templates' })
  async getRecommendedTemplates(
    @Query('industry') industry?: string,
    @Query('category') category?: string,
    @Query('projectType') projectType?: string,
  ) {
    return this.templatesService.getRecommendedTemplates({
      industry,
      category,
      projectType,
    });
  }

  @Get(':id')
  @ApiOperation({ summary: 'Get template by ID' })
  @ApiResponse({ status: 200, description: 'Template details' })
  @ApiResponse({ status: 404, description: 'Template not found' })
  async getTemplateById(@Param('id') id: string) {
    return this.templatesService.getTemplateById(id);
  }

  @Post()
  @ApiOperation({ summary: 'Create a new template' })
  @ApiResponse({ status: 201, description: 'Template created' })
  async createTemplate(@Body() dto: CreateTemplateDto) {
    return this.templatesService.createTemplate(dto);
  }

  @Put(':id')
  @ApiOperation({ summary: 'Update a template' })
  @ApiResponse({ status: 200, description: 'Template updated' })
  @ApiResponse({ status: 404, description: 'Template not found' })
  async updateTemplate(
    @Param('id') id: string,
    @Body() updates: Partial<CreateTemplateDto>,
  ) {
    return this.templatesService.updateTemplate(id, updates);
  }

  @Delete(':id')
  @ApiOperation({ summary: 'Delete a template' })
  @ApiResponse({ status: 200, description: 'Template deleted' })
  @ApiResponse({ status: 404, description: 'Template not found' })
  async deleteTemplate(@Param('id') id: string) {
    await this.templatesService.deleteTemplate(id);
    return { success: true };
  }

  @Post(':id/use')
  @ApiOperation({ summary: 'Record template usage' })
  @ApiResponse({ status: 200, description: 'Usage recorded' })
  async recordUsage(@Param('id') id: string) {
    await this.templatesService.recordUsage(id);
    return { success: true };
  }
}
