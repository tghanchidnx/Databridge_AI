import {
  Controller,
  Get,
  Post,
  Put,
  Delete,
  Body,
  Param,
  UseGuards,
  Request,
} from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiBearerAuth } from '@nestjs/swagger';
import { OrganizationsService } from './organizations.service';
import { CreateOrganizationDto, UpdateOrganizationDto, AddMemberDto } from './dto/organization.dto';
import { JwtAuthGuard } from '../../common/guards/jwt-auth.guard';

@ApiTags('organizations')
@Controller('organizations')
@UseGuards(JwtAuthGuard)
@ApiBearerAuth()
export class OrganizationsController {
  constructor(private readonly organizationsService: OrganizationsService) {}

  @Post()
  @ApiOperation({ summary: 'Create organization' })
  @ApiResponse({ status: 201, description: 'Organization created' })
  async create(@Request() req, @Body() dto: CreateOrganizationDto) {
    console.log('=== Organization Create Request ===');
    console.log('Request headers:', req.headers);
    console.log('Request user:', req.user);
    console.log('Request body:', dto);

    // Try to get user from req.user first
    let userId = req.user?.sub || req.user?.id;

    // Fallback: decode JWT token manually if req.user is not populated
    if (!userId) {
      const token = req.headers.authorization?.replace('Bearer ', '');
      if (token) {
        try {
          const jwt = require('jsonwebtoken');
          const decoded: any = jwt.decode(token);
          console.log('Manually decoded token:', decoded);
          userId = decoded?.sub || decoded?.id;

          if (userId) {
            console.log('Using userId from manually decoded token:', userId);
          }
        } catch (error) {
          console.error('Failed to decode token manually:', error);
        }
      }
    }

    if (!userId) {
      console.error('CRITICAL: Could not determine user ID from req.user or token');
      throw new Error('User not found in request - JWT guard may not be working');
    }

    console.log('Creating organization for userId:', userId);
    return this.organizationsService.create(userId, dto);
  }

  @Get()
  @ApiOperation({ summary: 'Get user organizations' })
  @ApiResponse({ status: 200, description: 'Organizations retrieved' })
  async getUserOrgs(@Request() req) {
    return this.organizationsService.getUserOrganizations(req.user.sub);
  }

  @Get(':id')
  @ApiOperation({ summary: 'Get organization by ID' })
  @ApiResponse({ status: 200, description: 'Organization found' })
  @ApiResponse({ status: 404, description: 'Organization not found' })
  async getById(@Param('id') id: string) {
    return this.organizationsService.getById(id);
  }

  @Put(':id')
  @ApiOperation({ summary: 'Update organization' })
  @ApiResponse({ status: 200, description: 'Organization updated' })
  async update(@Param('id') id: string, @Request() req, @Body() dto: UpdateOrganizationDto) {
    return this.organizationsService.update(id, req.user.sub, dto);
  }

  @Post(':id/members')
  @ApiOperation({ summary: 'Add member to organization' })
  @ApiResponse({ status: 201, description: 'Member added' })
  async addMember(@Param('id') id: string, @Request() req, @Body() dto: AddMemberDto) {
    return this.organizationsService.addMember(id, dto, req.user.sub);
  }

  @Get(':id/members')
  @ApiOperation({ summary: 'Get organization members' })
  @ApiResponse({ status: 200, description: 'Members retrieved' })
  async getMembers(@Param('id') id: string) {
    return this.organizationsService.getMembers(id);
  }

  @Get(':id/billing/history')
  @ApiOperation({ summary: 'Get billing history' })
  @ApiResponse({ status: 200, description: 'Billing history retrieved' })
  async getBillingHistory(@Param('id') id: string, @Request() req) {
    return this.organizationsService.getBillingHistory(id, req.user.sub);
  }

  @Get(':id/billing/plan')
  @ApiOperation({ summary: 'Get current billing plan' })
  @ApiResponse({ status: 200, description: 'Billing plan retrieved' })
  async getCurrentPlan(@Param('id') id: string) {
    return this.organizationsService.getCurrentPlan(id);
  }

  @Put(':id/billing/plan')
  @ApiOperation({ summary: 'Update billing plan' })
  @ApiResponse({ status: 200, description: 'Plan updated successfully' })
  async updatePlan(
    @Param('id') id: string,
    @Body() body: { plan: 'free' | 'pro' | 'enterprise' },
    @Request() req,
  ) {
    return this.organizationsService.updatePlan(id, body.plan, req.user.sub);
  }

  @Get(':id/invitation-key')
  @ApiOperation({ summary: 'Get organization invitation key (ID) for owners to share' })
  @ApiResponse({ status: 200, description: 'Organization key retrieved' })
  @ApiResponse({ status: 403, description: 'Only owner can access organization key' })
  async getOrganizationKey(@Param('id') id: string, @Request() req) {
    return this.organizationsService.getOrganizationKey(id, req.user.sub);
  }

  @Delete(':id')
  @ApiOperation({ summary: 'Delete organization' })
  @ApiResponse({ status: 200, description: 'Organization deleted' })
  @ApiResponse({ status: 403, description: 'Only owner can delete organization' })
  async delete(@Param('id') id: string, @Request() req) {
    return this.organizationsService.delete(id, req.user.sub);
  }
}
