import { Injectable, Logger, NotFoundException, ConflictException } from '@nestjs/common';
import { PrismaService } from '../../database/prisma/prisma.service';
import { CreateOrganizationDto, UpdateOrganizationDto, AddMemberDto } from './dto/organization.dto';

@Injectable()
export class OrganizationsService {
  private readonly logger = new Logger(OrganizationsService.name);

  constructor(private prisma: PrismaService) {}

  /**
   * Create new organization
   */
  async create(userId: string, dto: CreateOrganizationDto) {
    try {
      // Create organization
      const organization = await this.prisma.organization.create({
        data: {
          name: dto.name,
          description: dto.description,
          plan: dto.plan,
          ownerId: userId,
        },
        include: {
          members: true,
        },
      });

      // Add creator as owner member
      await this.prisma.organizationMember.create({
        data: {
          organizationId: organization.id,
          userId: userId,
          role: 'owner',
        },
      });

      // Update user's organization_id and onboarding status
      await this.prisma.user.update({
        where: { id: userId },
        data: {
          organizationId: organization.id,
          onboardingCompleted: true,
        },
      });

      // Create billing plan
      await this.prisma.billingPlan.create({
        data: {
          organizationId: organization.id,
          planType: dto.plan,
          status: 'active',
          amount: this.getPlanAmount(dto.plan),
        },
      });

      this.logger.log(`Organization created: ${organization.id} by user ${userId}`);
      return organization;
    } catch (error) {
      this.logger.error('Failed to create organization', error.stack);
      throw error;
    }
  }

  /**
   * Get organization by ID
   */
  async getById(id: string) {
    const organization = await this.prisma.organization.findUnique({
      where: { id, deletedAt: null },
      include: {
        members: {
          include: {
            user: {
              select: {
                id: true,
                name: true,
                email: true,
              },
            },
          },
        },
        _count: {
          select: {
            members: true,
          },
        },
      },
    });

    if (!organization) {
      throw new NotFoundException('Organization not found');
    }

    return {
      ...organization,
      memberCount: organization._count.members,
    };
  }

  /**
   * Get user's organizations
   */
  async getUserOrganizations(userId: string) {
    return this.prisma.organization.findMany({
      where: {
        deletedAt: null,
        members: {
          some: {
            userId: userId,
          },
        },
      },
      include: {
        members: {
          where: { userId: userId },
          select: { role: true },
        },
        _count: {
          select: { members: true },
        },
      },
      orderBy: { createdAt: 'desc' },
    });
  }

  /**
   * Update organization
   */
  async update(id: string, userId: string, dto: UpdateOrganizationDto) {
    // Check if user is owner or admin
    const member = await this.checkMemberPermission(id, userId, ['owner', 'admin']);
    if (!member) {
      throw new ConflictException('Insufficient permissions');
    }

    return this.prisma.organization.update({
      where: { id },
      data: {
        name: dto.name,
        description: dto.description,
        plan: dto.plan,
      },
      include: {
        _count: {
          select: { members: true },
        },
      },
    });
  }

  /**
   * Add member to organization
   */
  async addMember(orgId: string, dto: AddMemberDto, requestUserId: string) {
    // Check if requester is owner or admin
    const member = await this.checkMemberPermission(orgId, requestUserId, ['owner', 'admin']);
    if (!member) {
      throw new ConflictException('Insufficient permissions');
    }

    // Check if user already a member
    const existing = await this.prisma.organizationMember.findUnique({
      where: {
        organizationId_userId: {
          organizationId: orgId,
          userId: dto.userId,
        },
      },
    });

    if (existing) {
      throw new ConflictException('User is already a member');
    }

    await this.prisma.organizationMember.create({
      data: {
        organizationId: orgId,
        userId: dto.userId,
        role: dto.role,
      },
    });

    // Update user's organization_id
    await this.prisma.user.update({
      where: { id: dto.userId },
      data: { organizationId: orgId },
    });

    return { message: 'Member added successfully' };
  }

  /**
   * Get organization members
   */
  async getMembers(orgId: string) {
    return this.prisma.organizationMember.findMany({
      where: { organizationId: orgId },
      include: {
        user: {
          select: {
            id: true,
            name: true,
            email: true,
            createdAt: true,
          },
        },
      },
      orderBy: { joinedAt: 'desc' },
    });
  }

  /**
   * Check member permission
   */
  private async checkMemberPermission(orgId: string, userId: string, allowedRoles: string[]) {
    const member = await this.prisma.organizationMember.findUnique({
      where: {
        organizationId_userId: {
          organizationId: orgId,
          userId: userId,
        },
      },
    });

    if (!member) return null;
    return allowedRoles.includes(member.role) ? member : null;
  }

  /**
   * Get plan amount
   */
  private getPlanAmount(plan: string): number {
    switch (plan) {
      case 'pro':
        return 49.0;
      case 'enterprise':
        return 199.0;
      default:
        return 0.0;
    }
  }

  /**
   * Get billing history
   */
  async getBillingHistory(orgId: string, userId: string) {
    // Check if user is member
    const member = await this.checkMemberPermission(orgId, userId, ['owner', 'admin', 'member']);
    if (!member) {
      throw new ConflictException('Insufficient permissions');
    }

    return this.prisma.billingHistory.findMany({
      where: { organizationId: orgId },
      orderBy: { createdAt: 'desc' },
      take: 50,
    });
  }

  /**
   * Get current billing plan
   */
  async getCurrentPlan(orgId: string) {
    return this.prisma.billingPlan.findFirst({
      where: {
        organizationId: orgId,
        status: 'active',
      },
      orderBy: { startedAt: 'desc' },
    });
  }

  /**
   * Update billing plan
   */
  async updatePlan(orgId: string, newPlan: 'free' | 'pro' | 'enterprise', userId: string) {
    // Verify user is owner or admin
    const membership = await this.prisma.organizationMember.findFirst({
      where: {
        organizationId: orgId,
        userId: userId,
        role: { in: ['owner', 'admin'] },
      },
    });

    if (!membership) {
      throw new Error('Only organization owners or admins can update the plan');
    }

    // Update organization plan
    const organization = await this.prisma.organization.update({
      where: { id: orgId },
      data: { plan: newPlan },
    });

    // Deactivate old billing plans
    await this.prisma.billingPlan.updateMany({
      where: {
        organizationId: orgId,
        status: 'active',
      },
      data: { status: 'inactive' },
    });

    // Create new billing plan
    const planPrices = {
      free: 0,
      pro: 49,
      enterprise: 99,
    };

    await this.prisma.billingPlan.create({
      data: {
        organizationId: orgId,
        planType: newPlan,
        amount: planPrices[newPlan],
        currency: 'USD',
        billingCycle: newPlan === 'free' ? 'never' : 'monthly',
        status: 'active',
        startedAt: new Date(),
      },
    });

    this.logger.log(`Plan updated to ${newPlan} for organization: ${orgId}`);
    return organization;
  }

  /**
   * Delete organization
   */
  async getOrganizationKey(orgId: string, userId: string) {
    // Verify organization exists and user is the owner
    const organization = await this.prisma.organization.findUnique({
      where: { id: orgId },
      select: {
        id: true,
        name: true,
        ownerId: true,
      },
    });

    if (!organization) {
      throw new NotFoundException(`Organization with ID ${orgId} not found`);
    }

    if (organization.ownerId !== userId) {
      throw new Error('Only the organization owner can access the organization key');
    }

    this.logger.log(`Organization key accessed for: ${orgId}`);
    return {
      organizationId: organization.id,
      organizationName: organization.name,
      invitationKey: organization.id,
      invitationUrl: `${process.env.FRONTEND_URL || 'http://localhost:3000'}/signup?orgKey=${organization.id}`,
    };
  }

  async delete(orgId: string, userId: string) {
    // Verify user is owner
    const organization = await this.prisma.organization.findUnique({
      where: { id: orgId },
      include: {
        members: {
          where: { userId: userId },
        },
      },
    });

    if (!organization) {
      throw new NotFoundException(`Organization with ID ${orgId} not found`);
    }

    const userMembership = organization.members.find((m) => m.userId === userId);
    if (!userMembership || userMembership.role !== 'owner') {
      throw new Error('Only the organization owner can delete the organization');
    }

    // Delete organization (cascade will handle members, billing plans, etc.)
    await this.prisma.organization.delete({
      where: { id: orgId },
    });

    this.logger.log(`Organization deleted: ${orgId}`);
    return { message: 'Organization deleted successfully' };
  }
}
