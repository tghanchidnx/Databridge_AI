import { BaseApiService } from "./base.service";

export interface Organization {
  id: string;
  name: string;
  description?: string;
  plan: "free" | "pro" | "enterprise";
  status: string;
  ownerId: string;
  createdAt: string;
  updatedAt: string;
}

export interface OrganizationMember {
  id: string;
  userId: string;
  organizationId: string;
  role: "owner" | "admin" | "member";
  joinedAt: string;
  user?: {
    id: string;
    name: string;
    email: string;
  };
}

export interface CreateOrganizationDto {
  name: string;
  description?: string;
  plan?: "free" | "pro" | "enterprise";
}

export interface UpdateOrganizationDto {
  name?: string;
  description?: string;
  plan?: "free" | "pro" | "enterprise";
}

export interface AddMemberDto {
  email: string;
  role?: "admin" | "member";
}

export class OrganizationsService extends BaseApiService {
  /**
   * Create organization
   */
  async createOrganization(data: CreateOrganizationDto): Promise<Organization> {
    console.log("OrganizationsService: Creating organization", data);
    const token = this.getAuthToken();
    console.log("OrganizationsService: Token exists:", !!token);

    const response = await this.api.post("/organizations", data);
    console.log("OrganizationsService: Response:", response.data);

    return this.extractData(response);
  }

  /**
   * Get user's organizations
   */
  async getUserOrganizations(): Promise<Organization[]> {
    const response = await this.api.get("/organizations");
    return this.extractData(response);
  }

  /**
   * Get organization by ID
   */
  async getOrganization(id: string): Promise<Organization> {
    const response = await this.api.get(`/organizations/${id}`);
    return this.extractData(response);
  }

  /**
   * Update organization
   */
  async updateOrganization(
    id: string,
    data: UpdateOrganizationDto
  ): Promise<Organization> {
    console.log("OrganizationsService: Updating organization", { id, data });
    const response = await this.api.put(`/organizations/${id}`, data);
    return this.extractData(response);
  }

  /**
   * Delete organization
   */
  async deleteOrganization(id: string): Promise<void> {
    console.log("OrganizationsService: Deleting organization", id);
    await this.api.delete(`/organizations/${id}`);
  }

  /**
   * Get organization members
   */
  async getOrganizationMembers(id: string): Promise<OrganizationMember[]> {
    const response = await this.api.get(`/organizations/${id}/members`);
    return this.extractData(response);
  }

  /**
   * Add member to organization
   */
  async addMember(id: string, data: AddMemberDto): Promise<OrganizationMember> {
    const response = await this.api.post(`/organizations/${id}/members`, data);
    return this.extractData(response);
  }

  /**
   * Remove member from organization
   */
  async removeMember(organizationId: string, memberId: string): Promise<void> {
    await this.api.delete(
      `/organizations/${organizationId}/members/${memberId}`
    );
  }

  /**
   * Update member role
   */
  async updateMemberRole(
    organizationId: string,
    memberId: string,
    role: "admin" | "member"
  ): Promise<OrganizationMember> {
    const response = await this.api.patch(
      `/organizations/${organizationId}/members/${memberId}`,
      { role }
    );
    return this.extractData(response);
  }

  /**
   * Get organization invitation key (owner only)
   */
  async getOrganizationKey(organizationId: string): Promise<{
    organizationId: string;
    organizationName: string;
    invitationKey: string;
    invitationUrl: string;
  }> {
    const response = await this.api.get(
      `/organizations/${organizationId}/invitation-key`
    );
    return this.extractData(response);
  }
}

export const organizationsService = new OrganizationsService();
