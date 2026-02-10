import { BaseApiService } from "../base.service";

export interface Project {
  id: string;
  name: string;
  description?: string;
  userId: string;
  organizationId?: string;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface CreateProjectDto {
  name: string;
  description?: string;
  organizationId?: string;
}

export interface UpdateProjectDto {
  name?: string;
  description?: string;
  isActive?: boolean;
}

export interface ProjectMember {
  id: string;
  projectId: string;
  userId?: string;
  userEmail?: string;
  role: "owner" | "editor" | "viewer";
  accessType: "direct" | "organization";
  invitationStatus?: "pending" | "accepted" | "declined";
  invitedBy?: string;
  invitedAt: string;
  acceptedAt?: string;
  isActive: boolean;
  user?: {
    id: string;
    name: string;
    email: string;
    avatarUrl?: string;
  };
  inviter?: {
    id: string;
    name: string;
    email: string;
  };
}

export interface InviteMemberDto {
  userEmail?: string;
  inviteUserId?: string;
  role: "editor" | "viewer";
  accessType?: "direct" | "organization";
}

export interface UpdateMemberDto {
  role?: "editor" | "viewer";
  isActive?: boolean;
}

class ProjectService extends BaseApiService {
  /**
   * Get all projects for current user
   */
  async getProjects(): Promise<Project[]> {
    try {
      const response = await this.api.get("/smart-hierarchy/projects");
      return this.extractData<Project[]>(response);
    } catch (error) {
      console.error("Failed to fetch projects:", error);
      throw error;
    }
  }

  /**
   * Get project by ID
   */
  async getProject(projectId: string): Promise<Project> {
    try {
      const response = await this.api.get(
        `/smart-hierarchy/projects/${projectId}`
      );
      return this.extractData<Project>(response);
    } catch (error) {
      console.error(`Failed to fetch project ${projectId}:`, error);
      throw error;
    }
  }

  /**
   * Create a new project
   */
  async createProject(data: CreateProjectDto): Promise<Project> {
    try {
      const response = await this.api.post("/smart-hierarchy/projects", data);
      return this.extractData<Project>(response);
    } catch (error) {
      console.error("Failed to create project:", error);
      throw error;
    }
  }

  /**
   * Update a project
   */
  async updateProject(
    projectId: string,
    data: UpdateProjectDto
  ): Promise<Project> {
    try {
      const response = await this.api.put(
        `/smart-hierarchy/projects/${projectId}`,
        data
      );
      return this.extractData<Project>(response);
    } catch (error) {
      console.error(`Failed to update project ${projectId}:`, error);
      throw error;
    }
  }

  /**
   * Delete a project (soft delete)
   */
  async deleteProject(projectId: string): Promise<void> {
    try {
      await this.api.delete(`/smart-hierarchy/projects/${projectId}`);
    } catch (error) {
      console.error(`Failed to delete project ${projectId}:`, error);
      throw error;
    }
  }

  // ============================================================================
  // Project Member Management
  // ============================================================================

  /**
   * Get all members of a project
   */
  async getProjectMembers(projectId: string): Promise<ProjectMember[]> {
    try {
      const response = await this.api.get(
        `/smart-hierarchy/projects/${projectId}/members`
      );
      return this.extractData<ProjectMember[]>(response);
    } catch (error) {
      console.error(`Failed to fetch project members:`, error);
      throw error;
    }
  }

  /**
   * Invite a member to project
   */
  async inviteProjectMember(
    projectId: string,
    data: InviteMemberDto
  ): Promise<ProjectMember> {
    try {
      const response = await this.api.post(
        `/smart-hierarchy/projects/${projectId}/members`,
        data
      );
      return this.extractData<ProjectMember>(response);
    } catch (error) {
      console.error(`Failed to invite member:`, error);
      throw error;
    }
  }

  /**
   * Share project with entire organization
   */
  async shareWithOrganization(
    projectId: string,
    role: "editor" | "viewer"
  ): Promise<{ added: number; members: ProjectMember[] }> {
    try {
      const response = await this.api.post(
        `/smart-hierarchy/projects/${projectId}/share-organization`,
        { role }
      );
      return this.extractData<{ added: number; members: ProjectMember[] }>(
        response
      );
    } catch (error) {
      console.error(`Failed to share with organization:`, error);
      throw error;
    }
  }

  /**
   * Update member permissions
   */
  async updateProjectMember(
    projectId: string,
    memberId: string,
    data: UpdateMemberDto
  ): Promise<ProjectMember> {
    try {
      const response = await this.api.put(
        `/smart-hierarchy/projects/${projectId}/members/${memberId}`,
        data
      );
      return this.extractData<ProjectMember>(response);
    } catch (error) {
      console.error(`Failed to update member:`, error);
      throw error;
    }
  }

  /**
   * Remove member from project
   */
  async removeProjectMember(
    projectId: string,
    memberId: string
  ): Promise<void> {
    try {
      await this.api.delete(
        `/smart-hierarchy/projects/${projectId}/members/${memberId}`
      );
    } catch (error) {
      console.error(`Failed to remove member:`, error);
      throw error;
    }
  }

  /**
   * Get pending project invitations for current user
   */
  async getPendingInvitations(): Promise<
    Array<{
      member: ProjectMember;
      project: Project;
      inviter: { id: string; name: string; email: string };
    }>
  > {
    try {
      const response = await this.api.get(
        "/smart-hierarchy/projects/pending-invitations"
      );
      return this.extractData(response);
    } catch (error) {
      console.error(`Failed to fetch pending invitations:`, error);
      throw error;
    }
  }

  /**
   * Accept project invitation
   */
  async acceptProjectInvitation(
    projectId: string,
    memberId: string
  ): Promise<ProjectMember> {
    try {
      const response = await this.api.post(
        `/smart-hierarchy/projects/${projectId}/members/${memberId}/accept`
      );
      return this.extractData<ProjectMember>(response);
    } catch (error) {
      console.error(`Failed to accept invitation:`, error);
      throw error;
    }
  }

  /**
   * Decline project invitation
   */
  async declineProjectInvitation(
    projectId: string,
    memberId: string
  ): Promise<void> {
    try {
      await this.api.post(
        `/smart-hierarchy/projects/${projectId}/members/${memberId}/decline`
      );
    } catch (error) {
      console.error(`Failed to decline invitation:`, error);
      throw error;
    }
  }

  // ============================================================================
  // Import/Export
  // ============================================================================

  /**
   * Export project data (JSON format - complete project configuration)
   */
  async exportProject(
    projectId: string,
    format: "json" | "csv",
    projectName?: string
  ): Promise<any> {
    try {
      // Use project name or fallback to generic name
      const exportName = projectName || `project-${projectId.slice(0, 8)}`;

      const response = await this.api.post(`/smart-hierarchy/export`, {
        projectId,
        exportName,
        format,
        exportType: "manual",
      });

      const result = this.extractData<any>(response);

      // Return the exportData content (could be JSON object or CSV string)
      return result.exportData;
    } catch (error) {
      console.error(`Failed to export project:`, error);
      throw error;
    }
  }

  /**
   * Import project data
   */
  async importProject(
    projectId: string,
    file: File,
    format: "json" | "csv"
  ): Promise<void> {
    try {
      // Read file content
      const fileContent = await file.text();

      // Parse based on format
      let exportData;
      if (format === "json") {
        exportData = JSON.parse(fileContent);
      } else {
        // For CSV, send as string
        exportData = fileContent;
      }

      // Send as JSON body
      await this.api.post(`/smart-hierarchy/import`, {
        projectId,
        exportData,
        format,
      });
    } catch (error) {
      console.error(`Failed to import project:`, error);
      throw error;
    }
  }
}

export const projectService = new ProjectService();
