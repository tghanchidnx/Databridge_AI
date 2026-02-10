import { BaseApiService } from "./base.service";

export interface User {
  id: string;
  email: string;
  name: string;
  bio?: string;
  avatarUrl?: string;
  authType?: string;
  organizationId?: string;
  onboardingCompleted?: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface UpdateUserDto {
  name?: string;
  email?: string;
  bio?: string;
  teamSize?: string;
  primaryUseCase?: string;
  authType?: string;
}

export class UsersService extends BaseApiService {
  /**
   * Get current user profile
   */
  async getCurrentUser(): Promise<User> {
    const token = this.getAuthToken();
    if (!token) throw new Error("No auth token");

    // Decode JWT to get user ID
    const userId = this.getUserIdFromToken(token);
    return this.getUser(userId);
  }

  /**
   * Get user by ID
   */
  async getUser(userId: string): Promise<User> {
    const response = await this.api.get(`/users/${userId}`);
    return this.extractData(response);
  }

  /**
   * Update user profile
   */
  async updateUser(userId: string, data: UpdateUserDto): Promise<User> {
    console.log("UsersService: Updating user", { userId, data });

    // Send all provided fields to backend
    const payload: UpdateUserDto = {};
    if (data.name !== undefined) payload.name = data.name;
    if (data.email !== undefined) payload.email = data.email;
    if (data.bio !== undefined) payload.bio = data.bio;
    if (data.teamSize !== undefined) payload.teamSize = data.teamSize;
    if (data.primaryUseCase !== undefined)
      payload.primaryUseCase = data.primaryUseCase;
    if (data.authType !== undefined) payload.authType = data.authType;

    console.log("UsersService: Sending payload:", payload);

    const response = await this.api.patch(`/users/${userId}`, payload);
    return this.extractData(response);
  }

  /**
   * Delete user
   */
  async deleteUser(userId: string): Promise<void> {
    await this.api.delete(`/users/${userId}`);
  }

  /**
   * Change user password
   */
  async changePassword(
    userId: string,
    currentPassword: string,
    newPassword: string
  ): Promise<{ message: string }> {
    console.log("UsersService: Changing password for user", userId);
    const response = await this.api.post(`/users/${userId}/change-password`, {
      currentPassword,
      newPassword,
    });
    return this.extractData(response);
  }

  /**
   * Setup password for SSO users who don't have one
   */
  async setupPassword(
    userId: string,
    newPassword: string
  ): Promise<{ message: string }> {
    console.log("UsersService: Setting up password for SSO user", userId);
    const response = await this.api.post(`/users/${userId}/setup-password`, {
      newPassword,
    });
    return this.extractData(response);
  }

  /**
   * Upload user avatar
   */
  async uploadAvatar(userId: string, formData: FormData): Promise<User> {
    console.log("UsersService: Uploading avatar for user", userId);
    const response = await this.api.post(`/users/${userId}/avatar`, formData, {
      headers: {
        "Content-Type": "multipart/form-data",
      },
    });
    return this.extractData(response);
  }

  /**
   * Helper: Decode JWT token to get user ID
   */
  private getUserIdFromToken(token: string): string {
    try {
      const base64Url = token.split(".")[1];
      const base64 = base64Url.replace(/-/g, "+").replace(/_/g, "/");
      const jsonPayload = decodeURIComponent(
        atob(base64)
          .split("")
          .map((c) => "%" + ("00" + c.charCodeAt(0).toString(16)).slice(-2))
          .join("")
      );
      const decoded = JSON.parse(jsonPayload);
      return decoded?.sub || decoded?.id;
    } catch (error) {
      throw new Error("Invalid token");
    }
  }
}

export const usersService = new UsersService();
