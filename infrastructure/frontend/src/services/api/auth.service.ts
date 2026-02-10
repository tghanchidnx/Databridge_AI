import { BaseApiService } from "./base.service";

export interface LoginResponse {
  token: string;
  accessToken?: string;
  user: {
    id: string;
    email: string;
    name: string;
    organizationId?: string;
    onboardingCompleted?: boolean;
    createdAt?: string;
  };
  organization?: {
    id: string;
    name: string;
    description?: string;
    plan: string;
    status: string;
    ownerId: string;
    createdAt: string;
  };
}

export class AuthService extends BaseApiService {
  /**
   * Login with Microsoft OAuth
   */
  async loginWithMicrosoft(accessToken: string): Promise<LoginResponse> {
    console.log("AuthService: Login with Microsoft");
    const response = await this.api.post("/auth/login", {
      authType: "MICROSOFT",
      accessToken,
    });

    console.log("AuthService: Raw response.data:", response.data);
    const data: any = this.extractData(response);
    console.log("AuthService: Actual data:", data);

    const token =
      data.token ||
      data.accessToken ||
      response.data.token ||
      response.data.accessToken;
    const user = data.user || response.data.user;

    if (token) {
      this.setAuthToken(token);
      console.log("AuthService: Token saved to localStorage");
    }

    return {
      token,
      user,
      organization: data.organization || response.data.organization,
    };
  }

  /**
   * Login with email and password
   */
  async loginWithEmail(
    email: string,
    password: string
  ): Promise<LoginResponse> {
    console.log("AuthService: Login with email");
    const response = await this.api.post("/auth/login", {
      authType: "EMAIL",
      email,
      password,
    });

    const data: any = this.extractData(response);
    const token = data.token || data.accessToken || response.data.token;
    const user = data.user || response.data.user;

    if (token) {
      this.setAuthToken(token);
    }

    return {
      token,
      user,
      organization: data.organization || response.data.organization,
    };
  }

  /**
   * Signup with email and password
   */
  async signup(
    email: string,
    password: string,
    name: string,
    organizationKey?: string,
    bio?: string,
    teamSize?: string,
    primaryUseCase?: string
  ): Promise<LoginResponse> {
    console.log("AuthService: Signup", { organizationKey });
    const response = await this.api.post("/auth/signup", {
      email,
      password,
      name,
      organizationKey,
      bio,
      teamSize,
      primaryUseCase,
    });

    const data: any = this.extractData(response);
    const token = data.token || data.accessToken || response.data.token;
    const user = data.user || response.data.user;

    if (token) {
      this.setAuthToken(token);
    }

    return {
      token,
      user,
      organization: data.organization,
    };
  }

  /**
   * Logout
   */
  async logout(): Promise<void> {
    this.clearAuthToken();
  }

  /**
   * Verify token
   */
  async verifyToken(): Promise<{ valid: boolean; user?: any }> {
    try {
      const response = await this.api.get("/auth/verify");
      return { valid: true, user: this.extractData(response) };
    } catch {
      return { valid: false };
    }
  }
}

export const authService = new AuthService();
