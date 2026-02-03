/**
 * Authentication Service
 *
 * Manages JWT tokens and API key authentication for the Excel plugin.
 */

import { apiService } from './api.service';

export interface AuthState {
  isAuthenticated: boolean;
  authType: 'jwt' | 'apiKey' | null;
  userId?: string;
  userName?: string;
  expiresAt?: Date;
}

export interface LoginCredentials {
  email: string;
  password: string;
}

export interface ApiKeyCredentials {
  apiKey: string;
}

class AuthService {
  private state: AuthState = {
    isAuthenticated: false,
    authType: null,
  };

  private listeners: Array<(state: AuthState) => void> = [];

  constructor() {
    this.loadStoredAuth();
  }

  // =========================================================================
  // State Management
  // =========================================================================

  getState(): AuthState {
    return { ...this.state };
  }

  subscribe(listener: (state: AuthState) => void): () => void {
    this.listeners.push(listener);
    return () => {
      this.listeners = this.listeners.filter((l) => l !== listener);
    };
  }

  private notifyListeners(): void {
    this.listeners.forEach((listener) => listener(this.getState()));
  }

  private setState(updates: Partial<AuthState>): void {
    this.state = { ...this.state, ...updates };
    this.notifyListeners();
  }

  // =========================================================================
  // Authentication Methods
  // =========================================================================

  /**
   * Authenticate with API Key (simpler, recommended for automation)
   */
  async loginWithApiKey(credentials: ApiKeyCredentials): Promise<boolean> {
    console.log('Attempting API Key login...');
    try {
      apiService.setApiKey(credentials.apiKey);
      console.log('API key set, checking health...');

      // Verify the API key works
      const healthCheck = await apiService.checkHealth();
      console.log('Health check result:', healthCheck);

      if (!healthCheck.success) {
        console.error('Health check failed:', healthCheck.error);
        this.clearAuth();
        return false;
      }

      console.log('Health check passed, setting auth state...');
      this.setState({
        isAuthenticated: true,
        authType: 'apiKey',
      });

      localStorage.setItem('databridge_auth_type', 'apiKey');
      console.log('Login successful!');
      return true;
    } catch (error) {
      console.error('API Key authentication failed:', error);
      this.clearAuth();
      return false;
    }
  }

  /**
   * Authenticate with JWT (for interactive users)
   */
  async loginWithJwt(credentials: LoginCredentials): Promise<boolean> {
    try {
      // This would call your authentication endpoint
      // For now, we'll use a mock implementation
      const response = await fetch(`${this.getBaseUrl()}/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(credentials),
      });

      if (!response.ok) {
        return false;
      }

      const data = await response.json();
      const { accessToken, user, expiresIn } = data;

      apiService.setToken(accessToken);

      const expiresAt = new Date(Date.now() + expiresIn * 1000);

      this.setState({
        isAuthenticated: true,
        authType: 'jwt',
        userId: user.id,
        userName: user.name,
        expiresAt,
      });

      localStorage.setItem('databridge_auth_type', 'jwt');
      localStorage.setItem('databridge_user', JSON.stringify(user));
      localStorage.setItem('databridge_expires_at', expiresAt.toISOString());

      // Set up token refresh
      this.scheduleTokenRefresh(expiresIn);

      return true;
    } catch (error) {
      console.error('JWT authentication failed:', error);
      this.clearAuth();
      return false;
    }
  }

  /**
   * Log out and clear all credentials
   */
  logout(): void {
    this.clearAuth();
  }

  // =========================================================================
  // Token Management
  // =========================================================================

  private loadStoredAuth(): void {
    try {
      const authType = localStorage.getItem('databridge_auth_type');
      apiService.loadStoredCredentials();

      if (authType === 'apiKey') {
        const apiKey = localStorage.getItem('databridge_api_key');
        if (apiKey) {
          this.setState({
            isAuthenticated: true,
            authType: 'apiKey',
          });
        }
      } else if (authType === 'jwt') {
        const token = localStorage.getItem('databridge_token');
        const expiresAtStr = localStorage.getItem('databridge_expires_at');
        const userStr = localStorage.getItem('databridge_user');

        if (token && expiresAtStr) {
          const expiresAt = new Date(expiresAtStr);

          if (expiresAt > new Date()) {
            const user = userStr ? JSON.parse(userStr) : {};
            this.setState({
              isAuthenticated: true,
              authType: 'jwt',
              userId: user.id,
              userName: user.name,
              expiresAt,
            });

            // Schedule refresh
            const expiresIn = (expiresAt.getTime() - Date.now()) / 1000;
            this.scheduleTokenRefresh(expiresIn);
          } else {
            // Token expired
            this.clearAuth();
          }
        }
      }
    } catch (error) {
      console.error('Failed to load stored auth:', error);
      this.clearAuth();
    }
  }

  private clearAuth(): void {
    apiService.clearCredentials();
    localStorage.removeItem('databridge_auth_type');
    localStorage.removeItem('databridge_user');
    localStorage.removeItem('databridge_expires_at');

    this.setState({
      isAuthenticated: false,
      authType: null,
      userId: undefined,
      userName: undefined,
      expiresAt: undefined,
    });
  }

  private refreshTimeoutId: NodeJS.Timeout | null = null;

  private scheduleTokenRefresh(expiresInSeconds: number): void {
    // Clear existing timeout
    if (this.refreshTimeoutId) {
      clearTimeout(this.refreshTimeoutId);
    }

    // Refresh 1 minute before expiry
    const refreshIn = Math.max(0, (expiresInSeconds - 60) * 1000);

    this.refreshTimeoutId = setTimeout(() => {
      this.refreshToken();
    }, refreshIn);
  }

  private async refreshToken(): Promise<void> {
    try {
      const response = await fetch(`${this.getBaseUrl()}/auth/refresh`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${localStorage.getItem('databridge_token')}`,
        },
      });

      if (!response.ok) {
        throw new Error('Refresh failed');
      }

      const data = await response.json();
      const { accessToken, expiresIn } = data;

      apiService.setToken(accessToken);

      const expiresAt = new Date(Date.now() + expiresIn * 1000);
      this.setState({ expiresAt });
      localStorage.setItem('databridge_expires_at', expiresAt.toISOString());

      this.scheduleTokenRefresh(expiresIn);
    } catch (error) {
      console.error('Token refresh failed:', error);
      this.clearAuth();
    }
  }

  private getBaseUrl(): string {
    return localStorage.getItem('databridge_api_url') || 'http://localhost:3002/api';
  }
}

// Export singleton instance
export const authService = new AuthService();
export default authService;
