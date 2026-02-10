import axios, { AxiosInstance, AxiosError } from "axios";

// API URL: /api for Docker (nginx proxy), http://localhost:3001/api for local dev
const API_URL = import.meta.env.VITE_API_URL || "http://localhost:3001/api";

export interface ApiError {
  message: string;
  status?: number;
  details?: any;
}

export class BaseApiService {
  protected api: AxiosInstance;

  constructor() {
    this.api = axios.create({
      baseURL: API_URL,
      headers: {
        "Content-Type": "application/json",
      },
      timeout: 30000,
    });

    // Request interceptor
    this.api.interceptors.request.use((config) => {
      const token = this.getAuthToken();
      if (token) {
        config.headers.Authorization = `Bearer ${token}`;
      }
      return config;
    });

    // Response interceptor
    this.api.interceptors.response.use(
      (response) => response,
      (error) => this.handleError(error)
    );
  }

  protected getAuthToken(): string | null {
    return localStorage.getItem("auth_token");
  }

  protected setAuthToken(token: string) {
    localStorage.setItem("auth_token", token);
  }

  protected clearAuthToken() {
    localStorage.removeItem("auth_token");
  }

  protected handleError(error: AxiosError): Promise<never> {
    const apiError: ApiError = {
      message: "An unexpected error occurred",
      status: error.response?.status,
      details: error.response?.data,
    };

    if (error.response) {
      apiError.message = (error.response.data as any)?.message || error.message;
    } else if (error.request) {
      apiError.message =
        "No response from server. Please check your connection.";
    }

    // Handle 401 unauthorized - clear all auth data and redirect to login
    if (error.response?.status === 401) {
      // Only clear token if user is actually logged in (not during login attempt)
      const isLoginAttempt =
        error.config?.url?.includes("/auth/login") ||
        error.config?.url?.includes("/auth/signup");
      if (!isLoginAttempt) {
        console.error("401 Unauthorized - Logging out user");

        // Clear all authentication data from localStorage
        localStorage.removeItem("auth_token");
        localStorage.removeItem("auth_user");
        localStorage.removeItem("user");
        localStorage.removeItem("currentOrganization");
        localStorage.removeItem("auth-storage");

        // Redirect to login page
        window.location.href = "/auth";
      }
    }

    console.error("API Error:", apiError);
    return Promise.reject(apiError);
  }

  protected extractData<T>(response: any): T {
    // Handle nested response structure: { data: { data: ... } } or { data: ... }
    return (response.data?.data || response.data) as T;
  }
}
