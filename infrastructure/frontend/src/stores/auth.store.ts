import { create } from "zustand";
import { persist } from "zustand/middleware";
import type { User } from "@/types";
import { authService } from "@/services/api";
import { loginWithMicrosoft as msalLogin } from "@/lib/microsoft-auth";
import { toast } from "sonner";

interface BackendResponse {
  data: {
    user: User;
    token: string;
    accessToken?: string;
  };
  statusCode: number;
  message: string;
}

interface AuthState {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  error: string | null;

  // Actions
  setUser: (user: User | null) => void;
  setToken: (token: string | null) => void;
  loginWithMicrosoft: () => Promise<void>;
  loginWithEmail: (email: string, password: string) => Promise<void>;
  signup: (
    name: string,
    email: string,
    password: string,
    organizationKey?: string,
    bio?: string,
    teamSize?: string,
    primaryUseCase?: string
  ) => Promise<void>;
  logout: () => void;
  clearError: () => void;
  checkAuth: () => boolean;
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set, get) => ({
      user: null,
      token: null,
      isAuthenticated: false,
      isLoading: false,
      error: null,

      setUser: (user) =>
        set({
          user,
          isAuthenticated: !!user,
        }),

      setToken: (token) => {
        if (token) {
          localStorage.setItem("auth_token", token);
        } else {
          localStorage.removeItem("auth_token");
        }
        set({ token, isAuthenticated: !!token });
      },

      loginWithMicrosoft: async () => {
        set({ isLoading: true, error: null });
        try {
          // Get Microsoft access token
          const msalResponse = await msalLogin();
          console.log("Auth Store: MSAL response received");

          // Send to backend for validation - returns unwrapped data
          const response = await authService.loginWithMicrosoft(
            msalResponse.accessToken
          );
          console.log("Auth Store: Backend response:", response);

          if (!response.user) {
            throw new Error("No user data received from backend");
          }

          console.log("Auth Store: User data:", {
            organizationId: response.user.organizationId,
            onboardingCompleted: response.user.onboardingCompleted,
            email: response.user.email,
          });

          // Store token FIRST before updating state
          if (response.token) {
            localStorage.setItem("auth_token", response.token);
            console.log("Auth Store: Token saved to localStorage");
          }

          // Update store
          set({
            user: response.user as User,
            token: response.token,
            isAuthenticated: true,
            isLoading: false,
            error: null,
          });

          console.log(
            "Auth Store: User state updated, should trigger navigation"
          );
          toast.success("Successfully logged in!");
        } catch (error: any) {
          console.error("Login failed:", error);

          // Extract error message
          let errorMessage = "Failed to login. Please try again.";

          if (error.response?.data?.message) {
            errorMessage = error.response.data.message;
          } else if (error.message) {
            errorMessage = error.message;
          }

          set({
            error: errorMessage,
            isLoading: false,
            isAuthenticated: false,
          });

          toast.error(errorMessage, {
            duration: 5000,
            position: "top-center",
          });
          throw error;
        }
      },

      loginWithEmail: async (email: string, password: string) => {
        set({ isLoading: true, error: null });
        try {
          const response = await authService.loginWithEmail(email, password);

          // Store token FIRST
          if (response.token) {
            localStorage.setItem("auth_token", response.token);
          }

          set({
            user: response.user as User,
            token: response.token,
            isAuthenticated: true,
            isLoading: false,
            error: null,
          });

          toast.success(`Welcome back, ${response.user.name}!`);
        } catch (error: any) {
          console.error("Email login error:", error);

          // Extract error message from different possible response structures
          let errorMessage = "Invalid email or password";

          if (error.response?.data?.message) {
            errorMessage = error.response.data.message;
          } else if (error.message) {
            errorMessage = error.message;
          }

          // Make error message more user-friendly
          if (
            errorMessage.toLowerCase().includes("invalid") ||
            errorMessage.toLowerCase().includes("password") ||
            errorMessage.toLowerCase().includes("unauthorized")
          ) {
            errorMessage =
              "❌ Invalid username or password. Please check your credentials and try again.";
          }

          set({
            error: errorMessage,
            isLoading: false,
            isAuthenticated: false,
          });

          toast.error(errorMessage, {
            duration: 5000,
            position: "top-center",
          });
          throw error;
        }
      },

      signup: async (
        name: string,
        email: string,
        password: string,
        organizationKey?: string,
        bio?: string,
        teamSize?: string,
        primaryUseCase?: string
      ) => {
        set({ isLoading: true, error: null });
        try {
          const response = await authService.signup(
            email,
            password,
            name,
            organizationKey,
            bio,
            teamSize,
            primaryUseCase
          );

          // Store token FIRST
          if (response.token) {
            localStorage.setItem("auth_token", response.token);
          }

          set({
            user: response.user as User,
            token: response.token,
            isAuthenticated: true,
            isLoading: false,
            error: null,
          });

          const welcomeMessage = organizationKey
            ? `Welcome to the organization, ${response.user.name}!`
            : `Account created successfully! Welcome, ${response.user.name}!`;
          toast.success(welcomeMessage);
        } catch (error: any) {
          console.error("Signup error:", error);

          // Extract error message
          let errorMessage = "Failed to create account. Please try again.";

          if (error.response?.data?.message) {
            errorMessage = error.response.data.message;
          } else if (error.message) {
            errorMessage = error.message;
          }

          set({
            error: errorMessage,
            isLoading: false,
            isAuthenticated: false,
          });

          toast.error(errorMessage, {
            duration: 5000,
            position: "top-center",
          });
          throw error;
        }
      },

      logout: () => {
        localStorage.removeItem("auth_token");
        localStorage.removeItem("auth_user");
        authService.logout();

        set({
          user: null,
          token: null,
          isAuthenticated: false,
          error: null,
        });

        toast.success("Logged out successfully");
      },

      clearError: () => set({ error: null }),

      checkAuth: () => {
        const token = localStorage.getItem("auth_token");
        const { user } = get();

        if (!token || !user) {
          return false;
        }

        // Check token expiration
        try {
          const payload = JSON.parse(atob(token.split(".")[1]));
          const isExpired = Date.now() >= payload.exp * 1000;

          if (isExpired) {
            get().logout();
            return false;
          }

          return true;
        } catch {
          get().logout();
          return false;
        }
      },
    }),
    {
      name: "auth-storage",
      partialize: (state) => ({
        user: state.user,
        token: state.token,
        isAuthenticated: state.isAuthenticated,
      }),
    }
  )
);
