import {
  createContext,
  useContext,
  useState,
  useEffect,
  ReactNode,
} from "react";
import type { User, Organization } from "@/types";
import { apiService } from "@/lib/api-service";
import { toast } from "sonner";

interface AuthContextType {
  user: User | null;
  currentOrganization: Organization | null;
  userOrganizations: Organization[];
  isAuthenticated: boolean;
  isNewUser: boolean;
  login: (email: string, password: string) => Promise<boolean>;
  loginWithOAuth: (
    provider: "microsoft" | "snowflake" | "google",
    accessToken: string,
    refreshToken?: string
  ) => Promise<void>;
  loginWithMicrosoft: (accessToken: string) => Promise<void>;
  loginWithSnowflake: (
    accessToken: string,
    refreshToken?: string
  ) => Promise<void>;
  signup: (
    fullName: string,
    email: string,
    password: string
  ) => Promise<boolean>;
  logout: () => void;
  updateUser: (updates: Partial<User>) => Promise<void>;
  updateOrganization: (updates: Partial<Organization>) => Promise<void>;
  switchOrganization: (organizationId: string) => Promise<void>;
  completeOnboarding: (organizationData: {
    name: string;
    plan: "free" | "pro" | "enterprise";
  }) => Promise<void>;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [authUser, setAuthUser] = useState<User | null>(() => {
    const stored = localStorage.getItem("auth_user");
    return stored ? JSON.parse(stored) : null;
  });
  const [allOrganizations, setAllOrganizations] = useState<Organization[]>([]);
  const [isNewUser, setIsNewUser] = useState<boolean>(false);
  const [currentOrganization, setCurrentOrganization] =
    useState<Organization | null>(null);
  const [userOrganizations, setUserOrganizations] = useState<Organization[]>(
    []
  );

  // Sync authUser to localStorage
  useEffect(() => {
    if (authUser) {
      localStorage.setItem("auth_user", JSON.stringify(authUser));
    } else {
      localStorage.removeItem("auth_user");
    }
  }, [authUser]);

  // Load user's organizations from backend
  useEffect(() => {
    const loadOrganizations = async () => {
      if (authUser && authUser.organizationId) {
        try {
          const organizations = await apiService.getUserOrganizations();
          setAllOrganizations(organizations);
          setUserOrganizations(organizations);

          // Set current organization
          const current = organizations.find(
            (org) => org.id === authUser.organizationId
          );
          if (current) {
            setCurrentOrganization(current);
          }
        } catch (error) {
          console.error("Failed to load organizations:", error);
        }
      } else {
        setUserOrganizations([]);
        setCurrentOrganization(null);
      }
    };

    loadOrganizations();
  }, [authUser?.organizationId]);

  const login = async (email: string, password: string): Promise<boolean> => {
    // For now, use dummy authentication until password-based login API is available
    const usersList = allUsers ?? [];
    let user = usersList.find((u) => u.email === email);
    if (user) {
      user = await ensureUserHasWorkspace(user);
      setIsNewUser(false);
      return true;
    }
    return false;
  };

  const loginWithMicrosoft = async (accessToken: string): Promise<void> => {
    try {
      const response = await apiService.loginWithMicrosoft(accessToken);

      // Store JWT token
      if (response.token) {
        localStorage.setItem("auth_token", response.token);
      }

      // Create user from API response
      const user: User = {
        id: response.user.id,
        email: response.user.email,
        name: response.user.name,
        role: "owner",
        createdAt: response.user.createdAt || new Date().toISOString(),
        organizationId: response.user.organizationId,
        onboardingCompleted: response.user.onboardingCompleted,
      };

      // Check if user is new (no organization)
      if (!user.organizationId) {
        setIsNewUser(true);
      } else {
        setIsNewUser(false);

        // Load organization if user has one
        if (response.organization) {
          const organization: Organization = {
            id: response.organization.id,
            name: response.organization.name,
            description: response.organization.description,
            plan: response.organization.plan || "free",
            status: response.organization.status || "active",
            memberCount: response.organization.memberCount || 1,
            ownerId: response.organization.ownerId,
            createdAt: response.organization.createdAt,
            members: [],
          };

          setAllOrganizations([organization]);
          setUserOrganizations([organization]);
          setCurrentOrganization(organization);
        }
      }

      setAuthUser(user);
      toast.success("Successfully logged in with Microsoft");
    } catch (error: any) {
      console.error("Microsoft login failed:", error);
      toast.error(error.message || "Failed to login with Microsoft");
      throw error;
    }
  };

  const loginWithSnowflake = async (
    accessToken: string,
    refreshToken?: string
  ): Promise<void> => {
    try {
      const response = await apiService.loginWithSnowflake(
        accessToken,
        refreshToken
      );

      // Create user from API response
      const userId = response.user.id || `user_${Date.now()}`;
      const user: User = {
        id: userId,
        email: response.user.email,
        name: response.user.name,
        role: "owner",
        createdAt: new Date().toISOString(),
        workspaceIds: response.workspace ? [response.workspace.id] : [],
      };

      // Check if user already exists
      const usersList = allUsers ?? [];
      const existingUser = usersList.find((u) => u.email === user.email);

      if (!existingUser) {
        setAllUsers((currentUsers) => [...(currentUsers || []), user]);
        setIsNewUser(true);
      } else {
        setIsNewUser(false);
      }

      // Create or use default workspace
      if (response.workspace) {
        const workspace: Workspace = {
          id: response.workspace.id,
          name: response.workspace.name,
          plan: "free",
          memberCount: 1,
          ownerId: userId,
          createdAt: new Date().toISOString(),
          members: [
            {
              userId,
              role: "owner",
              joinedAt: new Date().toISOString(),
            },
          ],
        };

        const workspacesList = allWorkspaces ?? [];
        const existingWorkspace = workspacesList.find(
          (w) => w.id === workspace.id
        );
        if (!existingWorkspace) {
          setAllWorkspaces((currentWorkspaces) => [
            ...(currentWorkspaces || []),
            workspace,
          ]);
        }

        user.currentWorkspaceId = workspace.id;
      }

      const finalUser = await ensureUserHasWorkspace(user);
      setAuthUser(finalUser);

      toast.success("Successfully logged in with Snowflake");
    } catch (error: any) {
      console.error("Snowflake login failed:", error);
      toast.error(error.message || "Failed to login with Snowflake");
      throw error;
    }
  };

  const loginWithOAuth = async (
    provider: "microsoft" | "snowflake" | "google",
    accessToken: string,
    refreshToken?: string
  ): Promise<void> => {
    if (provider === "microsoft") {
      await loginWithMicrosoft(accessToken);
    } else if (provider === "snowflake") {
      await loginWithSnowflake(accessToken, refreshToken);
    } else {
      // Fallback for other providers
      toast.error(`${provider} OAuth not yet implemented`);
    }
  };

  const signup = async (
    fullName: string,
    email: string,
    password: string
  ): Promise<boolean> => {
    const usersList = allUsers ?? [];
    const existingUser = usersList.find((u) => u.email === email);
    if (existingUser) {
      return false;
    }

    const userId = `user_${Date.now()}`;
    const newUser: User = {
      id: userId,
      email,
      name: fullName,
      role: "owner",
      createdAt: new Date().toISOString(),
      workspaceIds: [],
    };

    setAllUsers((currentUsers) => [...(currentUsers || []), newUser]);
    setAuthUser(newUser);
    setIsNewUser(true);

    return true;
  };

  const updateUser = async (updates: Partial<User>): Promise<void> => {
    if (!authUser) return;

    const updatedUser = { ...authUser, ...updates };
    setAuthUser(updatedUser);

    setAllUsers((currentUsers) =>
      (currentUsers || []).map((u) => (u.id === authUser.id ? updatedUser : u))
    );
  };

  const updateWorkspace = async (
    updates: Partial<Workspace>
  ): Promise<void> => {
    if (!currentWorkspace) return;

    const updatedWorkspace = { ...currentWorkspace, ...updates };
    setCurrentWorkspace(updatedWorkspace);

    setAllWorkspaces((currentWorkspaces) =>
      (currentWorkspaces || []).map((ws) =>
        ws.id === currentWorkspace.id ? updatedWorkspace : ws
      )
    );
  };

  const switchWorkspace = async (workspaceId: string): Promise<void> => {
    if (!authUser) return;

    const workspace = userWorkspaces.find((ws) => ws.id === workspaceId);
    if (workspace) {
      setCurrentWorkspace(workspace);
      await updateUser({ currentWorkspaceId: workspaceId });
    }
  };

  const completeOnboarding = async (organizationData: {
    name: string;
    plan: "free" | "pro" | "enterprise";
  }) => {
    if (!authUser) return;

    try {
      // Call backend to create organization
      const organization = await apiService.createOrganization({
        name: organizationData.name,
        plan: organizationData.plan,
      });

      // Create organization object
      const newOrganization: Organization = {
        id: organization.id,
        name: organization.name,
        description: organization.description,
        plan: organization.plan,
        status: "active",
        memberCount: 1,
        ownerId: authUser.id,
        createdAt: organization.created_at,
        members: [],
      };

      setAllOrganizations([newOrganization]);
      setUserOrganizations([newOrganization]);
      setCurrentOrganization(newOrganization);

      // Fetch updated user data from backend to get onboardingCompleted status
      const updatedUserData = await apiService.getCurrentUser();
      const updatedUser = {
        ...authUser,
        organizationId: organization.id,
        onboardingCompleted: updatedUserData.onboardingCompleted || true,
      };
      setAuthUser(updatedUser);
      setIsNewUser(false);

      toast.success("Organization created successfully!");
    } catch (error: any) {
      console.error("Onboarding failed:", error);
      toast.error(error.message || "Failed to complete onboarding");
      throw error;
    }
  };

  const logout = () => {
    apiService.logout();
    localStorage.removeItem("auth_token");
    setAuthUser(null);
    setCurrentOrganization(null);
    setUserOrganizations([]);
    setIsNewUser(false);
  };

  return (
    <AuthContext.Provider
      value={{
        user: authUser ?? null,
        currentOrganization,
        userOrganizations,
        isAuthenticated: !!authUser,
        isNewUser: isNewUser ?? false,
        login,
        loginWithOAuth,
        loginWithMicrosoft,
        loginWithSnowflake,
        signup,
        logout,
        updateUser,
        updateOrganization: updateUser, // Placeholder
        switchOrganization: async () => {}, // Placeholder
        completeOnboarding,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within AuthProvider");
  }
  return context;
}
