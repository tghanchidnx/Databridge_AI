import { create } from "zustand";
import { persist } from "zustand/middleware";
import type { Organization } from "@/types";
import { organizationsService } from "@/services/api";
import { toast } from "sonner";

interface OrganizationState {
  currentOrganization: Organization | null;
  organizations: Organization[];
  isLoading: boolean;
  error: string | null;

  // Actions
  setCurrentOrganization: (organization: Organization | null) => void;
  createOrganization: (data: {
    name: string;
    plan: "free" | "pro" | "enterprise";
    description?: string;
  }) => Promise<Organization>;
  loadOrganizations: () => Promise<void>;
  loadOrganization: (id: string) => Promise<void>;
  updateOrganization: (
    id: string,
    data: Partial<Organization>
  ) => Promise<void>;
  clearError: () => void;
  reset: () => void;
}

export const useOrganizationStore = create<OrganizationState>()(
  persist(
    (set, get) => ({
      currentOrganization: null,
      organizations: [],
      isLoading: false,
      error: null,

      setCurrentOrganization: (organization) =>
        set({ currentOrganization: organization }),

      createOrganization: async (data) => {
        set({ isLoading: true, error: null });
        try {
          console.log(
            "Organization Store: Creating organization with data:",
            data
          );
          console.log(
            "Organization Store: Auth token in localStorage:",
            localStorage.getItem("auth_token") ? "exists" : "missing"
          );

          const organization = await organizationsService.createOrganization(
            data
          );
          console.log(
            "Organization Store: Organization created:",
            organization
          );

          set({
            currentOrganization: organization,
            organizations: [...get().organizations, organization],
            isLoading: false,
          });

          toast.success("Organization created successfully!");
          return organization;
        } catch (error: any) {
          const errorMessage =
            error.response?.data?.message ||
            error.message ||
            "Failed to create organization";
          set({
            error: errorMessage,
            isLoading: false,
          });
          toast.error(errorMessage);
          throw error;
        }
      },

      loadOrganizations: async () => {
        set({ isLoading: true, error: null });
        try {
          const organizations =
            await organizationsService.getUserOrganizations();

          set({
            organizations,
            currentOrganization: organizations[0] || null,
            isLoading: false,
          });
        } catch (error: any) {
          const errorMessage =
            error.response?.data?.message ||
            error.message ||
            "Failed to load organizations";
          set({
            error: errorMessage,
            isLoading: false,
          });
          console.error("Failed to load organizations:", error);
        }
      },

      loadOrganization: async (id: string) => {
        set({ isLoading: true, error: null });
        try {
          const organization = await organizationsService.getOrganization(id);

          set({
            currentOrganization: organization,
            isLoading: false,
          });
        } catch (error: any) {
          const errorMessage =
            error.response?.data?.message ||
            error.message ||
            "Failed to load organization";
          set({
            error: errorMessage,
            isLoading: false,
          });
          toast.error(errorMessage);
          throw error;
        }
      },

      updateOrganization: async (id: string, data: Partial<Organization>) => {
        set({ isLoading: true, error: null });
        try {
          const updated = await organizationsService.updateOrganization(
            id,
            data as any
          );

          set({
            currentOrganization: updated,
            organizations: get().organizations.map((org) =>
              org.id === id ? updated : org
            ),
            isLoading: false,
          });

          toast.success("Organization updated successfully!");
        } catch (error: any) {
          const errorMessage =
            error.response?.data?.message ||
            error.message ||
            "Failed to update organization";
          set({
            error: errorMessage,
            isLoading: false,
          });
          toast.error(errorMessage);
          throw error;
        }
      },

      clearError: () => set({ error: null }),

      reset: () =>
        set({
          currentOrganization: null,
          organizations: [],
          error: null,
        }),
    }),
    {
      name: "organization-storage",
      partialize: (state) => ({
        currentOrganization: state.currentOrganization,
        organizations: state.organizations,
      }),
    }
  )
);
