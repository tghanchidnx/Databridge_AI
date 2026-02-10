import { create } from "zustand";
import { projectService } from "@/services/api/hierarchy/project.service";
import type {
  Project,
  ProjectMember,
  CreateProjectDto,
  UpdateProjectDto,
  InviteMemberDto,
} from "@/services/api/hierarchy/project.service";

interface ProjectStore {
  // State
  projects: Project[];
  currentProjectId: string | null;
  projectMembers: ProjectMember[];
  loading: boolean;
  error: string | null;
  searchQuery: string;

  // Actions - Project Management
  loadProjects: () => Promise<void>;
  createProject: (data: CreateProjectDto) => Promise<Project>;
  updateProject: (projectId: string, data: UpdateProjectDto) => Promise<void>;
  deleteProject: (projectId: string) => Promise<void>;
  setCurrentProjectId: (projectId: string | null) => void;
  setSearchQuery: (query: string) => void;

  // Actions - Member Management
  loadProjectMembers: (projectId: string) => Promise<void>;
  inviteMember: (projectId: string, data: InviteMemberDto) => Promise<void>;
  shareWithOrganization: (
    projectId: string,
    role: "editor" | "viewer"
  ) => Promise<void>;
  updateMemberRole: (
    projectId: string,
    memberId: string,
    role: "editor" | "viewer"
  ) => Promise<void>;
  removeMember: (projectId: string, memberId: string) => Promise<void>;

  // Actions - Import/Export
  exportProject: (projectId: string, format: "json" | "csv") => Promise<void>;
  importProject: (
    projectId: string,
    file: File,
    format: "json" | "csv"
  ) => Promise<void>;

  // Computed
  filteredProjects: () => Project[];
  currentProject: () => Project | null;
}

// Load saved project ID from localStorage
const getSavedProjectId = (): string | null => {
  try {
    return localStorage.getItem("currentProjectId");
  } catch {
    return null;
  }
};

// Save project ID to localStorage
const saveProjectId = (projectId: string | null) => {
  try {
    if (projectId) {
      localStorage.setItem("currentProjectId", projectId);
    } else {
      localStorage.removeItem("currentProjectId");
    }
  } catch {
    // Ignore localStorage errors
  }
};

export const useProjectStore = create<ProjectStore>((set, get) => ({
  // Initial State
  projects: [],
  currentProjectId: getSavedProjectId(),
  projectMembers: [],
  loading: false,
  error: null,
  searchQuery: "",

  // Load all projects
  loadProjects: async () => {
    set({ loading: true, error: null });
    try {
      const projects = await projectService.getProjects();
      const state = get();

      // If no current project, auto-select first active project
      let currentProjectId = state.currentProjectId;
      if (!currentProjectId && projects.length > 0) {
        // Try to find first active project, or just use first project
        const firstActive = projects.find((p) => p.isActive) || projects[0];
        currentProjectId = firstActive.id;
        saveProjectId(currentProjectId);
      }

      set({ projects, currentProjectId, loading: false });
    } catch (error: any) {
      set({
        error:
          error.response?.data?.message ||
          error.message ||
          "Failed to load projects",
        loading: false,
      });
      throw error;
    }
  },

  // Create new project
  createProject: async (data: CreateProjectDto) => {
    set({ loading: true, error: null });
    try {
      const newProject = await projectService.createProject(data);
      saveProjectId(newProject.id);
      set((state) => ({
        projects: [...state.projects, newProject],
        currentProjectId: newProject.id,
        loading: false,
      }));
      return newProject;
    } catch (error: any) {
      set({
        error:
          error.response?.data?.message ||
          error.message ||
          "Failed to create project",
        loading: false,
      });
      throw error;
    }
  },

  // Update project
  updateProject: async (projectId: string, data: UpdateProjectDto) => {
    set({ loading: true, error: null });
    try {
      await projectService.updateProject(projectId, data);
      set((state) => ({
        projects: state.projects.map((p) =>
          p.id === projectId ? { ...p, ...data } : p
        ),
        loading: false,
      }));
    } catch (error: any) {
      set({
        error:
          error.response?.data?.message ||
          error.message ||
          "Failed to update project",
        loading: false,
      });
      throw error;
    }
  },

  // Delete project
  deleteProject: async (projectId: string) => {
    set({ loading: true, error: null });
    try {
      await projectService.deleteProject(projectId);
      set((state) => {
        const remainingProjects = state.projects.filter(
          (p) => p.id !== projectId
        );
        const newCurrentProjectId =
          state.currentProjectId === projectId
            ? remainingProjects.length > 0
              ? remainingProjects[0].id
              : null
            : state.currentProjectId;

        saveProjectId(newCurrentProjectId);

        return {
          projects: remainingProjects,
          currentProjectId: newCurrentProjectId,
          loading: false,
        };
      });
    } catch (error: any) {
      set({
        error:
          error.response?.data?.message ||
          error.message ||
          "Failed to delete project",
        loading: false,
      });
      throw error;
    }
  },

  // Set current project
  setCurrentProjectId: (projectId: string | null) => {
    saveProjectId(projectId);
    set({ currentProjectId: projectId });
  },

  // Set search query
  setSearchQuery: (query: string) => {
    set({ searchQuery: query });
  },

  // Load project members
  loadProjectMembers: async (projectId: string) => {
    set({ loading: true, error: null });
    try {
      const members = await projectService.getProjectMembers(projectId);
      set({ projectMembers: members, loading: false });
    } catch (error: any) {
      set({
        error:
          error.response?.data?.message ||
          error.message ||
          "Failed to load members",
        loading: false,
      });
      throw error;
    }
  },

  // Invite member
  inviteMember: async (projectId: string, data: InviteMemberDto) => {
    set({ loading: true, error: null });
    try {
      await projectService.inviteProjectMember(projectId, data);
      // Reload members after invitation
      await get().loadProjectMembers(projectId);
    } catch (error: any) {
      set({
        error:
          error.response?.data?.message ||
          error.message ||
          "Failed to invite member",
        loading: false,
      });
      throw error;
    }
  },

  // Share with organization
  shareWithOrganization: async (
    projectId: string,
    role: "editor" | "viewer"
  ) => {
    set({ loading: true, error: null });
    try {
      await projectService.shareWithOrganization(projectId, role);
      // Reload members after sharing
      await get().loadProjectMembers(projectId);
    } catch (error: any) {
      set({
        error:
          error.response?.data?.message ||
          error.message ||
          "Failed to share with organization",
        loading: false,
      });
      throw error;
    }
  },

  // Update member role
  updateMemberRole: async (
    projectId: string,
    memberId: string,
    role: "editor" | "viewer"
  ) => {
    set({ loading: true, error: null });
    try {
      await projectService.updateProjectMember(projectId, memberId, { role });
      // Update local state
      set((state) => ({
        projectMembers: state.projectMembers.map((m) =>
          m.id === memberId ? { ...m, role } : m
        ),
        loading: false,
      }));
    } catch (error: any) {
      set({
        error:
          error.response?.data?.message ||
          error.message ||
          "Failed to update member",
        loading: false,
      });
      throw error;
    }
  },

  // Remove member
  removeMember: async (projectId: string, memberId: string) => {
    set({ loading: true, error: null });
    try {
      await projectService.removeProjectMember(projectId, memberId);
      // Remove from local state
      set((state) => ({
        projectMembers: state.projectMembers.filter((m) => m.id !== memberId),
        loading: false,
      }));
    } catch (error: any) {
      set({
        error:
          error.response?.data?.message ||
          error.message ||
          "Failed to remove member",
        loading: false,
      });
      throw error;
    }
  },

  // Export project
  exportProject: async (projectId: string, format: "json" | "csv") => {
    set({ loading: true, error: null });
    try {
      const project = get().projects.find((p) => p.id === projectId);
      const exportData = await projectService.exportProject(
        projectId,
        format,
        project?.name
      );

      // Create download content based on format
      let content: string;
      let mimeType: string;

      if (format === "json") {
        content = JSON.stringify(exportData, null, 2);
        mimeType = "application/json";
      } else {
        // CSV format - exportData should already be a string
        content =
          typeof exportData === "string"
            ? exportData
            : JSON.stringify(exportData);
        mimeType = "text/csv";
      }

      // Create download link
      const blob = new Blob([content], { type: mimeType });
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;

      const fileName = `${
        project?.name || "project"
      }_export_${Date.now()}.${format}`;
      link.download = fileName;

      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);

      set({ loading: false });
    } catch (error: any) {
      set({
        error:
          error.response?.data?.message ||
          error.message ||
          "Failed to export project",
        loading: false,
      });
      throw error;
    }
  },

  // Import project
  importProject: async (
    projectId: string,
    file: File,
    format: "json" | "csv"
  ) => {
    set({ loading: true, error: null });
    try {
      await projectService.importProject(projectId, file, format);
      set({ loading: false });
    } catch (error: any) {
      set({
        error:
          error.response?.data?.message ||
          error.message ||
          "Failed to import project",
        loading: false,
      });
      throw error;
    }
  },

  // Computed: Get filtered projects based on search
  filteredProjects: () => {
    const { projects, searchQuery } = get();
    if (!searchQuery.trim()) return projects;

    const query = searchQuery.toLowerCase();
    return projects.filter(
      (p) =>
        p.name.toLowerCase().includes(query) ||
        p.description?.toLowerCase().includes(query)
    );
  },

  // Computed: Get current project
  currentProject: () => {
    const { projects, currentProjectId } = get();
    return projects.find((p) => p.id === currentProjectId) || null;
  },
}));
