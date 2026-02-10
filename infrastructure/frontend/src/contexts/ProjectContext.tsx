import React, {
  createContext,
  useContext,
  useState,
  useEffect,
  ReactNode,
} from "react";

interface ProjectContextType {
  currentProjectId: string | null;
  setCurrentProjectId: (projectId: string) => void;
}

const ProjectContext = createContext<ProjectContextType | undefined>(undefined);

export function ProjectProvider({ children }: { children: ReactNode }) {
  const [currentProjectId, setCurrentProjectId] = useState<string | null>(
    () => {
      return localStorage.getItem("activeProjectId");
    }
  );

  useEffect(() => {
    if (currentProjectId) {
      localStorage.setItem("activeProjectId", currentProjectId);
    }
  }, [currentProjectId]);

  return (
    <ProjectContext.Provider value={{ currentProjectId, setCurrentProjectId }}>
      {children}
    </ProjectContext.Provider>
  );
}

export function useProjectContext() {
  const context = useContext(ProjectContext);
  if (context === undefined) {
    throw new Error("useProjectContext must be used within a ProjectProvider");
  }
  return context;
}
