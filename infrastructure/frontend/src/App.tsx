import { useState, useEffect } from "react";
import {
  BrowserRouter,
  Routes,
  Route,
  Navigate,
  useNavigate,
  useLocation,
} from "react-router-dom";
import {
  ChartBar,
  Database,
  Gear,
  GitBranch,
  MagnifyingGlass,
  Robot,
  Table,
} from "@phosphor-icons/react";
import { Sidebar } from "@/components/layout/Sidebar";
import { TopBar } from "@/components/layout/TopBar";
import { Dashboard } from "@/components/views/Dashboard";
import { PlaceholderView } from "@/components/views/PlaceholderView";
import { LoginView } from "@/components/views/LoginView";
import { SignupView } from "@/components/views/SignupView";
import { OnboardingView } from "@/components/views/OnboardingView";
import { SettingsView } from "@/components/views/SettingsView";
import { DatabaseConnectionsView } from "@/components/views/DatabaseConnectionsView";
import { VersionControlView } from "@/components/views/VersionControlView";
import { SchemaMatcherView } from "@/components/views/SchemaMatcherView";
import { HierarchyKnowledgeBaseView } from "@/components/hierarchy-knowledge-base";
import { AIConfigView } from "@/components/views/AIConfigView";
import { SnowflakeOAuthCallback } from "@/components/views/SnowflakeOAuthCallback";
import { ProtectedRoute } from "@/components/auth/ProtectedRoute";
import { Toaster } from "@/components/ui/sonner";
import { DevConsole } from "@/components/dev-console";
import { AuthProvider, useAuth } from "@/contexts/AuthContext";
import { ThemeProvider } from "@/contexts/ThemeContext";
import { ProjectProvider } from "@/contexts/ProjectContext";

// Documentation and Onboarding
import { DocumentationPage } from "@/pages/DocumentationPage";
import { FeatureDemoPage } from "@/pages/FeatureDemoPage";
import { HierarchyViewerPage } from "@/pages/HierarchyViewerPage";
import { OnboardingTourProvider, useOnboardingTour } from "@/components/onboarding/OnboardingTour";
import { WelcomeDialog, useWelcomeDialog } from "@/components/onboarding/WelcomeDialog";

function MainLayout() {
  const navigate = useNavigate();
  const location = useLocation();
  const [activeView, setActiveView] = useState("dashboard");

  // Sync activeView with current route
  useEffect(() => {
    const pathToView: Record<string, string> = {
      "/": "dashboard",
      "/dashboard": "dashboard",
      "/connections": "connections",
      "/version-control": "version-control",
      "/settings": "settings",
      "/schema-matcher": "schema-matcher",
      "/hierarchy-knowledge-base": "hierarchy-knowledge-base",
      "/hierarchy-viewer": "hierarchy-viewer",
      "/report-matcher": "report-matcher",
      "/query-builder": "query-builder",
      "/ai-assistant": "ai-assistant",
      "/ai-config": "ai-config",
      "/docs": "docs",
      "/demo": "demo",
    };

    const view = pathToView[location.pathname] || "dashboard";
    setActiveView(view);
  }, [location.pathname]);

  const handleViewChange = (view: string) => {
    setActiveView(view);
    const viewToPath: Record<string, string> = {
      dashboard: "/dashboard",
      connections: "/connections",
      "version-control": "/version-control",
      settings: "/settings",
      "schema-matcher": "/schema-matcher",
      "hierarchy-knowledge-base": "/hierarchy-knowledge-base",
      "hierarchy-viewer": "/hierarchy-viewer",
      "report-matcher": "/report-matcher",
      "query-builder": "/query-builder",
      "ai-assistant": "/ai-assistant",
      "ai-config": "/ai-config",
      docs: "/docs",
      demo: "/demo",
    };
    navigate(viewToPath[view] || "/dashboard");
  };

  return (
    <div className="flex h-screen overflow-hidden bg-background text-foreground">
      <Sidebar activeView={activeView} onViewChange={handleViewChange} />

      <div className="flex flex-1 flex-col overflow-hidden min-w-0">
        <TopBar onViewChange={handleViewChange} />

        <main className="flex-1 overflow-hidden">
          <div className="container  p-0 lg:px-2 max-w-[1600px] h-full overflow-auto">
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/dashboard" element={<Dashboard />} />
              <Route
                path="/connections"
                element={<DatabaseConnectionsView />}
              />
              <Route path="/version-control" element={<VersionControlView />} />
              <Route path="/settings" element={<SettingsView />} />
              <Route path="/schema-matcher" element={<SchemaMatcherView />} />
              <Route
                path="/hierarchy-knowledge-base"
                element={<HierarchyKnowledgeBaseView />}
              />
              <Route
                path="/hierarchy-viewer"
                element={<HierarchyViewerPage />}
              />
              <Route
                path="/ai-config"
                element={<AIConfigView />}
              />
              <Route
                path="/docs"
                element={<DocumentationPage />}
              />
              <Route
                path="/demo"
                element={<FeatureDemoPage />}
              />
              <Route
                path="/report-matcher"
                element={
                  <PlaceholderView
                    title="Report Matcher"
                    description="Compare data reports and identify discrepancies at cell level"
                    icon={ChartBar}
                    features={[
                      "Support for CSV, Excel, and direct database query comparison",
                      "Cell-by-cell difference highlighting with color-coded changes",
                      "Configurable matching rules and tolerance thresholds",
                      "Statistical summaries of differences and match rates",
                      "Export comparison results to multiple formats",
                    ]}
                  />
                }
              />
              <Route
                path="/query-builder"
                element={
                  <PlaceholderView
                    title="Query Builder"
                    description="Build and execute SQL queries with AI-powered assistance"
                    icon={MagnifyingGlass}
                    features={[
                      "SQL syntax highlighting and auto-completion",
                      "Natural language to SQL conversion using AI",
                      "Query execution with real-time results",
                      "Query history and favorites management",
                      "Export results to CSV, JSON, or Excel",
                    ]}
                  />
                }
              />
              <Route
                path="/ai-assistant"
                element={
                  <PlaceholderView
                    title="AI Assistant"
                    description="Context-aware AI assistant for database operations"
                    icon={Robot}
                    features={[
                      "Natural language query generation and explanation",
                      "Schema-aware suggestions and optimizations",
                      "Self-healing recommendations for schema mismatches",
                      "Project-specific chat history and context",
                      "Code generation for data transformations",
                    ]}
                  />
                }
              />
            </Routes>
          </div>
        </main>
      </div>

      <Toaster />
      <DevConsole />
    </div>
  );
}

function AppContent() {
  return (
    <Routes>
      {/* Public routes */}
      <Route path="/auth" element={<LoginView />} />
      <Route path="/signup" element={<SignupView />} />
      <Route
        path="/auth/snowflake-callback"
        element={<SnowflakeOAuthCallback />}
      />

      {/* Onboarding route - requires authentication but not organization */}
      <Route
        path="/onboarding"
        element={
          <ProtectedRoute requireOrganization={false}>
            <OnboardingView />
          </ProtectedRoute>
        }
      />

      {/* Protected routes - require authentication and organization */}
      <Route
        path="/*"
        element={
          <ProtectedRoute requireOrganization={true}>
            <MainLayout />
          </ProtectedRoute>
        }
      />
    </Routes>
  );
}

function AppWithOnboarding() {
  const welcomeDialog = useWelcomeDialog();
  const { startTour } = useOnboardingTour();

  // Show welcome dialog on first load
  useEffect(() => {
    welcomeDialog.showWelcome();
  }, []);

  const handleStartTour = () => {
    // Start the onboarding tour
    startTour();
  };

  const handleSkipTour = () => {
    console.log("Tour skipped");
  };

  return (
    <>
      <AppContent />
      <WelcomeDialog
        open={welcomeDialog.isOpen}
        onOpenChange={welcomeDialog.setIsOpen}
        onStartTour={handleStartTour}
        onSkipTour={handleSkipTour}
        isNewUser={true}
        version="2.0"
      />
    </>
  );
}

function App() {
  return (
    <BrowserRouter>
      <ThemeProvider>
        <AuthProvider>
          <ProjectProvider>
            <OnboardingTourProvider>
              <AppWithOnboarding />
            </OnboardingTourProvider>
          </ProjectProvider>
        </AuthProvider>
      </ThemeProvider>
    </BrowserRouter>
  );
}

export default App;
