import React, {
  useState,
  useEffect,
  useCallback,
  useRef,
  useMemo,
} from "react";
import {
  smartHierarchyService,
  type SmartHierarchyMaster,
} from "@/services/api/hierarchy";
import { connectionService, type Connection } from "@/services/api/connection";
import { EnhancedHierarchyTree } from "../components/EnhancedHierarchyTree";
import { EnhancedGraphView } from "../components/EnhancedGraphView";
import { SmartHierarchyEditor } from "../components/SmartHierarchyEditor";
import { FormulaBuilder } from "../components/FormulaBuilder";
import {
  CreateHierarchyDialog,
  ProjectManagementDialog,
  ProjectDetailsDialog,
  ScriptDialog,
  DeploymentDialog,
} from "../dialogs";
import { ManageFormulasDialog } from "../dialogs/ManageFormulasDialog";
import { TemplateGalleryDialog, type HierarchyTemplate } from "../dialogs/TemplateGalleryDialog";
import { templatesService, type TemplateMetadata } from "@/services/api/templates.service";
import { useToast } from "@/hooks/use-toast";
import { useProjectStore } from "@/stores/projectStore";
import { useAuth } from "@/contexts/AuthContext";
import {
  Loader2,
  List,
  Network,
  Plus,
  RefreshCw,
  FileDown,
  FileUp,
  Code,
  Search,
  Filter,
  Trash2,
  Database,
  Play,
  Check,
  ChevronsUpDown,
  FolderKanban,
  Settings,
  Calculator,
  LayoutTemplate,
  Sparkles,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  DropdownMenuSeparator,
} from "@/components/ui/dropdown-menu";
import { Separator } from "@/components/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Checkbox } from "@/components/ui/checkbox";
import {
  ResizablePanelGroup,
  ResizablePanel,
  ResizableHandle,
} from "@/components/ui/resizable";
import { HelpTooltip } from "@/components/ui/help-tooltip";
// import { ResizablePanelGroup, ResizablePanel, ResizableHandle } from "@/components/ui/resizable";

export const HierarchyKnowledgeBaseView: React.FC = () => {
  const { toast } = useToast();
  const { user: authUser } = useAuth();
  const {
    currentProjectId,
    setCurrentProjectId,
    currentProject,
    projects,
    loadProjects,
  } = useProjectStore();
  const [loading, setLoading] = useState(true);
  const [loadingDetails, setLoadingDetails] = useState(false);
  const [allHierarchies, setAllHierarchies] = useState<SmartHierarchyMaster[]>(
    []
  );
  const [filteredHierarchies, setFilteredHierarchies] = useState<
    SmartHierarchyMaster[]
  >([]);
  const [selectedHierarchyId, setSelectedHierarchyId] = useState<string | null>(
    null
  );
  const [selectedHierarchy, setSelectedHierarchy] =
    useState<SmartHierarchyMaster | null>(null);
  const [editMode, setEditMode] = useState<"view" | "edit" | "create">("view");
  const [activeView, setActiveView] = useState<"tree" | "graph">("tree");
  const [selectedForFormula, setSelectedForFormula] = useState<Set<string>>(
    new Set()
  );

  // Tree expansion state - persisted across updates
  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());
  const [expandedDetails, setExpandedDetails] = useState<Set<string>>(
    new Set()
  );

  // Dialogs
  const [showCreateHierarchyDialog, setShowCreateHierarchyDialog] =
    useState(false);
  const [showProjectManagementDialog, setShowProjectManagementDialog] =
    useState(false);
  const [showProjectDetailsDialog, setShowProjectDetailsDialog] =
    useState(false);
  const [selectedProjectIdForDetails, setSelectedProjectIdForDetails] =
    useState<string | null>(null);
  const [projectDetailsInitialTab, setProjectDetailsInitialTab] = useState<
    "details" | "members" | "import-export"
  >("details");
  const [projectDialogInitialTab, setProjectDialogInitialTab] = useState<
    "projects" | "details" | "members" | "import-export"
  >("projects");
  const [showScriptDialog, setShowScriptDialog] = useState(false);
  const [showManageFormulasDialog, setShowManageFormulasDialog] =
    useState(false);
  const [showTemplateGalleryDialog, setShowTemplateGalleryDialog] =
    useState(false);
  const [availableTemplates, setAvailableTemplates] = useState<HierarchyTemplate[]>([]);
  const [loadingTemplates, setLoadingTemplates] = useState(false);

  // Search and Filter
  const [searchQuery, setSearchQuery] = useState("");
  const [filterType, setFilterType] = useState<"all" | "root" | "child">("all");
  const [filterActive, setFilterActive] = useState<
    "all" | "active" | "inactive"
  >("all");

  // Forms
  const [newHierarchyForm, setNewHierarchyForm] = useState({
    name: "",
    description: "",
    parentId: null as string | null,
    isRoot: true,
  });
  const [generatedScript, setGeneratedScript] = useState("");

  // Script generation state
  const [selectedHierarchiesForScript, setSelectedHierarchiesForScript] =
    useState<Set<string>>(new Set());
  const [deployAll, setDeployAll] = useState(false);
  const [selectedDatabaseType, setSelectedDatabaseType] = useState<
    "snowflake" | "postgres" | "mysql" | "sqlserver" | "all"
  >("snowflake");

  // Global database selection (used throughout the app)
  const [globalSelectedDatabase, setGlobalSelectedDatabase] =
    useState<string>("");

  // Script tabs state
  const [generatedScripts, setGeneratedScripts] = useState<{
    insert?: string;
    view?: string;
    mapping?: string;
    dynamicTable?: string;
  }>({});
  const [activeScriptTab, setActiveScriptTab] = useState<
    "insert" | "view" | "mapping" | "dynamicTable"
  >("insert");

  // Deployment configuration state
  const [showDeploymentDialog, setShowDeploymentDialog] = useState(false);
  const [deploymentConfig, setDeploymentConfig] = useState({
    connectionId: "",
    database: "",
    schema: "",
    masterTableName: "HIERARCHY_MASTER",
    masterViewName: "VW_{PROJECT}_HIERARCHY_MASTER",
    databaseType: "snowflake" as
      | "snowflake"
      | "postgres"
      | "mysql"
      | "sqlserver",
    createTables: true,
    createViews: true,
    createDynamicTables: false,
  });
  const [availableConnections, setAvailableConnections] = useState<
    Connection[]
  >([]);
  const [availableDatabases, setAvailableDatabases] = useState<string[]>([]);
  const [availableSchemas, setAvailableSchemas] = useState<string[]>([]);
  const [deploymentLoading, setDeploymentLoading] = useState(false);
  const [loadingDatabases, setLoadingDatabases] = useState(false);
  const [loadingSchemas, setLoadingSchemas] = useState(false);

  // Refs to prevent infinite loops
  const prevConnectionIdRef = useRef<string>("");
  const prevDatabaseRef = useRef<string>("");

  const isLoadingRef = useRef(false);

  // Set default database
  useEffect(() => {
    if (!globalSelectedDatabase) {
      setGlobalSelectedDatabase("snowflake");
    }
  }, []);

  // Sync global database selection with selectedDatabaseType
  useEffect(() => {
    if (globalSelectedDatabase && globalSelectedDatabase !== "all") {
      setSelectedDatabaseType(
        globalSelectedDatabase as
          | "snowflake"
          | "postgres"
          | "mysql"
          | "sqlserver"
      );
    }
  }, [globalSelectedDatabase]);

  // Load projects from store on mount
  useEffect(() => {
    const initializeProjects = async () => {
      setLoading(true);
      await loadProjects();
      setLoading(false);
    };
    initializeProjects();
  }, [loadProjects]);

  // Load hierarchies for current project
  const loadHierarchies = useCallback(async () => {
    if (!currentProjectId || isLoadingRef.current) return;

    isLoadingRef.current = true;
    try {
      setLoading(true);
      console.log("Loading hierarchies for project:", currentProjectId);
      // Use full API to get complete hierarchy data including hierarchyLevel
      const hierarchies = await smartHierarchyService.findAll(currentProjectId);
      console.log("Loaded hierarchies:", hierarchies);
      setAllHierarchies(Array.isArray(hierarchies) ? hierarchies : []);
      setFilteredHierarchies(Array.isArray(hierarchies) ? hierarchies : []);

      if (!hierarchies || hierarchies.length === 0) {
        console.log("No hierarchies found for project:", currentProjectId);
      }
    } catch (error: any) {
      console.error("Failed to load hierarchies:", error);
      console.error("Error details:", error.response?.data);
      toast({
        title: "Error loading hierarchies",
        description: error.response?.data?.message || error.message,
        variant: "destructive",
      });
      setAllHierarchies([]);
      setFilteredHierarchies([]);
    } finally {
      setLoading(false);
      isLoadingRef.current = false;
    }
  }, [currentProjectId]); // toast is stable from useToast hook

  // Load hierarchies when project changes
  useEffect(() => {
    if (currentProjectId) {
      loadHierarchies();
    }
  }, [currentProjectId, loadHierarchies]);

  // Load connections when deployment dialog opens
  useEffect(() => {
    const loadConnections = async () => {
      if (showDeploymentDialog) {
        try {
          setDeploymentLoading(true);
          const connections = await connectionService.getConnections();
          setAvailableConnections(connections);
        } catch (error) {
          console.error("Failed to load connections:", error);
          toast({
            title: "Error loading connections",
            description: "Failed to fetch database connections",
            variant: "destructive",
          });
        } finally {
          setDeploymentLoading(false);
        }
      }
    };
    loadConnections();
  }, [showDeploymentDialog]); // toast is stable

  // Load templates when template gallery dialog opens
  const loadTemplates = useCallback(async () => {
    if (loadingTemplates) return;
    setLoadingTemplates(true);
    try {
      const templates = await templatesService.listTemplates();
      // Transform backend templates to dialog format
      const formattedTemplates: HierarchyTemplate[] = templates.map((t: TemplateMetadata) => ({
        id: t.id,
        name: t.name,
        description: t.description,
        category: (t.domain === "accounting" || t.domain === "finance" || t.domain === "operations")
          ? t.domain as "accounting" | "finance" | "operations"
          : "accounting",
        industry: t.industry,
        structure: null, // Will be loaded on preview
        nodeCount: t.hierarchy_count || 0,
        maxDepth: 5,
        tags: [t.hierarchy_type, t.industry].filter(Boolean),
        isRecommended: false,
      }));
      setAvailableTemplates(formattedTemplates);
    } catch (error: any) {
      console.error("Failed to load templates:", error);
      toast({
        title: "Error loading templates",
        description: error.message || "Failed to fetch templates",
        variant: "destructive",
      });
    } finally {
      setLoadingTemplates(false);
    }
  }, [loadingTemplates, toast]);

  useEffect(() => {
    if (showTemplateGalleryDialog && availableTemplates.length === 0) {
      loadTemplates();
    }
  }, [showTemplateGalleryDialog, availableTemplates.length, loadTemplates]);

  // Load databases when connection changes
  useEffect(() => {
    const loadDatabases = async () => {
      // Only load if connection actually changed
      if (
        deploymentConfig.connectionId &&
        deploymentConfig.connectionId !== prevConnectionIdRef.current
      ) {
        prevConnectionIdRef.current = deploymentConfig.connectionId;
        prevDatabaseRef.current = ""; // Reset database ref

        try {
          setLoadingDatabases(true);
          const databases = await connectionService.getDatabases(
            deploymentConfig.connectionId
          );
          console.log("databases?.data", databases);
          setAvailableDatabases(databases);
          setAvailableSchemas([]);
        } catch (error) {
          console.error("Failed to load databases:", error);
          toast({
            title: "Error loading databases",
            description: "Failed to fetch databases for selected connection",
            variant: "destructive",
          });
          setAvailableDatabases([]);
        } finally {
          setLoadingDatabases(false);
        }
      } else if (!deploymentConfig.connectionId) {
        prevConnectionIdRef.current = "";
        setAvailableDatabases([]);
        setAvailableSchemas([]);
      }
    };
    loadDatabases();
  }, [deploymentConfig.connectionId]); // toast is stable

  // Load schemas when database changes
  useEffect(() => {
    const loadSchemas = async () => {
      // Only load if database actually changed
      if (
        deploymentConfig.connectionId &&
        deploymentConfig.database &&
        deploymentConfig.database !== prevDatabaseRef.current
      ) {
        prevDatabaseRef.current = deploymentConfig.database;

        try {
          setLoadingSchemas(true);
          const schemas = await connectionService.getSchemas(
            deploymentConfig.connectionId,
            deploymentConfig.database
          );
          setAvailableSchemas(schemas || []);
        } catch (error) {
          console.error("Failed to load schemas:", error);
          toast({
            title: "Error loading schemas",
            description: "Failed to fetch schemas for selected database",
            variant: "destructive",
          });
          setAvailableSchemas([]);
        } finally {
          setLoadingSchemas(false);
        }
      } else if (!deploymentConfig.database) {
        prevDatabaseRef.current = "";
        setAvailableSchemas([]);
      }
    };
    loadSchemas();
  }, [deploymentConfig.connectionId, deploymentConfig.database]); // toast is stable

  // Apply search and filters
  useEffect(() => {
    let filtered = [...allHierarchies];

    // Search filter
    if (searchQuery) {
      filtered = filtered.filter(
        (h) =>
          h.hierarchyName.toLowerCase().includes(searchQuery.toLowerCase()) ||
          h.description?.toLowerCase().includes(searchQuery.toLowerCase())
      );
    }

    // Type filter (root/child)
    if (filterType !== "all") {
      filtered = filtered.filter((h) =>
        filterType === "root" ? h.isRoot : !h.isRoot
      );
    }

    // Active filter
    if (filterActive !== "all") {
      filtered = filtered.filter((h) =>
        filterActive === "active" ? h.flags?.active_flag : !h.flags?.active_flag
      );
    }

    setFilteredHierarchies(filtered);
  }, [searchQuery, filterType, filterActive, allHierarchies]);

  // Load selected hierarchy details (fetch full data on demand)
  const loadHierarchyDetails = useCallback(
    async (hierarchyId: string) => {
      if (!currentProjectId) return;

      try {
        setLoadingDetails(true);
        const hierarchy = await smartHierarchyService.findOne(
          currentProjectId,
          hierarchyId
        );
        setSelectedHierarchy(hierarchy);
        setEditMode("view");
      } catch (error: any) {
        console.error("Failed to load hierarchy details:", error);
        toast({
          title: "Error loading hierarchy",
          description: error.response?.data?.message || error.message,
          variant: "destructive",
        });
      } finally {
        setLoadingDetails(false);
      }
    },
    [currentProjectId, toast]
  );

  // Handle hierarchy selection
  const handleSelectHierarchy = useCallback(
    (hierarchyId: string) => {
      setSelectedHierarchyId(hierarchyId);
      loadHierarchyDetails(hierarchyId);
    },
    [loadHierarchyDetails]
  );

  // Handle create new hierarchy
  const handleCreateHierarchy = useCallback(async () => {
    if (!newHierarchyForm.name.trim() || !currentProjectId) {
      toast({
        title: "Validation Error",
        description: "Hierarchy name and project are required",
        variant: "destructive",
      });
      return;
    }

    try {
      // Calculate sortOrder based on siblings
      const siblings = allHierarchies.filter(
        (h) =>
          (h.parentId || null) === (newHierarchyForm.parentId || null) &&
          h.isRoot === newHierarchyForm.isRoot
      );
      const maxSortOrder = Math.max(
        0,
        ...siblings.map((h) => h.sortOrder || 0)
      );

      // Generate temporary ID from hierarchy name (will be replaced by backend)
      const slug = newHierarchyForm.name
        .toUpperCase()
        .replace(/[^A-Z0-9]+/g, "_")
        .replace(/^_+|_+$/g, "")
        .substring(0, 50);
      const tempId = `HIER_${slug}_${Date.now()}`;

      const emptyHierarchy = smartHierarchyService.createEmptyHierarchy(
        currentProjectId,
        tempId
      );
      emptyHierarchy.hierarchyName = newHierarchyForm.name;
      emptyHierarchy.description = newHierarchyForm.description;
      emptyHierarchy.parentId = newHierarchyForm.parentId;
      emptyHierarchy.isRoot = newHierarchyForm.isRoot;
      emptyHierarchy.sortOrder = maxSortOrder + 1;

      setSelectedHierarchy(emptyHierarchy as SmartHierarchyMaster);
      setSelectedHierarchyId(tempId);
      setEditMode("create");
      setShowCreateHierarchyDialog(false);
      setNewHierarchyForm({
        name: "",
        description: "",
        parentId: null,
        isRoot: true,
      });
    } catch (error: any) {
      console.error("Failed to create hierarchy form:", error);
      toast({
        title: "Error",
        description: error.message || "Failed to create hierarchy",
        variant: "destructive",
      });
    }
  }, [currentProjectId, newHierarchyForm, allHierarchies, toast]);

  // Handle save hierarchy
  const handleSave = useCallback(
    async (hierarchy: SmartHierarchyMaster) => {
      if (!currentProjectId) return;

      try {
        // Get current user email from localStorage
        const userStr = localStorage.getItem("user");
        const userEmail = userStr
          ? JSON.parse(userStr).email
          : "unknown@user.com";

        // Remove read-only fields that backend doesn't accept
        const { id, createdAt, updatedAt, children, ...hierarchyData } =
          hierarchy;

        if (editMode === "create") {
          await smartHierarchyService.create({
            ...hierarchyData,
            updatedBy: userEmail,
          });
          toast({
            title: "Hierarchy created",
            description: `${hierarchy.hierarchyName} has been created successfully`,
          });
        } else {
          // For update, also remove projectId, hierarchyId, createdBy
          const { projectId, hierarchyId, createdBy, ...updateData } =
            hierarchyData;

          await smartHierarchyService.update(
            currentProjectId,
            hierarchy.hierarchyId,
            {
              ...updateData,
              updatedBy: userEmail,
            }
          );
          toast({
            title: "Hierarchy updated",
            description: `${hierarchy.hierarchyName} has been updated successfully`,
          });
        }

        await loadHierarchies();
        setEditMode("view");
      } catch (error: any) {
        console.error("Failed to save hierarchy:", error);
        toast({
          title: "Error saving hierarchy",
          description: error.response?.data?.message || error.message,
          variant: "destructive",
        });
      }
    },
    [editMode, currentProjectId, loadHierarchies, toast]
  );

  // Handle delete hierarchy
  const handleDelete = useCallback(
    async (hierarchyId: string) => {
      if (!currentProjectId) {
        return;
      }

      try {
        await smartHierarchyService.delete(currentProjectId, hierarchyId);
        toast({
          title: "Hierarchy deleted",
          description: "The hierarchy has been deleted successfully",
        });

        setSelectedHierarchy(null);
        setSelectedHierarchyId(null);
        await loadHierarchies();
      } catch (error: any) {
        console.error("Failed to delete hierarchy:", error);
        toast({
          title: "Error deleting hierarchy",
          description: error.response?.data?.message || error.message,
          variant: "destructive",
        });
      }
    },
    [currentProjectId, loadHierarchies, toast]
  );

  // Handle multi-delete using existing checkbox selection
  const handleBulkDelete = useCallback(async () => {
    if (!currentProjectId || selectedForFormula.size === 0) {
      return;
    }

    try {
      const hierarchyIds = Array.from(selectedForFormula);
      const result = await smartHierarchyService.bulkDelete(
        currentProjectId,
        hierarchyIds
      );

      // Show single summary toast
      if (result.failed === 0) {
        toast({
          title: "Success",
          description: `Successfully deleted ${result.deleted} ${
            result.deleted === 1 ? "hierarchy" : "hierarchies"
          }`,
        });
      } else if (result.deleted === 0) {
        // Show first few error messages
        const errorSummary = result.errors
          .slice(0, 3)
          .map((e) => `• ${e.hierarchyId}`)
          .join("\n");
        const moreErrors =
          result.errors.length > 3
            ? `\n...and ${result.errors.length - 3} more`
            : "";

        toast({
          title: "Delete Failed",
          description: `Failed to delete ${result.failed} ${
            result.failed === 1 ? "hierarchy" : "hierarchies"
          }:\n${errorSummary}${moreErrors}`,
          variant: "destructive",
        });
      } else {
        // Show summary for partial success
        const errorSummary = result.errors
          .slice(0, 2)
          .map((e) => `• ${e.hierarchyId}`)
          .join("\n");
        const moreErrors =
          result.errors.length > 2
            ? `\n...and ${result.errors.length - 2} more`
            : "";

        toast({
          title: "Partially Completed",
          description: `Deleted ${result.deleted}, failed ${result.failed}:\n${errorSummary}${moreErrors}`,
          variant: "default",
        });
      }

      // Log full errors to console for debugging
      if (result.errors.length > 0) {
        console.error("Delete errors summary:", {
          total: result.errors.length,
          errors: result.errors,
        });
      }

      // Clear selection and reload
      setSelectedForFormula(new Set());
      setSelectedHierarchy(null);
      setSelectedHierarchyId(null);
      await loadHierarchies();
    } catch (error: any) {
      console.error("Failed to bulk delete:", error);
      toast({
        title: "Error deleting hierarchies",
        description: error.response?.data?.message || error.message,
        variant: "destructive",
      });
    }
  }, [currentProjectId, selectedForFormula, loadHierarchies, toast]);

  // Select all hierarchies
  const handleSelectAllHierarchies = useCallback(() => {
    const allIds = filteredHierarchies.map((h) => h.hierarchyId);
    setSelectedForFormula(new Set(allIds));
  }, [filteredHierarchies]);

  // Clear selection
  const handleClearSelection = useCallback(() => {
    setSelectedForFormula(new Set());
  }, []);

  // Handle clone hierarchy
  const handleClone = useCallback(
    async (sourceHierarchy: SmartHierarchyMaster, fullClone: boolean) => {
      if (!currentProjectId) return;

      const newHierarchyName = `${sourceHierarchy.hierarchyName} (Copy)`;

      // Generate temporary ID from cloned name (will be replaced by backend)
      const slug = newHierarchyName
        .toUpperCase()
        .replace(/[^A-Z0-9]+/g, "_")
        .replace(/^_+|_+$/g, "")
        .substring(0, 50);
      const tempId = `HIER_${slug}_${Date.now()}`;

      // Calculate sortOrder based on siblings at same level
      const siblings = allHierarchies.filter(
        (h) => (h.parentId || null) === (sourceHierarchy.parentId || null)
      );
      const maxSortOrder = Math.max(
        0,
        ...siblings.map((h) => h.sortOrder || 0)
      );

      // Remove read-only fields that should not be sent to backend
      const { id, createdAt, updatedAt, children, ...sourceData } =
        sourceHierarchy;

      const clonedHierarchy: any = {
        ...sourceData,
        projectId: currentProjectId,
        hierarchyId: tempId,
        hierarchyName: newHierarchyName,
        description: sourceHierarchy.description,
        parentId: sourceHierarchy.parentId, // Keep same parent as source
        isRoot: sourceHierarchy.isRoot,
        sortOrder: maxSortOrder + 1,
        hierarchyLevel: sourceHierarchy.hierarchyLevel,
        flags: sourceHierarchy.flags,
        mapping: fullClone ? sourceHierarchy.mapping : [],
        formulaConfig: fullClone ? sourceHierarchy.formulaConfig : undefined,
        filterConfig: fullClone ? sourceHierarchy.filterConfig : undefined,
        pivotConfig: fullClone ? sourceHierarchy.pivotConfig : undefined,
        metadata: {
          ...sourceHierarchy.metadata,
          clonedFrom: sourceHierarchy.hierarchyId,
          cloneType: fullClone ? "full" : "structure",
        },
      };

      setSelectedHierarchy(clonedHierarchy);
      setSelectedHierarchyId(tempId);
      setEditMode("create");
      toast({
        title: "Clone created",
        description: fullClone
          ? "Full hierarchy clone created. Review and save."
          : "Structure clone created. Add mappings and save.",
      });
    },
    [currentProjectId, allHierarchies, toast]
  );

  // Handle add child hierarchy
  const handleAddChild = useCallback(
    async (parentHierarchy: SmartHierarchyMaster) => {
      if (!currentProjectId) return;

      const newHierarchyName = `${parentHierarchy.hierarchyName} - Child`;

      // Generate temporary ID from child name (will be replaced by backend)
      const slug = newHierarchyName
        .toUpperCase()
        .replace(/[^A-Z0-9]+/g, "_")
        .replace(/^_+|_+$/g, "")
        .substring(0, 50);
      const tempId = `HIER_${slug}_${Date.now()}`;

      // Calculate sortOrder based on existing children of this parent
      const existingChildren = allHierarchies.filter(
        (h) => h.parentId === parentHierarchy.hierarchyId
      );
      const maxSortOrder = Math.max(
        0,
        ...existingChildren.map((h) => h.sortOrder || 0)
      );

      // Create child hierarchy without read-only fields
      const childHierarchy: any = {
        projectId: currentProjectId,
        hierarchyId: tempId,
        hierarchyName: newHierarchyName,
        description: `Child of ${parentHierarchy.hierarchyName}`,
        parentId: parentHierarchy.hierarchyId, // Set parent ID to create relationship
        isRoot: false,
        sortOrder: maxSortOrder + 1,
        hierarchyLevel: parentHierarchy.hierarchyLevel, // Inherit levels structure
        flags: {
          include_flag: true,
          exclude_flag: false,
          transform_flag: false,
          active_flag: true,
          is_leaf_node: false,
          customFlags: {},
        },
        mapping: [],
        metadata: {
          parentHierarchyName: parentHierarchy.hierarchyName,
          parentHierarchyId: parentHierarchy.hierarchyId,
        },
      };

      setSelectedHierarchy(childHierarchy);
      setSelectedHierarchyId(tempId);
      setEditMode("create");
      toast({
        title: "Child hierarchy created",
        description: `New child of ${parentHierarchy.hierarchyName}. Edit and save.`,
      });
    },
    [currentProjectId, allHierarchies, toast]
  );

  // Handle formula selection toggle
  const handleToggleFormulaSelection = useCallback((hierarchyId: string) => {
    setSelectedForFormula((prev) => {
      const newSet = new Set(prev);
      if (newSet.has(hierarchyId)) {
        newSet.delete(hierarchyId);
      } else {
        newSet.add(hierarchyId);
      }
      return newSet;
    });
  }, []);

  // Handle hierarchy reorder (drag and drop)
  const handleReorder = useCallback(
    async (reorderedHierarchies: SmartHierarchyMaster[]) => {
      if (!currentProjectId) return;

      try {
        // Build updates array with only changed hierarchies
        const updates = reorderedHierarchies.map((h) => ({
          hierarchyId: h.hierarchyId,
          parentId: h.parentId || null,
          isRoot: h.isRoot || false,
          sortOrder: h.sortOrder || 0,
        }));

        // Optimistically update the local state first (prevents collapse)
        setAllHierarchies(reorderedHierarchies);

        // Use bulk update endpoint for efficiency
        await smartHierarchyService.bulkUpdateOrder(currentProjectId, updates);

        toast({
          title: "Order updated",
          description: "Hierarchy order has been updated successfully",
        });
      } catch (error: any) {
        console.error("Failed to update order:", error);
        // On error, reload to get the correct state
        await loadHierarchies();
        toast({
          title: "Error updating order",
          description: error.response?.data?.message || error.message,
          variant: "destructive",
        });
      }
    },
    [currentProjectId, loadHierarchies, toast]
  );

  // Get selected hierarchies for formula
  const selectedHierarchiesForFormula = useMemo(
    () => allHierarchies.filter((h) => selectedForFormula.has(h.hierarchyId)),
    [allHierarchies, selectedForFormula]
  );

  // Handle generate scripts
  // Handle database type change - regenerate scripts
  const handleDatabaseTypeChange = useCallback(
    async (
      newDatabaseType: "snowflake" | "postgres" | "mysql" | "sqlserver" | "all"
    ) => {
      setSelectedDatabaseType(newDatabaseType);

      // If the script dialog is open, regenerate scripts with new database type
      if (showScriptDialog) {
        const hierarchyIds =
          selectedHierarchiesForScript.size > 0
            ? Array.from(selectedHierarchiesForScript)
            : selectedHierarchyId
            ? [selectedHierarchyId]
            : [];

        if (hierarchyIds.length === 0) return;

        try {
          const result = await smartHierarchyService.generateScripts(
            currentProjectId!,
            "all",
            hierarchyIds,
            newDatabaseType
          );

          // Organize scripts by type - combine multiple hierarchies
          const scriptsObj: any = {
            insert: "",
            view: "",
            mapping: "",
            dynamicTable: "",
          };
          const hierarchyNames: string[] = [];

          result.scripts.forEach((s) => {
            if (!hierarchyNames.includes(s.hierarchyName)) {
              hierarchyNames.push(s.hierarchyName);
            }

            if (s.scriptType === "insert") {
              scriptsObj.insert += (scriptsObj.insert ? "\n\n" : "") + s.script;
            } else if (s.scriptType === "view") {
              scriptsObj.view += (scriptsObj.view ? "\n\n" : "") + s.script;
            } else if (s.scriptType === "mapping") {
              scriptsObj.mapping +=
                (scriptsObj.mapping ? "\n\n" : "") + s.script;
            } else if (s.scriptType === "dt") {
              scriptsObj.dynamicTable +=
                (scriptsObj.dynamicTable ? "\n\n" : "") + s.script;
            }
          });

          // Store hierarchy names for display
          (scriptsObj as any).hierarchyNames = hierarchyNames;

          setGeneratedScripts(scriptsObj);
        } catch (error: any) {
          console.error("Failed to regenerate scripts:", error);
          toast({
            title: "Error regenerating scripts",
            description: error.response?.data?.message || error.message,
            variant: "destructive",
          });
        }
      }
    },
    [
      showScriptDialog,
      selectedHierarchiesForScript,
      selectedHierarchyId,
      currentProjectId,
      toast,
    ]
  );

  const handleGenerateScripts = useCallback(async () => {
    if (!currentProjectId) {
      toast({
        title: "No project selected",
        description: "Please select a project first",
        variant: "destructive",
      });
      return;
    }

    // Determine which hierarchies to use
    const hierarchyIds = deployAll
      ? [] // Empty array signals backend to use all hierarchies
      : selectedHierarchiesForScript.size > 0
      ? Array.from(selectedHierarchiesForScript)
      : selectedHierarchyId
      ? [selectedHierarchyId]
      : [];

    if (!deployAll && hierarchyIds.length === 0) {
      toast({
        title: "No hierarchy selected",
        description:
          "Please select at least one hierarchy to generate scripts or check 'Deploy All'",
        variant: "destructive",
      });
      return;
    }

    try {
      const result = await smartHierarchyService.generateScripts(
        currentProjectId,
        "all",
        hierarchyIds,
        selectedDatabaseType
      );

      // Organize scripts by type - combine multiple hierarchies
      const scriptsObj: any = {
        insert: "",
        view: "",
        mapping: "",
        dynamicTable: "",
      };
      const hierarchyNames: string[] = [];

      result.scripts.forEach((s) => {
        if (!hierarchyNames.includes(s.hierarchyName)) {
          hierarchyNames.push(s.hierarchyName);
        }

        if (s.scriptType === "insert") {
          scriptsObj.insert += (scriptsObj.insert ? "\n\n" : "") + s.script;
        } else if (s.scriptType === "view") {
          scriptsObj.view += (scriptsObj.view ? "\n\n" : "") + s.script;
        } else if (s.scriptType === "mapping") {
          scriptsObj.mapping += (scriptsObj.mapping ? "\n\n" : "") + s.script;
        } else if (s.scriptType === "dt") {
          scriptsObj.dynamicTable +=
            (scriptsObj.dynamicTable ? "\n\n" : "") + s.script;
        }
      });

      // Store hierarchy names for display
      (scriptsObj as any).hierarchyNames = hierarchyNames;

      setGeneratedScripts(scriptsObj);
      setActiveScriptTab(
        scriptsObj.insert
          ? "insert"
          : scriptsObj.view
          ? "view"
          : scriptsObj.mapping
          ? "mapping"
          : "dynamicTable"
      );
      setShowScriptDialog(true);
    } catch (error: any) {
      console.error("Failed to generate scripts:", error);
      toast({
        title: "Error generating scripts",
        description: error.response?.data?.message || error.message,
        variant: "destructive",
      });
    }
  }, [
    currentProjectId,
    selectedHierarchyId,
    selectedHierarchiesForScript,
    selectedDatabaseType,
    toast,
  ]);

  // Clear filters
  const handleClearFilters = () => {
    setSearchQuery("");
    setFilterType("all");
    setFilterActive("all");
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <Loader2 className="w-8 h-8 animate-spin text-primary" />
      </div>
    );
  }

  // No projects exist - show empty state
  if (!loading && projects.length === 0) {
    return (
      <>
        <div className="flex items-center justify-center h-full">
          <div className="text-center max-w-md px-6">
            <div className="mb-6">
              <div className="mx-auto w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center mb-4">
                <FolderKanban className="w-8 h-8 text-primary" />
              </div>
              <h2 className="text-2xl font-semibold mb-2">No Projects Yet</h2>
              <p className="text-muted-foreground mb-6">
                Get started by creating your first project. Projects help you
                organize and manage your hierarchies effectively.
              </p>
            </div>
            <Button
              size="lg"
              onClick={() => {
                setProjectDialogInitialTab("projects");
                setShowProjectManagementDialog(true);
              }}
              className="gap-2"
            >
              <Plus className="w-5 h-5" />
              Create Your First Project
            </Button>
          </div>
        </div>

        {/* Project Management Dialog */}
        <ProjectManagementDialog
          open={showProjectManagementDialog}
          onOpenChange={setShowProjectManagementDialog}
          onOpenProjectDetails={(projectId, tab) => {
            setSelectedProjectIdForDetails(projectId);
            setProjectDetailsInitialTab(tab || "details");
            setShowProjectDetailsDialog(true);
          }}
        />

        {/* Project Details Dialog */}
        <ProjectDetailsDialog
          open={showProjectDetailsDialog}
          onOpenChange={setShowProjectDetailsDialog}
          projectId={selectedProjectIdForDetails}
          initialTab={projectDetailsInitialTab}
          onImportSuccess={loadHierarchies}
        />
      </>
    );
  }

  // No current project selected - show selection prompt
  if (!currentProjectId) {
    return (
      <>
        <div className="flex items-center justify-center h-full">
          <div className="text-center max-w-md px-6">
            <div className="mb-6">
              <div className="mx-auto w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center mb-4">
                <FolderKanban className="w-8 h-8 text-primary" />
              </div>
              <h2 className="text-2xl font-semibold mb-2">Select a Project</h2>
              <p className="text-muted-foreground mb-6">
                You have {projects.length} project
                {projects.length !== 1 ? "s" : ""} available. Please select one
                to view and manage its hierarchies.
              </p>
            </div>
            <div className="flex flex-col sm:flex-row gap-3">
              <Button
                size="lg"
                onClick={() => {
                  setProjectDialogInitialTab("projects");
                  setShowProjectManagementDialog(true);
                }}
                className="gap-2"
              >
                <FolderKanban className="w-5 h-5" />
                Manage Projects
              </Button>
              <Button
                size="lg"
                variant="outline"
                onClick={() => setShowTemplateGalleryDialog(true)}
                className="gap-2"
              >
                <LayoutTemplate className="w-5 h-5" />
                Use Template
              </Button>
            </div>
          </div>
        </div>

        {/* Project Management Dialog */}
        <ProjectManagementDialog
          open={showProjectManagementDialog}
          onOpenChange={setShowProjectManagementDialog}
          onOpenProjectDetails={(projectId, tab) => {
            setSelectedProjectIdForDetails(projectId);
            setProjectDetailsInitialTab(tab || "details");
            setShowProjectDetailsDialog(true);
          }}
        />

        {/* Project Details Dialog */}
        <ProjectDetailsDialog
          open={showProjectDetailsDialog}
          onOpenChange={setShowProjectDetailsDialog}
          projectId={selectedProjectIdForDetails}
          initialTab={projectDetailsInitialTab}
        />

        {/* Template Gallery Dialog */}
        <TemplateGalleryDialog
          open={showTemplateGalleryDialog}
          onOpenChange={setShowTemplateGalleryDialog}
          templates={availableTemplates}
          onSelectTemplate={async (template) => {
            try {
              const result = await templatesService.createProjectFromTemplate(
                template.id,
                `${template.name} Project`,
                template.description
              );
              await loadProjects();
              if (result?.project?.id) {
                setCurrentProjectId(result.project.id);
              }
              setShowTemplateGalleryDialog(false);
              toast({
                title: "Project Created",
                description: `Successfully created project from "${template.name}" template`,
              });
            } catch (error: any) {
              console.error("Failed to create project from template:", error);
              toast({
                title: "Error",
                description: error.message || "Failed to create project from template",
                variant: "destructive",
              });
            }
          }}
        />
      </>
    );
  }

  return (
    <div className="flex h-full flex-col overflow-hidden">
      {/* Unified Toolbar */}
      <div className="border-b bg-background/95 backdrop-blur">
        <div className="flex items-center justify-between px-0 py-3">
          <div className="flex items-center gap-4">
            <h2 className="text-lg font-semibold ml-5 flex items-center gap-2">
              Hierarchy KnowledgeBase
              <HelpTooltip topicId="hierarchyTree" iconOnly iconSize="md" />
            </h2>
            {/* <span className="text-sm text-muted-foreground">
              {filteredHierarchies.length} hierarchies
            </span> */}
          </div>

          <div className="flex items-center gap-1.5">
            {/* Project Selector Dropdown */}
            <Select
              value={currentProjectId || undefined}
              onValueChange={setCurrentProjectId}
            >
              <SelectTrigger className="w-[200px]">
                <SelectValue placeholder="Select a project" />
              </SelectTrigger>
              <SelectContent>
                {projects.map((project) => (
                  <SelectItem key={project.id} value={project.id}>
                    <div className="flex items-center gap-2">
                      <FolderKanban className="w-4 h-4" />
                      {project.name}
                    </div>
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>

            {/* Project Management Button */}
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                setProjectDialogInitialTab("projects");
                setShowProjectManagementDialog(true);
              }}
              className="py-4.5"
            >
              <Settings className="w-4 h-4 " />
              Projects
              <HelpTooltip topicId="projectManagement" iconOnly iconSize="sm" />
            </Button>

            {/* Import Button */}
            <Button
              variant="outline"
              size="sm"
              disabled={!currentProjectId}
              className="py-4.5"
              onClick={() => {
                if (currentProjectId) {
                  setSelectedProjectIdForDetails(currentProjectId);
                  setProjectDetailsInitialTab("import-export");
                  setShowProjectDetailsDialog(true);
                }
              }}
            >
              <FileUp className="w-4 h-4 " />
              Import/Export
              <HelpTooltip topicId="csvImport" iconOnly iconSize="sm" />
            </Button>

            {/* Total Formula Button */}
            <Button
              variant="outline"
              size="sm"
              className="py-4.5"
              onClick={() => setShowManageFormulasDialog(true)}
            >
              <Calculator className="w-4 h-4 " />
              Manage Formulas
              <HelpTooltip topicId="formulaBuilder" iconOnly iconSize="sm" />
            </Button>

            {/* Templates Button */}
            <Button
              variant="outline"
              size="sm"
              className="py-4.5"
              onClick={() => setShowTemplateGalleryDialog(true)}
            >
              <LayoutTemplate className="w-4 h-4" />
              Templates
            </Button>

            {/* AI Features Button */}
            <Button
              variant="outline"
              size="sm"
              className="py-4.5 bg-gradient-to-r from-purple-500/10 to-blue-500/10 hover:from-purple-500/20 hover:to-blue-500/20 border-purple-300/50"
              onClick={() => {
                toast({
                  title: "AI Features",
                  description: "AI-powered mapping suggestions are available when editing hierarchy mappings. Select a hierarchy and click Edit to access AI suggestions.",
                });
              }}
            >
              <Sparkles className="w-4 h-4 text-purple-500" />
              AI Features
            </Button>

            {/* <Separator orientation="vertical" className="h-6" /> */}

            {/* Global Database Selection */}
            <div className="flex items-center gap-2">
              <Select
                value={globalSelectedDatabase}
                onValueChange={setGlobalSelectedDatabase}
              >
                <SelectTrigger className="w-[150px] h-8">
                  <SelectValue placeholder="Select database" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="snowflake">Snowflake</SelectItem>
                  <SelectItem value="postgres">PostgreSQL</SelectItem>
                  <SelectItem value="mysql">MySQL</SelectItem>
                  <SelectItem value="sqlserver">SQL Server</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <Separator orientation="vertical" className="h-6" />

            {/* Generate Script */}
            <Button
              variant="outline"
              size="sm"
              onClick={handleGenerateScripts}
              disabled={
                !deployAll &&
                !selectedHierarchyId &&
                selectedHierarchiesForScript.size === 0
              }
              className="py-4.5"
            >
              <Code className="w-4 h-4 " />
              Generate Script
            </Button>

            {/* <Separator orientation="vertical" className="h-6" /> */}

            {/* View Toggle */}
            <div className="flex items-center gap-1 bg-muted/50 rounded-md p-1">
              <Button
                variant={activeView === "tree" ? "secondary" : "ghost"}
                size="sm"
                className="h-7 px-2"
                onClick={() => setActiveView("tree")}
                title="Tree View"
              >
                <List className="w-4 h-4" />
              </Button>
              <Button
                variant={activeView === "graph" ? "secondary" : "ghost"}
                size="sm"
                className="h-7 px-2"
                onClick={() => setActiveView("graph")}
                title="Graph View"
              >
                <Network className="w-4 h-4" />
              </Button>
            </div>

            <Separator orientation="vertical" className="h-6" />

            {/* Refresh */}
            <Button
              variant="outline"
              size="sm"
              onClick={loadHierarchies}
              className="mr-4"
            >
              <RefreshCw className="w-4 h-4 " />
            </Button>
          </div>
        </div>
      </div>

      {/* Main Content with Resizable Panels */}
      <ResizablePanelGroup
        direction="horizontal"
        className="flex-1 overflow-hidden"
      >
        {/* Left Panel - Hierarchy Tree */}
        <ResizablePanel defaultSize={25} minSize={15} maxSize={40}>
          <div className="h-full flex flex-col overflow-hidden bg-muted/30">
            {/* Search and Filters */}
            <div className="p-4 space-y-3 border-b">
              {/* Filters */}
              <div className="flex items-center gap-2">
                <div className="relative w-full">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                  <Input
                    placeholder="Search hierarchies..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="pl-9 "
                  />
                </div>

                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <Button
                      variant="outline"
                      size="sm"
                      className="m-0 p-0 h-8 justify-center"
                    >
                      <Filter className="w-4 h-4" />
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent className="w-48">
                    <div className="p-2 space-y-2">
                      <div className="space-y-1">
                        <Label className="text-xs">Type</Label>
                        <Select
                          value={filterType}
                          onValueChange={(v: any) => setFilterType(v)}
                        >
                          <SelectTrigger className="h-8 w-full">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">All</SelectItem>
                            <SelectItem value="root">Root Only</SelectItem>
                            <SelectItem value="child">Child Only</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                      <div className="space-y-1">
                        <Label className="text-xs">Status</Label>
                        <Select
                          value={filterActive}
                          onValueChange={(v: any) => setFilterActive(v)}
                        >
                          <SelectTrigger className="h-8 w-full">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">All</SelectItem>
                            <SelectItem value="active">Active</SelectItem>
                            <SelectItem value="inactive">Inactive</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    </div>
                    <DropdownMenuSeparator />
                    <DropdownMenuItem onClick={handleClearFilters}>
                      Clear Filters
                    </DropdownMenuItem>
                  </DropdownMenuContent>
                </DropdownMenu>

                {/* Add Hierarchy Button */}
                <Button
                  size="sm"
                  onClick={() => setShowCreateHierarchyDialog(true)}
                >
                  <Plus className="w-4 h-4" />
                </Button>
              </div>
            </div>

            {/* Multi-delete Toolbar */}
            {selectedForFormula.size > 0 && (
              <div className="px-4 py-2 bg-accent/50 border-b">
                <div className="flex items-center justify-between gap-2">
                  <div className="flex items-center gap-2">
                    <Badge variant="secondary" className="text-xs">
                      {selectedForFormula.size} selected
                    </Badge>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => {
                        setSelectedHierarchiesForScript(new Set());
                        setSelectedForFormula(new Set());
                      }}
                      className="h-6 text-xs"
                    >
                      Clear
                    </Button>
                  </div>
                  <div className="flex items-center gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        const allIds = new Set(
                          filteredHierarchies.map((h) => h.hierarchyId)
                        );
                        setSelectedHierarchiesForScript(allIds);
                        setSelectedForFormula(allIds);
                      }}
                      className="h-7 text-xs"
                    >
                      Select All
                    </Button>
                    <Button
                      variant="destructive"
                      size="sm"
                      onClick={handleBulkDelete}
                      className="h-7"
                    >
                      <Trash2 className="w-3 h-3" />
                      {/* Delete ({selectedForFormula.size}) */}
                    </Button>
                  </div>
                </div>
              </div>
            )}

            {/* Hierarchy Tree List */}
            <div className="flex-1 overflow-auto p-2">
              {loading ? (
                <div className="flex items-center justify-center py-8">
                  <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
                </div>
              ) : (
                <EnhancedHierarchyTree
                  hierarchies={filteredHierarchies}
                  selectedId={selectedHierarchyId}
                  selectedForFormula={selectedForFormula}
                  selectedForScript={selectedHierarchiesForScript}
                  onSelect={handleSelectHierarchy}
                  onToggleFormulaSelection={handleToggleFormulaSelection}
                  onToggleScriptSelection={(id) => {
                    setSelectedHierarchiesForScript((prev) => {
                      const newSet = new Set(prev);
                      if (newSet.has(id)) {
                        newSet.delete(id);
                      } else {
                        newSet.add(id);
                      }
                      return newSet;
                    });
                  }}
                  onReorder={handleReorder}
                  expandedNodes={expandedNodes}
                  expandedDetails={expandedDetails}
                  onExpandedNodesChange={setExpandedNodes}
                  onExpandedDetailsChange={setExpandedDetails}
                />
              )}
            </div>
          </div>
        </ResizablePanel>

        {/* Resizable Handle */}
        <ResizableHandle withHandle />

        {/* Right Panel - Content Area */}
        <ResizablePanel defaultSize={75} minSize={50}>
          <div className="h-full overflow-hidden">
            {activeView === "tree" ? (
              loadingDetails ? (
                <div className="flex items-center justify-center h-full">
                  <div className="text-center">
                    <Loader2 className="w-8 h-8 animate-spin mx-auto mb-3 text-primary" />
                    <p className="text-sm text-muted-foreground">
                      Loading hierarchy details...
                    </p>
                  </div>
                </div>
              ) : selectedHierarchy ? (
                <SmartHierarchyEditor
                  hierarchy={selectedHierarchy}
                  mode={editMode}
                  onSave={handleSave}
                  onChange={setSelectedHierarchy}
                  onCancel={() => {
                    setEditMode("view");
                    if (editMode === "create") {
                      setSelectedHierarchy(null);
                      setSelectedHierarchyId(null);
                    }
                  }}
                  onEdit={() => setEditMode("edit")}
                  onDelete={handleDelete}
                  allHierarchies={allHierarchies}
                  selectedForFormula={selectedForFormula}
                  onToggleFormulaSelection={handleToggleFormulaSelection}
                  selectedHierarchiesForFormula={selectedHierarchiesForFormula}
                  onClone={handleClone}
                  onAddChild={handleAddChild}
                  databaseType={
                    globalSelectedDatabase as
                      | "snowflake"
                      | "postgres"
                      | "mysql"
                      | "sqlserver"
                  }
                />
              ) : (
                <div className="flex items-center justify-center h-full text-muted-foreground">
                  <div className="text-center">
                    <p className="text-lg mb-2">No hierarchy selected</p>
                    <p className="text-sm mb-4">
                      Select a hierarchy from the list or create a new one
                    </p>
                    <Button onClick={() => setShowCreateHierarchyDialog(true)}>
                      <Plus className="w-4 h-4 mr-2" />
                      Create New Hierarchy
                    </Button>
                  </div>
                </div>
              )
            ) : (
              <EnhancedGraphView
                hierarchies={allHierarchies}
                selectedId={selectedHierarchyId}
                selectedForFormula={selectedForFormula}
                onSelect={handleSelectHierarchy}
                onToggleFormulaSelection={handleToggleFormulaSelection}
              />
            )}
          </div>
        </ResizablePanel>
      </ResizablePanelGroup>

      {/* Dialogs */}
      <CreateHierarchyDialog
        open={showCreateHierarchyDialog}
        onOpenChange={setShowCreateHierarchyDialog}
        formData={newHierarchyForm}
        onFormChange={(data) =>
          setNewHierarchyForm({ ...newHierarchyForm, ...data })
        }
        allHierarchies={allHierarchies}
        onCreate={handleCreateHierarchy}
      />

      {/* Project Management Dialog */}
      <ProjectManagementDialog
        open={showProjectManagementDialog}
        onOpenChange={setShowProjectManagementDialog}
        onOpenProjectDetails={(projectId, tab) => {
          setSelectedProjectIdForDetails(projectId);
          setProjectDetailsInitialTab(tab || "details");
          setShowProjectDetailsDialog(true);
        }}
      />

      {/* Project Details Dialog */}
      <ProjectDetailsDialog
        open={showProjectDetailsDialog}
        onOpenChange={setShowProjectDetailsDialog}
        projectId={selectedProjectIdForDetails}
        initialTab={projectDetailsInitialTab}
        onImportSuccess={loadHierarchies}
      />

      <ScriptDialog
        open={showScriptDialog}
        onOpenChange={setShowScriptDialog}
        generatedScripts={generatedScripts}
        selectedHierarchiesCount={selectedHierarchiesForScript.size}
        selectedHierarchyId={selectedHierarchyId}
        onClearSelection={() => setSelectedHierarchiesForScript(new Set())}
        onOpenDeployment={() => {
          setShowDeploymentDialog(true);
          const project = projects.find((p) => p.id === currentProjectId);
          if (project) {
            const projectName = project.name
              .toUpperCase()
              .replace(/[^A-Z0-9_]/g, "_");
            setDeploymentConfig({
              ...deploymentConfig,
              masterTableName: `HIERARCHY_MASTER`,
              masterViewName: `VW_${projectName}`,
            });
          }
        }}
        toast={toast}
      />

      <DeploymentDialog
        open={showDeploymentDialog}
        onOpenChange={setShowDeploymentDialog}
        projectId={currentProjectId || ""}
        deploymentConfig={deploymentConfig}
        onConfigChange={(config) =>
          setDeploymentConfig({ ...deploymentConfig, ...config })
        }
        availableConnections={availableConnections}
        availableDatabases={availableDatabases}
        availableSchemas={availableSchemas}
        loadingDatabases={loadingDatabases}
        loadingSchemas={loadingSchemas}
        deploymentLoading={deploymentLoading}
        deployAll={deployAll}
        setDeployAll={setDeployAll}
        onDeploy={async () => {
          const project = currentProject();
          if (!project?.id || !deploymentConfig.connectionId) {
            toast({
              title: "Error",
              description: "Please select a connection",
              variant: "destructive",
            });
            return;
          }

          if (
            !deploymentConfig.database ||
            !deploymentConfig.schema ||
            !deploymentConfig.masterTableName ||
            !deploymentConfig.masterViewName
          ) {
            toast({
              title: "Error",
              description: "Please fill in all required fields",
              variant: "destructive",
            });
            return;
          }

          // Get selected hierarchy IDs (not all hierarchies!)
          const hierarchyIds = Array.from(selectedHierarchiesForScript);
          if (hierarchyIds.length === 0) {
            toast({
              title: "Error",
              description: "Please select at least one hierarchy",
              variant: "destructive",
            });
            return;
          }

          // Get user email from AuthContext
          const userEmail = authUser?.email || "unknown@user.com";

          setDeploymentLoading(true);
          try {
            const result = await smartHierarchyService.pushToSnowflake(
              project.id,
              hierarchyIds,
              deploymentConfig.connectionId,
              deploymentConfig.database,
              deploymentConfig.schema,
              deploymentConfig.masterTableName,
              deploymentConfig.masterViewName,
              deploymentConfig.databaseType,
              userEmail,
              {
                createTables: deploymentConfig.createTables,
                createViews: deploymentConfig.createViews,
                createDynamicTables: deploymentConfig.createDynamicTables,
                saveAsDeploymentConfig: true, // Always save config
              }
            );

            toast({
              title: "Deployment Successful",
              description: `Successfully deployed ${
                result.success || 0
              } script(s) to ${deploymentConfig.database}.${
                deploymentConfig.schema
              }. Failed: ${result.failed || 0}`,
            });

            setShowDeploymentDialog(false);
            setSelectedHierarchiesForScript(new Set()); // Clear selection
          } catch (error: any) {
            console.error("Deployment failed:", error);
            toast({
              title: "Deployment Failed",
              description: error.message || "Failed to deploy to database",
              variant: "destructive",
            });
          } finally {
            setDeploymentLoading(false);
          }
        }}
        toast={toast}
      />

      {/* Manage Formulas Dialog (Total Formula & Formula Group) */}
      {currentProjectId && (
        <ManageFormulasDialog
          open={showManageFormulasDialog}
          onOpenChange={setShowManageFormulasDialog}
          projectId={currentProjectId}
          selectedHierarchyIds={Array.from(selectedHierarchiesForScript)}
          allHierarchies={allHierarchies}
          onSuccess={() => {
            loadHierarchies();
          }}
        />
      )}

      {/* Template Gallery Dialog */}
      <TemplateGalleryDialog
        open={showTemplateGalleryDialog}
        onOpenChange={setShowTemplateGalleryDialog}
        templates={availableTemplates}
        onSelectTemplate={async (template) => {
          try {
            // Create project from template
            const result = await templatesService.createProjectFromTemplate(
              template.id,
              `${template.name} Project`,
              template.description
            );

            // Reload projects and select the new one
            await loadProjects();
            if (result?.project?.id) {
              setCurrentProjectId(result.project.id);
            }

            setShowTemplateGalleryDialog(false);
            toast({
              title: "Project Created",
              description: `Successfully created project from "${template.name}" template`,
            });
          } catch (error: any) {
            console.error("Failed to create project from template:", error);
            toast({
              title: "Error",
              description: error.message || "Failed to create project from template",
              variant: "destructive",
            });
          }
        }}
        currentProjectName={currentProject()?.name}
      />
    </div>
  );
};
