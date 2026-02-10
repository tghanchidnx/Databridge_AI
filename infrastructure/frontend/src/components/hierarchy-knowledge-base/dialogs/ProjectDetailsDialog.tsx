import React, { useState, useEffect } from "react";
import { useProjectStore } from "@/stores/projectStore";
import { smartHierarchyService } from "@/services/api/hierarchy";
import { useToast } from "@/hooks/use-toast";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Separator } from "@/components/ui/separator";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Trash2,
  Users,
  UserPlus,
  Shield,
  FileUp,
  FileDown,
  Settings,
  Mail,
  FolderKanban,
  CheckCircle2,
  XCircle,
  Building2,
  Globe,
  Loader2,
  Info,
  Database,
  Eye,
} from "lucide-react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  DropdownMenuSeparator,
} from "@/components/ui/dropdown-menu";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { getFullAvatarUrl } from "@/lib/avatar-utils";
import { DeploymentLogsTab } from "./DeploymentLogsTab";
import { ScriptPreviewModal } from "./ScriptPreviewModal";

interface ProjectDetailsDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  projectId: string | null;
  initialTab?: "details" | "members" | "import-export";
  onImportSuccess?: () => void;
}

export const ProjectDetailsDialog: React.FC<ProjectDetailsDialogProps> = ({
  open,
  onOpenChange,
  projectId,
  initialTab = "details",
  onImportSuccess,
}) => {
  const { toast } = useToast();
  const {
    projects,
    projectMembers,
    loading,
    loadProjects,
    updateProject,
    deleteProject,
    loadProjectMembers,
    inviteMember,
    shareWithOrganization,
    updateMemberRole,
    removeMember,
    exportProject,
    importProject,
  } = useProjectStore();

  const [activeTab, setActiveTab] = useState<
    "details" | "members" | "deployment-logs" | "import-export"
  >(initialTab);

  // Edit project form
  const [editForm, setEditForm] = useState({
    name: "",
    description: "",
    isActive: true,
  });

  // Member invitation form
  const [inviteForm, setInviteForm] = useState({
    email: "",
    role: "viewer" as "editor" | "viewer",
  });

  // Import/Export state
  const [importFile, setImportFile] = useState<File | null>(null);
  const [orgShareRole, setOrgShareRole] = useState<"editor" | "viewer">(
    "viewer"
  );

  // Hierarchy/Mapping CSV state
  const [hierarchyImportFile, setHierarchyImportFile] = useState<File | null>(
    null
  );
  const [mappingImportFile, setMappingImportFile] = useState<File | null>(null);
  const [csvLoading, setCsvLoading] = useState(false);
  const [isLegacyFormat, setIsLegacyFormat] = useState(true); // Default to checked

  // Delete confirmation
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);

  // Deployment logs state
  const [deploymentHistory, setDeploymentHistory] = useState<any[]>([]);
  const [deploymentLoading, setDeploymentLoading] = useState(false);
  const [selectedScript, setSelectedScript] = useState<any>(null);
  const [showScriptPreview, setShowScriptPreview] = useState(false);

  const currentProject = projects.find((p) => p.id === projectId);

  // Update form when project changes
  useEffect(() => {
    if (currentProject) {
      setEditForm({
        name: currentProject.name,
        description: currentProject.description || "",
        isActive: currentProject.isActive,
      });
    }
  }, [currentProject]);

  // Update active tab when initialTab changes
  useEffect(() => {
    if (open) {
      setActiveTab(initialTab);
    }
  }, [open, initialTab]);

  // Load members when switching to members tab
  useEffect(() => {
    if (open && activeTab === "members" && projectId) {
      loadProjectMembers(projectId);
    }
  }, [open, activeTab, projectId, loadProjectMembers]);

  const handleSaveChanges = async () => {
    if (!projectId) return;

    try {
      await updateProject(projectId, {
        name: editForm.name,
        description: editForm.description,
        isActive: editForm.isActive,
      });
      toast({
        title: "Success",
        description: "Project updated successfully",
      });
    } catch (error: any) {
      toast({
        title: "Error",
        description: error.message || "Failed to update project",
        variant: "destructive",
      });
    }
  };

  const handleDeleteProject = async () => {
    if (!projectId) return;

    try {
      await deleteProject(projectId);
      toast({
        title: "Success",
        description: "Project deleted successfully",
      });
      setShowDeleteConfirm(false);
      onOpenChange(false);
    } catch (error: any) {
      toast({
        title: "Error",
        description: error.message || "Failed to delete project",
        variant: "destructive",
      });
    }
  };

  const handleInviteMember = async () => {
    if (!projectId || !inviteForm.email) return;

    try {
      await inviteMember(projectId, {
        userEmail: inviteForm.email,
        role: inviteForm.role,
      });
      toast({
        title: "Success",
        description: "Invitation sent successfully",
      });
      setInviteForm({ email: "", role: "viewer" });
    } catch (error: any) {
      toast({
        title: "Error",
        description: error.message || "Failed to invite member",
        variant: "destructive",
      });
    }
  };

  const handleShareWithOrg = async () => {
    if (!projectId) return;

    try {
      await shareWithOrganization(projectId, orgShareRole);
      toast({
        title: "Success",
        description: "Project shared with organization",
      });
    } catch (error: any) {
      toast({
        title: "Error",
        description: error.message || "Failed to share project",
        variant: "destructive",
      });
    }
  };

  const handleUpdateMemberRole = async (
    memberId: string,
    role: "editor" | "viewer"
  ) => {
    if (!projectId) return;

    try {
      await updateMemberRole(projectId, memberId, role);
      toast({
        title: "Success",
        description: "Member role updated",
      });
    } catch (error: any) {
      toast({
        title: "Error",
        description: error.message || "Failed to update role",
        variant: "destructive",
      });
    }
  };

  const handleRemoveMember = async (memberId: string) => {
    if (!projectId) return;

    try {
      await removeMember(projectId, memberId);
      toast({
        title: "Success",
        description: "Member removed from project",
      });
    } catch (error: any) {
      toast({
        title: "Error",
        description: error.message || "Failed to remove member",
        variant: "destructive",
      });
    }
  };

  // Load deployment history when tab opens
  const loadDeploymentHistory = async () => {
    if (!projectId) return;

    setDeploymentLoading(true);
    try {
      const data = await smartHierarchyService.getDeploymentHistory(
        projectId,
        50,
        0
      );
      setDeploymentHistory(data.deployments || []);
    } catch (error: any) {
      toast({
        title: "Error",
        description: "Failed to load deployment history",
        variant: "destructive",
      });
    } finally {
      setDeploymentLoading(false);
    }
  };

  // Load deployment history when deployment logs tab is active
  useEffect(() => {
    if (open && activeTab === "deployment-logs" && projectId) {
      loadDeploymentHistory();
    }
  }, [open, activeTab, projectId]);

  const handleViewScript = (deployment: any) => {
    setSelectedScript(deployment);
    setShowScriptPreview(true);
  };

  const handleExport = async () => {
    if (!projectId) return;

    try {
      // Project configuration export is always JSON
      await exportProject(projectId, "json");
      toast({
        title: "Success",
        description: "Project exported as JSON",
      });
    } catch (error: any) {
      toast({
        title: "Error",
        description: error.message || "Failed to export project",
        variant: "destructive",
      });
    }
  };

  const handleImport = async () => {
    if (!projectId || !importFile) return;

    try {
      // Project configuration import is always JSON
      await importProject(projectId, importFile, "json");
      toast({
        title: "Success",
        description: "Project data imported successfully",
      });
      setImportFile(null);

      // Refresh projects to show updated data
      await loadProjects();

      // Trigger hierarchy refresh
      if (onImportSuccess) {
        onImportSuccess();
      }

      // Close the dialog after successful import
      onOpenChange(false);
    } catch (error: any) {
      toast({
        title: "Error",
        description: error.message || "Failed to import project",
        variant: "destructive",
      });
    }
  };

  const handleExportHierarchyCSV = async () => {
    if (!projectId) return;

    try {
      setCsvLoading(true);
      const result = await smartHierarchyService.exportHierarchyCSV(projectId);

      // Create and download file
      const blob = new Blob([result.content], { type: result.contentType });
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = result.filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);

      toast({
        title: "Export successful",
        description: `Hierarchy CSV exported as ${result.filename}`,
      });
    } catch (error: any) {
      toast({
        title: "Export failed",
        description: error.response?.data?.message || error.message,
        variant: "destructive",
      });
    } finally {
      setCsvLoading(false);
    }
  };

  const handleExportMappingCSV = async () => {
    if (!projectId) return;

    try {
      setCsvLoading(true);
      const result = await smartHierarchyService.exportMappingCSV(projectId);

      // Create and download file
      const blob = new Blob([result.content], { type: result.contentType });
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = result.filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);

      toast({
        title: "Export successful",
        description: `Mapping CSV exported as ${result.filename}`,
      });
    } catch (error: any) {
      toast({
        title: "Export failed",
        description: error.response?.data?.message || error.message,
        variant: "destructive",
      });
    } finally {
      setCsvLoading(false);
    }
  };

  const handleImportHierarchyCSV = async () => {
    if (!projectId || !hierarchyImportFile) return;

    try {
      setCsvLoading(true);
      const content = await hierarchyImportFile.text();

      // Pass legacy format flag to the import service
      const result = await smartHierarchyService.importHierarchyCSV(
        projectId,
        content,
        isLegacyFormat
      );

      toast({
        title: "Import successful",
        description: `Imported ${result.imported} hierarchies, skipped ${result.skipped}`,
      });

      if (result.errors && result.errors.length > 0) {
        toast({
          title: "Import completed with errors",
          description: `${result.imported} imported, ${
            result.skipped
          } skipped, ${result.errors.length} errors: ${result.errors
            .slice(0, 3)
            .join("; ")}${result.errors.length > 3 ? "..." : ""}`,
          variant: "destructive",
        });
      }

      setHierarchyImportFile(null);
      await loadProjects();

      // Trigger hierarchy refresh
      if (onImportSuccess) {
        onImportSuccess();
      }

      // Close the dialog after successful import
      onOpenChange(false);
    } catch (error: any) {
      toast({
        title: "Import failed",
        description: error.response?.data?.message || error.message,
        variant: "destructive",
      });
    } finally {
      setCsvLoading(false);
    }
  };

  const handleImportMappingCSV = async () => {
    if (!projectId || !mappingImportFile) return;

    try {
      setCsvLoading(true);
      const content = await mappingImportFile.text();
      const result = await smartHierarchyService.importMappingCSV(
        projectId,
        content
      );

      toast({
        title: "Import successful",
        description: `Imported ${result.imported} mappings, skipped ${result.skipped}`,
      });

      if (result.errors && result.errors.length > 0) {
        toast({
          title: "Import completed with errors",
          description: `${result.imported} imported, ${
            result.skipped
          } skipped, ${result.errors.length} errors: ${result.errors
            .slice(0, 3)
            .join("; ")}${result.errors.length > 3 ? "..." : ""}`,
          variant: "destructive",
        });
      }

      setMappingImportFile(null);
      await loadProjects();

      // Trigger hierarchy refresh
      if (onImportSuccess) {
        onImportSuccess();
      }

      // Close the dialog after successful import
      onOpenChange(false);
    } catch (error: any) {
      toast({
        title: "Import failed",
        description: error.response?.data?.message || error.message,
        variant: "destructive",
      });
    } finally {
      setCsvLoading(false);
    }
  };

  const handleImportBoth = async () => {
    if (!projectId || (!hierarchyImportFile && !mappingImportFile)) return;

    try {
      setCsvLoading(true);
      let hierarchyResult = null;
      let mappingResult = null;

      // Import hierarchy first if provided
      if (hierarchyImportFile) {
        const content = await hierarchyImportFile.text();
        hierarchyResult = await smartHierarchyService.importHierarchyCSV(
          projectId,
          content,
          isLegacyFormat
        );
      }

      // Then import mapping if provided
      if (mappingImportFile) {
        const content = await mappingImportFile.text();
        mappingResult = await smartHierarchyService.importMappingCSV(
          projectId,
          content
        );
      }

      // Show success message based on what was imported
      let successMessage = "";
      if (hierarchyResult && mappingResult) {
        successMessage = `Imported ${hierarchyResult.imported} hierarchies and ${mappingResult.imported} mappings`;
      } else if (hierarchyResult) {
        successMessage = `Imported ${hierarchyResult.imported} hierarchies, skipped ${hierarchyResult.skipped}`;
      } else if (mappingResult) {
        successMessage = `Imported ${mappingResult.imported} mappings, skipped ${mappingResult.skipped}`;
      }

      toast({
        title: "Import successful",
        description: successMessage,
      });

      // Check for errors
      const allErrors = [
        ...(hierarchyResult?.errors || []),
        ...(mappingResult?.errors || []),
      ];

      if (allErrors.length > 0) {
        toast({
          title: "Import completed with some errors",
          description: `${allErrors.length} errors: ${allErrors
            .slice(0, 3)
            .join("; ")}${allErrors.length > 3 ? "..." : ""}`,
          variant: "destructive",
        });
      }

      // Clear files
      setHierarchyImportFile(null);
      setMappingImportFile(null);
      await loadProjects();

      // Trigger hierarchy refresh
      if (onImportSuccess) {
        onImportSuccess();
      }

      // Close the dialog after successful import
      onOpenChange(false);
    } catch (error: any) {
      toast({
        title: "Import failed",
        description: error.response?.data?.message || error.message,
        variant: "destructive",
      });
    } finally {
      setCsvLoading(false);
    }
  };

  const getRoleBadgeVariant = (role: string) => {
    switch (role) {
      case "owner":
        return "default";
      case "editor":
        return "secondary";
      case "viewer":
        return "outline";
      default:
        return "outline";
    }
  };

  if (!currentProject) {
    return null;
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-6xl! max-h-[85vh] overflow-hidden flex flex-col p-0">
        <DialogHeader className="px-6 pt-6 pb-4 border-b">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-primary/10">
              <FolderKanban className="h-5 w-5 text-primary" />
            </div>
            <div>
              <DialogTitle className="text-xl">
                {currentProject.name}
              </DialogTitle>
              <DialogDescription>
                {currentProject.description || "No description"}
              </DialogDescription>
            </div>
          </div>
        </DialogHeader>

        <Tabs
          value={activeTab}
          onValueChange={(v) => setActiveTab(v as any)}
          className="flex-1 flex flex-col overflow-hidden"
        >
          <div className="px-6">
            <TabsList className="grid w-full grid-cols-4 h-8">
              <TabsTrigger value="details" className="gap-2">
                <Settings className="h-4 w-4" />
                Details
              </TabsTrigger>
              <TabsTrigger value="members" className="gap-2">
                <Users className="h-4 w-4" />
                Members
              </TabsTrigger>
              <TabsTrigger value="deployment-logs" className="gap-2">
                <Database className="h-4 w-4" />
                Deployment Logs
              </TabsTrigger>
              <TabsTrigger value="import-export" className="gap-2">
                <FileDown className="h-4 w-4" />
                Import/Export
              </TabsTrigger>
            </TabsList>
          </div>

          {/* Details Tab */}
          <TabsContent
            value="details"
            className="flex-1 overflow-auto px-6 pb-6 mt-4 space-y-4"
          >
            <Card className="border-0">
              <CardHeader>
                <CardTitle className="text-lg">Project Information</CardTitle>
                <CardDescription>Update your project details</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="edit-name">Project Name</Label>
                  <Input
                    id="edit-name"
                    value={editForm.name}
                    onChange={(e) =>
                      setEditForm({ ...editForm, name: e.target.value })
                    }
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="edit-desc">Description</Label>
                  <Textarea
                    id="edit-desc"
                    value={editForm.description}
                    onChange={(e) =>
                      setEditForm({ ...editForm, description: e.target.value })
                    }
                    rows={3}
                  />
                </div>
                <div className="space-y-2">
                  <Label>Status</Label>
                  <Select
                    value={editForm.isActive ? "active" : "inactive"}
                    onValueChange={(v) =>
                      setEditForm({ ...editForm, isActive: v === "active" })
                    }
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="active">
                        <div className="flex items-center gap-2">
                          <CheckCircle2 className="h-4 w-4 text-green-500" />
                          Active
                        </div>
                      </SelectItem>
                      <SelectItem value="inactive">
                        <div className="flex items-center gap-2">
                          <XCircle className="h-4 w-4 text-gray-500" />
                          Inactive
                        </div>
                      </SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </CardContent>
            </Card>

            <div className="flex justify-between items-center pt-4">
              <Button
                variant="destructive"
                onClick={() => setShowDeleteConfirm(true)}
              >
                <Trash2 className="h-4 w-4 mr-2" />
                Delete Project
              </Button>
              <Button onClick={handleSaveChanges} disabled={loading}>
                <CheckCircle2 className="h-4 w-4 mr-2" />
                Save Changes
              </Button>
            </div>
          </TabsContent>

          {/* Members Tab */}
          <TabsContent
            value="members"
            className="flex-1 overflow-auto px-6 pb-6 mt-4 space-y-4"
          >
            <Card className="border-0">
              <CardHeader>
                <CardTitle className="text-lg">
                  Team Members ({projectMembers.length})
                </CardTitle>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Member</TableHead>
                      <TableHead>Email</TableHead>
                      <TableHead>Role</TableHead>
                      <TableHead className="w-[100px]">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {projectMembers.length === 0 ? (
                      <TableRow>
                        <TableCell
                          colSpan={4}
                          className="text-center text-muted-foreground"
                        >
                          No members yet. Invite someone to collaborate!
                        </TableCell>
                      </TableRow>
                    ) : (
                      projectMembers.map((member) => (
                        <TableRow key={member.id}>
                          <TableCell>
                            <div className="flex items-center gap-2">
                              <Avatar className="h-8 w-8">
                                <AvatarImage
                                  src={getFullAvatarUrl(member.user?.avatarUrl)}
                                />
                                <AvatarFallback>
                                  {member.user?.name
                                    ?.substring(0, 2)
                                    .toUpperCase() || "??"}
                                </AvatarFallback>
                              </Avatar>
                              <div className="flex flex-col">
                                <span className="font-medium">
                                  {member.user?.name ||
                                    member.userEmail ||
                                    "Unknown"}
                                </span>
                                {member.invitationStatus === "pending" && (
                                  <span className="text-xs text-muted-foreground">
                                    Invitation pending
                                  </span>
                                )}
                              </div>
                            </div>
                          </TableCell>
                          <TableCell className="text-sm text-muted-foreground">
                            {member.user?.email || member.userEmail || "-"}
                          </TableCell>
                          <TableCell>
                            <div className="flex items-center gap-2">
                              {member.role === "owner" ? (
                                <Badge
                                  variant={getRoleBadgeVariant(member.role)}
                                >
                                  <Shield className="h-3 w-3 mr-1" />
                                  {member.role}
                                </Badge>
                              ) : (
                                <Select
                                  value={member.role}
                                  onValueChange={(v: "editor" | "viewer") =>
                                    handleUpdateMemberRole(member.id, v)
                                  }
                                  disabled={
                                    loading ||
                                    member.invitationStatus === "pending"
                                  }
                                >
                                  <SelectTrigger className="w-32 h-8">
                                    <SelectValue />
                                  </SelectTrigger>
                                  <SelectContent>
                                    <SelectItem value="editor">
                                      Editor
                                    </SelectItem>
                                    <SelectItem value="viewer">
                                      Viewer
                                    </SelectItem>
                                  </SelectContent>
                                </Select>
                              )}
                              {member.invitationStatus === "pending" && (
                                <Badge
                                  variant="outline"
                                  className="text-orange-600 border-orange-600"
                                >
                                  Pending
                                </Badge>
                              )}
                            </div>
                          </TableCell>
                          <TableCell>
                            {member.role !== "owner" && (
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={() => handleRemoveMember(member.id)}
                                disabled={loading}
                              >
                                <Trash2 className="h-4 w-4 text-destructive" />
                              </Button>
                            )}
                          </TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
            <Card className="border-0">
              <CardHeader>
                <CardTitle className="text-lg">Invite Team Member</CardTitle>
                <CardDescription>
                  Send an invitation to join this project
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-2">
                    <Label htmlFor="member-email">Email Address</Label>
                    <Input
                      id="member-email"
                      type="email"
                      placeholder="colleague@example.com"
                      value={inviteForm.email}
                      onChange={(e) =>
                        setInviteForm({ ...inviteForm, email: e.target.value })
                      }
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="member-role">Role</Label>
                    <Select
                      value={inviteForm.role}
                      onValueChange={(v: "editor" | "viewer") =>
                        setInviteForm({ ...inviteForm, role: v })
                      }
                    >
                      <SelectTrigger id="member-role">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="editor">Editor</SelectItem>
                        <SelectItem value="viewer">Viewer</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </div>
                <Button onClick={handleInviteMember} disabled={loading}>
                  <UserPlus className="h-4 w-4 mr-2" />
                  Send Invitation
                </Button>
              </CardContent>
            </Card>

            <Card className="border-0">
              <CardHeader>
                <CardTitle className="text-lg">
                  Share with Organization
                </CardTitle>
                <CardDescription>
                  Make this project accessible to all organization members
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="flex gap-4">
                  <Select
                    value={orgShareRole}
                    onValueChange={(v: "editor" | "viewer") =>
                      setOrgShareRole(v)
                    }
                  >
                    <SelectTrigger className="w-40">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="editor">Editor</SelectItem>
                      <SelectItem value="viewer">Viewer</SelectItem>
                    </SelectContent>
                  </Select>
                  <Button onClick={handleShareWithOrg} disabled={loading}>
                    <Globe className="h-4 w-4 mr-2" />
                    Share with Organization
                  </Button>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          {/* Unified Import/Export Tab */}
          <TabsContent
            value="import-export"
            className="flex-1 overflow-auto px-6 pb-6 mt-4 space-y-6"
          >
            {/* IMPORT SECTION */}
            <Card className="border-0">
              <CardHeader>
                <CardTitle className="text-lg">Import Data</CardTitle>
                <CardDescription>
                  Import hierarchy structure and mappings into your project
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="bg-muted border border-blue-200 rounded-lg p-4 mb-4">
                  <div className="flex gap-3">
                    <Info className="h-5 w-5 text-blue-600 shrink-0 mt-0.5" />
                    <div className="text-sm text-blue-900 space-y-1">
                      <ul className="list-disc list-inside space-y-1">
                        <li>
                          You can import hierarchy CSV, mapping CSV, or both
                        </li>
                        <li>
                          If both files are provided, hierarchy will be imported
                          first, then mappings
                        </li>
                        <li>Check "Legacy CSV Format" if importing from older app versions</li>
                      </ul>
                    </div>
                  </div>
                </div>

                <div className="grid gap-4 md:grid-cols-2 mb-4">
                  {/* Hierarchy Import Column */}
                  <div className="space-y-3">
                    <div className="flex items-center gap-2 mb-2">
                      <FileUp className="h-5 w-5 text-primary" />
                      <Label className="text-base font-semibold">
                        Hierarchy CSV
                      </Label>
                    </div>
                    <div className="space-y-2">
                      <Input
                        id="hierarchy-import-file"
                        type="file"
                        accept=".csv"
                        onChange={(e) =>
                          setHierarchyImportFile(e.target.files?.[0] || null)
                        }
                      />
                      <p className="text-xs text-muted-foreground">
                        Contains all hierarchies with expanded formulas
                      </p>
                    </div>
                  </div>

                  {/* Mapping Import Column */}
                  <div className="space-y-3">
                    <div className="flex items-center gap-2 mb-2">
                      <FileUp className="h-5 w-5 text-primary" />
                      <Label className="text-base font-semibold">
                        Mapping CSV
                      </Label>
                    </div>
                    <div className="space-y-2">
                      <Input
                        id="mapping-import-file"
                        type="file"
                        accept=".csv"
                        onChange={(e) =>
                          setMappingImportFile(e.target.files?.[0] || null)
                        }
                      />
                      <p className="text-xs text-muted-foreground">
                        Maps hierarchies to Snowflake dimensions
                      </p>
                    </div>
                  </div>
                </div>

                {/* Legacy Format Checkbox */}
                <div className="flex items-start space-x-3 rounded-md border p-3 bg-amber-50 dark:bg-amber-950/20 mb-4">
                  <Checkbox
                    id="legacy-format-checkbox"
                    checked={isLegacyFormat}
                    onCheckedChange={(checked) =>
                      setIsLegacyFormat(checked as boolean)
                    }
                  />
                  <div className="space-y-1">
                    <Label
                      htmlFor="legacy-format-checkbox"
                      className="text-sm font-medium leading-none cursor-pointer flex items-center gap-2"
                    >
                      <Info className="w-4 h-4 text-amber-600" />
                      Legacy/Older CSV Format
                    </Label>
                    <p className="text-xs text-muted-foreground">
                      Check this if importing from an older version of the application.
                      This adjusts column mapping and parsing rules for compatibility.
                    </p>
                  </div>
                </div>

                <Button
                  onClick={handleImportBoth}
                  disabled={
                    csvLoading || (!hierarchyImportFile && !mappingImportFile)
                  }
                  className="w-full"
                  size="lg"
                >
                  {csvLoading ? (
                    <>
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                      Importing...
                    </>
                  ) : (
                    <>
                      <FileUp className="h-4 w-4 mr-2" />
                      Import{" "}
                      {hierarchyImportFile && mappingImportFile
                        ? "Both"
                        : hierarchyImportFile
                        ? "Hierarchy"
                        : "Mapping"}
                    </>
                  )}
                </Button>
              </CardContent>
            </Card>

            <Separator />

            {/* EXPORT SECTION */}
            <Card className="border-0">
              <CardHeader>
                <CardTitle className="text-lg">Export Data</CardTitle>
                <CardDescription>
                  Download hierarchy structure and mappings from your project
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="grid gap-4 md:grid-cols-2">
                  {/* Hierarchy Export Column */}
                  <div className="space-y-3">
                    <div className="flex items-center gap-2 mb-2">
                      <FileDown className="h-5 w-5 text-primary" />
                      <Label className="text-base font-semibold">
                        Hierarchy CSV
                      </Label>
                    </div>
                    <div className="space-y-2">
                      <p className="text-sm text-muted-foreground min-h-[40px] flex items-center">
                        Download current project hierarchies as CSV
                      </p>
                    </div>
                    <Button
                      onClick={handleExportHierarchyCSV}
                      disabled={csvLoading}
                      variant="outline"
                      className="w-full"
                    >
                      {csvLoading ? (
                        <>
                          <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                          Exporting...
                        </>
                      ) : (
                        <>
                          <FileDown className="h-4 w-4 mr-2" />
                          Export Hierarchy
                        </>
                      )}
                    </Button>
                  </div>

                  {/* Mapping Export Column */}
                  <div className="space-y-3">
                    <div className="flex items-center gap-2 mb-2">
                      <FileDown className="h-5 w-5 text-primary" />
                      <Label className="text-base font-semibold">
                        Mapping CSV
                      </Label>
                    </div>
                    <div className="space-y-2">
                      <p className="text-sm text-muted-foreground min-h-[40px] flex items-center">
                        Download current project mappings as CSV
                      </p>
                    </div>
                    <Button
                      onClick={handleExportMappingCSV}
                      disabled={csvLoading}
                      variant="outline"
                      className="w-full"
                    >
                      {csvLoading ? (
                        <>
                          <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                          Exporting...
                        </>
                      ) : (
                        <>
                          <FileDown className="h-4 w-4 mr-2" />
                          Export Mapping
                        </>
                      )}
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Separator />

            {/* JSON CONFIGURATION SECTION */}
            <Card className="border-0">
              <CardHeader>
                <CardTitle className="text-lg">
                  Project Configuration (JSON)
                </CardTitle>
                <CardDescription>
                  Complete project backup for version control and deployment
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="bg-muted border border-blue-200 rounded-lg p-4 mb-4">
                  <div className="flex gap-3">
                    <Info className="h-5 w-5 text-blue-600 shrink-0 mt-0.5" />
                    <div className="text-sm text-blue-900 space-y-1">
                      <ul className="list-disc list-inside space-y-1">
                        <li>
                          Complete backup: hierarchies, formulas, filters,
                          mappings
                        </li>
                        <li>
                          Best for: CI/CD deployment, version control,
                          environment migration
                        </li>
                        <li>Formulas stored in collapsed JSON format</li>
                      </ul>
                    </div>
                  </div>
                </div>

                <div className="grid gap-4 md:grid-cols-2">
                  {/* Import Column */}
                  <div className="space-y-3">
                    <div className="flex items-center gap-2 mb-2">
                      <FileUp className="h-5 w-5 text-primary" />
                      <Label className="text-base font-semibold">Import</Label>
                    </div>
                    <div className="space-y-2">
                      <Input
                        id="import-file"
                        type="file"
                        accept=".json"
                        onChange={(e) =>
                          setImportFile(e.target.files?.[0] || null)
                        }
                      />
                    </div>
                    <Button
                      onClick={handleImport}
                      disabled={loading || !importFile}
                      className="w-full"
                    >
                      {loading ? (
                        <>
                          <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                          Importing...
                        </>
                      ) : (
                        <>
                          <FileUp className="h-4 w-4 mr-2" />
                          Import Configuration
                        </>
                      )}
                    </Button>
                  </div>

                  {/* Export Column */}
                  <div className="space-y-3">
                    <div className="flex items-center gap-2 mb-2">
                      <FileDown className="h-5 w-5 text-primary" />
                      <Label className="text-base font-semibold">Export</Label>
                    </div>
                    <div className="space-y-2">
                      <p className="text-sm text-muted-foreground min-h-[40px] flex items-center">
                        {currentProject.name}_export_timestamp.json
                      </p>
                    </div>
                    <Button
                      onClick={handleExport}
                      disabled={loading}
                      variant="outline"
                      className="w-full"
                    >
                      {loading ? (
                        <>
                          <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                          Exporting...
                        </>
                      ) : (
                        <>
                          <FileDown className="h-4 w-4 mr-2" />
                          Export Configuration
                        </>
                      )}
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent
            value="deployment-logs"
            className="flex-1 overflow-auto px-6 pb-6 mt-4"
          >
            <DeploymentLogsTab
              deployments={deploymentHistory}
              loading={deploymentLoading}
              onViewScript={handleViewScript}
            />
          </TabsContent>
        </Tabs>

        <div className="px-6 py-4 border-t flex justify-end">
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Close
          </Button>
        </div>
      </DialogContent>

      {/* Delete Confirmation Dialog */}
      <AlertDialog open={showDeleteConfirm} onOpenChange={setShowDeleteConfirm}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Project?</AlertDialogTitle>
            <AlertDialogDescription>
              This action will soft-delete "{currentProject.name}" and all its
              hierarchies. You won't be able to access this project anymore.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleDeleteProject}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              Delete Project
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <ScriptPreviewModal
        open={showScriptPreview}
        onOpenChange={setShowScriptPreview}
        deployment={selectedScript}
      />
    </Dialog>
  );
};
