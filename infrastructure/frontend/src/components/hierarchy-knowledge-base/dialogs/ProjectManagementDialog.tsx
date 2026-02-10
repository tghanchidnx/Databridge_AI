import React, { useState, useEffect } from "react";
import { useProjectStore } from "@/stores/projectStore";
import { useToast } from "@/hooks/use-toast";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
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
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Plus,
  Search,
  Eye,
  Trash2,
  Users,
  FileDown,
  FolderKanban,
  Check,
  X,
  Clock,
} from "lucide-react";
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

interface ProjectManagementDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onOpenProjectDetails?: (
    projectId: string,
    tab?: "details" | "members" | "import-export"
  ) => void;
}

export const ProjectManagementDialog: React.FC<
  ProjectManagementDialogProps
> = ({ open, onOpenChange, onOpenProjectDetails }) => {
  const { toast } = useToast();
  const {
    projects,
    loading,
    filteredProjects,
    loadProjects,
    createProject,
    deleteProject,
    setSearchQuery,
  } = useProjectStore();

  const [searchInput, setSearchInput] = useState("");
  const [showAddProject, setShowAddProject] = useState(false);
  const [pendingInvitations, setPendingInvitations] = useState<any[]>([]);
  const [loadingInvitations, setLoadingInvitations] = useState(false);

  // New project form
  const [newProjectForm, setNewProjectForm] = useState({
    name: "",
    description: "",
  });

  // Delete confirmation
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [projectToDelete, setProjectToDelete] = useState<string | null>(null);

  // Load projects and pending invitations on open
  useEffect(() => {
    if (open) {
      loadProjects();
      loadPendingInvitations();
    }
  }, [open, loadProjects]);

  const loadPendingInvitations = async () => {
    setLoadingInvitations(true);
    try {
      const { projectService } = await import("@/services/api");
      const invitations = await projectService.getPendingInvitations();
      setPendingInvitations(invitations);
    } catch (error: any) {
      console.error("Failed to load pending invitations:", error);
    } finally {
      setLoadingInvitations(false);
    }
  };

  const handleAcceptInvitation = async (
    projectId: string,
    memberId: string
  ) => {
    try {
      const { projectService } = await import("@/services/api");
      await projectService.acceptProjectInvitation(projectId, memberId);
      toast({
        title: "Invitation accepted",
        description: "You can now access this project",
      });
      loadPendingInvitations();
      loadProjects();
    } catch (error: any) {
      toast({
        title: "Failed to accept invitation",
        description: error.message || "An error occurred",
        variant: "destructive",
      });
    }
  };

  const handleDeclineInvitation = async (
    projectId: string,
    memberId: string
  ) => {
    try {
      const { projectService } = await import("@/services/api");
      await projectService.declineProjectInvitation(projectId, memberId);
      toast({
        title: "Invitation declined",
        description: "The invitation has been removed",
      });
      loadPendingInvitations();
    } catch (error: any) {
      toast({
        title: "Failed to decline invitation",
        description: error.message || "An error occurred",
        variant: "destructive",
      });
    }
  };

  const handleSearch = (value: string) => {
    setSearchInput(value);
    setSearchQuery(value);
  };

  const handleViewProject = (
    projectId: string,
    tab?: "details" | "members" | "import-export"
  ) => {
    if (onOpenProjectDetails) {
      onOpenProjectDetails(projectId, tab);
    }
  };

  const handleCreateProject = async () => {
    if (!newProjectForm.name.trim()) {
      toast({
        title: "Validation Error",
        description: "Project name is required",
        variant: "destructive",
      });
      return;
    }

    try {
      const newProject = await createProject({
        name: newProjectForm.name,
        description: newProjectForm.description,
      });

      toast({
        title: "Success",
        description: "Project created successfully",
      });

      // Reset form
      setNewProjectForm({ name: "", description: "" });
      setShowAddProject(false);

      // Open details dialog for new project
      if (onOpenProjectDetails) {
        onOpenProjectDetails(newProject.id, "details");
      }
    } catch (error: any) {
      toast({
        title: "Error",
        description: error.message || "Failed to create project",
        variant: "destructive",
      });
    }
  };

  const handleDeleteProject = async (projectId: string) => {
    try {
      await deleteProject(projectId);
      toast({
        title: "Success",
        description: "Project deleted successfully",
      });
      setShowDeleteConfirm(false);
      setProjectToDelete(null);
    } catch (error: any) {
      toast({
        title: "Error",
        description: error.message || "Failed to delete project",
        variant: "destructive",
      });
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="!max-w-4xl max-h-[85vh]  flex flex-col p-0">
        <DialogHeader className="px-6 pt-6 pb-4 border-b">
          <div className="flex items-center gap-3">
            <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-primary/10">
              <FolderKanban className="h-6 w-6 text-primary" />
            </div>
            <div>
              <DialogTitle className="text-2xl">All Projects</DialogTitle>
              <DialogDescription>
                View and manage all your projects
              </DialogDescription>
            </div>
          </div>
        </DialogHeader>

        <div className="flex-1  px-6 pb-6 mt-4 w-full !h-[100px] overflow-y-scroll ">
          <div className="flex flex-col gap-4 h-full">
            {/* Pending Invitations */}
            {pendingInvitations.length > 0 && (
              <Card className="border-primary/50 bg-primary/5">
                <CardHeader className="pb-3">
                  <CardTitle className="text-base flex items-center gap-2">
                    <Clock className="h-4 w-4" />
                    Pending Invitations ({pendingInvitations.length})
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-2 overflow-auto">
                  {pendingInvitations.map((invitation) => (
                    <div
                      key={invitation.member.id}
                      className="flex items-center justify-between p-3 bg-background rounded-lg border"
                    >
                      <div className="flex-1">
                        <p className="font-medium">{invitation.project.name}</p>
                        <p className="text-sm text-muted-foreground">
                          Invited by {invitation.inviter.name} •{" "}
                          {new Date(
                            invitation.member.invitedAt
                          ).toLocaleDateString()}
                        </p>
                      </div>
                      <div className="flex gap-2">
                        <Button
                          size="sm"
                          variant="default"
                          onClick={() =>
                            handleAcceptInvitation(
                              invitation.project.id,
                              invitation.member.id
                            )
                          }
                          disabled={loadingInvitations}
                        >
                          <Check className="h-4 w-4 mr-1" />
                          Accept
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() =>
                            handleDeclineInvitation(
                              invitation.project.id,
                              invitation.member.id
                            )
                          }
                          disabled={loadingInvitations}
                        >
                          <X className="h-4 w-4 mr-1" />
                          Decline
                        </Button>
                      </div>
                    </div>
                  ))}
                </CardContent>
              </Card>
            )}

            {/* Search and New Project Bar */}
            <div className="flex gap-3">
              <div className="relative flex-1">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                <Input
                  placeholder="Search projects..."
                  value={searchInput}
                  onChange={(e) => handleSearch(e.target.value)}
                  className="pl-9"
                />
              </div>
              <Button onClick={() => setShowAddProject(!showAddProject)}>
                <Plus className="w-4 h-4 mr-2" />
                New Project
              </Button>
            </div>

            {/* Create Project Form */}
            {showAddProject && (
              <Card>
                <CardHeader className="pb-3 ">
                  <CardTitle className="text-base">
                    Create New Project
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-3 overflow-auto">
                  <div className="space-y-1.5">
                    <Label htmlFor="project-name" className="text-sm">
                      Project Name *
                    </Label>
                    <Input
                      id="project-name"
                      value={newProjectForm.name}
                      onChange={(e) =>
                        setNewProjectForm({
                          ...newProjectForm,
                          name: e.target.value,
                        })
                      }
                      placeholder="Enter project name"
                    />
                  </div>

                  <div className="space-y-1.5">
                    <Label htmlFor="project-desc" className="text-sm">
                      Description
                    </Label>
                    <Textarea
                      id="project-desc"
                      value={newProjectForm.description}
                      onChange={(e) =>
                        setNewProjectForm({
                          ...newProjectForm,
                          description: e.target.value,
                        })
                      }
                      placeholder="Enter project description"
                      rows={2}
                      className="resize-none"
                    />
                  </div>

                  <div className="flex justify-end gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        setShowAddProject(false);
                        setNewProjectForm({ name: "", description: "" });
                      }}
                    >
                      Cancel
                    </Button>
                    <Button
                      size="sm"
                      onClick={handleCreateProject}
                      disabled={loading}
                    >
                      Create
                    </Button>
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Projects Table */}
            <Card className="flex-1 m-0 flex flex-col p-0 border-0 rounded-lg overflow-hidden">
              <CardContent className="p-0 flex-1 overflow-auto">
                <Table className="m-0">
                  <TableHeader className="sticky top-0 bg-background z-10">
                    <TableRow>
                      <TableHead className="w-[200px]">Name</TableHead>
                      <TableHead>Description</TableHead>
                      <TableHead className="w-[100px]">Status</TableHead>
                      <TableHead className="w-[120px]">Created</TableHead>
                      <TableHead className="w-[180px]">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {filteredProjects().length === 0 ? (
                      <TableRow>
                        <TableCell
                          colSpan={5}
                          className="text-center h-32 text-muted-foreground"
                        >
                          {searchInput
                            ? "No projects found"
                            : "No projects yet. Create one to get started!"}
                        </TableCell>
                      </TableRow>
                    ) : (
                      filteredProjects().map((project) => (
                        <TableRow
                          key={project.id}
                          className="cursor-pointer hover:bg-muted/50"
                        >
                          <TableCell className="font-medium">
                            {project.name}
                          </TableCell>
                          <TableCell className="text-sm text-muted-foreground">
                            {project.description || "-"}
                          </TableCell>
                          <TableCell>
                            <Badge
                              variant={
                                project.isActive ? "default" : "secondary"
                              }
                            >
                              {project.isActive ? "Active" : "Inactive"}
                            </Badge>
                          </TableCell>
                          <TableCell className="text-sm text-muted-foreground">
                            {new Date(project.createdAt).toLocaleDateString()}
                          </TableCell>
                          <TableCell>
                            <div className="flex items-center gap-1">
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleViewProject(project.id, "details");
                                }}
                                title="View Details"
                              >
                                <Eye className="h-4 w-4" />
                              </Button>
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleViewProject(project.id, "members");
                                }}
                                title="Manage Members"
                              >
                                <Users className="h-4 w-4" />
                              </Button>
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleViewProject(
                                    project.id,
                                    "import-export"
                                  );
                                }}
                                title="Import/Export"
                              >
                                <FileDown className="h-4 w-4" />
                              </Button>
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setProjectToDelete(project.id);
                                  setShowDeleteConfirm(true);
                                }}
                                title="Delete"
                                className="text-destructive hover:text-destructive"
                              >
                                <Trash2 className="h-4 w-4" />
                              </Button>
                            </div>
                          </TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </div>
        </div>

        <div className="flex justify-end px-6 pt-4 pb-6 border-t">
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
              This action will soft-delete the project and all its hierarchies.
              You won't be able to access this project anymore.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={() => setProjectToDelete(null)}>
              Cancel
            </AlertDialogCancel>
            <AlertDialogAction
              onClick={() =>
                projectToDelete && handleDeleteProject(projectToDelete)
              }
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              Delete Project
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </Dialog>
  );
};
