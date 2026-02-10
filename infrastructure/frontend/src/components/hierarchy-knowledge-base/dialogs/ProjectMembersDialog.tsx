import React, { useState, useEffect } from "react";
import type { Project } from "@/services/api/hierarchy/project.service";
import type { ProjectMember } from "@/services/api/hierarchy/project.service";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { UserPlus, Users, Mail, Trash2, Shield, Eye, Edit } from "lucide-react";
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
import { Badge } from "@/components/ui/badge";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import { Separator } from "@/components/ui/separator";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

interface ProjectMembersDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  project: Project | null;
  members: ProjectMember[];
  onInvite: (data: {
    userEmail?: string;
    inviteUserId?: string;
    role: "editor" | "viewer";
  }) => void;
  onShareWithOrg: (role: "editor" | "viewer") => void;
  onUpdateMember: (memberId: string, role: "editor" | "viewer") => void;
  onRemoveMember: (memberId: string) => void;
  onRefresh: () => void;
}

export const ProjectMembersDialog: React.FC<ProjectMembersDialogProps> = ({
  open,
  onOpenChange,
  project,
  members,
  onInvite,
  onShareWithOrg,
  onUpdateMember,
  onRemoveMember,
  onRefresh,
}) => {
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteRole, setInviteRole] = useState<"editor" | "viewer">("viewer");
  const [orgShareRole, setOrgShareRole] = useState<"editor" | "viewer">(
    "viewer"
  );

  useEffect(() => {
    if (open) {
      onRefresh();
    }
  }, [open, onRefresh]);

  const getRoleIcon = (role: string) => {
    switch (role) {
      case "owner":
        return <Shield className="w-4 h-4 text-yellow-500" />;
      case "editor":
        return <Edit className="w-4 h-4 text-blue-500" />;
      case "viewer":
        return <Eye className="w-4 h-4 text-gray-500" />;
      default:
        return null;
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

  const handleInvite = () => {
    if (!inviteEmail.trim()) return;
    onInvite({ userEmail: inviteEmail, role: inviteRole });
    setInviteEmail("");
    setInviteRole("viewer");
  };

  if (!project) return null;

  const ownerCount = members.filter((m) => m.role === "owner").length;
  const editorCount = members.filter((m) => m.role === "editor").length;
  const viewerCount = members.filter((m) => m.role === "viewer").length;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-4xl max-h-[90vh] overflow-hidden flex flex-col">
        <DialogHeader>
          <DialogTitle>Project Members</DialogTitle>
          <DialogDescription>
            Manage who has access to "{project.name}" and their permissions
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-6 overflow-y-auto flex-1">
          {/* Member Statistics */}
          <div className="grid grid-cols-3 gap-4">
            <div className="bg-muted rounded-lg p-3">
              <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                <Shield className="w-4 h-4" />
                Owners
              </div>
              <div className="text-2xl font-bold">{ownerCount}</div>
            </div>
            <div className="bg-muted rounded-lg p-3">
              <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                <Edit className="w-4 h-4" />
                Editors
              </div>
              <div className="text-2xl font-bold">{editorCount}</div>
            </div>
            <div className="bg-muted rounded-lg p-3">
              <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                <Eye className="w-4 h-4" />
                Viewers
              </div>
              <div className="text-2xl font-bold">{viewerCount}</div>
            </div>
          </div>

          <Separator />

          {/* Invite Member */}
          <div className="space-y-4">
            <h3 className="text-sm font-semibold">Invite Member</h3>
            <div className="flex gap-2">
              <div className="flex-1">
                <Input
                  type="email"
                  placeholder="Enter email address"
                  value={inviteEmail}
                  onChange={(e) => setInviteEmail(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") handleInvite();
                  }}
                />
              </div>
              <Select
                value={inviteRole}
                onValueChange={(value: "editor" | "viewer") =>
                  setInviteRole(value)
                }
              >
                <SelectTrigger className="w-[130px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="viewer">Viewer</SelectItem>
                  <SelectItem value="editor">Editor</SelectItem>
                </SelectContent>
              </Select>
              <Button onClick={handleInvite} disabled={!inviteEmail.trim()}>
                <UserPlus className="w-4 h-4 mr-2" />
                Invite
              </Button>
            </div>
            <p className="text-xs text-muted-foreground">
              💡 Tip: They'll receive an email invitation to join the project
            </p>
          </div>

          {/* Share with Organization */}
          {project.organizationId && (
            <>
              <Separator />
              <div className="space-y-4">
                <h3 className="text-sm font-semibold flex items-center gap-2">
                  <Users className="w-4 h-4" />
                  Share with Organization
                </h3>
                <div className="flex gap-2">
                  <Select
                    value={orgShareRole}
                    onValueChange={(value: "editor" | "viewer") =>
                      setOrgShareRole(value)
                    }
                  >
                    <SelectTrigger className="flex-1">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="viewer">
                        Viewer Access for Organization
                      </SelectItem>
                      <SelectItem value="editor">
                        Editor Access for Organization
                      </SelectItem>
                    </SelectContent>
                  </Select>
                  <Button
                    onClick={() => {
                      onShareWithOrg(orgShareRole);
                    }}
                    variant="secondary"
                  >
                    <Users className="w-4 h-4 mr-2" />
                    Share with Org
                  </Button>
                </div>
                <p className="text-xs text-muted-foreground">
                  All members of your organization will be added to this project
                </p>
              </div>
            </>
          )}

          <Separator />

          {/* Members List */}
          <div className="space-y-4">
            <h3 className="text-sm font-semibold">
              Members ({members.length})
            </h3>

            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>User</TableHead>
                  <TableHead>Role</TableHead>
                  <TableHead>Access Type</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="w-[100px]">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {members.length === 0 ? (
                  <TableRow>
                    <TableCell
                      colSpan={5}
                      className="text-center text-muted-foreground"
                    >
                      No members found
                    </TableCell>
                  </TableRow>
                ) : (
                  members.map((member) => (
                    <TableRow key={member.id}>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <Avatar className="h-8 w-8">
                            <AvatarImage src={member.user?.avatarUrl} />
                            <AvatarFallback>
                              {member.user?.name?.[0] ||
                                member.userEmail?.[0]?.toUpperCase() ||
                                "?"}
                            </AvatarFallback>
                          </Avatar>
                          <div>
                            <div className="font-medium text-sm">
                              {member.user?.name || "Pending"}
                            </div>
                            <div className="text-xs text-muted-foreground">
                              {member.userEmail || member.user?.email}
                            </div>
                          </div>
                        </div>
                      </TableCell>
                      <TableCell>
                        {member.role === "owner" ? (
                          <Badge variant={getRoleBadgeVariant(member.role)}>
                            <Shield className="w-3 h-3 mr-1" />
                            Owner
                          </Badge>
                        ) : (
                          <Select
                            value={member.role as "editor" | "viewer"}
                            onValueChange={(value: "editor" | "viewer") =>
                              onUpdateMember(member.id, value)
                            }
                          >
                            <SelectTrigger className="w-[110px] h-8">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="viewer">Viewer</SelectItem>
                              <SelectItem value="editor">Editor</SelectItem>
                            </SelectContent>
                          </Select>
                        )}
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline" className="text-xs">
                          {member.accessType === "organization" ? (
                            <>
                              <Users className="w-3 h-3 mr-1" />
                              Organization
                            </>
                          ) : (
                            <>
                              <Mail className="w-3 h-3 mr-1" />
                              Direct
                            </>
                          )}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        {member.acceptedAt ? (
                          <Badge variant="secondary" className="text-xs">
                            Active
                          </Badge>
                        ) : (
                          <Badge variant="outline" className="text-xs">
                            Pending
                          </Badge>
                        )}
                      </TableCell>
                      <TableCell>
                        {member.role !== "owner" && (
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => {
                              if (
                                window.confirm(
                                  `Remove ${
                                    member.user?.name || member.userEmail
                                  } from this project?`
                                )
                              ) {
                                onRemoveMember(member.id);
                              }
                            }}
                          >
                            <Trash2 className="w-4 h-4 text-destructive" />
                          </Button>
                        )}
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </div>
        </div>

        <div className="flex justify-between items-center pt-4 border-t">
          <p className="text-xs text-muted-foreground">
            💡 Owner: Full access | Editor: Can edit hierarchies | Viewer:
            Read-only
          </p>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Close
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
};
