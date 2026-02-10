import React from "react";
import type { SmartHierarchyMaster } from "@/services/api/hierarchy";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Switch } from "@/components/ui/switch";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
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

interface CreateHierarchyDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  formData: {
    name: string;
    description: string;
    parentId: string | null;
    isRoot: boolean;
  };
  onFormChange: (data: Partial<CreateHierarchyDialogProps["formData"]>) => void;
  allHierarchies: SmartHierarchyMaster[];
  onCreate: () => void;
}

export const CreateHierarchyDialog: React.FC<CreateHierarchyDialogProps> = ({
  open,
  onOpenChange,
  formData,
  onFormChange,
  allHierarchies,
  onCreate,
}) => {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Create New Hierarchy</DialogTitle>
          <DialogDescription>
            Create a new hierarchy for your knowledge base
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-4">
          <div className="space-y-2">
            <Label htmlFor="name">Hierarchy Name *</Label>
            <Input
              id="name"
              value={formData.name}
              onChange={(e) =>
                onFormChange({
                  name: e.target.value,
                })
              }
              placeholder="e.g., Financial Hierarchy"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="description">Description</Label>
            <Textarea
              id="description"
              value={formData.description}
              onChange={(e) =>
                onFormChange({
                  description: e.target.value,
                })
              }
              placeholder="Describe the purpose of this hierarchy"
              rows={3}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="parent">Parent Hierarchy (Optional)</Label>
            <Select
              value={formData.parentId || "none"}
              onValueChange={(v) =>
                onFormChange({
                  parentId: v === "none" ? null : v,
                  isRoot: v === "none",
                })
              }
            >
              <SelectTrigger>
                <SelectValue placeholder="No parent (root hierarchy)" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="none">No parent (root hierarchy)</SelectItem>
                {allHierarchies
                  .filter((h) => h.isRoot)
                  .map((h) => (
                    <SelectItem key={h.hierarchyId} value={h.hierarchyId}>
                      {h.hierarchyName}
                    </SelectItem>
                  ))}
              </SelectContent>
            </Select>
          </div>
          <div className="flex items-center space-x-2">
            <Switch
              id="isRoot"
              checked={formData.isRoot}
              onCheckedChange={(checked) =>
                onFormChange({
                  isRoot: checked,
                  parentId: checked ? null : formData.parentId,
                })
              }
              disabled={!!formData.parentId}
            />
            <Label htmlFor="isRoot">Root Hierarchy (Level 0)</Label>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={onCreate}>Create Hierarchy</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};
