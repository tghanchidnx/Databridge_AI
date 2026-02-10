import React, { useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { SmartHierarchyMaster } from "@/services/api/hierarchy";
import { TotalFormulaTab } from "./TotalFormulaTab";
import { FormulaGroupTab } from "./FormulaGroupTab";

interface ManageFormulasDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  projectId: string;
  selectedHierarchyIds: string[];
  allHierarchies: SmartHierarchyMaster[];
  onSuccess?: () => void;
}

export const ManageFormulasDialog: React.FC<ManageFormulasDialogProps> = ({
  open,
  onOpenChange,
  projectId,
  selectedHierarchyIds,
  allHierarchies,
  onSuccess,
}) => {
  const [activeTab, setActiveTab] = useState<"total" | "group">("total");

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-4xl! max-h-[90vh] flex flex-col p-0 overflow-hidden">
        <DialogHeader className="px-6 pt-6 pb-4 shrink-0">
          <DialogTitle className="text-2xl font-semibold flex items-center gap-2">
            Manage Formulas
          </DialogTitle>
        </DialogHeader>

        <Tabs
          value={activeTab}
          onValueChange={(v) => setActiveTab(v as "total" | "group")}
          className="flex-1 flex flex-col overflow-hidden"
        >
          <TabsList className="grid w-full grid-cols-2 mb-4 mx-6 shrink-0">
            <TabsTrigger value="total">Total Formula</TabsTrigger>
            <TabsTrigger value="group">Formula Group</TabsTrigger>
          </TabsList>

          <TabsContent
            value="total"
            className="flex-1 mt-0 overflow-auto px-6 pb-6"
          >
            <TotalFormulaTab
              projectId={projectId}
              selectedHierarchyIds={selectedHierarchyIds}
              allHierarchies={allHierarchies}
              onSuccess={onSuccess}
            />
          </TabsContent>

          <TabsContent
            value="group"
            className="flex-1 mt-0 overflow-auto px-6 pb-6"
          >
            <FormulaGroupTab
              projectId={projectId}
              selectedHierarchyIds={selectedHierarchyIds}
              allHierarchies={allHierarchies}
              onSuccess={onSuccess}
            />
          </TabsContent>
        </Tabs>
      </DialogContent>
    </Dialog>
  );
};
