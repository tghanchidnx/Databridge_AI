import React from "react";
import type { SmartHierarchyMaster } from "@/services/api/hierarchy";
import { smartHierarchyService } from "@/services/api/hierarchy";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { FileDown, Database } from "lucide-react";
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
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

interface ScriptDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  generatedScripts: {
    insert?: string;
    view?: string;
    mapping?: string;
    dynamicTable?: string;
    hierarchyNames?: string[];
  };
  selectedHierarchiesCount: number;
  selectedHierarchyId: string | null;
  onClearSelection: () => void;
  onOpenDeployment: () => void;
  toast: any;
}

export const ScriptDialog: React.FC<ScriptDialogProps> = ({
  open,
  onOpenChange,
  generatedScripts,
  selectedHierarchiesCount,
  selectedHierarchyId,
  onClearSelection,
  onOpenDeployment,
  toast,
}) => {
  const [activeScriptTab, setActiveScriptTab] = React.useState<
    "insert" | "view" | "mapping" | "dynamicTable"
  >("insert");

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-[90vw]! w-[90vw] max-h-[90vh] overflow-hidden flex flex-col">
        <DialogHeader>
          <DialogTitle>Generated SQL Scripts</DialogTitle>
        </DialogHeader>

        <Tabs
          value={activeScriptTab}
          onValueChange={(v) => setActiveScriptTab(v as any)}
          className="flex-1 flex flex-col overflow-hidden"
        >
          <TabsList className="grid w-full grid-cols-4">
            {generatedScripts.insert && (
              <TabsTrigger value="insert">INSERT Script</TabsTrigger>
            )}
            {generatedScripts.view && (
              <TabsTrigger value="view">VIEW Script</TabsTrigger>
            )}
            {generatedScripts.mapping && (
              <TabsTrigger value="mapping">MAPPING View</TabsTrigger>
            )}
            {generatedScripts.dynamicTable && (
              <TabsTrigger value="dynamicTable">
                Dynamic Table Script
              </TabsTrigger>
            )}
          </TabsList>

          {generatedScripts.insert && (
            <TabsContent
              value="insert"
              className="flex-1 flex flex-col overflow-auto mt-4"
            >
              <div className="relative flex-1 overflow-auto">
                <Textarea
                  value={generatedScripts.insert}
                  readOnly
                  className="font-mono text-xs h-full w-full resize-none"
                />
                <Button
                  size="sm"
                  className="absolute top-2 right-2"
                  onClick={() => {
                    navigator.clipboard.writeText(generatedScripts.insert!);
                    toast({
                      title: "Copied to clipboard",
                      description: "INSERT script copied",
                    });
                  }}
                >
                  Copy
                </Button>
              </div>
              <div className="flex gap-2 mt-4">
                <Button
                  size="sm"
                  onClick={() => {
                    smartHierarchyService.downloadScript(
                      generatedScripts.insert!,
                      "hierarchy-insert.sql"
                    );
                    toast({
                      title: "Downloaded",
                      description: "INSERT script downloaded",
                    });
                  }}
                >
                  <FileDown className="w-4 h-4 mr-2" />
                  Download INSERT
                </Button>
              </div>
            </TabsContent>
          )}

          {generatedScripts.view && (
            <TabsContent
              value="view"
              className="flex-1 flex flex-col overflow-hidden mt-4"
            >
              <div className="relative flex-1 overflow-auto">
                <Textarea
                  value={generatedScripts.view}
                  readOnly
                  className="font-mono text-xs h-full w-full resize-none"
                />
                <Button
                  size="sm"
                  className="absolute top-2 right-2"
                  onClick={() => {
                    navigator.clipboard.writeText(generatedScripts.view!);
                    toast({
                      title: "Copied to clipboard",
                      description: "VIEW script copied",
                    });
                  }}
                >
                  Copy
                </Button>
              </div>
              <div className="flex gap-2 mt-4">
                <Button
                  size="sm"
                  onClick={() => {
                    smartHierarchyService.downloadScript(
                      generatedScripts.view!,
                      "hierarchy-view.sql"
                    );
                    toast({
                      title: "Downloaded",
                      description: "VIEW script downloaded",
                    });
                  }}
                >
                  <FileDown className="w-4 h-4 mr-2" />
                  Download VIEW
                </Button>
              </div>
            </TabsContent>
          )}

          {generatedScripts.mapping && (
            <TabsContent
              value="mapping"
              className="flex-1 flex flex-col overflow-hidden mt-4"
            >
              <div className="relative flex-1 overflow-auto">
                <Textarea
                  value={generatedScripts.mapping}
                  readOnly
                  className="font-mono text-xs h-full w-full resize-none"
                />
                <Button
                  size="sm"
                  className="absolute top-2 right-2"
                  onClick={() => {
                    navigator.clipboard.writeText(generatedScripts.mapping!);
                    toast({
                      title: "Copied to clipboard",
                      description: "MAPPING VIEW script copied",
                    });
                  }}
                >
                  Copy
                </Button>
              </div>
              <div className="flex gap-2 mt-4">
                <Button
                  size="sm"
                  onClick={() => {
                    smartHierarchyService.downloadScript(
                      generatedScripts.mapping!,
                      "hierarchy-mapping-expansion.sql"
                    );
                    toast({
                      title: "Downloaded",
                      description: "MAPPING VIEW script downloaded",
                    });
                  }}
                >
                  <FileDown className="w-4 h-4 mr-2" />
                  Download MAPPING VIEW
                </Button>
              </div>
            </TabsContent>
          )}

          {generatedScripts.dynamicTable && (
            <TabsContent
              value="dynamicTable"
              className="flex-1 flex flex-col overflow-hidden mt-4"
            >
              <div className="relative flex-1 overflow-auto">
                <Textarea
                  value={generatedScripts.dynamicTable}
                  readOnly
                  className="font-mono text-xs h-full w-full resize-none"
                />
                <Button
                  size="sm"
                  className="absolute top-2 right-2"
                  onClick={() => {
                    navigator.clipboard.writeText(
                      generatedScripts.dynamicTable!
                    );
                    toast({
                      title: "Copied to clipboard",
                      description: "Dynamic Table script copied",
                    });
                  }}
                >
                  Copy
                </Button>
              </div>
              <div className="flex gap-2 mt-4">
                <Button
                  size="sm"
                  onClick={() => {
                    smartHierarchyService.downloadScript(
                      generatedScripts.dynamicTable!,
                      "hierarchy-dynamic-table.sql"
                    );
                    toast({
                      title: "Downloaded",
                      description: "Dynamic Table script downloaded",
                    });
                  }}
                >
                  <FileDown className="w-4 h-4 mr-2" />
                  Download Dynamic Table
                </Button>
              </div>
            </TabsContent>
          )}
        </Tabs>

        <DialogFooter className="mt-4">
          <Button variant="outline" onClick={onOpenDeployment}>
            <Database className="w-4 h-4 mr-2" />
            Deploy to Database
          </Button>
          <Button onClick={() => onOpenChange(false)}>Close</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};
