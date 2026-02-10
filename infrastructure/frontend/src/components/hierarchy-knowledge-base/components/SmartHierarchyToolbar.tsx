import React from "react";
import { smartHierarchyService } from "@/services/api/hierarchy";
import { Button } from "@/components/ui/button";
import { Plus, RefreshCw, Download, Upload, CloudUpload } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

interface SmartHierarchyToolbarProps {
  projectId: string;
  selectedHierarchyId: string | null;
  onCreateNew: () => void;
  onRefresh: () => void;
}

export const SmartHierarchyToolbar: React.FC<SmartHierarchyToolbarProps> = ({
  projectId,
  selectedHierarchyId,
  onCreateNew,
  onRefresh,
}) => {
  const { toast } = useToast();

  const handleExportProject = async () => {
    try {
      const result = await smartHierarchyService.exportProject(
        projectId,
        `Export_${new Date().toISOString().split("T")[0]}`
      );

      smartHierarchyService.downloadJSON(
        result.exportData,
        `project_${projectId}_export.json`
      );

      toast({
        title: "Project exported",
        description: "Project has been exported successfully",
      });
    } catch (error: any) {
      toast({
        title: "Export failed",
        description: error.response?.data?.message || error.message,
        variant: "destructive",
      });
    }
  };

  const handleImportProject = () => {
    const input = document.createElement("input");
    input.type = "file";
    input.accept = ".json";
    input.onchange = async (e: any) => {
      const file = e.target.files?.[0];
      if (!file) return;

      try {
        const text = await file.text();
        const exportData = JSON.parse(text);

        const result = await smartHierarchyService.importProject(
          projectId,
          exportData
        );

        toast({
          title: "Import complete",
          description: `Imported ${result.imported} hierarchies, skipped ${result.skipped}`,
        });

        onRefresh();
      } catch (error: any) {
        toast({
          title: "Import failed",
          description: error.response?.data?.message || error.message,
          variant: "destructive",
        });
      }
    };
    input.click();
  };

  const handleGenerateScripts = async () => {
    if (!selectedHierarchyId) {
      toast({
        title: "No hierarchy selected",
        description: "Please select a hierarchy first",
        variant: "destructive",
      });
      return;
    }

    try {
      const result = await smartHierarchyService.generateScripts(
        projectId,
        "all",
        [selectedHierarchyId]
      );

      // Download each script
      result.scripts.forEach((script) => {
        smartHierarchyService.downloadScript(
          script.script,
          `${selectedHierarchyId}_${script.scriptType}.sql`
        );
      });

      toast({
        title: "Scripts generated",
        description: `Generated ${result.scripts.length} SQL scripts`,
      });
    } catch (error: any) {
      toast({
        title: "Script generation failed",
        description: error.response?.data?.message || error.message,
        variant: "destructive",
      });
    }
  };

  return (
    <div className="border-b px-4 py-3 bg-muted/30">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Smart Hierarchy Builder</h2>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={onCreateNew}>
            <Plus className="w-4 h-4" />
            New Hierarchy
          </Button>
          <Button variant="outline" size="sm" onClick={onRefresh}>
            <RefreshCw className="w-4 h-4" />
          </Button>
          <Button variant="outline" size="sm" onClick={handleExportProject}>
            <Download className="w-4 h-4" />
            Export Project
          </Button>
          <Button variant="outline" size="sm" onClick={handleImportProject}>
            <Upload className="w-4 h-4" />
            Import Project
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={handleGenerateScripts}
            disabled={!selectedHierarchyId}
          >
            <CloudUpload className="w-4 h-4" />
            Generate Scripts
          </Button>
        </div>
      </div>
    </div>
  );
};
