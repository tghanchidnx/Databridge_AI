import React from "react";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Checkbox } from "@/components/ui/checkbox";
import { FileUp, AlertCircle } from "lucide-react";
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

interface ImportDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  importFormat: "json" | "csv";
  onFormatChange: (format: "json" | "csv") => void;
  importFile: File | null;
  onFileChange: (file: File | null) => void;
  onImport: () => void;
  isLegacyFormat?: boolean;
  onLegacyFormatChange?: (isLegacy: boolean) => void;
}

export const ImportDialog: React.FC<ImportDialogProps> = ({
  open,
  onOpenChange,
  importFormat,
  onFormatChange,
  importFile,
  onFileChange,
  onImport,
  isLegacyFormat = true,
  onLegacyFormatChange,
}) => {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Import Project</DialogTitle>
          <DialogDescription>
            Import hierarchies into the current project from a file
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="import-format">Import Format</Label>
            <Select value={importFormat} onValueChange={onFormatChange}>
              <SelectTrigger id="import-format" className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="json">JSON</SelectItem>
                <SelectItem value="csv">CSV</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label htmlFor="import-file">Select File</Label>
            <input
              id="import-file"
              type="file"
              accept={importFormat === "json" ? ".json" : ".csv"}
              className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
              onChange={(e) => {
                const file = e.target.files?.[0];
                if (file) onFileChange(file);
              }}
            />
            {importFile && (
              <p className="text-xs text-muted-foreground">
                Selected: {importFile.name}
              </p>
            )}
          </div>

          {/* Legacy CSV Format Checkbox - only show for CSV imports */}
          {importFormat === "csv" && (
            <div className="flex items-start space-x-3 rounded-md border p-3 bg-amber-50 dark:bg-amber-950/20">
              <Checkbox
                id="legacy-format"
                checked={isLegacyFormat}
                onCheckedChange={(checked) =>
                  onLegacyFormatChange?.(checked as boolean)
                }
              />
              <div className="space-y-1">
                <Label
                  htmlFor="legacy-format"
                  className="text-sm font-medium leading-none cursor-pointer flex items-center gap-2"
                >
                  <AlertCircle className="w-4 h-4 text-amber-600" />
                  Legacy/Older CSV Format
                </Label>
                <p className="text-xs text-muted-foreground">
                  Check this if importing from an older version of the application.
                  This adjusts column mapping and parsing rules for compatibility.
                </p>
              </div>
            </div>
          )}

          {/* CSV Import Instructions */}
          {importFormat === "csv" && (
            <div className="rounded-md bg-blue-50 dark:bg-blue-950/20 border border-blue-200 dark:border-blue-800 p-3 text-sm">
              <p className="font-medium mb-1 text-blue-800 dark:text-blue-200">📁 CSV Import requires 2 files:</p>
              <ul className="list-disc list-inside space-y-1 text-blue-700 dark:text-blue-300 text-xs">
                <li><strong>_HIERARCHY.CSV</strong> - Hierarchy structure (import first)</li>
                <li><strong>_HIERARCHY_MAPPING.CSV</strong> - Source mappings (import second)</li>
              </ul>
            </div>
          )}

          <div className="rounded-md bg-muted p-3 text-sm">
            <p className="font-medium mb-1">⚠️ Import Notes:</p>
            <ul className="list-disc list-inside space-y-1 text-muted-foreground">
              <li>Existing hierarchies with same ID will be skipped</li>
              <li>All hierarchies will be imported into the current project</li>
              <li>Parent-child relationships will be preserved</li>
            </ul>
          </div>
        </div>

        <DialogFooter>
          <Button
            variant="outline"
            onClick={() => {
              onOpenChange(false);
              onFileChange(null);
            }}
          >
            Cancel
          </Button>
          <Button onClick={onImport} disabled={!importFile}>
            <FileUp className="w-4 h-4 mr-2" />
            Import from {importFormat.toUpperCase()}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};
