/**
 * Excel Import Dialog
 * Drag-and-drop Excel upload with AI-powered column mapping
 */
import { useState, useCallback, useMemo } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Label } from "@/components/ui/label";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  FileSpreadsheet,
  Upload,
  Check,
  X,
  AlertCircle,
  Sparkles,
  ArrowRight,
  Loader2,
  FileText,
  Table2,
  Download,
  RefreshCw,
  Info,
} from "lucide-react";

interface SheetData {
  name: string;
  rowCount: number;
  columnCount: number;
  headers: string[];
  sampleRows: Record<string, any>[];
}

interface ExcelParseResult {
  fileName: string;
  sheets: SheetData[];
  detectedFormat: "hierarchy" | "mapping" | "combined" | "unknown";
  suggestions: {
    hierarchyColumns: string[];
    mappingColumns: string[];
    levelColumns: string[];
    idColumn?: string;
    parentColumn?: string;
    nameColumn?: string;
  };
}

interface ColumnMapping {
  excelColumn: string;
  targetField: string;
  transform?: "none" | "uppercase" | "lowercase" | "trim" | "number";
}

interface ExcelImportDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  projectId: string;
  onImport: (data: any) => Promise<void>;
  onDownloadTemplate?: () => void;
}

const TARGET_FIELDS = [
  { value: "hierarchyId", label: "Hierarchy ID", required: true },
  { value: "hierarchyName", label: "Hierarchy Name", required: true },
  { value: "parentId", label: "Parent ID", required: false },
  { value: "description", label: "Description", required: false },
  { value: "level_1", label: "Level 1", required: false },
  { value: "level_2", label: "Level 2", required: false },
  { value: "level_3", label: "Level 3", required: false },
  { value: "level_4", label: "Level 4", required: false },
  { value: "level_5", label: "Level 5", required: false },
  { value: "level_6", label: "Level 6", required: false },
  { value: "level_7", label: "Level 7", required: false },
  { value: "level_8", label: "Level 8", required: false },
  { value: "level_9", label: "Level 9", required: false },
  { value: "level_10", label: "Level 10", required: false },
  { value: "sortOrder", label: "Sort Order", required: false },
  { value: "source_database", label: "Source Database", required: false },
  { value: "source_schema", label: "Source Schema", required: false },
  { value: "source_table", label: "Source Table", required: false },
  { value: "source_column", label: "Source Column", required: false },
  { value: "join_type", label: "Join Type", required: false },
  { value: "system_type", label: "System Type", required: false },
  { value: "dimension_role", label: "Dimension Role", required: false },
  { value: "ignore", label: "(Ignore this column)", required: false },
];

function DropZone({
  onFileSelect,
  isDragging,
}: {
  onFileSelect: (file: File) => void;
  isDragging: boolean;
}) {
  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      const files = e.dataTransfer.files;
      if (files.length > 0) {
        onFileSelect(files[0]);
      }
    },
    [onFileSelect]
  );

  const handleChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const files = e.target.files;
      if (files && files.length > 0) {
        onFileSelect(files[0]);
      }
    },
    [onFileSelect]
  );

  return (
    <div
      className={cn(
        "border-2 border-dashed rounded-lg p-12 text-center transition-colors",
        isDragging
          ? "border-primary bg-primary/5"
          : "border-muted-foreground/25 hover:border-primary/50"
      )}
      onDrop={handleDrop}
      onDragOver={(e) => e.preventDefault()}
    >
      <input
        type="file"
        accept=".xlsx,.xls"
        onChange={handleChange}
        className="hidden"
        id="excel-upload"
      />
      <label htmlFor="excel-upload" className="cursor-pointer">
        <div className="flex flex-col items-center gap-4">
          <div className="p-4 bg-primary/10 rounded-full">
            <FileSpreadsheet className="h-10 w-10 text-primary" />
          </div>
          <div>
            <p className="font-medium">Drag and drop your Excel file here</p>
            <p className="text-sm text-muted-foreground mt-1">
              or click to browse (.xlsx, .xls)
            </p>
          </div>
        </div>
      </label>
    </div>
  );
}

function ColumnMappingTable({
  headers,
  mappings,
  suggestions,
  onUpdateMapping,
}: {
  headers: string[];
  mappings: Map<string, ColumnMapping>;
  suggestions: ExcelParseResult["suggestions"];
  onUpdateMapping: (excelColumn: string, targetField: string) => void;
}) {
  const getAutoDetectedField = (header: string): string | undefined => {
    if (suggestions.idColumn === header) return "hierarchyId";
    if (suggestions.nameColumn === header) return "hierarchyName";
    if (suggestions.parentColumn === header) return "parentId";
    if (suggestions.levelColumns.includes(header)) {
      const match = header.match(/(\d+)/);
      if (match) return `level_${match[1]}`;
    }
    return undefined;
  };

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Excel Column</TableHead>
          <TableHead>Maps To</TableHead>
          <TableHead className="w-20">AI Suggestion</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {headers.map((header) => {
          const mapping = mappings.get(header);
          const autoDetected = getAutoDetectedField(header);

          return (
            <TableRow key={header}>
              <TableCell className="font-mono">{header}</TableCell>
              <TableCell>
                <Select
                  value={mapping?.targetField || "ignore"}
                  onValueChange={(value) => onUpdateMapping(header, value)}
                >
                  <SelectTrigger className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {TARGET_FIELDS.map((field) => (
                      <SelectItem key={field.value} value={field.value}>
                        {field.label}
                        {field.required && " *"}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </TableCell>
              <TableCell>
                {autoDetected && (
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger>
                        <Badge
                          variant="outline"
                          className="gap-1 cursor-pointer"
                          onClick={() => onUpdateMapping(header, autoDetected)}
                        >
                          <Sparkles className="h-3 w-3" />
                          {autoDetected}
                        </Badge>
                      </TooltipTrigger>
                      <TooltipContent>Click to apply AI suggestion</TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                )}
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}

function DataPreviewTable({
  headers,
  rows,
  mappings,
}: {
  headers: string[];
  rows: Record<string, any>[];
  mappings: Map<string, ColumnMapping>;
}) {
  const mappedHeaders = headers.filter((h) => {
    const mapping = mappings.get(h);
    return mapping && mapping.targetField !== "ignore";
  });

  return (
    <div className="border rounded-lg overflow-hidden">
      <ScrollArea className="w-full">
        <Table>
          <TableHeader>
            <TableRow className="bg-muted/50">
              {mappedHeaders.map((header) => (
                <TableHead key={header} className="whitespace-nowrap">
                  <div className="flex flex-col gap-0.5">
                    <span className="text-xs text-muted-foreground">{header}</span>
                    <ArrowRight className="h-3 w-3 text-muted-foreground" />
                    <span className="font-medium">{mappings.get(header)?.targetField}</span>
                  </div>
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row, i) => (
              <TableRow key={i}>
                {mappedHeaders.map((header) => (
                  <TableCell key={header} className="font-mono text-sm">
                    {row[header] ?? "(empty)"}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </ScrollArea>
    </div>
  );
}

export function ExcelImportDialog({
  open,
  onOpenChange,
  projectId,
  onImport,
  onDownloadTemplate,
}: ExcelImportDialogProps) {
  const [step, setStep] = useState<"upload" | "mapping" | "preview" | "importing">("upload");
  const [file, setFile] = useState<File | null>(null);
  const [parseResult, setParseResult] = useState<ExcelParseResult | null>(null);
  const [selectedSheet, setSelectedSheet] = useState<string>("");
  const [columnMappings, setColumnMappings] = useState<Map<string, ColumnMapping>>(new Map());
  const [isDragging, setIsDragging] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [importProgress, setImportProgress] = useState(0);
  const [conflictResolution, setConflictResolution] = useState<"merge" | "replace" | "skip">("merge");

  const currentSheet = useMemo(() => {
    return parseResult?.sheets.find((s) => s.name === selectedSheet);
  }, [parseResult, selectedSheet]);

  const handleFileSelect = useCallback(async (selectedFile: File) => {
    setFile(selectedFile);
    setIsLoading(true);
    setError(null);

    try {
      // In production, call API to parse file
      // Simulating API response
      await new Promise((r) => setTimeout(r, 1000));

      const mockResult: ExcelParseResult = {
        fileName: selectedFile.name,
        sheets: [
          {
            name: "Hierarchies",
            rowCount: 50,
            columnCount: 15,
            headers: [
              "HIERARCHY_ID",
              "HIERARCHY_NAME",
              "PARENT_ID",
              "DESCRIPTION",
              "LEVEL_1",
              "LEVEL_2",
              "LEVEL_3",
              "SORT_ORDER",
            ],
            sampleRows: [
              {
                HIERARCHY_ID: "REVENUE_001",
                HIERARCHY_NAME: "Total Revenue",
                PARENT_ID: "",
                DESCRIPTION: "Root revenue node",
                LEVEL_1: "Revenue",
                LEVEL_2: "",
                LEVEL_3: "",
                SORT_ORDER: 1,
              },
              {
                HIERARCHY_ID: "REVENUE_002",
                HIERARCHY_NAME: "Product Revenue",
                PARENT_ID: "REVENUE_001",
                DESCRIPTION: "Product sales",
                LEVEL_1: "Revenue",
                LEVEL_2: "Product",
                LEVEL_3: "",
                SORT_ORDER: 2,
              },
            ],
          },
          {
            name: "Mappings",
            rowCount: 30,
            columnCount: 10,
            headers: [
              "HIERARCHY_ID",
              "SOURCE_DATABASE",
              "SOURCE_SCHEMA",
              "SOURCE_TABLE",
              "SOURCE_COLUMN",
              "SYSTEM_TYPE",
            ],
            sampleRows: [
              {
                HIERARCHY_ID: "REVENUE_001",
                SOURCE_DATABASE: "FINANCE_DB",
                SOURCE_SCHEMA: "GL",
                SOURCE_TABLE: "FACT_REVENUE",
                SOURCE_COLUMN: "AMOUNT",
                SYSTEM_TYPE: "ACTUALS",
              },
            ],
          },
        ],
        detectedFormat: "combined",
        suggestions: {
          hierarchyColumns: ["HIERARCHY_ID", "HIERARCHY_NAME", "PARENT_ID", "LEVEL_1", "LEVEL_2", "LEVEL_3"],
          mappingColumns: ["SOURCE_DATABASE", "SOURCE_SCHEMA", "SOURCE_TABLE", "SOURCE_COLUMN"],
          levelColumns: ["LEVEL_1", "LEVEL_2", "LEVEL_3"],
          idColumn: "HIERARCHY_ID",
          parentColumn: "PARENT_ID",
          nameColumn: "HIERARCHY_NAME",
        },
      };

      setParseResult(mockResult);
      setSelectedSheet(mockResult.sheets[0]?.name || "");

      // Auto-map columns based on AI suggestions
      const autoMappings = new Map<string, ColumnMapping>();
      for (const sheet of mockResult.sheets) {
        for (const header of sheet.headers) {
          let targetField = "ignore";
          if (header === mockResult.suggestions.idColumn) targetField = "hierarchyId";
          else if (header === mockResult.suggestions.nameColumn) targetField = "hierarchyName";
          else if (header === mockResult.suggestions.parentColumn) targetField = "parentId";
          else if (header.toLowerCase().includes("description")) targetField = "description";
          else if (/level.?(\d+)/i.test(header)) {
            const match = header.match(/(\d+)/);
            if (match) targetField = `level_${match[1]}`;
          } else if (header.toLowerCase().includes("sort")) targetField = "sortOrder";
          else if (header.toLowerCase().includes("database")) targetField = "source_database";
          else if (header.toLowerCase().includes("schema")) targetField = "source_schema";
          else if (header.toLowerCase().includes("table")) targetField = "source_table";
          else if (header.toLowerCase().includes("column")) targetField = "source_column";
          else if (header.toLowerCase().includes("system")) targetField = "system_type";

          autoMappings.set(header, { excelColumn: header, targetField });
        }
      }
      setColumnMappings(autoMappings);
      setStep("mapping");
    } catch (err: any) {
      setError(err.message || "Failed to parse Excel file");
    } finally {
      setIsLoading(false);
    }
  }, []);

  const handleUpdateMapping = useCallback((excelColumn: string, targetField: string) => {
    setColumnMappings((prev) => {
      const updated = new Map(prev);
      updated.set(excelColumn, { excelColumn, targetField });
      return updated;
    });
  }, []);

  const handleImport = useCallback(async () => {
    if (!parseResult || !currentSheet) return;

    setStep("importing");
    setImportProgress(0);

    try {
      // Simulate import progress
      for (let i = 0; i <= 100; i += 10) {
        await new Promise((r) => setTimeout(r, 200));
        setImportProgress(i);
      }

      await onImport({
        projectId,
        sheetName: selectedSheet,
        columnMappings: Array.from(columnMappings.values()),
        conflictResolution,
      });

      onOpenChange(false);
    } catch (err: any) {
      setError(err.message || "Import failed");
      setStep("preview");
    }
  }, [parseResult, currentSheet, projectId, selectedSheet, columnMappings, conflictResolution, onImport, onOpenChange]);

  const handleReset = useCallback(() => {
    setStep("upload");
    setFile(null);
    setParseResult(null);
    setColumnMappings(new Map());
    setError(null);
  }, []);

  const requiredFieldsMapped = useMemo(() => {
    const mappedFields = new Set(
      Array.from(columnMappings.values())
        .filter((m) => m.targetField !== "ignore")
        .map((m) => m.targetField)
    );
    return mappedFields.has("hierarchyId") && mappedFields.has("hierarchyName");
  }, [columnMappings]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-4xl max-h-[90vh]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <FileSpreadsheet className="h-5 w-5" />
            Import from Excel
          </DialogTitle>
          <DialogDescription>
            Upload an Excel file to import hierarchies and mappings
          </DialogDescription>
        </DialogHeader>

        {/* Progress Steps */}
        <div className="flex items-center gap-2 py-2">
          {["upload", "mapping", "preview", "importing"].map((s, i) => (
            <div key={s} className="flex items-center gap-2">
              <div
                className={cn(
                  "w-8 h-8 rounded-full flex items-center justify-center text-sm font-medium",
                  step === s
                    ? "bg-primary text-primary-foreground"
                    : i < ["upload", "mapping", "preview", "importing"].indexOf(step)
                    ? "bg-green-500 text-white"
                    : "bg-muted text-muted-foreground"
                )}
              >
                {i < ["upload", "mapping", "preview", "importing"].indexOf(step) ? (
                  <Check className="h-4 w-4" />
                ) : (
                  i + 1
                )}
              </div>
              {i < 3 && <div className="w-12 h-0.5 bg-muted" />}
            </div>
          ))}
        </div>

        <Separator />

        <ScrollArea className="max-h-[60vh]">
          <div className="p-1">
            {/* Step 1: Upload */}
            {step === "upload" && (
              <div className="space-y-6">
                <DropZone onFileSelect={handleFileSelect} isDragging={isDragging} />

                {isLoading && (
                  <div className="flex items-center justify-center gap-2 py-4">
                    <Loader2 className="h-5 w-5 animate-spin" />
                    <span>Parsing Excel file...</span>
                  </div>
                )}

                {error && (
                  <div className="flex items-center gap-2 p-3 bg-red-50 border border-red-200 rounded-lg text-red-700">
                    <AlertCircle className="h-5 w-5" />
                    {error}
                  </div>
                )}

                {onDownloadTemplate && (
                  <div className="flex items-center justify-center">
                    <Button variant="outline" size="sm" onClick={onDownloadTemplate} className="gap-2">
                      <Download className="h-4 w-4" />
                      Download Import Template
                    </Button>
                  </div>
                )}
              </div>
            )}

            {/* Step 2: Column Mapping */}
            {step === "mapping" && parseResult && currentSheet && (
              <div className="space-y-6">
                {/* Sheet selector */}
                {parseResult.sheets.length > 1 && (
                  <div className="flex items-center gap-4">
                    <Label>Select Sheet:</Label>
                    <Tabs value={selectedSheet} onValueChange={setSelectedSheet}>
                      <TabsList>
                        {parseResult.sheets.map((sheet) => (
                          <TabsTrigger key={sheet.name} value={sheet.name} className="gap-2">
                            <Table2 className="h-4 w-4" />
                            {sheet.name}
                            <Badge variant="secondary" className="text-xs">
                              {sheet.rowCount}
                            </Badge>
                          </TabsTrigger>
                        ))}
                      </TabsList>
                    </Tabs>
                  </div>
                )}

                {/* Format detection badge */}
                <div className="flex items-center gap-2">
                  <Badge variant="outline" className="gap-1">
                    <Sparkles className="h-3 w-3" />
                    Detected: {parseResult.detectedFormat} format
                  </Badge>
                  <span className="text-sm text-muted-foreground">
                    {currentSheet.rowCount} rows, {currentSheet.columnCount} columns
                  </span>
                </div>

                {/* Column mapping table */}
                <Card>
                  <CardHeader className="pb-3">
                    <CardTitle className="text-sm">Column Mapping</CardTitle>
                    <CardDescription>
                      Map Excel columns to hierarchy fields. Required fields are marked with *
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <ScrollArea className="max-h-[300px]">
                      <ColumnMappingTable
                        headers={currentSheet.headers}
                        mappings={columnMappings}
                        suggestions={parseResult.suggestions}
                        onUpdateMapping={handleUpdateMapping}
                      />
                    </ScrollArea>
                  </CardContent>
                </Card>

                {!requiredFieldsMapped && (
                  <div className="flex items-center gap-2 p-3 bg-yellow-50 border border-yellow-200 rounded-lg text-yellow-700">
                    <AlertCircle className="h-5 w-5" />
                    Please map Hierarchy ID and Hierarchy Name (required fields)
                  </div>
                )}
              </div>
            )}

            {/* Step 3: Preview */}
            {step === "preview" && currentSheet && (
              <div className="space-y-6">
                <Card>
                  <CardHeader className="pb-3">
                    <CardTitle className="text-sm">Data Preview</CardTitle>
                    <CardDescription>
                      First 5 rows of data that will be imported
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <DataPreviewTable
                      headers={currentSheet.headers}
                      rows={currentSheet.sampleRows}
                      mappings={columnMappings}
                    />
                  </CardContent>
                </Card>

                {/* Conflict resolution */}
                <div className="flex items-center gap-4">
                  <Label>If hierarchy exists:</Label>
                  <Select
                    value={conflictResolution}
                    onValueChange={(v) => setConflictResolution(v as any)}
                  >
                    <SelectTrigger className="w-[200px]">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="merge">Merge with existing</SelectItem>
                      <SelectItem value="replace">Replace existing</SelectItem>
                      <SelectItem value="skip">Skip duplicates</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            )}

            {/* Step 4: Importing */}
            {step === "importing" && (
              <div className="py-12 space-y-6">
                <div className="text-center">
                  <Loader2 className="h-12 w-12 animate-spin text-primary mx-auto mb-4" />
                  <p className="font-medium">Importing data...</p>
                  <p className="text-sm text-muted-foreground mt-1">
                    Please wait while we process your file
                  </p>
                </div>
                <Progress value={importProgress} className="w-full max-w-md mx-auto" />
                <p className="text-center text-sm text-muted-foreground">
                  {importProgress}% complete
                </p>
              </div>
            )}
          </div>
        </ScrollArea>

        <DialogFooter>
          {step === "upload" && (
            <Button variant="outline" onClick={() => onOpenChange(false)}>
              Cancel
            </Button>
          )}

          {step === "mapping" && (
            <>
              <Button variant="outline" onClick={handleReset}>
                <RefreshCw className="h-4 w-4 mr-2" />
                Start Over
              </Button>
              <Button
                onClick={() => setStep("preview")}
                disabled={!requiredFieldsMapped}
              >
                Preview Import
                <ArrowRight className="h-4 w-4 ml-2" />
              </Button>
            </>
          )}

          {step === "preview" && (
            <>
              <Button variant="outline" onClick={() => setStep("mapping")}>
                Back to Mapping
              </Button>
              <Button onClick={handleImport}>
                <Upload className="h-4 w-4 mr-2" />
                Import {currentSheet?.rowCount} Rows
              </Button>
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export default ExcelImportDialog;
