/**
 * Live Data Preview
 * Real-time data preview with query generation and execution
 */
import { useState, useCallback, useMemo, useEffect } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ScrollArea, ScrollBar } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Database,
  Play,
  Loader2,
  Download,
  RefreshCw,
  Copy,
  Check,
  Clock,
  AlertCircle,
  Eye,
  Code2,
  BarChart3,
  TableIcon,
  ArrowUpDown,
  ChevronLeft,
  ChevronRight,
  Hash,
  Type,
  Calendar,
  ToggleLeft,
} from "lucide-react";

export interface ColumnInfo {
  name: string;
  type: string;
  nullable: boolean;
}

export interface PreviewResult {
  columns: ColumnInfo[];
  rows: Record<string, any>[];
  rowCount: number;
  executionTimeMs: number;
  truncated: boolean;
  query: string;
}

export interface ColumnStats {
  min?: number | string;
  max?: number | string;
  distinctCount?: number;
  nullCount?: number;
}

interface LiveDataPreviewProps {
  query: string;
  connectionId: string;
  connectionName?: string;
  onExecute: (query: string, limit: number) => Promise<PreviewResult>;
  onExport?: (format: "csv" | "xlsx") => void;
  isLoading?: boolean;
  error?: string;
  result?: PreviewResult;
  className?: string;
}

function getColumnIcon(type: string) {
  const lowerType = type.toLowerCase();
  if (lowerType.includes("int") || lowerType.includes("float") || lowerType.includes("number") || lowerType.includes("decimal")) {
    return <Hash className="h-3.5 w-3.5" />;
  }
  if (lowerType.includes("date") || lowerType.includes("time")) {
    return <Calendar className="h-3.5 w-3.5" />;
  }
  if (lowerType.includes("bool")) {
    return <ToggleLeft className="h-3.5 w-3.5" />;
  }
  return <Type className="h-3.5 w-3.5" />;
}

function formatValue(value: any): string {
  if (value === null || value === undefined) return "(null)";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function DataTable({
  result,
  onSort,
}: {
  result: PreviewResult;
  onSort?: (column: string) => void;
}) {
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc");

  const handleSort = (column: string) => {
    if (sortColumn === column) {
      setSortDirection((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortColumn(column);
      setSortDirection("asc");
    }
    onSort?.(column);
  };

  const sortedRows = useMemo(() => {
    if (!sortColumn) return result.rows;
    return [...result.rows].sort((a, b) => {
      const aVal = a[sortColumn];
      const bVal = b[sortColumn];
      if (aVal === null || aVal === undefined) return 1;
      if (bVal === null || bVal === undefined) return -1;
      if (aVal < bVal) return sortDirection === "asc" ? -1 : 1;
      if (aVal > bVal) return sortDirection === "asc" ? 1 : -1;
      return 0;
    });
  }, [result.rows, sortColumn, sortDirection]);

  return (
    <div className="border rounded-lg overflow-hidden">
      <ScrollArea className="w-full">
        <Table>
          <TableHeader>
            <TableRow className="bg-muted/50">
              {result.columns.map((col) => (
                <TableHead
                  key={col.name}
                  className="whitespace-nowrap cursor-pointer hover:bg-muted/70 transition-colors"
                  onClick={() => handleSort(col.name)}
                >
                  <div className="flex items-center gap-2">
                    <TooltipProvider>
                      <Tooltip>
                        <TooltipTrigger>{getColumnIcon(col.type)}</TooltipTrigger>
                        <TooltipContent>
                          <p>{col.type}</p>
                          <p className="text-xs text-muted-foreground">
                            {col.nullable ? "Nullable" : "Not Null"}
                          </p>
                        </TooltipContent>
                      </Tooltip>
                    </TooltipProvider>
                    <span>{col.name}</span>
                    {sortColumn === col.name && (
                      <ArrowUpDown className="h-3 w-3 text-primary" />
                    )}
                  </div>
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {sortedRows.map((row, rowIndex) => (
              <TableRow key={rowIndex} className="hover:bg-muted/30">
                {result.columns.map((col) => (
                  <TableCell key={col.name} className="font-mono text-sm whitespace-nowrap">
                    <span
                      className={cn(
                        row[col.name] === null || row[col.name] === undefined
                          ? "text-muted-foreground italic"
                          : ""
                      )}
                    >
                      {formatValue(row[col.name])}
                    </span>
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
        <ScrollBar orientation="horizontal" />
      </ScrollArea>
    </div>
  );
}

function ColumnStatsPanel({ result }: { result: PreviewResult }) {
  const stats = useMemo(() => {
    return result.columns.map((col) => {
      const values = result.rows.map((r) => r[col.name]).filter((v) => v != null);
      const nullCount = result.rows.length - values.length;
      const distinctCount = new Set(values.map((v) => JSON.stringify(v))).size;

      let min: any, max: any;
      if (values.length > 0) {
        if (typeof values[0] === "number") {
          min = Math.min(...(values as number[]));
          max = Math.max(...(values as number[]));
        } else {
          min = values.sort()[0];
          max = values.sort()[values.length - 1];
        }
      }

      return { ...col, min, max, distinctCount, nullCount };
    });
  }, [result]);

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3">
      {stats.map((col) => (
        <Card key={col.name} className="bg-muted/30">
          <CardContent className="p-3 space-y-2">
            <div className="flex items-center gap-2">
              {getColumnIcon(col.type)}
              <span className="font-medium text-sm truncate">{col.name}</span>
            </div>
            <div className="text-xs text-muted-foreground">{col.type}</div>
            <Separator />
            <div className="space-y-1 text-xs">
              <div className="flex justify-between">
                <span className="text-muted-foreground">Distinct:</span>
                <span className="font-mono">{col.distinctCount}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Nulls:</span>
                <span className="font-mono">{col.nullCount}</span>
              </div>
              {col.min !== undefined && (
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Min:</span>
                  <span className="font-mono truncate max-w-[100px]">{formatValue(col.min)}</span>
                </div>
              )}
              {col.max !== undefined && (
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Max:</span>
                  <span className="font-mono truncate max-w-[100px]">{formatValue(col.max)}</span>
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

function QueryPanel({ query, copied, onCopy }: { query: string; copied: boolean; onCopy: () => void }) {
  return (
    <div className="relative">
      <pre className="bg-muted rounded-lg p-4 overflow-x-auto text-sm font-mono whitespace-pre-wrap">
        {query}
      </pre>
      <Button
        variant="ghost"
        size="icon"
        className="absolute top-2 right-2"
        onClick={onCopy}
      >
        {copied ? <Check className="h-4 w-4 text-green-600" /> : <Copy className="h-4 w-4" />}
      </Button>
    </div>
  );
}

export function LiveDataPreview({
  query,
  connectionId,
  connectionName,
  onExecute,
  onExport,
  isLoading,
  error,
  result,
  className,
}: LiveDataPreviewProps) {
  const [limit, setLimit] = useState(100);
  const [copied, setCopied] = useState(false);
  const [activeTab, setActiveTab] = useState<"data" | "stats" | "query">("data");
  const [page, setPage] = useState(0);
  const pageSize = 50;

  const handleExecute = async () => {
    setPage(0);
    await onExecute(query, limit);
  };

  const handleCopyQuery = useCallback(() => {
    navigator.clipboard.writeText(query);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [query]);

  const paginatedRows = useMemo(() => {
    if (!result) return [];
    return result.rows.slice(page * pageSize, (page + 1) * pageSize);
  }, [result, page, pageSize]);

  const totalPages = result ? Math.ceil(result.rows.length / pageSize) : 0;

  return (
    <Card className={cn("", className)}>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Database className="h-5 w-5 text-primary" />
            <CardTitle className="text-base">Live Data Preview</CardTitle>
          </div>
          {connectionName && (
            <Badge variant="outline" className="gap-1">
              <Database className="h-3 w-3" />
              {connectionName}
            </Badge>
          )}
        </div>
        <CardDescription>
          Preview data from your mappings in real-time
        </CardDescription>
      </CardHeader>

      <Separator />

      {/* Controls */}
      <div className="p-4 flex items-center justify-between gap-4 bg-muted/30">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <Label htmlFor="limit" className="text-sm whitespace-nowrap">
              Row Limit:
            </Label>
            <Input
              id="limit"
              type="number"
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value) || 100)}
              className="w-20 h-8"
              min={1}
              max={10000}
            />
          </div>
          <Button onClick={handleExecute} disabled={isLoading || !query} className="gap-2">
            {isLoading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Play className="h-4 w-4" />
            )}
            Execute
          </Button>
        </div>

        <div className="flex items-center gap-2">
          {result && (
            <>
              <Badge variant="secondary" className="gap-1">
                <Clock className="h-3 w-3" />
                {result.executionTimeMs}ms
              </Badge>
              <Badge variant="outline">{result.rowCount} rows</Badge>
            </>
          )}
          {onExport && result && (
            <Button variant="outline" size="sm" onClick={() => onExport("csv")} className="gap-1">
              <Download className="h-4 w-4" />
              Export
            </Button>
          )}
        </div>
      </div>

      <Separator />

      {/* Content */}
      {error ? (
        <CardContent className="py-8">
          <div className="flex items-start gap-3 p-4 bg-red-50 border border-red-200 rounded-lg">
            <AlertCircle className="h-5 w-5 text-red-600 shrink-0 mt-0.5" />
            <div>
              <p className="font-medium text-red-800">Query Error</p>
              <pre className="text-sm text-red-700 mt-1 whitespace-pre-wrap">{error}</pre>
            </div>
          </div>
        </CardContent>
      ) : isLoading ? (
        <CardContent className="py-8 space-y-4">
          <div className="flex items-center justify-center gap-3">
            <Loader2 className="h-6 w-6 animate-spin text-primary" />
            <span className="text-sm text-muted-foreground">Executing query...</span>
          </div>
          <div className="space-y-2">
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
          </div>
        </CardContent>
      ) : result ? (
        <>
          <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as any)}>
            <div className="px-4 pt-3">
              <TabsList>
                <TabsTrigger value="data" className="gap-1">
                  <TableIcon className="h-4 w-4" />
                  Data
                </TabsTrigger>
                <TabsTrigger value="stats" className="gap-1">
                  <BarChart3 className="h-4 w-4" />
                  Column Stats
                </TabsTrigger>
                <TabsTrigger value="query" className="gap-1">
                  <Code2 className="h-4 w-4" />
                  Query
                </TabsTrigger>
              </TabsList>
            </div>

            <TabsContent value="data" className="m-0">
              <div className="p-4 space-y-4">
                <DataTable
                  result={{ ...result, rows: paginatedRows }}
                  onSort={() => {}}
                />

                {/* Pagination */}
                {totalPages > 1 && (
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">
                      Showing {page * pageSize + 1} - {Math.min((page + 1) * pageSize, result.rows.length)} of {result.rows.length}
                    </span>
                    <div className="flex items-center gap-2">
                      <Button
                        variant="outline"
                        size="icon"
                        onClick={() => setPage((p) => Math.max(0, p - 1))}
                        disabled={page === 0}
                      >
                        <ChevronLeft className="h-4 w-4" />
                      </Button>
                      <span className="text-sm">
                        Page {page + 1} of {totalPages}
                      </span>
                      <Button
                        variant="outline"
                        size="icon"
                        onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
                        disabled={page >= totalPages - 1}
                      >
                        <ChevronRight className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                )}

                {result.truncated && (
                  <div className="flex items-center gap-2 text-sm text-yellow-600 bg-yellow-50 p-2 rounded border border-yellow-200">
                    <AlertCircle className="h-4 w-4" />
                    Results truncated at {result.rowCount} rows
                  </div>
                )}
              </div>
            </TabsContent>

            <TabsContent value="stats" className="m-0">
              <ScrollArea className="max-h-[400px]">
                <div className="p-4">
                  <ColumnStatsPanel result={result} />
                </div>
              </ScrollArea>
            </TabsContent>

            <TabsContent value="query" className="m-0">
              <div className="p-4">
                <QueryPanel query={result.query} copied={copied} onCopy={handleCopyQuery} />
              </div>
            </TabsContent>
          </Tabs>
        </>
      ) : (
        <CardContent className="py-12">
          <div className="text-center">
            <Eye className="h-10 w-10 text-muted-foreground mx-auto mb-3" />
            <p className="font-medium">No data to preview</p>
            <p className="text-sm text-muted-foreground mt-1">
              Configure your mappings and click Execute to preview data
            </p>
          </div>
        </CardContent>
      )}
    </Card>
  );
}

export default LiveDataPreview;
