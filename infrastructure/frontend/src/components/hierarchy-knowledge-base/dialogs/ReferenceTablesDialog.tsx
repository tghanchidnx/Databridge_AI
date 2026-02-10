import React, { useState, useEffect, useCallback } from 'react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { useToast } from '@/hooks/use-toast';
import { useDropzone } from 'react-dropzone';
import {
  referenceTableService,
  ReferenceTableSummary,
} from '@/services/api/hierarchy/reference-table.service';
import {
  Upload,
  FileSpreadsheet,
  Trash2,
  RefreshCw,
  Database,
  Table2,
  Columns,
  FileText,
} from 'lucide-react';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog';

interface ReferenceTablesDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onTablesChanged?: () => void;
}

export function ReferenceTablesDialog({ open, onOpenChange, onTablesChanged }: ReferenceTablesDialogProps) {
  const [tables, setTables] = useState<ReferenceTableSummary[]>([]);
  const [loading, setLoading] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [deleteConfirm, setDeleteConfirm] = useState<string | null>(null);
  const { toast } = useToast();

  const loadTables = useCallback(async () => {
    setLoading(true);
    try {
      const result = await referenceTableService.listTables();
      setTables(result || []);
    } catch (error: any) {
      toast({
        title: 'Error',
        description: error.message || 'Failed to load reference tables',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  }, [toast]);

  useEffect(() => {
    if (open) {
      loadTables();
    }
  }, [open, loadTables]);

  const onDrop = useCallback(
    async (acceptedFiles: File[]) => {
      setUploading(true);
      let successCount = 0;
      let errorCount = 0;

      for (const file of acceptedFiles) {
        try {
          await referenceTableService.loadFromFile(file);
          successCount++;
        } catch (error: any) {
          errorCount++;
          console.error(`Failed to upload ${file.name}:`, error);
        }
      }

      setUploading(false);

      if (successCount > 0) {
        toast({
          title: 'Upload Complete',
          description: `Successfully uploaded ${successCount} table(s)${errorCount > 0 ? `, ${errorCount} failed` : ''}`,
        });
        loadTables();
        onTablesChanged?.();
      } else if (errorCount > 0) {
        toast({
          title: 'Upload Failed',
          description: `Failed to upload ${errorCount} file(s)`,
          variant: 'destructive',
        });
      }
    },
    [toast, loadTables, onTablesChanged]
  );

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'text/csv': ['.csv'],
    },
    multiple: true,
  });

  const handleDelete = async (tableName: string) => {
    try {
      await referenceTableService.deleteTable(tableName);
      toast({
        title: 'Deleted',
        description: `Reference table "${tableName}" has been deleted`,
      });
      loadTables();
      onTablesChanged?.();
    } catch (error: any) {
      toast({
        title: 'Error',
        description: error.message || 'Failed to delete table',
        variant: 'destructive',
      });
    } finally {
      setDeleteConfirm(null);
    }
  };

  const getTypeColor = (type: string) => {
    switch (type) {
      case 'string':
        return 'bg-blue-500/10 text-blue-600 border-blue-200';
      case 'number':
        return 'bg-green-500/10 text-green-600 border-green-200';
      case 'boolean':
        return 'bg-purple-500/10 text-purple-600 border-purple-200';
      case 'date':
        return 'bg-orange-500/10 text-orange-600 border-orange-200';
      default:
        return 'bg-gray-500/10 text-gray-600 border-gray-200';
    }
  };

  return (
    <>
      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent className="max-w-4xl max-h-[85vh] overflow-hidden flex flex-col">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <Database className="h-5 w-5" />
              Reference Tables
            </DialogTitle>
            <DialogDescription>
              Upload CSV files to create reference/dimension tables for use in hierarchy viewers
            </DialogDescription>
          </DialogHeader>

          <div className="flex-1 overflow-auto space-y-4">
            {/* Upload Zone */}
            <div
              {...getRootProps()}
              className={`border-2 border-dashed rounded-lg p-6 text-center cursor-pointer transition-colors ${
                isDragActive
                  ? 'border-primary bg-primary/5'
                  : 'border-muted-foreground/25 hover:border-primary/50'
              }`}
            >
              <input {...getInputProps()} />
              {uploading ? (
                <div className="flex flex-col items-center gap-2">
                  <RefreshCw className="h-8 w-8 animate-spin text-muted-foreground" />
                  <p className="text-sm text-muted-foreground">Uploading...</p>
                </div>
              ) : (
                <div className="flex flex-col items-center gap-2">
                  <Upload className="h-8 w-8 text-muted-foreground" />
                  <p className="text-sm text-muted-foreground">
                    {isDragActive
                      ? 'Drop CSV files here...'
                      : 'Drag & drop CSV files here, or click to select'}
                  </p>
                  <p className="text-xs text-muted-foreground/70">
                    Supports dimension tables (DIM_*.csv)
                  </p>
                </div>
              )}
            </div>

            {/* Tables List */}
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <h3 className="font-medium flex items-center gap-2">
                  <Table2 className="h-4 w-4" />
                  Loaded Tables ({tables.length})
                </h3>
                <Button variant="outline" size="sm" onClick={loadTables} disabled={loading}>
                  <RefreshCw className={`h-4 w-4 mr-1 ${loading ? 'animate-spin' : ''}`} />
                  Refresh
                </Button>
              </div>

              {tables.length === 0 ? (
                <Card>
                  <CardContent className="py-8 text-center text-muted-foreground">
                    <FileSpreadsheet className="h-12 w-12 mx-auto mb-2 opacity-50" />
                    <p>No reference tables loaded yet</p>
                    <p className="text-sm">Upload CSV files to get started</p>
                  </CardContent>
                </Card>
              ) : (
                <div className="rounded-md border">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Table Name</TableHead>
                        <TableHead>Source File</TableHead>
                        <TableHead className="text-center">Rows</TableHead>
                        <TableHead>Columns</TableHead>
                        <TableHead className="w-[80px]"></TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {tables.map((table) => (
                        <TableRow key={table.id}>
                          <TableCell>
                            <div className="font-medium">{table.displayName}</div>
                            <div className="text-xs text-muted-foreground font-mono">
                              {table.name}
                            </div>
                          </TableCell>
                          <TableCell>
                            <div className="flex items-center gap-1 text-sm text-muted-foreground">
                              <FileText className="h-3 w-3" />
                              {table.sourceFile || 'N/A'}
                            </div>
                          </TableCell>
                          <TableCell className="text-center">
                            <Badge variant="secondary">{table.rowCount.toLocaleString()}</Badge>
                          </TableCell>
                          <TableCell>
                            <div className="flex flex-wrap gap-1 max-w-[300px]">
                              {table.columns.slice(0, 4).map((col) => (
                                <Badge
                                  key={col.name}
                                  variant="outline"
                                  className={`text-xs ${getTypeColor(col.type)}`}
                                >
                                  {col.name}
                                </Badge>
                              ))}
                              {table.columns.length > 4 && (
                                <Badge variant="outline" className="text-xs">
                                  +{table.columns.length - 4} more
                                </Badge>
                              )}
                            </div>
                          </TableCell>
                          <TableCell>
                            <Button
                              variant="ghost"
                              size="icon"
                              className="h-8 w-8 text-destructive hover:text-destructive hover:bg-destructive/10"
                              onClick={() => setDeleteConfirm(table.name)}
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}
            </div>
          </div>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation */}
      <AlertDialog open={!!deleteConfirm} onOpenChange={() => setDeleteConfirm(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Reference Table?</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete the reference table "{deleteConfirm}"? This action
              cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onClick={() => deleteConfirm && handleDelete(deleteConfirm)}
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}
