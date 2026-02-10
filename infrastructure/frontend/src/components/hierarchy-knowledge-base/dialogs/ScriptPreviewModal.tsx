import { X } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ScrollArea } from "@/components/ui/scroll-area";

interface ScriptPreviewModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  deployment: any;
}

export function ScriptPreviewModal({
  open,
  onOpenChange,
  deployment,
}: ScriptPreviewModalProps) {
  if (!deployment) return null;

  const hasInsertScript = deployment.insertScript;
  const hasViewScript = deployment.viewScript;
  const hasMappingScript = deployment.mappingScript;
  const hasDynamicTableScript = deployment.dynamicTableScript;

  const downloadScript = (script: string, filename: string) => {
    const blob = new Blob([script], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-4xl! h-[80vh]">
        <DialogHeader>
          <DialogTitle>Deployment Scripts</DialogTitle>
          <div className="text-sm text-muted-foreground">
            Deployed on {new Date(deployment.deployedAt).toLocaleString()} by{" "}
            {deployment.deployedBy}
          </div>
        </DialogHeader>

        <Tabs defaultValue="insert" className=" w-full flex flex-col mt-4">
          <TabsList className="grid grid-cols-4">
            <TabsTrigger value="insert" disabled={!hasInsertScript}>
              Insert Script
            </TabsTrigger>
            <TabsTrigger value="view" disabled={!hasViewScript}>
              View Script
            </TabsTrigger>
            <TabsTrigger value="mapping" disabled={!hasMappingScript}>
              Mapping Script
            </TabsTrigger>
            <TabsTrigger value="dynamic" disabled={!hasDynamicTableScript}>
              Dynamic Table
            </TabsTrigger>
          </TabsList>

          {hasInsertScript && (
            <TabsContent value="insert" className="max-w-[850px] overflow-auto">
              <div className="space-y-2 w-full overflow-auto">
                <div className="flex justify-end">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() =>
                      downloadScript(
                        deployment.insertScript,
                        `insert_script_${deployment.id}.sql`
                      )
                    }
                    // className="m-[-40px] "
                  >
                    Download
                  </Button>
                </div>
                <ScrollArea className="h-[calc(80vh-200px)] w-full rounded-md  p-4">
                  <pre className="text-xs">
                    <code>{deployment.insertScript}</code>
                  </pre>
                </ScrollArea>
              </div>
            </TabsContent>
          )}

          {hasViewScript && (
            <TabsContent value="view" className="max-w-[850px] overflow-auto">
              <div className="space-y-2">
                <div className="flex justify-end">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() =>
                      downloadScript(
                        deployment.viewScript,
                        `view_script_${deployment.id}.sql`
                      )
                    }
                  >
                    Download
                  </Button>
                </div>
                <ScrollArea className="h-[calc(80vh-200px)] rounded-md  p-4">
                  <pre className="text-xs">
                    <code>{deployment.viewScript}</code>
                  </pre>
                </ScrollArea>
              </div>
            </TabsContent>
          )}

          {hasMappingScript && (
            <TabsContent
              value="mapping"
              className="max-w-[850px] overflow-auto"
            >
              <div className="space-y-2">
                <div className="flex justify-end">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() =>
                      downloadScript(
                        deployment.mappingScript,
                        `mapping_script_${deployment.id}.sql`
                      )
                    }
                  >
                    Download
                  </Button>
                </div>
                <ScrollArea className="h-[calc(80vh-200px)] rounded-md  p-4">
                  <pre className="text-xs">
                    <code>{deployment.mappingScript}</code>
                  </pre>
                </ScrollArea>
              </div>
            </TabsContent>
          )}

          {hasDynamicTableScript && (
            <TabsContent value="dynamic" className="flex-1">
              <div className="space-y-2">
                <div className="flex justify-end">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() =>
                      downloadScript(
                        deployment.dynamicTableScript,
                        `dynamic_table_${deployment.id}.sql`
                      )
                    }
                  >
                    Download
                  </Button>
                </div>
                <ScrollArea className="h-[calc(80vh-200px)] rounded-md border p-4">
                  <pre className="text-xs">
                    <code>{deployment.dynamicTableScript}</code>
                  </pre>
                </ScrollArea>
              </div>
            </TabsContent>
          )}
        </Tabs>
      </DialogContent>
    </Dialog>
  );
}
