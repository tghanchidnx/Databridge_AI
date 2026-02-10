import { useState } from 'react'
import { Copy, Download, Play, FileCode, Gear } from "@phosphor-icons/react"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Checkbox } from "@/components/ui/checkbox"
import { Label } from "@/components/ui/label"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import type { SchemaComparison, ScriptOptions } from '@/types'
import { toast } from 'sonner'

interface ScriptGeneratorProps {
  comparison: SchemaComparison
}

export function ScriptGenerator({ comparison }: ScriptGeneratorProps) {
  const [showOptions, setShowOptions] = useState(false)
  const [scriptOptions, setScriptOptions] = useState<ScriptOptions>({
    includeTransactions: true,
    includeRollbackScript: true,
    includeValidations: true,
    includeComments: true,
    includeBackups: false,
    dropBeforeCreate: false,
    useIfExists: true,
    batchSize: 100,
    targetDatabase: comparison.target.database,
    executionMode: 'manual',
    objectHandling: {
      tables: {
        onModify: 'alter',
        onMissing: 'create',
        onExtra: 'ignore'
      },
      views: {
        onModify: 'create_or_replace',
        onMissing: 'create',
        onExtra: 'ignore'
      }
    }
  })

  const deploymentScript = `-- ============================================================
-- Deployment Script: ${comparison.source.connectionName} → ${comparison.target.connectionName}
-- Generated: ${new Date().toISOString()}
-- Comparison ID: ${comparison.id}
-- ============================================================

-- ============================================================
-- SECTION 1: PRE-DEPLOYMENT VALIDATION
-- ============================================================
SELECT 'Starting validation...' AS status;

-- Check if target database is accessible
USE DATABASE ${comparison.target.database};
SELECT COUNT(*) AS table_count FROM INFORMATION_SCHEMA.TABLES;

-- ============================================================
-- SECTION 2: BACKUP RECOMMENDATIONS
-- ============================================================
${scriptOptions.includeBackups ? `-- RECOMMENDED: Create backup before deployment
-- CREATE TABLE customers_backup AS SELECT * FROM customers;
-- CREATE TABLE orders_backup AS SELECT * FROM orders;` : '-- Backups disabled in options'}

-- ============================================================
-- SECTION 3: DEPLOYMENT ORDER (Respecting Dependencies)
-- ============================================================

${scriptOptions.includeTransactions ? 'BEGIN TRANSACTION;' : ''}

-- Step 1: Modify existing tables (no dependencies)
-- Object: customers (TABLE) - MODIFIED
ALTER TABLE customers
  MODIFY COLUMN email VARCHAR(255);

ALTER TABLE customers
  ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Step 2: Recreate indexes
DROP INDEX IF EXISTS idx_email ON customers;
CREATE UNIQUE INDEX idx_email ON customers(email);

-- Step 3: Update views (depends on customers table)
CREATE OR REPLACE VIEW vw_active_customers AS
SELECT
  id,
  name,
  email,
  created_at,
  updated_at
FROM customers
WHERE status = 'active';

${scriptOptions.includeValidations ? `-- Step 4: Verify deployment
SELECT 'Deployment validation...' AS status;
SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM vw_active_customers;` : ''}

${scriptOptions.includeTransactions ? 'COMMIT;' : ''}

${scriptOptions.includeValidations ? `-- ============================================================
-- SECTION 4: POST-DEPLOYMENT VALIDATION
-- ============================================================
-- Verify column changes
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'customers'
  AND COLUMN_NAME = 'email';

-- Verify index exists
SHOW INDEXES FROM customers;` : ''}

-- ============================================================
-- DEPLOYMENT COMPLETE
-- ============================================================
SELECT 'Deployment completed successfully!' AS status;`

  const rollbackScript = `-- ============================================================
-- Rollback Script: ${comparison.source.connectionName} → ${comparison.target.connectionName}
-- Generated: ${new Date().toISOString()}
-- Comparison ID: ${comparison.id}
-- ============================================================

${scriptOptions.includeTransactions ? 'BEGIN TRANSACTION;' : ''}

-- Rollback column changes
ALTER TABLE customers MODIFY COLUMN email VARCHAR(100);
ALTER TABLE customers DROP COLUMN updated_at;

-- Recreate previous index
CREATE UNIQUE INDEX idx_email ON customers(email);

-- Rollback view changes
CREATE OR REPLACE VIEW vw_active_customers AS
SELECT
  id,
  name,
  email,
  created_at
FROM customers
WHERE status = 'active';

${scriptOptions.includeTransactions ? 'COMMIT;' : ''}

SELECT 'Rollback completed successfully!' AS status;`

  const handleCopy = (script: string) => {
    navigator.clipboard.writeText(script)
    toast.success('Script copied to clipboard')
  }

  const handleDownload = (script: string, filename: string) => {
    const blob = new Blob([script], { type: 'text/plain' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = filename
    a.click()
    URL.revokeObjectURL(url)
    toast.success('Script downloaded')
  }

  return (
    <div className="space-y-6">
      <Card className="p-6">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h3 className="text-lg font-semibold">Deployment Script Generator</h3>
            <p className="text-sm text-muted-foreground mt-1">
              Generate SQL scripts for deploying schema changes
            </p>
          </div>
          
          <div className="flex items-center gap-2">
            <Button variant="outline" onClick={() => setShowOptions(!showOptions)}>
              <Gear className="h-4 w-4 mr-2" />
              Options
            </Button>
          </div>
        </div>

        {showOptions && (
          <Card className="p-4 bg-muted/50 mb-6">
            <div className="space-y-4">
              <div>
                <Label className="text-base font-semibold">Script Options</Label>
                <div className="grid grid-cols-2 md:grid-cols-3 gap-3 mt-3">
                  <div className="flex items-center space-x-2">
                    <Checkbox
                      id="include-transactions"
                      checked={scriptOptions.includeTransactions}
                      onCheckedChange={(checked) => 
                        setScriptOptions({ ...scriptOptions, includeTransactions: !!checked })
                      }
                    />
                    <Label htmlFor="include-transactions" className="cursor-pointer text-sm">
                      Include Transactions
                    </Label>
                  </div>
                  
                  <div className="flex items-center space-x-2">
                    <Checkbox
                      id="include-rollback"
                      checked={scriptOptions.includeRollbackScript}
                      onCheckedChange={(checked) => 
                        setScriptOptions({ ...scriptOptions, includeRollbackScript: !!checked })
                      }
                    />
                    <Label htmlFor="include-rollback" className="cursor-pointer text-sm">
                      Include Rollback
                    </Label>
                  </div>
                  
                  <div className="flex items-center space-x-2">
                    <Checkbox
                      id="include-validations"
                      checked={scriptOptions.includeValidations}
                      onCheckedChange={(checked) => 
                        setScriptOptions({ ...scriptOptions, includeValidations: !!checked })
                      }
                    />
                    <Label htmlFor="include-validations" className="cursor-pointer text-sm">
                      Include Validations
                    </Label>
                  </div>
                  
                  <div className="flex items-center space-x-2">
                    <Checkbox
                      id="include-comments"
                      checked={scriptOptions.includeComments}
                      onCheckedChange={(checked) => 
                        setScriptOptions({ ...scriptOptions, includeComments: !!checked })
                      }
                    />
                    <Label htmlFor="include-comments" className="cursor-pointer text-sm">
                      Include Comments
                    </Label>
                  </div>
                  
                  <div className="flex items-center space-x-2">
                    <Checkbox
                      id="include-backups"
                      checked={scriptOptions.includeBackups}
                      onCheckedChange={(checked) => 
                        setScriptOptions({ ...scriptOptions, includeBackups: !!checked })
                      }
                    />
                    <Label htmlFor="include-backups" className="cursor-pointer text-sm">
                      Include Backups
                    </Label>
                  </div>
                  
                  <div className="flex items-center space-x-2">
                    <Checkbox
                      id="use-if-exists"
                      checked={scriptOptions.useIfExists}
                      onCheckedChange={(checked) => 
                        setScriptOptions({ ...scriptOptions, useIfExists: !!checked })
                      }
                    />
                    <Label htmlFor="use-if-exists" className="cursor-pointer text-sm">
                      Use IF EXISTS
                    </Label>
                  </div>
                </div>
              </div>
            </div>
          </Card>
        )}

        <div className="grid gap-4">
          <div>
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-2">
                <FileCode className="h-5 w-5" />
                <span className="font-semibold">Script Preview</span>
                <Badge>{comparison.results?.deploymentOrder.length || 0} operations</Badge>
              </div>
              
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => handleCopy(deploymentScript)}
                >
                  <Copy className="h-4 w-4 mr-2" />
                  Copy
                </Button>
                
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => handleDownload(
                    deploymentScript,
                    `deploy-${comparison.id}-${Date.now()}.sql`
                  )}
                >
                  <Download className="h-4 w-4 mr-2" />
                  Download
                </Button>
              </div>
            </div>
          </div>
        </div>
      </Card>

      <Tabs defaultValue="deployment" className="w-full">
        <TabsList className="grid w-full grid-cols-2">
          <TabsTrigger value="deployment">Deployment Script</TabsTrigger>
          <TabsTrigger value="rollback" disabled={!scriptOptions.includeRollbackScript}>
            Rollback Script
          </TabsTrigger>
        </TabsList>

        <TabsContent value="deployment">
          <Card>
            <ScrollArea className="h-[600px]">
              <pre className="p-6 text-sm font-mono whitespace-pre-wrap">
                <code>{deploymentScript}</code>
              </pre>
            </ScrollArea>
          </Card>
        </TabsContent>

        <TabsContent value="rollback">
          <Card>
            <ScrollArea className="h-[600px]">
              <pre className="p-6 text-sm font-mono whitespace-pre-wrap">
                <code>{rollbackScript}</code>
              </pre>
            </ScrollArea>
          </Card>
        </TabsContent>
      </Tabs>

      <Card className="p-6 bg-yellow-500/5 border-yellow-500/20">
        <div className="flex gap-3">
          <div className="text-yellow-500 mt-1">
            <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
            </svg>
          </div>
          <div className="flex-1">
            <h4 className="font-semibold text-yellow-500 mb-1">Important Notice</h4>
            <p className="text-sm text-muted-foreground">
              Always review generated scripts carefully before executing them in production environments. 
              It's recommended to test in a staging environment first and create backups of critical data.
            </p>
          </div>
        </div>
      </Card>
    </div>
  )
}
