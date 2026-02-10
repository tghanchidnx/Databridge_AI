import React, { useState, useMemo, useEffect } from "react";
import type { SmartHierarchyMaster } from "@/services/api/hierarchy";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Copy, FileDown } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

type DatabaseDialect = "snowflake" | "mysql" | "postgresql" | "sqlserver";

interface MappingQueryGeneratorProps {
  hierarchy: SmartHierarchyMaster;
  selectedMappingIndexes: number[];
  databaseType?: DatabaseDialect;
}

export const MappingQueryGenerator: React.FC<MappingQueryGeneratorProps> = ({
  hierarchy,
  selectedMappingIndexes,
  databaseType,
}) => {
  const { toast } = useToast();
  const [dbDialect, setDbDialect] = useState<DatabaseDialect>(
    databaseType || "snowflake"
  );
  const [rowLimit, setRowLimit] = useState<number>(1000);
  const [editableQuery, setEditableQuery] = useState<string>("");
  const [isQueryEditable, setIsQueryEditable] = useState<boolean>(false);

  // Update dbDialect when databaseType prop changes
  useEffect(() => {
    if (databaseType) {
      setDbDialect(databaseType);
    }
  }, [databaseType]);

  // Helper functions
  const formatTableName = (
    database: string,
    schema: string,
    table: string
  ): string => {
    switch (dbDialect) {
      case "snowflake":
        return `${database}.${schema}.${table}`;
      case "mysql":
        return `\`${database}\`.\`${table}\``;
      case "postgresql":
        return `"${schema}"."${table}"`;
      case "sqlserver":
        return `[${database}].[${schema}].[${table}]`;
      default:
        return `${database}.${schema}.${table}`;
    }
  };

  const quoteIdentifier = (identifier: string): string => {
    switch (dbDialect) {
      case "snowflake":
        return identifier;
      case "mysql":
        return `\`${identifier}\``;
      case "postgresql":
        return `"${identifier}"`;
      case "sqlserver":
        return `[${identifier}]`;
      default:
        return identifier;
    }
  };

  const quoteValue = (value: string | undefined = "N/A"): string => {
    const stringValue = String(value ?? "N/A");
    return `'${stringValue.replace(/'/g, "''")}'`;
  };

  // Generate SELECT query
  const selectQuery = useMemo(() => {
    const mappingsToUse =
      selectedMappingIndexes.length > 0
        ? hierarchy.mapping?.filter((m) =>
            selectedMappingIndexes.includes(m.mapping_index)
          )
        : hierarchy.mapping;

    if (!mappingsToUse || mappingsToUse.length === 0) return "";

    const columns: string[] = [];
    const tables = new Map<
      string,
      {
        database: string;
        schema: string;
        table: string;
        alias: string;
      }
    >();
    const whereConditions: string[] = ["1=1"];
    const joinConditions: string[] = [];

    // Build columns and track tables
    mappingsToUse.forEach((m, idx) => {
      const columnAlias = `${hierarchy.hierarchyName
        .replace(/\s+/g, "_")
        .toUpperCase()}_${m.source_column.toUpperCase()}`;
      const tableKey = `${m.source_database}.${m.source_schema}.${m.source_table}`;

      let tableAlias = `T${idx + 1}`;
      if (!tables.has(tableKey)) {
        tables.set(tableKey, {
          database: m.source_database,
          schema: m.source_schema,
          table: m.source_table,
          alias: tableAlias,
        });
      } else {
        tableAlias = tables.get(tableKey)!.alias;
      }

      const quotedColumn = quoteIdentifier(m.source_column);
      columns.push(
        `  ${tableAlias}.${quotedColumn} AS ${quoteIdentifier(columnAlias)}`
      );

      // Add UID filter
      if (m.source_uid) {
        whereConditions.push(
          `${tableAlias}.${quotedColumn} = ${quoteValue(m.source_uid)}`
        );
      }

      // Add precedence group filter
      if (m.precedence_group) {
        const precedenceCol = quoteIdentifier("PRECEDENCE_GROUP");
        whereConditions.push(
          `${tableAlias}.${precedenceCol} = ${quoteValue(m.precedence_group)}`
        );
      }
    });

    // Build FROM and JOINs
    const tableList = Array.from(tables.entries());
    if (tableList.length === 0) return "";

    const [firstTableKey, firstTableInfo] = tableList[0];
    const fromClause = `${formatTableName(
      firstTableInfo.database,
      firstTableInfo.schema,
      firstTableInfo.table
    )} AS ${firstTableInfo.alias}`;

    for (let i = 1; i < tableList.length; i++) {
      const [tableKey, tableInfo] = tableList[i];
      const tableName = formatTableName(
        tableInfo.database,
        tableInfo.schema,
        tableInfo.table
      );
      const joinKey = quoteIdentifier("ID");
      joinConditions.push(
        `LEFT JOIN ${tableName} AS ${tableInfo.alias} ON ${firstTableInfo.alias}.${joinKey} = ${tableInfo.alias}.${joinKey}`
      );
    }

    // Add hierarchy flags
    const tableInfo = tables.values().next().value;
    if (tableInfo && hierarchy.flags) {
      if (hierarchy.flags.active_flag) {
        const activeCol = quoteIdentifier("ACTIVE");
        const activeValue = dbDialect === "mysql" ? "1" : "TRUE";
        whereConditions.push(
          `${tableInfo.alias}.${activeCol} = ${activeValue}`
        );
      }

      if (hierarchy.flags.is_leaf_node) {
        const leafCol = quoteIdentifier("IS_LEAF_NODE");
        const leafValue = dbDialect === "mysql" ? "1" : "TRUE";
        whereConditions.push(`${tableInfo.alias}.${leafCol} = ${leafValue}`);
      }

      if (hierarchy.flags.include_flag) {
        const includeCol = quoteIdentifier("INCLUDE_FLAG");
        const includeValue = dbDialect === "mysql" ? "1" : "TRUE";
        whereConditions.push(
          `${tableInfo.alias}.${includeCol} = ${includeValue}`
        );
      }

      if (hierarchy.flags.transform_flag) {
        const transformCol = quoteIdentifier("TRANSFORM_FLAG");
        const transformValue = dbDialect === "mysql" ? "1" : "TRUE";
        whereConditions.push(
          `${tableInfo.alias}.${transformCol} = ${transformValue}`
        );
      }
    }

    // Build complete query
    let query = `-- Auto-generated SELECT query for ${hierarchy.hierarchyName}\n`;
    query += `-- Database: ${dbDialect.toUpperCase()} | Selected Mappings: ${
      mappingsToUse.length
    } | Limit: ${rowLimit}\n\n`;
    query += "SELECT\n" + columns.join(",\n");
    query += "\nFROM " + fromClause;

    if (joinConditions.length > 0) {
      query += "\n" + joinConditions.join("\n");
    }

    query += "\nWHERE " + whereConditions.join("\n  AND ");
    query += "\nORDER BY 1";

    if (dbDialect === "sqlserver") {
      query = query.replace("SELECT\n", `SELECT TOP ${rowLimit}\n`);
    } else {
      query += `\nLIMIT ${rowLimit}`;
    }

    query += ";";
    return query;
  }, [hierarchy, selectedMappingIndexes, dbDialect, rowLimit]);

  // Sync editable query
  useEffect(() => {
    if (!isQueryEditable && selectQuery) {
      setEditableQuery(selectQuery);
    }
  }, [selectQuery, isQueryEditable]);

  const handleCopyToClipboard = () => {
    const text = isQueryEditable ? editableQuery : selectQuery;
    navigator.clipboard.writeText(text);
    toast({
      title: "Copied to clipboard",
      description: "Query has been copied successfully",
    });
  };

  const handleExportSQL = () => {
    const content = isQueryEditable ? editableQuery : selectQuery;
    const blob = new Blob([content], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${hierarchy.hierarchyName.replace(
      /\s+/g,
      "_"
    )}_query_${dbDialect}.sql`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    toast({
      title: "SQL Exported",
      description: `Query has been downloaded`,
    });
  };

  const selectedCount = selectedMappingIndexes.length;
  const totalCount = hierarchy.mapping?.length || 0;

  return (
    <Card className="p-4 col-span-full">
      <div className="space-y-4">
        {/* Header with Database Selection */}
        <div className="flex items-center justify-between">
          <Label className="text-base font-semibold">
            📊 SELECT Query Generator
            {selectedCount > 0 && (
              <Badge variant="secondary" className="ml-2">
                {selectedCount} / {totalCount} mappings
              </Badge>
            )}
          </Label>
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2">
              <Label className="text-sm font-medium">Database:</Label>
              <Select
                value={dbDialect}
                onValueChange={(v: DatabaseDialect) => setDbDialect(v)}
              >
                <SelectTrigger className="w-[140px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="snowflake">Snowflake</SelectItem>
                  <SelectItem value="mysql">MySQL</SelectItem>
                  <SelectItem value="postgresql">PostgreSQL</SelectItem>
                  <SelectItem value="sqlserver">SQL Server</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-center gap-2">
              <Label className="text-sm font-medium">Limit:</Label>
              <Input
                type="number"
                value={rowLimit}
                onChange={(e) => setRowLimit(parseInt(e.target.value) || 1000)}
                className="w-[100px]"
                min={1}
                max={1000000}
              />
            </div>
          </div>
        </div>

        {/* Query Display */}
        <div>
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <Label>Generated Query</Label>
              {isQueryEditable && (
                <Badge variant="outline" className="bg-primary/10 text-primary">
                  ✏️ Editing Mode
                </Badge>
              )}
            </div>
            <div className="flex gap-2">
              <Button
                variant={isQueryEditable ? "default" : "outline"}
                size="sm"
                onClick={() => {
                  if (!isQueryEditable) {
                    setEditableQuery(selectQuery);
                  }
                  setIsQueryEditable(!isQueryEditable);
                }}
              >
                {isQueryEditable ? "Lock Query" : "Edit Query"}
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => {
                  setEditableQuery(selectQuery);
                  toast({
                    title: "Query Reset",
                    description: "Query has been regenerated",
                  });
                }}
                disabled={!isQueryEditable}
              >
                Reset
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={handleCopyToClipboard}
              >
                <Copy className="w-4 h-4 mr-1" />
                Copy
              </Button>
              <Button variant="outline" size="sm" onClick={handleExportSQL}>
                <FileDown className="w-4 h-4 mr-1" />
                Export SQL
              </Button>
            </div>
          </div>
          <Textarea
            value={isQueryEditable ? editableQuery : selectQuery}
            onChange={(e) => setEditableQuery(e.target.value)}
            readOnly={!isQueryEditable}
            className={`font-mono text-sm ${
              isQueryEditable ? "border-primary" : ""
            }`}
            rows={15}
            placeholder="Select mappings above to generate query..."
          />
        </div>

        {/* Info Panel */}
        {selectQuery && (
          <div className="text-xs text-muted-foreground bg-muted/30 p-3 rounded">
            <p className="font-medium mb-1">📋 Query Features:</p>
            <ul className="list-disc list-inside space-y-1">
              <li>
                <strong>
                  {selectedCount > 0 ? selectedCount : totalCount} mappings
                </strong>{" "}
                from {hierarchy.hierarchyName}
              </li>
              <li>
                Database-specific syntax for{" "}
                <strong>{dbDialect.toUpperCase()}</strong>
              </li>
              <li>Table aliases (T1, T2, ...) with LEFT JOIN logic</li>
              <li>WHERE clause with UID and precedence filters</li>
              <li>
                Hierarchy flags: active_flag, is_leaf_node, include_flag,
                transform_flag
              </li>
              <li>
                Row limit: {rowLimit} (
                {dbDialect === "sqlserver" ? "TOP" : "LIMIT"})
              </li>
              {selectedCount === 0 && totalCount > 0 && (
                <li className="text-yellow-600 font-semibold">
                  ⚠️ Using all {totalCount} mappings (none selected)
                </li>
              )}
              {selectedCount > 0 && (
                <li className="text-green-600 font-semibold">
                  ✓ Using {selectedCount} selected mappings
                </li>
              )}
            </ul>
          </div>
        )}
      </div>
    </Card>
  );
};
