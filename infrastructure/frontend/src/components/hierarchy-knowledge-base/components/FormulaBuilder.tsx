import React, { useMemo, useState, useEffect } from "react";
import type { SmartHierarchyMaster } from "@/services/api/hierarchy";
import { FormulaGroupBuilder, type FormulaGroup } from "./FormulaGroupBuilder";
import { FilterGroupBuilder } from "./FilterGroupBuilder";
import type { FilterConfig } from "@/services/api/hierarchy";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Copy, Download, Plus, Trash2, X, FileDown } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

interface FormulaBuilderProps {
  selectedHierarchies: SmartHierarchyMaster[];
  onClear: () => void;
  currentHierarchy?: SmartHierarchyMaster | null;
  onSaveFormula?: (formulaConfig: {
    formula_type: string;
    formula_text: string;
    formula_group?: FormulaGroup;
  }) => void;
  onSaveFilter?: (filterConfig: FilterConfig) => void;
  preSelectedMappings?: SelectedMapping[];
  allAvailableHierarchies?: SmartHierarchyMaster[];
  databaseType?: DatabaseDialect;
}

type AggregationType =
  | "SUM"
  | "AVG"
  | "COUNT"
  | "MIN"
  | "MAX"
  | "CONCAT"
  | "STRING_AGG";
type ExpressionOperator = "+" | "-" | "*" | "/" | "%" | "||";
type DatabaseDialect = "snowflake" | "mysql" | "postgresql" | "sqlserver";

export const FormulaBuilder: React.FC<FormulaBuilderProps> = ({
  selectedHierarchies,
  onClear,
  currentHierarchy,
  onSaveFormula,
  onSaveFilter,
  preSelectedMappings = [],
  allAvailableHierarchies = [],
  databaseType,
}) => {
  const { toast } = useToast();
  const [formulaType, setFormulaType] = useState<
    "group" | "filter" | "expression" | "sql" | "select" | "aggregation"
  >("group");
  const [aggregationType, setAggregationType] =
    useState<AggregationType>("SUM");
  const [customSQL, setCustomSQL] = useState(
    currentHierarchy?.formulaConfig?.formula_text || ""
  );
  const [formulaGroup, setFormulaGroup] = useState<FormulaGroup | undefined>(
    currentHierarchy?.formulaConfig?.formula_group
  );
  const [filterConfig, setFilterConfig] = useState<FilterConfig | undefined>(
    currentHierarchy?.filterConfig
  );

  // Update states when currentHierarchy changes
  useEffect(() => {
    setFormulaGroup(currentHierarchy?.formulaConfig?.formula_group);
    setFilterConfig(currentHierarchy?.filterConfig);
    setCustomSQL(currentHierarchy?.formulaConfig?.formula_text || "");
  }, [currentHierarchy?.hierarchyId]);
  const [expressionItems, setExpressionItems] = useState<
    Array<{
      type: "hierarchy" | "operator" | "literal";
      value: string;
      hierarchyId?: string;
    }>
  >([]);
  const [dbDialect, setDbDialect] = useState<DatabaseDialect>(
    databaseType || "snowflake"
  );
  const [rowLimit, setRowLimit] = useState<number>(1000);
  const [editableQuery, setEditableQuery] = useState<string>("");
  const [isQueryEditable, setIsQueryEditable] = useState<boolean>(false);
  const [selectedMappings, setSelectedMappings] =
    useState<SelectedMapping[]>(preSelectedMappings);

  // Update dbDialect when databaseType prop changes
  useEffect(() => {
    if (databaseType) {
      setDbDialect(databaseType);
    }
  }, [databaseType]);

  // Sync preSelectedMappings from parent
  useEffect(() => {
    if (preSelectedMappings.length > 0) {
      setSelectedMappings(preSelectedMappings);
    }
  }, [preSelectedMappings]);

  // Use all available hierarchies for expression builder, not just selected ones
  const expressionHierarchies =
    allAvailableHierarchies.length > 0
      ? allAvailableHierarchies
      : selectedHierarchies;

  // Helper functions for SQL formatting
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

  const quoteValue = (value: string | number | undefined | null): string => {
    if (value === undefined || value === null) return "NULL";
    const strValue = String(value);
    return `'${strValue.replace(/'/g, "''")}'`;
  };

  const dialectIdentifier = (
    database: string,
    schema: string,
    table: string,
    column: string
  ): string => {
    switch (dbDialect) {
      case "snowflake":
        return `${database}.${schema}.${table}.${column}`;
      case "mysql":
        return `\`${database}\`.\`${table}\`.\`${column}\``;
      case "postgresql":
        return `"${schema}"."${table}"."${column}"`;
      case "sqlserver":
        return `[${database}].[${schema}].[${table}].[${column}]`;
      default:
        return `${database}.${schema}.${table}.${column}`;
    }
  };

  // Get all unique mappings from selected hierarchies
  const allMappings = useMemo(() => {
    const mappings: Array<{
      hierarchyName: string;
      hierarchyId: string;
      database: string;
      schema: string;
      table: string;
      column: string;
      uid?: string;
      precedence?: string;
    }> = [];

    selectedHierarchies.forEach((h) => {
      h.mapping?.forEach((m) => {
        mappings.push({
          hierarchyName: h.hierarchyName,
          hierarchyId: h.hierarchyId,
          database: m.source_database,
          schema: m.source_schema,
          table: m.source_table,
          column: m.source_column,
          uid: m.source_uid,
          precedence: m.precedence_group,
        });
      });
    });

    return mappings;
  }, [selectedHierarchies]);

  // Use selected mappings for query generation, fall back to all mappings if none selected
  const queryMappings = useMemo(() => {
    if (selectedMappings.length > 0) {
      return selectedMappings.map((m) => ({
        hierarchyName: m.hierarchyName,
        hierarchyId: m.hierarchyId,
        database: m.database,
        schema: m.schema,
        table: m.table,
        column: m.column,
        uid: m.uid,
        precedence: m.precedence_group,
      }));
    }
    return allMappings;
  }, [selectedMappings, allMappings]);

  // Mapping selection handlers
  const handleMappingToggle = (mapping: SelectedMapping) => {
    setSelectedMappings((prev) => {
      const exists = prev.some(
        (m) =>
          m.hierarchyId === mapping.hierarchyId &&
          m.mappingIndex === mapping.mappingIndex
      );
      if (exists) {
        return prev.filter(
          (m) =>
            !(
              m.hierarchyId === mapping.hierarchyId &&
              m.mappingIndex === mapping.mappingIndex
            )
        );
      }
      return [...prev, mapping];
    });
  };

  const handleSelectAllMappings = () => {
    const allMaps: SelectedMapping[] = [];
    selectedHierarchies.forEach((h) => {
      h.mapping?.forEach((m) => {
        allMaps.push({
          hierarchyId: h.hierarchyId,
          hierarchyName: h.hierarchyName,
          mappingIndex: m.mapping_index,
          database: m.source_database,
          schema: m.source_schema,
          table: m.source_table,
          column: m.source_column,
          uid: m.source_uid,
          precedence_group: m.precedence_group,
        });
      });
    });
    setSelectedMappings(allMaps);
  };

  const handleClearAllMappings = () => {
    setSelectedMappings([]);
  };

  // Generate aggregation formula based on database dialect
  const aggregationFormula = useMemo(() => {
    if (queryMappings.length === 0) return "";

    const columns = queryMappings.map((m) => {
      const fullColumn = dialectIdentifier(
        m.database,
        m.schema,
        m.table,
        m.column
      );
      return fullColumn;
    });

    if (columns.length === 0) return "";

    switch (aggregationType) {
      case "SUM":
        return columns.map((col) => `SUM(${col})`).join(" + ");
      case "AVG":
        return `AVG(${columns.join(", ")})`;
      case "COUNT":
        return `COUNT(DISTINCT ${columns.join(", ")})`;
      case "MIN":
        return dbDialect === "mysql"
          ? `LEAST(${columns.join(", ")})`
          : `MIN(${columns.join(", ")})`;
      case "MAX":
        return dbDialect === "mysql"
          ? `GREATEST(${columns.join(", ")})`
          : `MAX(${columns.join(", ")})`;
      case "CONCAT":
        if (dbDialect === "postgresql") {
          return columns.join(" || ");
        } else if (dbDialect === "snowflake") {
          return `CONCAT(${columns.join(", ")})`;
        } else if (dbDialect === "mysql") {
          return `CONCAT(${columns.join(", ")})`;
        } else {
          return columns.join(" + ");
        }
      case "STRING_AGG":
        if (dbDialect === "postgresql") {
          return columns.map((col) => `STRING_AGG(${col}, ',')`).join(", ");
        } else if (dbDialect === "snowflake") {
          return columns.map((col) => `LISTAGG(${col}, ',')`).join(", ");
        } else if (dbDialect === "mysql") {
          return columns.map((col) => `GROUP_CONCAT(${col})`).join(", ");
        } else {
          return columns.map((col) => `STRING_AGG(${col}, ',')`).join(", ");
        }
      default:
        return "";
    }
  }, [queryMappings, aggregationType, dbDialect]);

  // Generate expression formula
  const expressionFormula = useMemo(() => {
    if (expressionItems.length === 0) return "";

    return expressionItems
      .map((item) => {
        if (item.type === "hierarchy") {
          const mapping = queryMappings.find(
            (m) => m.hierarchyId === item.hierarchyId
          );
          if (!mapping) return item.value;
          return dialectIdentifier(
            mapping.database,
            mapping.schema,
            mapping.table,
            mapping.column
          );
        }
        return item.value;
      })
      .join(" ");
  }, [expressionItems, queryMappings, dbDialect]);

  // Generate comprehensive SELECT query
  const selectQuery = useMemo(() => {
    if (queryMappings.length === 0) return "";

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

    // Build column list with aliases from selected mappings
    queryMappings.forEach((m, idx) => {
      const hierarchyAlias = m.hierarchyName.replace(/\s+/g, "_").toUpperCase();
      const columnAlias = `${hierarchyAlias}_${m.column.toUpperCase()}`;
      const tableKey = `${m.database}.${m.schema}.${m.table}`;

      // Create table alias
      let tableAlias = `T${idx + 1}`;
      if (!tables.has(tableKey)) {
        tables.set(tableKey, {
          database: m.database,
          schema: m.schema,
          table: m.table,
          alias: tableAlias,
        });
      } else {
        tableAlias = tables.get(tableKey)!.alias;
      }

      // Add column with table alias
      const quotedColumn = quoteIdentifier(m.column);
      columns.push(
        `  ${tableAlias}.${quotedColumn} AS ${quoteIdentifier(columnAlias)}`
      );

      // Add UID filter if present
      if (m.uid) {
        whereConditions.push(
          `${tableAlias}.${quotedColumn} = ${quoteValue(m.uid)}`
        );
      }

      // Add precedence group filter if present
      if (m.precedence) {
        const precedenceCol = quoteIdentifier("PRECEDENCE_GROUP");
        whereConditions.push(
          `${tableAlias}.${precedenceCol} = ${quoteValue(m.precedence)}`
        );
      }
    });

    // Build FROM and JOIN clauses
    const tableList = Array.from(tables.entries());
    if (tableList.length === 0) return "";

    const [firstTableKey, firstTableInfo] = tableList[0];
    const fromClause = `${formatTableName(
      firstTableInfo.database,
      firstTableInfo.schema,
      firstTableInfo.table
    )} AS ${firstTableInfo.alias}`;

    // Generate JOINs for additional tables with common key logic
    for (let i = 1; i < tableList.length; i++) {
      const [tableKey, tableInfo] = tableList[i];
      const tableName = formatTableName(
        tableInfo.database,
        tableInfo.schema,
        tableInfo.table
      );

      // Attempt to find common join keys (ID, primary keys, etc.)
      const joinKey = quoteIdentifier("ID"); // Default join key
      joinConditions.push(
        `LEFT JOIN ${tableName} AS ${tableInfo.alias} ON ${firstTableInfo.alias}.${joinKey} = ${tableInfo.alias}.${joinKey}`
      );
    }

    // Add hierarchy flags to WHERE clause
    selectedHierarchies.forEach((h) => {
      const hierarchyMapping = h.mapping?.[0];
      if (!hierarchyMapping) return;

      const tableKey = `${hierarchyMapping.source_database}.${hierarchyMapping.source_schema}.${hierarchyMapping.source_table}`;
      const tableInfo = tables.get(tableKey);
      if (!tableInfo) return;

      // Active flag
      if (h.flags?.active_flag) {
        const activeCol = quoteIdentifier("ACTIVE");
        const activeValue = dbDialect === "mysql" ? "1" : "TRUE";
        whereConditions.push(
          `${tableInfo.alias}.${activeCol} = ${activeValue}`
        );
      }

      // Leaf node flag
      if (h.flags?.is_leaf_node) {
        const leafCol = quoteIdentifier("IS_LEAF_NODE");
        const leafValue = dbDialect === "mysql" ? "1" : "TRUE";
        whereConditions.push(`${tableInfo.alias}.${leafCol} = ${leafValue}`);
      }

      // Include flag
      if (h.flags?.include_flag) {
        const includeCol = quoteIdentifier("INCLUDE_FLAG");
        const includeValue = dbDialect === "mysql" ? "1" : "TRUE";
        whereConditions.push(
          `${tableInfo.alias}.${includeCol} = ${includeValue}`
        );
      }

      // Exclude flag (inverted logic)
      if (h.flags?.exclude_flag === false) {
        const excludeCol = quoteIdentifier("EXCLUDE_FLAG");
        const excludeValue = dbDialect === "mysql" ? "0" : "FALSE";
        whereConditions.push(
          `${tableInfo.alias}.${excludeCol} = ${excludeValue}`
        );
      }

      // Transform flag
      if (h.flags?.transform_flag) {
        const transformCol = quoteIdentifier("TRANSFORM_FLAG");
        const transformValue = dbDialect === "mysql" ? "1" : "TRUE";
        whereConditions.push(
          `${tableInfo.alias}.${transformCol} = ${transformValue}`
        );
      }
    });

    // Build complete query
    let query = `-- Auto-generated SELECT query for ${selectedHierarchies.length} hierarchies\n`;
    query += `-- Database: ${dbDialect.toUpperCase()} | Selected Mappings: ${
      queryMappings.length
    } | Limit: ${rowLimit}\n\n`;
    query += "SELECT\n" + columns.join(",\n");
    query += "\nFROM " + fromClause;

    if (joinConditions.length > 0) {
      query += "\n" + joinConditions.join("\n");
    }

    query += "\nWHERE " + whereConditions.join("\n  AND ");

    // Add GROUP BY if needed (for aggregations)
    // query += "\nGROUP BY 1, 2";

    // Add ORDER BY
    query += "\nORDER BY 1";

    // Add LIMIT based on database dialect
    if (dbDialect === "sqlserver") {
      query = query.replace("SELECT\n", `SELECT TOP ${rowLimit}\n`);
    } else {
      query += `\nLIMIT ${rowLimit}`;
    }

    query += ";";

    return query;
  }, [queryMappings, selectedHierarchies, dbDialect, rowLimit]);

  // Sync editable query when selectQuery changes (if not in edit mode)
  useEffect(() => {
    if (!isQueryEditable && selectQuery) {
      setEditableQuery(selectQuery);
    }
  }, [selectQuery, isQueryEditable]);

  const handleCopyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    toast({
      title: "Copied to clipboard",
      description: "Content has been copied successfully",
    });
  };

  const handleExportSQL = (content: string, filename: string) => {
    const blob = new Blob([content], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    toast({
      title: "SQL Exported",
      description: `File ${filename} has been downloaded`,
    });
  };

  const handleAddToExpression = (hierarchyId: string) => {
    setExpressionItems([
      ...expressionItems,
      { type: "hierarchy", value: hierarchyId, hierarchyId },
    ]);
  };

  const handleAddOperator = (operator: ExpressionOperator) => {
    setExpressionItems([
      ...expressionItems,
      { type: "operator", value: operator },
    ]);
  };

  const handleRemoveExpressionItem = (index: number) => {
    setExpressionItems(expressionItems.filter((_, i) => i !== index));
  };

  if (selectedHierarchies.length === 0) {
    return (
      <Card className="p-6">
        <div className="text-center text-muted-foreground">
          <p className="mb-2">No hierarchies selected</p>
          <p className="text-sm">
            Check hierarchies in the tree view to build formulas
          </p>
        </div>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      {/* Formula Builder Tabs */}
      <Card className="p-4 border-0">
        <Tabs value={formulaType} onValueChange={(v: any) => setFormulaType(v)}>
          <TabsList className="grid w-full grid-cols-5">
            <TabsTrigger value="group">Formula Group</TabsTrigger>
            <TabsTrigger value="filter">Filter Group</TabsTrigger>
            {/* <TabsTrigger value="expression">Expression</TabsTrigger> */}
            <TabsTrigger value="sql">Custom SQL</TabsTrigger>
            <TabsTrigger value="select">SELECT Query</TabsTrigger>
            {/* <TabsTrigger value="aggregation">Aggregation</TabsTrigger> */}
          </TabsList>

          {/* Expression Tab */}
          <TabsContent value="expression" className="space-y-4 mt-4">
            <div>
              <Label>Build Expression (All Available Hierarchies)</Label>
              <div className="mt-2 space-y-2">
                <div className="flex flex-wrap gap-2">
                  {expressionHierarchies.map((h) => (
                    <Button
                      key={h.hierarchyId}
                      variant="outline"
                      size="sm"
                      onClick={() => handleAddToExpression(h.hierarchyId)}
                    >
                      <Plus className="w-3 h-3 mr-1" />
                      {h.hierarchyName}
                    </Button>
                  ))}
                </div>
                <div className="flex gap-2">
                  {["+", "-", "*", "/", "%", "||"].map((op) => (
                    <Button
                      key={op}
                      variant="secondary"
                      size="sm"
                      onClick={() =>
                        handleAddOperator(op as ExpressionOperator)
                      }
                    >
                      {op}
                    </Button>
                  ))}
                </div>
              </div>
            </div>

            <div>
              <Label>Expression Items</Label>
              <div className="mt-2 flex flex-wrap gap-2 min-h-[60px] p-3 bg-muted/30 rounded-md">
                {expressionItems.length === 0 ? (
                  <p className="text-sm text-muted-foreground">
                    Click hierarchies and operators above to build expression
                  </p>
                ) : (
                  expressionItems.map((item, index) => (
                    <Badge
                      key={index}
                      variant={
                        item.type === "operator" ? "secondary" : "default"
                      }
                      className="flex items-center gap-1"
                    >
                      {item.type === "hierarchy"
                        ? expressionHierarchies.find(
                            (h) => h.hierarchyId === item.hierarchyId
                          )?.hierarchyName || item.value
                        : item.value}
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-4 w-4 p-0 hover:bg-transparent"
                        onClick={() => handleRemoveExpressionItem(index)}
                      >
                        <X className="w-3 h-3" />
                      </Button>
                    </Badge>
                  ))
                )}
              </div>
            </div>

            <div>
              <Label>Generated Expression ({dbDialect})</Label>
              <div className="relative mt-2">
                <Textarea
                  value={expressionFormula}
                  readOnly
                  className="font-mono text-sm pr-24"
                  rows={6}
                />
                <div className="absolute top-2 right-2 flex gap-1">
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => handleCopyToClipboard(expressionFormula)}
                    title="Copy to clipboard"
                  >
                    <Copy className="w-4 h-4" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() =>
                      handleExportSQL(
                        expressionFormula,
                        `expression_${dbDialect}.sql`
                      )
                    }
                    title="Export to SQL file"
                  >
                    <FileDown className="w-4 h-4" />
                  </Button>
                </div>
              </div>
            </div>
          </TabsContent>

          {/* Custom SQL Tab */}
          <TabsContent value="sql" className="space-y-4 mt-4">
            <div>
              <Label>Custom SQL Formula</Label>
              <Textarea
                value={customSQL}
                onChange={(e) => setCustomSQL(e.target.value)}
                className="mt-2 font-mono text-sm"
                rows={12}
                placeholder="Enter your custom SQL formula..."
              />
              <div className="flex gap-2 mt-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => handleCopyToClipboard(customSQL)}
                >
                  <Copy className="w-4 h-4 mr-1" />
                  Copy
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() =>
                    handleExportSQL(customSQL, `custom_${dbDialect}.sql`)
                  }
                >
                  <FileDown className="w-4 h-4 mr-1" />
                  Export SQL
                </Button>
              </div>
            </div>
          </TabsContent>

          {/* SELECT Query Tab */}
          <TabsContent value="select" className="space-y-4 mt-4">
            <div>
              <div className="flex items-center justify-between mb-2">
                <Label>Complete SELECT Query ({dbDialect})</Label>
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

                  <Label className="text-sm font-medium">Row Limit</Label>
                  <Input
                    type="number"
                    value={rowLimit}
                    onChange={(e) =>
                      setRowLimit(parseInt(e.target.value) || 1000)
                    }
                    className=" w-[100px]"
                    min={1}
                    max={1000000}
                  />
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => {
                      setEditableQuery(selectQuery);
                      toast({
                        title: "Query Reset",
                        description:
                          "Query has been regenerated from current mappings",
                      });
                    }}
                    disabled={!isQueryEditable}
                  >
                    Reset
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() =>
                      handleCopyToClipboard(
                        isQueryEditable ? editableQuery : selectQuery
                      )
                    }
                  >
                    <Copy className="w-4 h-4 mr-1" />
                    Copy
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() =>
                      handleExportSQL(
                        isQueryEditable ? editableQuery : selectQuery,
                        `select_query_${dbDialect}.sql`
                      )
                    }
                  >
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
                rows={20}
                placeholder="Click 'Edit Query' to modify the auto-generated SQL..."
              />
              <div className="mt-3 text-xs text-muted-foreground bg-muted/30 p-3 rounded">
                <div className="flex items-start justify-between">
                  <div>
                    <p className="font-medium mb-1">
                      📋 Auto-Generated Query Features:
                    </p>
                    <ul className="list-disc list-inside space-y-1">
                      <li>
                        <strong>
                          {queryMappings.length} selected mappings
                        </strong>{" "}
                        from {selectedHierarchies.length} hierarchies
                      </li>
                      <li>
                        Database-specific syntax for{" "}
                        <strong>{dbDialect.toUpperCase()}</strong>
                      </li>
                      <li>Table aliases (T1, T2, ...) with LEFT JOIN logic</li>
                      <li>WHERE clause with UID and precedence filters</li>
                      <li>
                        Hierarchy flags: active_flag, is_leaf_node,
                        include_flag, exclude_flag, transform_flag
                      </li>
                      <li>Column aliases: HIERARCHY_NAME_COLUMN format</li>
                      <li>
                        Row limit: {rowLimit} (
                        {dbDialect === "sqlserver" ? "TOP" : "LIMIT"})
                      </li>
                      {selectedMappings.length === 0 &&
                        queryMappings.length > 0 && (
                          <li className="text-yellow-600 font-semibold">
                            ⚠️ Using all available mappings (none specifically
                            selected)
                          </li>
                        )}
                      {selectedMappings.length > 0 && (
                        <li className="text-green-600 font-semibold">
                          ✓ Using {selectedMappings.length} selected mappings
                          from Mapping tab
                        </li>
                      )}
                    </ul>
                  </div>
                  {isQueryEditable && (
                    <Badge
                      variant="outline"
                      className="bg-primary/10 text-primary"
                    >
                      ✏️ Editing Mode
                    </Badge>
                  )}
                </div>
              </div>
            </div>
          </TabsContent>

          {/* Formula Group Tab */}
          <TabsContent value="group" className="space-y-4 mt-4">
            <FormulaGroupBuilder
              value={formulaGroup}
              onChange={setFormulaGroup}
              availableHierarchies={expressionHierarchies}
              currentHierarchy={currentHierarchy || undefined}
              projectId={currentHierarchy?.projectId}
              onSave={(group) => {
                if (onSaveFormula) {
                  onSaveFormula({
                    formula_type: "AGGREGATE",
                    formula_text: `Formula Group: ${group.groupName}`,
                    formula_group: group,
                  });
                }
              }}
            />
          </TabsContent>

          {/* Filter Group Tab */}
          <TabsContent value="filter" className="space-y-4 mt-4">
            <FilterGroupBuilder
              value={filterConfig}
              onChange={setFilterConfig}
              currentHierarchy={currentHierarchy || undefined}
              projectId={currentHierarchy?.projectId}
              availableHierarchies={allAvailableHierarchies || []}
              onSave={(filter) => {
                if (onSaveFilter) {
                  onSaveFilter(filter);
                }
              }}
            />
          </TabsContent>
        </Tabs>
      </Card>
    </div>
  );
};
