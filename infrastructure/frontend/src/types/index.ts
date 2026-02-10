export interface User {
  id: string;
  email: string;
  name: string;
  avatar?: string;
  avatarUrl?: string;
  authType?: string;
  role: "owner" | "admin" | "member" | "viewer";
  createdAt: string;
  bio?: string;
  teamSize?: string;
  primaryUseCase?: string;
  organizationId?: string;
  onboardingCompleted?: boolean;
  isOrganizationOwner?: boolean;
}

export interface Organization {
  id: string;
  name: string;
  description?: string;
  plan: "free" | "pro" | "enterprise";
  status: "active" | "inactive" | "suspended";
  memberCount: number;
  ownerId: string;
  createdAt: string;
  members: OrganizationMember[];
}

export interface OrganizationMember {
  userId: string;
  role: "owner" | "admin" | "member" | "viewer";
  joinedAt: string;
}

export interface Connection {
  id: string;
  name?: string; // Generic name
  connectionName?: string; // Snowflake specific
  type?: "snowflake" | "postgresql" | "mysql" | "sqlserver" | "oracle";
  serverType?: "snowflake" | "postgresql" | "mysql" | "sqlserver" | "oracle"; // Backend uses this
  host?: string;
  port?: number;
  database?: string;
  databaseName?: string; // Alternative field name
  username?: string;
  status?: "connected" | "disconnected" | "error" | "active" | "inactive";
  lastUsed?: string;
  createdAt?: string;
  organizationId?: string;
  authType?: "password" | "keypair" | "oauth" | "sso";
  warehouse?: string;
  snowflakeWarehouse?: string; // Snowflake specific
  schema?: string;
  schemaName?: string; // Alternative field name
  role?: string;
  account?: string;
  snowflakeAccount?: string; // Snowflake specific
  snowflakeUser?: string;
  snowflakeRole?: string;
  snowflakeDatabase?: string;
  snowflakeSchema?: string;
}

export interface Notification {
  id: string;
  type: "info" | "success" | "warning" | "error";
  title: string;
  message: string;
  read: boolean;
  timestamp: string;
  actionUrl?: string;
}

export interface GitHubRepo {
  id: string;
  name: string;
  fullName: string;
  private: boolean;
  url: string;
}

export interface VersionHistory {
  id: string;
  commitHash: string;
  message: string;
  author: string;
  timestamp: string;
  branch: string;
  filesChanged: number;
}

export interface SchemaComparison {
  id: string;
  organizationId: string;
  name: string;
  source: {
    connectionId: string;
    connectionName: string;
    database: string;
    schema: string;
  };
  target: {
    connectionId: string;
    connectionName: string;
    database: string;
    schema: string;
  };
  options: ComparisonOptions;
  status: "pending" | "running" | "completed" | "failed";
  progress: number;
  results?: ComparisonResults;
  createdBy: string;
  createdAt: string;
  completedAt?: string;
  executionTimeSeconds?: number;
}

export interface ComparisonOptions {
  mode: "quick" | "standard" | "deep" | "schema-only" | "custom";
  ignoreCase: boolean;
  ignoreWhitespace: boolean;
  ignoreComments: boolean;
  compareRowCounts: boolean;
  compareDataSamples: boolean;
  sampleSize: number;
  includeSystemObjects: boolean;
  objectFilters: {
    includePatterns: string[];
    excludePatterns: string[];
  };
  objectTypes: {
    tables: boolean;
    views: boolean;
    procedures: boolean;
    functions: boolean;
    sequences: boolean;
    indexes: boolean;
    triggers: boolean;
  };
}

export interface ComparisonResults {
  summary: {
    totalObjects: number;
    matched: number;
    modified: number;
    sourceOnly: number;
    targetOnly: number;
  };
  differences: Difference[];
  deploymentOrder: string[];
  scriptGenerated: boolean;
  executionTime: number;
}

export interface Difference {
  objectName: string;
  objectType:
    | "table"
    | "view"
    | "procedure"
    | "function"
    | "sequence"
    | "index"
    | "constraint"
    | "trigger";
  status: "matched" | "modified" | "source_only" | "target_only";
  impact: "low" | "medium" | "high";
  details: any;
  suggestedAction: string;
  changes?: ChangeDetail[];
}

export interface ChangeDetail {
  type:
    | "column_added"
    | "column_removed"
    | "column_modified"
    | "index_added"
    | "index_removed"
    | "constraint_added"
    | "constraint_removed"
    | "definition_changed";
  element: string;
  source?: any;
  target?: any;
  description: string;
}

export interface TableComparison {
  tableName: string;
  columns: ColumnDiff[];
  indexes: IndexDiff[];
  constraints: ConstraintDiff[];
  triggers: TriggerDiff[];
  rowCountSource?: number;
  rowCountTarget?: number;
  dataSizeMbSource?: number;
  dataSizeMbTarget?: number;
}

export interface ColumnDiff {
  name: string;
  source?: {
    type: string;
    nullable: boolean;
    default: string | null;
    position: number;
  };
  target?: {
    type: string;
    nullable: boolean;
    default: string | null;
    position: number;
  };
  status: "matched" | "modified" | "source_only" | "target_only";
  impact: "low" | "medium" | "high";
  suggestion: string;
}

export interface IndexDiff {
  name: string;
  source?: {
    exists: boolean;
    columns: string[];
    unique: boolean;
    type?: string;
  };
  target?: {
    exists: boolean;
    columns: string[];
    unique: boolean;
    type?: string;
  };
  status: "matched" | "modified" | "source_only" | "target_only";
  suggestion: string;
}

export interface ConstraintDiff {
  name: string;
  type: "primary_key" | "foreign_key" | "unique" | "check";
  source?: {
    exists: boolean;
    references?: string;
    columns?: string[];
    definition?: string;
  };
  target?: {
    exists: boolean;
    references?: string;
    columns?: string[];
    definition?: string;
  };
  status: "matched" | "modified" | "source_only" | "target_only";
}

export interface TriggerDiff {
  name: string;
  source?: {
    exists: boolean;
    timing: string;
    event: string;
    definition: string;
  };
  target?: {
    exists: boolean;
    timing: string;
    event: string;
    definition: string;
  };
  status: "matched" | "modified" | "source_only" | "target_only";
}

export interface DependencyNode {
  objectId: string;
  objectName: string;
  objectType: string;
  dependsOn: {
    name: string;
    type: string;
  }[];
  referencedBy: {
    name: string;
    type: string;
  }[];
  dependencyLevel: number;
  deploymentOrder: number;
}

export interface DeploymentScript {
  id: string;
  comparisonId: string;
  deploymentScript: string;
  rollbackScript: string;
  validationScript: string;
  createdAt: string;
  options: ScriptOptions;
}

export interface ScriptOptions {
  includeTransactions: boolean;
  includeRollbackScript: boolean;
  includeValidations: boolean;
  includeComments: boolean;
  includeBackups: boolean;
  dropBeforeCreate: boolean;
  useIfExists: boolean;
  batchSize: number;
  targetDatabase: string;
  executionMode: "manual" | "semi-auto" | "auto";
  objectHandling: {
    tables: {
      onModify: "alter" | "drop_recreate";
      onMissing: "create" | "ignore";
      onExtra: "drop" | "ignore";
    };
    views: {
      onModify: "create_or_replace";
      onMissing: "create";
      onExtra: "drop" | "ignore";
    };
  };
}
