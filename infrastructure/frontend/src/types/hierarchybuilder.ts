// Types for Hierarchy Builder

export interface SQLDetail {
  database?: string;
  schema?: string;
  table?: string;
  column?: string;
  calculation?: string;
  aggregation?: string;
  filter?: string;
  order?: number;
}

export interface GraphNode {
  id: string;
  name: string;
  parentId?: string;
  order: number;
  nodeType?: string;
  description?: string;
  // Source configuration
  sourceDatabase?: string;
  sourceSchema?: string;
  sourceTable?: string;
  sourceColumn?: string;
  // Node flags
  isCalculated?: boolean;
  isRollup?: boolean;
  isFilter?: boolean;
  isDimension?: boolean;
  isMeasure?: boolean;
  // Advanced properties
  sqlDetails?: SQLDetail[];
  formula?: string;
  rollupParents?: string[];
  customFilters?: Record<string, any>;
  metadata?: Record<string, any>;
  // Display properties
  displayFormat?: string;
  aggregationType?: string;
  // Tree structure
  children?: GraphNode[];
  createdAt?: Date;
  updatedAt?: Date;
}

export interface GraphTree extends GraphNode {
  children: GraphTree[];
}

export interface HierarchyTemplate {
  id: string;
  name: string;
  description?: string;
  nodes: GraphNode[];
  tags?: string[];
  isGlobal?: boolean;
  createdBy: string;
  createdAt: Date;
}

export interface HierarchyVersion {
  id: string;
  projectId: string;
  name: string;
  baseVersionId?: string;
  status: "draft" | "submitted" | "approved" | "rejected";
  approvedBy?: string;
  approvedAt?: Date;
  metadata?: Record<string, any>;
  createdAt: Date;
  updatedAt: Date;
}

export interface CustomObject {
  id: string;
  name: string;
  dataType: string;
  validation?: Record<string, any>;
  metadata?: Record<string, any>;
  createdBy: string;
  createdAt: Date;
  updatedAt: Date;
}

export interface AIRecommendation {
  id: string;
  type: string;
  title: string;
  description: string;
  evidence?: Record<string, any>;
  status: "pending" | "approved" | "rejected" | "implemented";
  priority: number;
  affectedNodes?: string[];
  brdGenerated: boolean;
  createdAt: Date;
  updatedAt: Date;
}

export interface HierarchyProject {
  id: string;
  name: string;
  description?: string;
  userId: string;
  isActive: boolean;
  metadata?: Record<string, any>;
  createdAt: Date;
  updatedAt: Date;
}
