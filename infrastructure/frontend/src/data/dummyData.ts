import type { User, Workspace, Connection, Notification, VersionHistory } from '@/types'

export const dummyUser: User = {
  id: 'user_demo_001',
  email: 'john.doe@example.com',
  name: 'John Doe',
  avatar: 'https://api.dicebear.com/7.x/avataaars/svg?seed=John',
  role: 'owner',
  createdAt: '2024-01-15T10:30:00Z',
  bio: 'Data Engineer with 8 years of experience in building scalable data pipelines',
  teamSize: '11-50',
  primaryUseCase: 'Schema Management',
  workspaceIds: ['workspace_personal_001', 'workspace_acme_002', 'workspace_startup_003'],
  currentWorkspaceId: 'workspace_acme_002'
}

export const dummyWorkspaces: Workspace[] = [
  {
    id: 'workspace_personal_001',
    name: 'Personal Workspace',
    plan: 'free',
    memberCount: 1,
    ownerId: 'user_demo_001',
    createdAt: '2024-01-15T10:30:00Z',
    members: [
      {
        userId: 'user_demo_001',
        role: 'owner',
        joinedAt: '2024-01-15T10:30:00Z'
      }
    ]
  },
  {
    id: 'workspace_acme_002',
    name: 'Acme Corp',
    plan: 'enterprise',
    memberCount: 12,
    ownerId: 'user_demo_001',
    createdAt: '2024-02-01T09:00:00Z',
    members: [
      {
        userId: 'user_demo_001',
        role: 'owner',
        joinedAt: '2024-02-01T09:00:00Z'
      },
      {
        userId: 'user_002',
        role: 'admin',
        joinedAt: '2024-02-02T10:00:00Z'
      },
      {
        userId: 'user_003',
        role: 'member',
        joinedAt: '2024-02-05T14:30:00Z'
      }
    ]
  },
  {
    id: 'workspace_startup_003',
    name: 'TechStartup Inc',
    plan: 'pro',
    memberCount: 5,
    ownerId: 'user_demo_001',
    createdAt: '2024-03-10T11:15:00Z',
    members: [
      {
        userId: 'user_demo_001',
        role: 'owner',
        joinedAt: '2024-03-10T11:15:00Z'
      },
      {
        userId: 'user_004',
        role: 'member',
        joinedAt: '2024-03-11T09:00:00Z'
      },
      {
        userId: 'user_005',
        role: 'member',
        joinedAt: '2024-03-12T10:30:00Z'
      }
    ]
  }
]

export const dummyConnections: Connection[] = [
  {
    id: 'conn_personal_001',
    name: 'Personal PostgreSQL',
    type: 'postgresql',
    host: 'localhost',
    port: 5432,
    database: 'dev_db',
    username: 'dev_user',
    status: 'connected',
    lastUsed: '2024-03-22T10:30:00Z',
    createdAt: '2024-01-16T11:00:00Z',
    workspaceId: 'workspace_personal_001',
    authType: 'password',
    schema: 'public'
  },
  {
    id: 'conn_personal_002',
    name: 'Personal MySQL',
    type: 'mysql',
    host: 'localhost',
    port: 3306,
    database: 'test_db',
    username: 'root',
    status: 'disconnected',
    lastUsed: '2024-03-20T15:00:00Z',
    createdAt: '2024-01-20T14:30:00Z',
    workspaceId: 'workspace_personal_001',
    authType: 'password',
    schema: ''
  },
  {
    id: 'conn_acme_001',
    name: 'Acme Production Snowflake',
    type: 'snowflake',
    host: 'acme.snowflakecomputing.com',
    port: 443,
    database: 'PROD_DB',
    username: 'PROD_USER',
    status: 'connected',
    lastUsed: '2024-03-22T14:45:00Z',
    createdAt: '2024-02-01T10:00:00Z',
    workspaceId: 'workspace_acme_002',
    authType: 'keypair',
    account: 'acme',
    warehouse: 'COMPUTE_WH',
    schema: 'PUBLIC',
    role: 'DEVELOPER'
  },
  {
    id: 'conn_acme_002',
    name: 'Acme Staging PostgreSQL',
    type: 'postgresql',
    host: 'staging-db.acme.internal',
    port: 5432,
    database: 'staging_db',
    username: 'staging_user',
    status: 'connected',
    lastUsed: '2024-03-21T16:20:00Z',
    createdAt: '2024-02-05T09:30:00Z',
    workspaceId: 'workspace_acme_002',
    authType: 'password',
    schema: 'public'
  },
  {
    id: 'conn_acme_003',
    name: 'Acme Analytics MySQL',
    type: 'mysql',
    host: 'analytics-db.acme.internal',
    port: 3306,
    database: 'analytics',
    username: 'analytics_ro',
    status: 'connected',
    lastUsed: '2024-03-22T09:15:00Z',
    createdAt: '2024-02-10T13:00:00Z',
    workspaceId: 'workspace_acme_002',
    authType: 'password',
    schema: ''
  },
  {
    id: 'conn_acme_004',
    name: 'Acme Data Warehouse',
    type: 'snowflake',
    host: 'acme-dw.snowflakecomputing.com',
    port: 443,
    database: 'DW_PROD',
    username: 'DW_USER',
    status: 'disconnected',
    lastUsed: '2024-03-18T11:30:00Z',
    createdAt: '2024-02-15T13:45:00Z',
    workspaceId: 'workspace_acme_002',
    authType: 'oauth',
    account: 'acme-dw',
    warehouse: 'ETL_WH',
    schema: 'PUBLIC',
    role: 'ETL_ROLE'
  },
  {
    id: 'conn_startup_001',
    name: 'Startup Production DB',
    type: 'postgresql',
    host: 'prod.techstartup.com',
    port: 5432,
    database: 'production',
    username: 'prod_admin',
    status: 'connected',
    lastUsed: '2024-03-22T08:45:00Z',
    createdAt: '2024-03-10T12:00:00Z',
    workspaceId: 'workspace_startup_003',
    authType: 'password',
    schema: 'public'
  },
  {
    id: 'conn_startup_002',
    name: 'Startup Analytics Snowflake',
    type: 'snowflake',
    host: 'startup.snowflakecomputing.com',
    port: 443,
    database: 'ANALYTICS',
    username: 'ANALYTICS_USER',
    status: 'connected',
    lastUsed: '2024-03-22T11:00:00Z',
    createdAt: '2024-03-11T10:30:00Z',
    workspaceId: 'workspace_startup_003',
    authType: 'sso',
    account: 'startup',
    warehouse: 'ANALYTICS_WH',
    schema: 'PUBLIC',
    role: 'ANALYST'
  }
]

export const dummyNotifications: Notification[] = [
  {
    id: 'notif_001',
    type: 'success',
    title: 'Schema Comparison Complete',
    message: 'Your schema comparison between Production and Staging has finished successfully.',
    read: false,
    timestamp: '2024-03-22T14:30:00Z',
    actionUrl: '/schema-matcher'
  },
  {
    id: 'notif_002',
    type: 'info',
    title: 'New Team Member',
    message: 'Sarah Johnson has joined the Acme Corp workspace.',
    read: false,
    timestamp: '2024-03-22T12:15:00Z'
  },
  {
    id: 'notif_003',
    type: 'warning',
    title: 'Connection Issue',
    message: 'Unable to connect to Acme Data Warehouse. Please check your credentials.',
    read: true,
    timestamp: '2024-03-22T09:45:00Z',
    actionUrl: '/connections'
  },
  {
    id: 'notif_004',
    type: 'error',
    title: 'Report Comparison Failed',
    message: 'Data types mismatch detected in customer_orders table.',
    read: true,
    timestamp: '2024-03-21T16:20:00Z',
    actionUrl: '/report-matcher'
  },
  {
    id: 'notif_005',
    type: 'info',
    title: 'Workspace Upgraded',
    message: 'Your workspace has been upgraded to Enterprise plan.',
    read: true,
    timestamp: '2024-03-21T10:00:00Z'
  }
]

export const dummyVersionHistory: VersionHistory[] = [
  {
    id: 'ver_001',
    commitHash: 'a1b2c3d4',
    message: 'Updated customer table schema with new email validation',
    author: 'John Doe',
    timestamp: '2024-03-22T14:00:00Z',
    branch: 'main',
    filesChanged: 3
  },
  {
    id: 'ver_002',
    commitHash: 'e5f6g7h8',
    message: 'Added new analytics views for reporting',
    author: 'Sarah Johnson',
    timestamp: '2024-03-21T11:30:00Z',
    branch: 'main',
    filesChanged: 5
  },
  {
    id: 'ver_003',
    commitHash: 'i9j0k1l2',
    message: 'Fixed foreign key constraints in orders table',
    author: 'Mike Chen',
    timestamp: '2024-03-20T16:45:00Z',
    branch: 'develop',
    filesChanged: 2
  },
  {
    id: 'ver_004',
    commitHash: 'm3n4o5p6',
    message: 'Optimized query performance for large datasets',
    author: 'John Doe',
    timestamp: '2024-03-19T09:15:00Z',
    branch: 'main',
    filesChanged: 4
  },
  {
    id: 'ver_005',
    commitHash: 'q7r8s9t0',
    message: 'Initial schema setup for production environment',
    author: 'John Doe',
    timestamp: '2024-03-18T08:00:00Z',
    branch: 'main',
    filesChanged: 12
  }
]
