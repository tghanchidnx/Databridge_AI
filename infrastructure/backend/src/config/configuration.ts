export default () => ({
  // Application
  port: parseInt(process.env.PORT, 10) || 3000,
  nodeEnv: process.env.NODE_ENV || 'development',
  apiPrefix: process.env.API_PREFIX || 'api/v1',

  // CORS
  corsOrigins: process.env.CORS_ORIGINS || 'http://localhost:3000',

  // Security & JWT
  jwt: {
    secret: process.env.JWT_SECRET || 'change-me-in-production',
    expiresIn: process.env.JWT_EXPIRATION || '1d',
  },
  encryptionKey: process.env.ENCRYPTION_KEY || 'change-me-32-characters-long!!',

  // Database
  databaseUrl: process.env.DATABASE_URL,

  // Microsoft Azure AD
  azureTenantId: process.env.AZURE_TENANT_ID,
  azureClientId: process.env.AZURE_CLIENT_ID,
  azureClientSecret: process.env.AZURE_CLIENT_SECRET,

  // Snowflake
  snowflake: {
    account: process.env.SNOWFLAKE_ACCOUNT,
    clientId: process.env.SNOWFLAKE_CLIENT_ID,
    clientSecret: process.env.SNOWFLAKE_CLIENT_SECRET,
    redirectUri: process.env.SNOWFLAKE_REDIRECT_URI,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'COMPUTE_WH',
    database: process.env.SNOWFLAKE_DATABASE || 'DATAAMPLIFIRE',
    schema: process.env.SNOWFLAKE_SCHEMA || 'PUBLIC',
    maxConnections: parseInt(process.env.SNOWFLAKE_MAX_CONNECTIONS, 10) || 10,
    minConnections: parseInt(process.env.SNOWFLAKE_MIN_CONNECTIONS, 10) || 2,
  },

  // Redis
  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT, 10) || 6379,
    password: process.env.REDIS_PASSWORD,
    db: parseInt(process.env.REDIS_DB, 10) || 0,
  },

  // Logging
  logging: {
    level: process.env.LOG_LEVEL || 'info',
    dir: process.env.LOG_DIR || 'logs',
  },

  // Rate Limiting
  throttle: {
    ttl: parseInt(process.env.THROTTLE_TTL, 10) || 60,
    limit: parseInt(process.env.THROTTLE_LIMIT, 10) || 100,
  },

  // File Upload
  upload: {
    maxFileSize: parseInt(process.env.MAX_FILE_SIZE, 10) || 52428800, // 50MB default
    uploadDir: process.env.UPLOAD_DIR || 'uploads',
    // CSV Import settings
    csv: {
      maxFileSize: parseInt(process.env.CSV_MAX_FILE_SIZE, 10) || 52428800, // 50MB for CSV
      chunkSize: parseInt(process.env.CSV_CHUNK_SIZE, 10) || 1048576, // 1MB chunks
      batchSize: parseInt(process.env.CSV_BATCH_SIZE, 10) || 500, // Rows per batch
      autoFixEnabled: process.env.CSV_AUTO_FIX !== 'false', // Enable auto-fix by default
      logRetentionDays: parseInt(process.env.CSV_LOG_RETENTION_DAYS, 10) || 30,
    },
  },

  // Frontend
  frontendUrl: process.env.FRONTEND_URL || 'http://localhost:3000',

  // API Keys
  apiKeys: process.env.API_KEYS?.split(',') || [],
});
