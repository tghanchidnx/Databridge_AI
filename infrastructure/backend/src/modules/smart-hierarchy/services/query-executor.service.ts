/**
 * Query Executor Service
 * Executes preview queries and manages query caching
 */
import { Injectable, Logger, BadRequestException } from '@nestjs/common';
import { createHash } from 'crypto';

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

export interface QueryCacheEntry {
  cacheKey: string;
  result: PreviewResult;
  expiresAt: Date;
}

export interface ExecuteQueryDto {
  connectionId: string;
  query: string;
  limit?: number;
  params?: Record<string, any>;
}

export interface EstimateRowCountDto {
  connectionId: string;
  query: string;
}

@Injectable()
export class QueryExecutorService {
  private readonly logger = new Logger(QueryExecutorService.name);
  private readonly cache = new Map<string, QueryCacheEntry>();
  private readonly CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes
  private readonly MAX_CACHE_SIZE = 100;

  constructor() {
    // Clean up expired cache entries periodically
    setInterval(() => this.cleanupCache(), 60 * 1000);
  }

  /**
   * Generate a cache key from query and parameters
   */
  private generateCacheKey(connectionId: string, query: string, limit: number): string {
    const payload = JSON.stringify({ connectionId, query, limit });
    return createHash('sha256').update(payload).digest('hex');
  }

  /**
   * Check if a cached result exists and is valid
   */
  private getCachedResult(cacheKey: string): PreviewResult | null {
    const entry = this.cache.get(cacheKey);
    if (!entry) return null;
    if (new Date() > entry.expiresAt) {
      this.cache.delete(cacheKey);
      return null;
    }
    return entry.result;
  }

  /**
   * Store a result in the cache
   */
  private cacheResult(cacheKey: string, result: PreviewResult): void {
    // Enforce max cache size
    if (this.cache.size >= this.MAX_CACHE_SIZE) {
      const oldestKey = this.cache.keys().next().value;
      if (oldestKey) this.cache.delete(oldestKey);
    }

    this.cache.set(cacheKey, {
      cacheKey,
      result,
      expiresAt: new Date(Date.now() + this.CACHE_TTL_MS),
    });
  }

  /**
   * Clean up expired cache entries
   */
  private cleanupCache(): void {
    const now = new Date();
    for (const [key, entry] of this.cache.entries()) {
      if (now > entry.expiresAt) {
        this.cache.delete(key);
      }
    }
  }

  /**
   * Invalidate cache for a specific connection
   */
  public invalidateCache(connectionId: string): void {
    for (const [key, entry] of this.cache.entries()) {
      if (key.includes(connectionId)) {
        this.cache.delete(key);
      }
    }
  }

  /**
   * Execute a preview query
   * Note: Actual database execution would be done through connection service
   */
  async executePreviewQuery(dto: ExecuteQueryDto): Promise<PreviewResult> {
    const limit = dto.limit || 100;
    const cacheKey = this.generateCacheKey(dto.connectionId, dto.query, limit);

    // Check cache first
    const cachedResult = this.getCachedResult(cacheKey);
    if (cachedResult) {
      this.logger.debug(`Cache hit for query: ${cacheKey.substring(0, 16)}...`);
      return cachedResult;
    }

    const startTime = Date.now();

    try {
      // Validate query (basic SQL injection prevention)
      this.validateQuery(dto.query);

      // In production, this would execute through the connection service
      // For now, we'll simulate the structure
      const result = await this.executeQuery(dto.connectionId, dto.query, limit);

      const executionTimeMs = Date.now() - startTime;
      const previewResult: PreviewResult = {
        ...result,
        executionTimeMs,
        query: dto.query,
      };

      // Cache the result
      this.cacheResult(cacheKey, previewResult);

      return previewResult;
    } catch (error) {
      this.logger.error(`Query execution failed: ${error.message}`);
      throw new BadRequestException(`Query execution failed: ${error.message}`);
    }
  }

  /**
   * Estimate row count for a query
   */
  async estimateRowCount(dto: EstimateRowCountDto): Promise<number> {
    try {
      // Wrap query in COUNT(*)
      const countQuery = `SELECT COUNT(*) as row_count FROM (${dto.query}) as subquery`;

      // In production, execute through connection service
      // For now, return a simulated count
      return 1000;
    } catch (error) {
      this.logger.error(`Row count estimation failed: ${error.message}`);
      throw new BadRequestException(`Row count estimation failed: ${error.message}`);
    }
  }

  /**
   * Basic query validation
   */
  private validateQuery(query: string): void {
    const dangerousPatterns = [
      /;\s*DROP\s+/i,
      /;\s*DELETE\s+/i,
      /;\s*UPDATE\s+/i,
      /;\s*INSERT\s+/i,
      /;\s*TRUNCATE\s+/i,
      /;\s*ALTER\s+/i,
      /;\s*CREATE\s+/i,
      /--.*$/m,
      /\/\*[\s\S]*?\*\//,
    ];

    for (const pattern of dangerousPatterns) {
      if (pattern.test(query)) {
        throw new BadRequestException('Query contains potentially dangerous SQL');
      }
    }
  }

  /**
   * Execute query through connection
   * This is a placeholder - in production, use actual database connection
   */
  private async executeQuery(
    connectionId: string,
    query: string,
    limit: number,
  ): Promise<Omit<PreviewResult, 'executionTimeMs' | 'query'>> {
    // In production, this would:
    // 1. Get connection from connection service
    // 2. Execute query with limit
    // 3. Parse results

    // Simulated response structure
    return {
      columns: [
        { name: 'id', type: 'INTEGER', nullable: false },
        { name: 'name', type: 'VARCHAR', nullable: true },
        { name: 'amount', type: 'DECIMAL', nullable: true },
        { name: 'created_at', type: 'TIMESTAMP', nullable: false },
      ],
      rows: [
        { id: 1, name: 'Sample 1', amount: 100.5, created_at: new Date().toISOString() },
        { id: 2, name: 'Sample 2', amount: 200.75, created_at: new Date().toISOString() },
      ],
      rowCount: 2,
      truncated: false,
    };
  }

  /**
   * Generate preview SQL from mappings
   */
  generatePreviewSQL(
    mappings: any[],
    databaseType: 'snowflake' | 'postgres' | 'mysql' | 'sqlserver',
    limit: number = 100,
  ): string {
    if (!mappings || mappings.length === 0) {
      throw new BadRequestException('No mappings provided');
    }

    const columns: string[] = [];
    const tables: Map<string, any> = new Map();

    // Collect unique tables and columns
    for (const mapping of mappings) {
      const tableKey = `${mapping.source_database}.${mapping.source_schema}.${mapping.source_table}`;
      if (!tables.has(tableKey)) {
        tables.set(tableKey, mapping);
      }
      columns.push(`${mapping.source_table}.${mapping.source_column}`);
    }

    // Build SELECT clause
    const selectClause = columns.join(', ');

    // Build FROM clause with joins
    const tableEntries = Array.from(tables.entries());
    let fromClause = '';

    for (let i = 0; i < tableEntries.length; i++) {
      const [, mapping] = tableEntries[i];
      const tableName = `${mapping.source_database}.${mapping.source_schema}.${mapping.source_table}`;

      if (i === 0) {
        fromClause = tableName;
      } else {
        const joinType = mapping.join_type || 'LEFT';
        fromClause += `\n${joinType} JOIN ${tableName}`;
        // In production, add ON clause based on join_keys
        fromClause += ` ON 1=1`; // Placeholder
      }
    }

    // Build final query with dialect-specific LIMIT
    let sql = `SELECT ${selectClause}\nFROM ${fromClause}`;

    switch (databaseType) {
      case 'snowflake':
      case 'postgres':
      case 'mysql':
        sql += `\nLIMIT ${limit}`;
        break;
      case 'sqlserver':
        sql = sql.replace('SELECT', `SELECT TOP ${limit}`);
        break;
    }

    return sql;
  }
}
