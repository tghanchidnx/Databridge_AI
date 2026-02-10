import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import * as snowflake from 'snowflake-sdk';
import { ConfigService } from '@nestjs/config';
import * as fs from 'fs';

export interface SnowflakeConnectionConfig {
  account: string;
  username: string;
  password?: string;
  authenticator?: string;
  token?: string;
  database?: string;
  schema?: string;
  warehouse?: string;
  role?: string;
  privateKey?: string;
  privateKeyPath?: string;
}

export interface QueryResult {
  rows: any[];
  rowCount: number;
  columns: Array<{ name: string; type: string }>;
}

@Injectable()
export class SnowflakeService implements OnModuleDestroy {
  private readonly logger = new Logger(SnowflakeService.name);
  private connections: Map<string, snowflake.Connection> = new Map();

  constructor(private configService: ConfigService) {}

  async createConnection(
    config: SnowflakeConnectionConfig,
    connectionId?: string,
  ): Promise<string> {
    const id = connectionId || this.generateConnectionId();

    return new Promise((resolve, reject) => {
      // Prepare connection config
      const connectionConfig: any = {
        account: config.account,
        username: config.username,
        authenticator: config.authenticator || 'SNOWFLAKE',
        database: config.database,
        schema: config.schema,
        warehouse: config.warehouse,
        role: config.role,
      };

      // Add authentication method
      if (config.privateKey) {
        // Use provided private key directly
        connectionConfig.privateKey = config.privateKey;
        connectionConfig.authenticator = 'SNOWFLAKE_JWT';
      } else if (config.privateKeyPath) {
        // Read private key from file
        try {
          const privateKeyData = fs.readFileSync(config.privateKeyPath, 'utf8');
          connectionConfig.privateKey = privateKeyData;
          connectionConfig.authenticator = 'SNOWFLAKE_JWT';
        } catch (error) {
          this.logger.error(
            `Failed to read private key from ${config.privateKeyPath}: ${error.message}`,
          );
          return reject(new Error(`Failed to read RSA private key: ${error.message}`));
        }
      } else if (config.token) {
        // OAuth token
        connectionConfig.token = config.token;
      } else if (config.password) {
        // Password authentication
        connectionConfig.password = config.password;
      } else {
        return reject(
          new Error('No authentication method provided (password, privateKey, or token)'),
        );
      }

      const connection = snowflake.createConnection(connectionConfig);

      connection.connect((err, conn) => {
        if (err) {
          this.logger.error(`Failed to connect to Snowflake: ${err.message}`);
          reject(err);
        } else {
          this.connections.set(id, conn);
          this.logger.log(`✅ Snowflake connection established: ${id}`);
          resolve(id);
        }
      });
    });
  }

  async executeQuery(connectionId: string, sqlText: string, binds?: any[]): Promise<QueryResult> {
    const connection = this.connections.get(connectionId);

    if (!connection) {
      throw new Error(`Connection not found: ${connectionId}`);
    }

    return new Promise((resolve, reject) => {
      connection.execute({
        sqlText,
        binds,
        complete: (err, stmt, rows) => {
          if (err) {
            this.logger.error(`Query execution failed: ${err.message}`);
            reject(err);
          } else {
            const columns = stmt.getColumns().map((col) => ({
              name: col.getName(),
              type: col.getType(),
            }));

            resolve({
              rows: rows || [],
              rowCount: stmt.getNumRows(),
              columns,
            });
          }
        },
      });
    });
  }

  async getDatabases(connectionId: string): Promise<string[]> {
    const result = await this.executeQuery(connectionId, 'SHOW DATABASES');
    return result.rows.map((row) => row.name);
  }

  async getSchemas(connectionId: string, database: string): Promise<string[]> {
    const result = await this.executeQuery(connectionId, `SHOW SCHEMAS IN DATABASE ${database}`);
    return result.rows.map((row) => row.name);
  }

  async getTables(connectionId: string, database: string, schema: string): Promise<string[]> {
    const result = await this.executeQuery(connectionId, `SHOW TABLES IN ${database}.${schema}`);
    return result.rows.map((row: any) => row.name || row.NAME);
  }

  async getTableDDL(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
  ): Promise<string> {
    const result = await this.executeQuery(
      connectionId,
      `SELECT GET_DDL('TABLE', '${database}.${schema}.${table}') AS DDL`,
    );
    return result.rows[0]?.DDL || '';
  }

  async getTableColumns(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
  ): Promise<any[]> {
    const result = await this.executeQuery(
      connectionId,
      `DESC TABLE ${database}.${schema}.${table}`,
    );
    return result.rows;
  }

  async getTableData(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    limit: number = 100,
  ): Promise<QueryResult> {
    return this.executeQuery(
      connectionId,
      `SELECT * FROM ${database}.${schema}.${table} LIMIT ${limit}`,
    );
  }

  async testConnection(config: SnowflakeConnectionConfig): Promise<boolean> {
    const testId = `test_${Date.now()}`;

    try {
      await this.createConnection(config, testId);
      await this.closeConnection(testId);
      return true;
    } catch (error) {
      this.logger.error(`Connection test failed: ${error.message}`);
      return false;
    }
  }

  async closeConnection(connectionId: string): Promise<void> {
    const connection = this.connections.get(connectionId);

    if (connection) {
      return new Promise((resolve, reject) => {
        connection.destroy((err) => {
          if (err) {
            this.logger.error(`Failed to close connection: ${err.message}`);
            reject(err);
          } else {
            this.connections.delete(connectionId);
            this.logger.log(`Connection closed: ${connectionId}`);
            resolve();
          }
        });
      });
    }
  }

  async closeAllConnections(): Promise<void> {
    const promises = Array.from(this.connections.keys()).map((id) => this.closeConnection(id));
    await Promise.all(promises);
  }

  async onModuleDestroy() {
    await this.closeAllConnections();
  }

  private generateConnectionId(): string {
    return `sf_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  getActiveConnections(): string[] {
    return Array.from(this.connections.keys());
  }

  isConnectionActive(connectionId: string): boolean {
    return this.connections.has(connectionId);
  }
}
