import { BaseApiService } from "../base.service";

export interface Connection {
  id: string;
  connectionName: string;
  connectionType: string;
  host?: string;
  port?: number;
  database?: string;
  username?: string;
  userId: string;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
}

class ConnectionService extends BaseApiService {
  /**
   * Get all connections for current user
   */
  async getConnections(): Promise<Connection[]> {
    try {
      const response = await this.api.get("/connections");
      return this.extractData<Connection[]>(response);
    } catch (error) {
      console.error("Failed to fetch connections:", error);
      throw error;
    }
  }

  /**
   * Get connection by ID
   */
  async getConnection(connectionId: string): Promise<Connection> {
    try {
      const response = await this.api.get(`/connections/${connectionId}`);
      return this.extractData<Connection>(response);
    } catch (error) {
      console.error(`Failed to fetch connection ${connectionId}:`, error);
      throw error;
    }
  }

  /**
   * Test database connection
   */
  async testConnection(
    connectionId: string
  ): Promise<{ success: boolean; message: string }> {
    try {
      const response = await this.api.get(`/connections/${connectionId}/test`);
      return this.extractData<{ success: boolean; message: string }>(response);
    } catch (error) {
      console.error(`Failed to test connection ${connectionId}:`, error);
      throw error;
    }
  }

  /**
   * Get databases for a connection
   */
  async getDatabases(connectionId: string): Promise<string[]> {
    try {
      const response = await this.api.post("/connections/databases", {
        connectionId,
      });
      return this.extractData<string[]>(response);
    } catch (error) {
      console.error(
        `Failed to fetch databases for connection ${connectionId}:`,
        error
      );
      throw error;
    }
  }

  /**
   * Get schemas for a database
   */
  async getSchemas(connectionId: string, database: string): Promise<string[]> {
    try {
      const response = await this.api.post("/connections/schemas", {
        connectionId,
        database,
      });
      return this.extractData<string[]>(response);
    } catch (error) {
      console.error(`Failed to fetch schemas for database ${database}:`, error);
      throw error;
    }
  }
}

export const connectionService = new ConnectionService();
