import { Injectable, Logger, BadRequestException, UnauthorizedException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios from 'axios';
import * as snowflake from 'snowflake-sdk';
import { EncryptionService } from '../../../common/services/encryption.service';
import { SnowflakeTokenResponseDto } from '../../auth/dto/snowflake-sso-login.dto';

export interface SnowflakeConnectionConfig {
  account: string;
  username?: string;
  password?: string;
  authenticator?: string;
  token?: string;
  warehouse?: string;
  database?: string;
  schema?: string;
  role?: string;
}

export interface SnowflakeOAuthTokens {
  accessToken: string;
  refreshToken: string;
  expiresIn: number;
  refreshTokenExpiresIn: number;
  username: string;
  userFirstName?: string;
  userLastName?: string;
  scope?: string;
}

@Injectable()
export class SnowflakeService {
  private readonly logger = new Logger(SnowflakeService.name);
  private readonly encryptionService: EncryptionService;
  private readonly snowflakeAccount: string;
  private readonly snowflakeClientId: string;
  private readonly snowflakeClientSecret: string;
  private readonly snowflakeRedirectUri: string;
  private readonly snowflakeWarehouse: string;
  private readonly snowflakeDatabase: string;
  private readonly snowflakeSchema: string;

  constructor(private readonly configService: ConfigService) {
    this.encryptionService = new EncryptionService(
      this.configService.get<string>('ENCRYPTION_KEY'),
    );

    this.snowflakeAccount = this.configService.get<string>('SNOWFLAKE_ACCOUNT');
    this.snowflakeClientId = this.configService.get<string>('SNOWFLAKE_CLIENT_ID');
    this.snowflakeClientSecret = this.configService.get<string>('SNOWFLAKE_CLIENT_SECRET');
    this.snowflakeRedirectUri = this.configService.get<string>('SNOWFLAKE_REDIRECT_URI');
    this.snowflakeWarehouse = this.configService.get<string>('SNOWFLAKE_WAREHOUSE');
    this.snowflakeDatabase = this.configService.get<string>('SNOWFLAKE_DATABASE');
    this.snowflakeSchema = this.configService.get<string>('SNOWFLAKE_SCHEMA');
  }

  /**
   * Generate Snowflake OAuth authorization URL for SSO login
   */
  generateAuthUrl(clientId?: string, account?: string, redirectUri?: string): string {
    const effectiveClientId = clientId || this.snowflakeClientId;
    const effectiveAccount = account || this.snowflakeAccount;
    const effectiveRedirectUri = redirectUri || this.snowflakeRedirectUri;

    if (!effectiveClientId || !effectiveAccount) {
      throw new BadRequestException('Snowflake OAuth configuration is incomplete');
    }

    const baseUrl = `https://${effectiveAccount}.snowflakecomputing.com/oauth/authorize`;
    const params = new URLSearchParams({
      client_id: effectiveClientId,
      redirect_uri: effectiveRedirectUri,
      response_type: 'code',
      scope: 'refresh_token session:role:PUBLIC',
    });

    return `${baseUrl}?${params.toString()}`;
  }

  /**
   * Exchange authorization code for access and refresh tokens
   */
  async exchangeCodeForTokens(
    code: string,
    clientId: string,
    clientSecret: string,
    account: string,
    redirectUri?: string,
  ): Promise<SnowflakeOAuthTokens> {
    const tokenEndpoint = `https://${account}.snowflakecomputing.com/oauth/token-request`;
    const effectiveRedirectUri = redirectUri || this.snowflakeRedirectUri;

    try {
      const response = await axios.post(
        tokenEndpoint,
        new URLSearchParams({
          grant_type: 'authorization_code',
          code: code,
          redirect_uri: effectiveRedirectUri,
          client_id: clientId,
          client_secret: clientSecret,
        }),
        {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
          },
        },
      );

      const data = response.data;

      return {
        accessToken: data.access_token,
        refreshToken: data.refresh_token,
        expiresIn: data.expires_in,
        refreshTokenExpiresIn: data.refresh_token_expires_in,
        username: data.username,
        userFirstName: data.user_first_name,
        userLastName: data.user_last_name,
        scope: data.scope,
      };
    } catch (error) {
      this.logger.error('Failed to exchange code for tokens', error);

      if (error.response) {
        throw new UnauthorizedException(
          `Snowflake OAuth failed: ${error.response.data.error_description || error.response.data.error}`,
        );
      }

      throw new UnauthorizedException('Failed to authenticate with Snowflake');
    }
  }

  /**
   * Refresh Snowflake OAuth access token
   */
  async refreshAccessToken(
    refreshToken: string,
    clientId: string,
    clientSecret: string,
    account: string,
  ): Promise<SnowflakeOAuthTokens> {
    const tokenEndpoint = `https://${account}.snowflakecomputing.com/oauth/token-request`;

    try {
      const response = await axios.post(
        tokenEndpoint,
        new URLSearchParams({
          grant_type: 'refresh_token',
          refresh_token: refreshToken,
          client_id: clientId,
          client_secret: clientSecret,
        }),
        {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
          },
        },
      );

      const data = response.data;

      return {
        accessToken: data.access_token,
        refreshToken: data.refresh_token || refreshToken,
        expiresIn: data.expires_in,
        refreshTokenExpiresIn: data.refresh_token_expires_in,
        username: data.username,
        userFirstName: data.user_first_name,
        userLastName: data.user_last_name,
        scope: data.scope,
      };
    } catch (error) {
      this.logger.error('Failed to refresh access token', error);
      throw new UnauthorizedException('Failed to refresh Snowflake access token');
    }
  }

  /**
   * Create Snowflake connection with OAuth
   */
  async createOAuthConnection(config: SnowflakeConnectionConfig): Promise<snowflake.Connection> {
    return new Promise((resolve, reject) => {
      const connection = snowflake.createConnection({
        account: config.account,
        username: config.username,
        authenticator: 'oauth',
        token: config.token,
        warehouse: config.warehouse,
        database: config.database,
        schema: config.schema,
        role: config.role,
      });

      connection.connect((err, conn) => {
        if (err) {
          this.logger.error('Failed to connect to Snowflake with OAuth', err);
          reject(new BadRequestException('Failed to connect to Snowflake: ' + err.message));
        } else {
          this.logger.log('Successfully connected to Snowflake with OAuth');
          resolve(conn);
        }
      });
    });
  }

  /**
   * Create Snowflake connection with username/password
   */
  async createPasswordConnection(config: SnowflakeConnectionConfig): Promise<snowflake.Connection> {
    return new Promise((resolve, reject) => {
      this.logger.log(
        `Attempting to connect to Snowflake account: ${config.account} with username: ${config.username}`,
      );

      const connection = snowflake.createConnection({
        account: config.account,
        username: config.username,
        password: config.password,
        warehouse: config.warehouse,
        database: config.database,
        schema: config.schema,
        role: config.role,
      });

      connection.connect((err, conn) => {
        if (err) {
          this.logger.error(
            `Failed to connect to Snowflake - Account: ${config.account}, Username: ${config.username}`,
            err,
          );

          let errorMessage = err.message || 'Unknown error';

          // Provide more helpful error messages
          if (errorMessage.includes('Incorrect username or password')) {
            errorMessage =
              `Authentication failed for user '${config.username}' on account '${config.account}'. Please verify:\n` +
              `1. Username is correct (case-sensitive)\n` +
              `2. Password is correct\n` +
              `3. User exists in this Snowflake account\n` +
              `4. Account identifier is correct`;
          } else if (errorMessage.includes('does not exist')) {
            errorMessage = `Account '${config.account}' not found. Please verify the account identifier.`;
          } else if (errorMessage.includes('Network')) {
            errorMessage = `Network error connecting to Snowflake account '${config.account}'. Check your connection.`;
          }

          reject(new BadRequestException(errorMessage));
        } else {
          this.logger.log(
            `Successfully connected to Snowflake - Account: ${config.account}, Username: ${config.username}`,
          );
          resolve(conn);
        }
      });
    });
  }

  /**
   * Test Snowflake connection
   */
  async testConnection(connection: snowflake.Connection): Promise<boolean> {
    return new Promise((resolve, reject) => {
      connection.execute({
        sqlText: 'SELECT CURRENT_VERSION()',
        complete: (err, stmt, rows) => {
          if (err) {
            this.logger.error('Connection test failed', err);
            reject(err);
          } else {
            this.logger.log('Connection test successful');
            resolve(true);
          }
        },
      });
    });
  }

  /**
   * Execute SQL query on Snowflake
   */
  async executeQuery(
    connection: snowflake.Connection,
    sqlText: string,
    binds?: any[],
  ): Promise<any[]> {
    return new Promise((resolve, reject) => {
      connection.execute({
        sqlText,
        binds,
        complete: (err, stmt, rows) => {
          if (err) {
            this.logger.error('Query execution failed', err);
            reject(err);
          } else {
            resolve(rows);
          }
        },
      });
    });
  }

  /**
   * Get list of databases from Snowflake
   */
  async getDatabases(connection: snowflake.Connection): Promise<string[]> {
    try {
      const rows = await this.executeQuery(connection, 'SHOW DATABASES');
      return rows.map((row: any) => row.name);
    } catch (error) {
      this.logger.error('Failed to fetch databases', error);
      throw new BadRequestException('Failed to fetch databases from Snowflake');
    }
  }

  /**
   * Get list of schemas in a database
   */
  async getSchemas(connection: snowflake.Connection, database: string): Promise<string[]> {
    try {
      const rows = await this.executeQuery(connection, `SHOW SCHEMAS IN DATABASE ${database}`);
      return rows.map((row: any) => row.name);
    } catch (error) {
      this.logger.error(`Failed to fetch schemas from database ${database}`, error);
      throw new BadRequestException(`Failed to fetch schemas from database ${database}`);
    }
  }

  /**
   * Get list of tables in a schema
   */
  async getTables(
    connection: snowflake.Connection,
    database: string,
    schema: string,
  ): Promise<string[]> {
    try {
      const rows = await this.executeQuery(connection, `SHOW TABLES IN ${database}.${schema}`);
      return rows.map((row: any) => row.name);
    } catch (error) {
      this.logger.error(`Failed to fetch tables from ${database}.${schema}`, error);
      throw new BadRequestException(`Failed to fetch tables from ${database}.${schema}`);
    }
  }

  /**
   * Get list of columns in a table
   */
  async getColumns(
    connection: snowflake.Connection,
    database: string,
    schema: string,
    table: string,
  ): Promise<Array<{ name: string; type: string }>> {
    try {
      const rows = await this.executeQuery(
        connection,
        `SHOW COLUMNS IN ${database}.${schema}.${table}`,
      );
      return rows.map((row: any) => ({
        name: row.column_name,
        type: row.data_type,
      }));
    } catch (error) {
      this.logger.error(`Failed to fetch columns from ${database}.${schema}.${table}`, error);
      throw new BadRequestException(`Failed to fetch columns from ${database}.${schema}.${table}`);
    }
  }

  /**
   * Close Snowflake connection
   */
  async closeConnection(connection: snowflake.Connection): Promise<void> {
    return new Promise((resolve, reject) => {
      connection.destroy((err) => {
        if (err) {
          this.logger.error('Failed to close connection', err);
          reject(err);
        } else {
          this.logger.log('Connection closed successfully');
          resolve();
        }
      });
    });
  }

  /**
   * Encrypt sensitive connection data
   */
  encryptCredentials(credentials: Record<string, any>): string {
    return this.encryptionService.encrypt(JSON.stringify(credentials));
  }

  /**
   * Decrypt sensitive connection data
   */
  decryptCredentials(encryptedCredentials: string): Record<string, any> {
    try {
      const decrypted = this.encryptionService.decrypt(encryptedCredentials);
      return JSON.parse(decrypted);
    } catch (error) {
      this.logger.error('Failed to decrypt credentials', error);
      throw new BadRequestException('Failed to decrypt credentials');
    }
  }
}
