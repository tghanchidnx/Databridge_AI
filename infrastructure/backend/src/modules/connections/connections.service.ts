// Prisma type generation incomplete - using type assertions for extended schema fields
/* eslint-disable @typescript-eslint/no-explicit-any */
import { Injectable, NotFoundException, BadRequestException, Logger } from '@nestjs/common';
import { PrismaService } from '../../database/prisma/prisma.service';
import { CreateConnectionDto } from './dto/create-connection.dto';
import { UpdateConnectionDto } from './dto/update-connection.dto';
import { SnowflakeService } from './services/snowflake.service';
import { SnowflakeService as SnowflakeQueryService } from '../../database/snowflake/snowflake.service';
import {
  SnowflakeOAuthCallbackDto,
  SnowflakePasswordConnectionDto,
  RefreshSnowflakeTokenDto,
} from './dto/snowflake-oauth-connection.dto';
import * as mysql from 'mysql2';
import { Client } from 'pg';

@Injectable()
export class ConnectionsService {
  private readonly logger = new Logger(ConnectionsService.name);

  constructor(
    private prisma: PrismaService,
    private snowflakeService: SnowflakeService,
    private snowflakeQueryService: SnowflakeQueryService,
  ) {}

  async create(userId: string, createConnectionDto: CreateConnectionDto) {
    // Encrypt credentials if provided
    const encryptedCredentials = createConnectionDto.credentials
      ? this.snowflakeService.encryptCredentials(createConnectionDto.credentials)
      : null;

    const connection = await this.prisma.connection.create({
      data: {
        userId,
        connectionName: createConnectionDto.connectionName,
        connectionType: createConnectionDto.connectionType,
        credentials: encryptedCredentials,
        description: createConnectionDto.description,
        serverType: 'Snowflake',
        status: 'active',
      } as any,
    });

    this.logger.log(`Connection created: ${connection.connectionName}`);
    return this.sanitizeConnection(connection);
  }

  /**
   * Create Snowflake OAuth connection
   */
  async createSnowflakeOAuthConnection(userId: string, callbackDto: SnowflakeOAuthCallbackDto) {
    try {
      // Exchange code for tokens
      const tokens = await this.snowflakeService.exchangeCodeForTokens(
        callbackDto.code,
        callbackDto.snowflakeClientId,
        callbackDto.snowflakeClientSecret,
        callbackDto.snowflakeAccount,
      );

      // Calculate expiry times
      const accessExpiryTime = new Date(Date.now() + tokens.expiresIn * 1000);
      const refreshExpiryTime = new Date(Date.now() + tokens.refreshTokenExpiresIn * 1000);

      // Test the connection
      const snowflakeConn = await this.snowflakeService.createOAuthConnection({
        account: callbackDto.snowflakeAccount,
        username: tokens.username,
        token: tokens.accessToken,
        warehouse: callbackDto.snowflakeWarehouse,
        database: callbackDto.databaseName,
        schema: callbackDto.schemaName,
      });

      await this.snowflakeService.testConnection(snowflakeConn);
      await this.snowflakeService.closeConnection(snowflakeConn);

      // Check if connection exists
      const existingConnection = await this.prisma.connection.findFirst({
        where: {
          userId,
          connectionName: callbackDto.connectionName,
        },
      });

      let connection;
      if (existingConnection) {
        // Update existing connection
        // @ts-ignore - Prisma type generation issue
        connection = await this.prisma.connection.update({
          where: { id: existingConnection.id },
          data: {
            connectionType: 'oauth',
            serverType: 'Snowflake',
            snowflakeAccount: callbackDto.snowflakeAccount,
            snowflakeWarehouse: callbackDto.snowflakeWarehouse,
            snowflakeDatabase: callbackDto.databaseName,
            snowflakeSchema: callbackDto.schemaName,
            snowflakeUser: tokens.username,
            accessToken: this.snowflakeService
              .encryptCredentials({ token: tokens.accessToken })
              .toString(),
            refreshToken: this.snowflakeService
              .encryptCredentials({ token: tokens.refreshToken })
              .toString(),
            accessExpiryTime,
            refreshExpiryTime,
            snowflakeClientId: this.snowflakeService
              .encryptCredentials({ clientId: callbackDto.snowflakeClientId })
              .toString(),
            snowflakeClientSecret: this.snowflakeService
              .encryptCredentials({ clientSecret: callbackDto.snowflakeClientSecret })
              .toString(),
            status: 'active',
            lastTestedAt: new Date(),
          },
        });
      } else {
        // Create new connection
        connection = await this.prisma.connection.create({
          data: {
            userId,
            connectionName: callbackDto.connectionName,
            connectionType: 'oauth',
            serverType: 'Snowflake',
            snowflakeAccount: callbackDto.snowflakeAccount,
            snowflakeWarehouse: callbackDto.snowflakeWarehouse,
            snowflakeDatabase: callbackDto.databaseName,
            snowflakeSchema: callbackDto.schemaName,
            snowflakeUser: tokens.username,
            accessToken: this.snowflakeService
              .encryptCredentials({ token: tokens.accessToken })
              .toString(),
            refreshToken: this.snowflakeService
              .encryptCredentials({ token: tokens.refreshToken })
              .toString(),
            accessExpiryTime,
            refreshExpiryTime,
            snowflakeClientId: this.snowflakeService
              .encryptCredentials({ clientId: callbackDto.snowflakeClientId })
              .toString(),
            snowflakeClientSecret: this.snowflakeService
              .encryptCredentials({ clientSecret: callbackDto.snowflakeClientSecret })
              .toString(),
            status: 'active',
            lastTestedAt: new Date(),
          },
        });
      }

      this.logger.log(`Snowflake OAuth connection created/updated: ${connection.connectionName}`);
      return {
        message: 'Connection successfully created',
        connection: this.sanitizeConnection(connection),
      };
    } catch (error) {
      this.logger.error('Failed to create Snowflake OAuth connection', error);
      throw new BadRequestException(error.message);
    }
  }

  /**
   * Create Snowflake password-based connection
   */
  async createSnowflakePasswordConnection(
    userId: string,
    passwordDto: SnowflakePasswordConnectionDto,
  ) {
    try {
      // Test the connection
      const snowflakeConn = await this.snowflakeService.createPasswordConnection({
        account: passwordDto.snowflakeAccount,
        username: passwordDto.username,
        password: passwordDto.password,
        warehouse: passwordDto.snowflakeWarehouse,
        database: passwordDto.databaseName,
        schema: passwordDto.schemaName,
      });

      await this.snowflakeService.testConnection(snowflakeConn);
      await this.snowflakeService.closeConnection(snowflakeConn);

      // Encrypt password
      const encryptedCredentials = this.snowflakeService.encryptCredentials({
        username: passwordDto.username,
        password: passwordDto.password,
      });

      // Check if connection exists
      const existingConnection = await this.prisma.connection.findFirst({
        where: {
          userId,
          connectionName: passwordDto.connectionName,
        },
      });

      let connection;
      if (existingConnection) {
        connection = await this.prisma.connection.update({
          where: { id: existingConnection.id },
          data: {
            connectionType: 'password',
            serverType: 'Snowflake',
            snowflakeAccount: passwordDto.snowflakeAccount,
            snowflakeWarehouse: passwordDto.snowflakeWarehouse,
            snowflakeDatabase: passwordDto.databaseName,
            snowflakeSchema: passwordDto.schemaName,
            snowflakeUser: passwordDto.username,
            credentials: encryptedCredentials,
            status: 'active',
            lastTestedAt: new Date(),
          },
        });
      } else {
        connection = await this.prisma.connection.create({
          data: {
            userId,
            connectionName: passwordDto.connectionName,
            connectionType: 'password',
            serverType: 'Snowflake',
            snowflakeAccount: passwordDto.snowflakeAccount,
            snowflakeWarehouse: passwordDto.snowflakeWarehouse,
            snowflakeDatabase: passwordDto.databaseName,
            snowflakeSchema: passwordDto.schemaName,
            snowflakeUser: passwordDto.username,
            credentials: encryptedCredentials,
            status: 'active',
            lastTestedAt: new Date(),
          },
        });
      }

      this.logger.log(
        `Snowflake password connection created/updated: ${connection.connectionName}`,
      );
      return {
        message: 'Connection successfully created',
        connection: this.sanitizeConnection(connection),
      };
    } catch (error) {
      this.logger.error('Failed to create Snowflake password connection', error);
      throw new BadRequestException(error.message);
    }
  }

  /**
   * Refresh Snowflake OAuth token
   */
  async refreshSnowflakeToken(userId: string, connectionId: string) {
    const connection = (await this.prisma.connection.findFirst({
      where: { id: connectionId, userId },
    })) as any;

    if (!connection) {
      throw new NotFoundException('Connection not found');
    }

    if (connection.connectionType !== 'oauth') {
      throw new BadRequestException('Only OAuth connections can be refreshed');
    }

    try {
      const refreshTokenData = this.snowflakeService.decryptCredentials(connection.refreshToken);
      const clientIdData = this.snowflakeService.decryptCredentials(connection.snowflakeClientId);
      const clientSecretData = this.snowflakeService.decryptCredentials(
        connection.snowflakeClientSecret,
      );

      const tokens = await this.snowflakeService.refreshAccessToken(
        refreshTokenData.token,
        clientIdData.clientId,
        clientSecretData.clientSecret,
        connection.snowflakeAccount,
      );

      const accessExpiryTime = new Date(Date.now() + tokens.expiresIn * 1000);
      const refreshExpiryTime = new Date(Date.now() + tokens.refreshTokenExpiresIn * 1000);

      const updatedConnection = await this.prisma.connection.update({
        where: { id: connectionId },
        data: {
          accessToken: this.snowflakeService
            .encryptCredentials({ token: tokens.accessToken })
            .toString(),
          refreshToken: this.snowflakeService
            .encryptCredentials({ token: tokens.refreshToken })
            .toString(),
          accessExpiryTime,
          refreshExpiryTime,
        },
      });

      this.logger.log(`Token refreshed for connection: ${connection.connectionName}`);
      return {
        message: 'Token refreshed successfully',
        connection: this.sanitizeConnection(updatedConnection),
      };
    } catch (error) {
      this.logger.error('Failed to refresh token', error);
      throw new BadRequestException('Failed to refresh token: ' + error.message);
    }
  }

  async findAll(userId: string, userEmail?: string) {
    const connections = await this.prisma.connection.findMany({
      where: { userId },
      orderBy: { createdAt: 'desc' },
    });

    // Add system-wide default Hierarchy Builder Core connection ONLY for @datanexum.com users
    const isDatanexumUser = userEmail && userEmail.toLowerCase().endsWith('@datanexum.com');

    if (isDatanexumUser) {
      const useRsa =
        this.snowflakeService['configService'].get<string>('SNOWFLAKE_USE_RSA') === 'true';
      const defaultConnection = {
        id: 'hierarchy-builder-core',
        userId: 'system',
        connectionName: 'Hierarchy Builder Core (SBAIG)',
        connectionType: useRsa ? 'rsa' : 'password',
        serverType: 'Snowflake',
        status: 'active',
        description:
          'System default connection for Datanexum users - TRANSFORMATION.CONFIGURATION database',
        snowflakeAccount: this.snowflakeService['configService'].get<string>('SNOWFLAKE_ACCOUNT'),
        snowflakeUser: this.snowflakeService['configService'].get<string>('SNOWFLAKE_USER'),
        snowflakeWarehouse:
          this.snowflakeService['configService'].get<string>('SNOWFLAKE_WAREHOUSE'),
        snowflakeDatabase: this.snowflakeService['configService'].get<string>('SNOWFLAKE_DATABASE'),
        snowflakeSchema: this.snowflakeService['configService'].get<string>('SNOWFLAKE_SCHEMA'),
        isSystemDefault: true,
        isReadOnly: false, // Users can use this connection
        credentials: this.snowflakeService.encryptCredentials({
          password: this.snowflakeService['configService'].get<string>('SNOWFLAKE_PASSWORD'),
          useRsa: useRsa,
          rsaPrivateKeyPath: this.snowflakeService['configService'].get<string>(
            'SNOWFLAKE_RSA_PRIVATE_KEY_PATH',
          ),
        }),
        createdAt: new Date('2024-01-01'),
        updatedAt: new Date('2024-01-01'),
      };

      return [
        this.sanitizeConnection(defaultConnection),
        ...connections.map((conn) => this.sanitizeConnection(conn)),
      ];
    }

    return connections.map((conn) => this.sanitizeConnection(conn));
  }

  async findOne(id: string, userId: string) {
    const connection = await this.prisma.connection.findFirst({
      where: { id, userId },
      include: {
        serverIntegrations: true,
      },
    });

    if (!connection) {
      throw new NotFoundException(`Connection with ID ${id} not found`);
    }

    return this.sanitizeConnection(connection);
  }

  async update(id: string, userId: string, updateConnectionDto: UpdateConnectionDto) {
    await this.findOne(id, userId); // Check if connection exists

    const updateData: any = {
      connectionName: updateConnectionDto.connectionName,
      connectionType: updateConnectionDto.connectionType,
      description: updateConnectionDto.description,
    };

    if (updateConnectionDto.credentials) {
      updateData.credentials = this.snowflakeService.encryptCredentials(
        updateConnectionDto.credentials,
      );
    }

    const connection = await this.prisma.connection.update({
      where: { id },
      data: updateData,
    });

    this.logger.log(`Connection updated: ${connection.connectionName}`);
    return this.sanitizeConnection(connection);
  }

  async remove(id: string, userId: string) {
    await this.findOne(id, userId); // Check if connection exists

    await this.prisma.connection.delete({
      where: { id },
    });

    this.logger.log(`Connection deleted: ${id}`);
    return { message: 'Connection deleted successfully' };
  }

  async testConnection(id: string, userId: string) {
    const connection = (await this.prisma.connection.findFirst({
      where: { id, userId },
    })) as any;

    if (!connection) {
      throw new NotFoundException('Connection not found');
    }

    try {
      const serverType = connection.serverType || 'Snowflake';

      // Handle different database types
      if (serverType === 'mysql') {
        // Test MySQL connection
        const credentials = this.snowflakeService.decryptCredentials(connection.credentials);
        const mysqlConnection = mysql.createConnection({
          host: connection.host,
          port: connection.port,
          user: credentials.username,
          password: credentials.password,
          database: connection.databaseName,
        });

        await new Promise<void>((resolve, reject) => {
          mysqlConnection.connect((err) => {
            if (err) reject(err);
            else resolve();
          });
        });

        await new Promise<void>((resolve, reject) => {
          mysqlConnection.ping((err) => {
            mysqlConnection.end();
            if (err) reject(err);
            else resolve();
          });
        });

        await this.prisma.connection.update({
          where: { id },
          data: { lastTestedAt: new Date() },
        });

        return {
          success: true,
          message: 'MySQL connection successful',
        };
      } else if (serverType === 'postgresql') {
        // Test PostgreSQL connection
        const credentials = this.snowflakeService.decryptCredentials(connection.credentials);
        const client = new Client({
          host: connection.host,
          port: connection.port,
          user: credentials.username,
          password: credentials.password,
          database: connection.databaseName,
        });

        await client.connect();
        await client.query('SELECT 1');
        await client.end();

        await this.prisma.connection.update({
          where: { id },
          data: { lastTestedAt: new Date() },
        });

        return {
          success: true,
          message: 'PostgreSQL connection successful',
        };
      } else {
        // Default to Snowflake
        let snowflakeConn;

        if (connection.connectionType === 'oauth') {
          // Check if token is expired
          if (connection.accessExpiryTime && new Date() > connection.accessExpiryTime) {
            // Try to refresh token
            await this.refreshSnowflakeToken(userId, id);
            // Re-fetch connection with new token
            const refreshedConnection = (await this.prisma.connection.findUnique({
              where: { id },
            })) as any;
            const accessTokenData = this.snowflakeService.decryptCredentials(
              refreshedConnection.accessToken,
            );

            snowflakeConn = await this.snowflakeService.createOAuthConnection({
              account: refreshedConnection.snowflakeAccount,
              username: refreshedConnection.snowflakeUser,
              token: accessTokenData.token,
              warehouse: refreshedConnection.snowflakeWarehouse,
              database: refreshedConnection.snowflakeDatabase,
              schema: refreshedConnection.snowflakeSchema,
            });
          } else {
            const accessTokenData = this.snowflakeService.decryptCredentials(
              connection.accessToken,
            );

            snowflakeConn = await this.snowflakeService.createOAuthConnection({
              account: connection.snowflakeAccount,
              username: connection.snowflakeUser,
              token: accessTokenData.token,
              warehouse: connection.snowflakeWarehouse,
              database: connection.snowflakeDatabase,
              schema: connection.snowflakeSchema,
            });
          }
        } else if (connection.connectionType === 'password') {
          const credentials = this.snowflakeService.decryptCredentials(connection.credentials);

          snowflakeConn = await this.snowflakeService.createPasswordConnection({
            account: connection.snowflakeAccount,
            username: credentials.username,
            password: credentials.password,
            warehouse: connection.snowflakeWarehouse,
            database: connection.snowflakeDatabase,
            schema: connection.snowflakeSchema,
          });
        } else {
          throw new BadRequestException('Unsupported connection type');
        }

        await this.snowflakeService.testConnection(snowflakeConn);
        await this.snowflakeService.closeConnection(snowflakeConn);

        // Update last tested timestamp
        await this.prisma.connection.update({
          where: { id },
          data: { lastTestedAt: new Date() },
        });

        return {
          success: true,
          message: 'Snowflake connection successful',
        };
      }
    } catch (error) {
      this.logger.error(`Connection test failed: ${error.message}`);
      return {
        success: false,
        message: error.message,
      };
    }
  }

  private sanitizeConnection(connection: any) {
    const {
      credentials,
      accessToken,
      refreshToken,
      snowflakeClientId,
      snowflakeClientSecret,
      ...rest
    } = connection;

    return {
      ...rest,
      hasCredentials: !!credentials,
      hasOAuthTokens: !!accessToken && !!refreshToken,
    };
  }

  /**
   * Create MySQL database connection
   */
  async createMySQLConnection(
    userId: string,
    mysqlDto: {
      connectionName: string;
      host: string;
      port: number;
      database: string;
      username: string;
      password: string;
    },
  ) {
    try {
      // Test the MySQL connection first
      const mysql = require('mysql2/promise');
      const testConnection = await mysql.createConnection({
        host: mysqlDto.host,
        port: mysqlDto.port,
        user: mysqlDto.username,
        password: mysqlDto.password,
        database: mysqlDto.database,
      });

      await testConnection.ping();
      await testConnection.end();

      // Encrypt credentials
      const encryptedCredentials = this.snowflakeService.encryptCredentials({
        username: mysqlDto.username,
        password: mysqlDto.password,
      });

      // Save connection to database
      const connection = await this.prisma.connection.create({
        data: {
          userId,
          connectionName: mysqlDto.connectionName,
          serverType: 'mysql',
          connectionType: 'password',
          host: mysqlDto.host,
          port: mysqlDto.port,
          databaseName: mysqlDto.database,
          credentials: encryptedCredentials,
          status: 'active',
          authType: 'password',
        },
      });

      this.logger.log(`MySQL connection created: ${connection.connectionName}`);
      return this.sanitizeConnection(connection);
    } catch (error) {
      this.logger.error(`Failed to create MySQL connection: ${error.message}`);
      throw new BadRequestException(`Failed to connect to MySQL: ${error.message}`);
    }
  }

  /**
   * Create PostgreSQL database connection
   */
  async createPostgreSQLConnection(
    userId: string,
    pgDto: {
      connectionName: string;
      host: string;
      port: number;
      database: string;
      username: string;
      password: string;
      schema?: string;
    },
  ) {
    try {
      // Test the PostgreSQL connection first
      const { Client } = require('pg');
      const client = new Client({
        host: pgDto.host,
        port: pgDto.port,
        user: pgDto.username,
        password: pgDto.password,
        database: pgDto.database,
      });

      await client.connect();
      await client.query('SELECT 1');
      await client.end();

      // Encrypt credentials
      const encryptedCredentials = this.snowflakeService.encryptCredentials({
        username: pgDto.username,
        password: pgDto.password,
      });

      // Save connection to database
      const connection = await this.prisma.connection.create({
        data: {
          userId,
          connectionName: pgDto.connectionName,
          serverType: 'postgresql',
          connectionType: 'password',
          host: pgDto.host,
          port: pgDto.port,
          databaseName: pgDto.database,
          schemaName: pgDto.schema || 'public',
          credentials: encryptedCredentials,
          status: 'active',
          authType: 'password',
        },
      });

      this.logger.log(`PostgreSQL connection created: ${connection.connectionName}`);
      return this.sanitizeConnection(connection);
    } catch (error) {
      this.logger.error(`Failed to create PostgreSQL connection: ${error.message}`);
      throw new BadRequestException(`Failed to connect to PostgreSQL: ${error.message}`);
    }
  }

  /**
   * Get databases for a connection
   */
  async getDatabases(connectionId: string, userId: string): Promise<string[]> {
    let connection;

    // Handle system default connection
    if (connectionId === 'hierarchy-builder-core') {
      const useRsa =
        this.snowflakeService['configService'].get<string>('SNOWFLAKE_USE_RSA') === 'true';
      connection = {
        id: connectionId,
        serverType: 'Snowflake',
        snowflakeAccount: this.snowflakeService['configService'].get<string>('SNOWFLAKE_ACCOUNT'),
        snowflakeUser: this.snowflakeService['configService'].get<string>('SNOWFLAKE_USER'),
        snowflakeWarehouse:
          this.snowflakeService['configService'].get<string>('SNOWFLAKE_WAREHOUSE'),
        snowflakeDatabase: this.snowflakeService['configService'].get<string>('SNOWFLAKE_DATABASE'),
        snowflakeSchema: this.snowflakeService['configService'].get<string>('SNOWFLAKE_SCHEMA'),
        connectionType: useRsa ? 'rsa' : 'password',
        credentials: this.snowflakeService.encryptCredentials({
          password: this.snowflakeService['configService'].get<string>('SNOWFLAKE_PASSWORD'),
          useRsa: useRsa,
          rsaPrivateKeyPath: this.snowflakeService['configService'].get<string>(
            'SNOWFLAKE_RSA_PRIVATE_KEY_PATH',
          ),
        }),
      };
    } else {
      connection = await (this.prisma as any).connection.findFirst({
        where: { id: connectionId, userId },
      });
    }

    if (!connection) {
      throw new NotFoundException('Connection not found');
    }

    try {
      // Handle different database types
      if (
        (connection as any).serverType === 'Snowflake' ||
        (connection as any).serverType === 'Snowflake'
      ) {
        // For Snowflake, create connection and get databases
        const snowflakeConn = await this.createSnowflakeConnectionId(connection as any);
        const databases = await this.snowflakeQueryService.getDatabases(snowflakeConn);
        await this.snowflakeQueryService.closeConnection(snowflakeConn);
        return databases;
      } else if ((connection as any).serverType === 'mysql') {
        // For MySQL, query INFORMATION_SCHEMA
        const decryptedCreds = this.snowflakeService.decryptCredentials(
          (connection as any).credentials,
        );
        const mysqlConnection = mysql.createConnection({
          host: (connection as any).host,
          port: (connection as any).port,
          user: decryptedCreds.username,
          password: decryptedCreds.password,
        });

        return new Promise((resolve, reject) => {
          mysqlConnection.query('SHOW DATABASES', (error, results: any) => {
            mysqlConnection.end();
            if (error) reject(error);
            else resolve(Array.isArray(results) ? results.map((row: any) => row.Database) : []);
          });
        });
      } else if ((connection as any).serverType === 'postgresql') {
        // For PostgreSQL, query pg_database
        const decryptedCreds = this.snowflakeService.decryptCredentials(
          (connection as any).credentials,
        );
        const client = new Client({
          host: (connection as any).host,
          port: (connection as any).port,
          user: decryptedCreds.username,
          password: decryptedCreds.password,
          database: 'postgres', // Connect to default database
        });

        await client.connect();
        const result = await client.query(
          'SELECT datname FROM pg_database WHERE datistemplate = false',
        );
        await client.end();
        return result.rows.map((row: any) => row.datname);
      }

      throw new BadRequestException(
        `Database type ${(connection as any).serverType} not supported`,
      );
    } catch (error) {
      this.logger.error(`Failed to get databases: ${error.message}`);
      throw new BadRequestException(`Failed to get databases: ${error.message}`);
    }
  }

  /**
   * Get schemas for a database
   */
  async getSchemas(connectionId: string, database: string, userId: string): Promise<string[]> {
    let connection;

    // Handle system default connection
    if (connectionId === 'hierarchy-builder-core') {
      const useRsa =
        this.snowflakeService['configService'].get<string>('SNOWFLAKE_USE_RSA') === 'true';
      connection = {
        id: connectionId,
        serverType: 'Snowflake',
        snowflakeAccount: this.snowflakeService['configService'].get<string>('SNOWFLAKE_ACCOUNT'),
        snowflakeUser: this.snowflakeService['configService'].get<string>('SNOWFLAKE_USER'),
        snowflakeWarehouse:
          this.snowflakeService['configService'].get<string>('SNOWFLAKE_WAREHOUSE'),
        snowflakeDatabase: this.snowflakeService['configService'].get<string>('SNOWFLAKE_DATABASE'),
        snowflakeSchema: this.snowflakeService['configService'].get<string>('SNOWFLAKE_SCHEMA'),
        connectionType: useRsa ? 'rsa' : 'password',
        credentials: this.snowflakeService.encryptCredentials({
          password: this.snowflakeService['configService'].get<string>('SNOWFLAKE_PASSWORD'),
          useRsa: useRsa,
          rsaPrivateKeyPath: this.snowflakeService['configService'].get<string>(
            'SNOWFLAKE_RSA_PRIVATE_KEY_PATH',
          ),
        }),
      };
    } else {
      connection = await (this.prisma as any).connection.findFirst({
        where: { id: connectionId, userId },
      });
    }

    if (!connection) {
      throw new NotFoundException('Connection not found');
    }

    try {
      // Handle different database types
      if (
        (connection as any).serverType === 'Snowflake' ||
        (connection as any).serverType === 'Snowflake'
      ) {
        // For Snowflake, create connection and get schemas
        const snowflakeConn = await this.createSnowflakeConnectionId(connection as any);
        const schemas = await this.snowflakeQueryService.getSchemas(snowflakeConn, database);
        await this.snowflakeQueryService.closeConnection(snowflakeConn);
        return schemas;
      } else if ((connection as any).serverType === 'mysql') {
        // MySQL doesn't have schemas separate from databases
        return ['default'];
      } else if ((connection as any).serverType === 'postgresql') {
        // For PostgreSQL, query information_schema.schemata
        const decryptedCreds = this.snowflakeService.decryptCredentials(
          (connection as any).credentials,
        );
        const client = new Client({
          host: (connection as any).host,
          port: (connection as any).port,
          user: decryptedCreds.username,
          password: decryptedCreds.password,
          database: database,
        });

        await client.connect();
        const result = await client.query(
          "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog', 'information_schema')",
        );
        await client.end();
        return result.rows.map((row: any) => row.schema_name);
      }

      throw new BadRequestException(
        `Database type ${(connection as any).serverType} not supported`,
      );
    } catch (error) {
      this.logger.error(`Failed to get schemas: ${error.message}`);
      throw new BadRequestException(`Failed to get schemas: ${error.message}`);
    }
  }

  /**
   * Get tables for a schema
   */
  async getTables(
    connectionId: string,
    database: string,
    schema: string,
    userId: string,
  ): Promise<string[]> {
    let connection;

    // Handle system default connection
    if (connectionId === 'hierarchy-builder-core') {
      const useRsa =
        this.snowflakeService['configService'].get<string>('SNOWFLAKE_USE_RSA') === 'true';
      connection = {
        id: connectionId,
        serverType: 'Snowflake',
        snowflakeAccount: this.snowflakeService['configService'].get<string>('SNOWFLAKE_ACCOUNT'),
        snowflakeUser: this.snowflakeService['configService'].get<string>('SNOWFLAKE_USER'),
        snowflakeWarehouse:
          this.snowflakeService['configService'].get<string>('SNOWFLAKE_WAREHOUSE'),
        snowflakeDatabase: this.snowflakeService['configService'].get<string>('SNOWFLAKE_DATABASE'),
        snowflakeSchema: this.snowflakeService['configService'].get<string>('SNOWFLAKE_SCHEMA'),
        connectionType: useRsa ? 'rsa' : 'password',
        credentials: this.snowflakeService.encryptCredentials({
          password: useRsa
            ? undefined
            : this.snowflakeService['configService'].get<string>('SNOWFLAKE_PASSWORD'),
          useRsa: useRsa,
          rsaPrivateKeyPath: useRsa
            ? this.snowflakeService['configService'].get<string>('SNOWFLAKE_RSA_PRIVATE_KEY_PATH')
            : undefined,
        }),
      };
    } else {
      connection = await (this.prisma as any).connection.findFirst({
        where: { id: connectionId, userId },
      });
    }

    if (!connection) {
      throw new NotFoundException('Connection not found');
    }

    try {
      if (
        (connection as any).serverType === 'Snowflake' ||
        (connection as any).serverType === 'Snowflake'
      ) {
        const snowflakeConn = await this.createSnowflakeConnectionId(connection as any);
        const tables = await this.snowflakeQueryService.getTables(snowflakeConn, database, schema);
        await this.snowflakeQueryService.closeConnection(snowflakeConn);
        return tables;
      } else if ((connection as any).serverType === 'mysql') {
        const decryptedCreds = this.snowflakeService.decryptCredentials(
          (connection as any).credentials,
        );
        const mysqlConn = await mysql.createConnection({
          host: (connection as any).host,
          port: (connection as any).port,
          user: decryptedCreds.username,
          password: decryptedCreds.password,
          database: database,
        });

        const [rows] = (await mysqlConn.query('SHOW TABLES')) as unknown as [any[], any];
        await mysqlConn.end();
        return (rows as any[]).map((row: any) => Object.values(row)[0] as string);
      } else if ((connection as any).serverType === 'postgresql') {
        const decryptedCreds = this.snowflakeService.decryptCredentials(
          (connection as any).credentials,
        );
        const client = new Client({
          host: (connection as any).host,
          port: (connection as any).port,
          user: decryptedCreds.username,
          password: decryptedCreds.password,
          database: database,
        });

        await client.connect();
        const result = await client.query(
          `SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE'`,
          [schema],
        );
        await client.end();
        return result.rows.map((row: any) => row.table_name);
      }

      throw new BadRequestException(
        `Database type ${(connection as any).serverType} not supported`,
      );
    } catch (error) {
      this.logger.error(`Failed to get tables: ${error.message}`);
      throw new BadRequestException(`Failed to get tables: ${error.message}`);
    }
  }

  /**
   * Get columns for a table
   */
  async getColumns(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    userId: string,
  ): Promise<Array<{ name: string; type: string }>> {
    let connection;

    // Handle system default connection
    if (connectionId === 'hierarchy-builder-core') {
      const useRsa =
        this.snowflakeService['configService'].get<string>('SNOWFLAKE_USE_RSA') === 'true';
      connection = {
        id: connectionId,
        serverType: 'Snowflake',
        snowflakeAccount: this.snowflakeService['configService'].get<string>('SNOWFLAKE_ACCOUNT'),
        snowflakeUser: this.snowflakeService['configService'].get<string>('SNOWFLAKE_USER'),
        snowflakeWarehouse:
          this.snowflakeService['configService'].get<string>('SNOWFLAKE_WAREHOUSE'),
        snowflakeDatabase: this.snowflakeService['configService'].get<string>('SNOWFLAKE_DATABASE'),
        snowflakeSchema: this.snowflakeService['configService'].get<string>('SNOWFLAKE_SCHEMA'),
        connectionType: useRsa ? 'rsa' : 'password',
        credentials: this.snowflakeService.encryptCredentials({
          password: useRsa
            ? undefined
            : this.snowflakeService['configService'].get<string>('SNOWFLAKE_PASSWORD'),
          useRsa: useRsa,
          rsaPrivateKeyPath: useRsa
            ? this.snowflakeService['configService'].get<string>('SNOWFLAKE_RSA_PRIVATE_KEY_PATH')
            : undefined,
        }),
      };
    } else {
      connection = await (this.prisma as any).connection.findFirst({
        where: { id: connectionId, userId },
      });
    }

    if (!connection) {
      throw new NotFoundException('Connection not found');
    }

    try {
      if (
        (connection as any).serverType === 'Snowflake' ||
        (connection as any).serverType === 'Snowflake'
      ) {
        const snowflakeConn = await this.createSnowflakeConnectionId(connection as any);
        const columns = await this.snowflakeQueryService.getTableColumns(
          snowflakeConn,
          database,
          schema,
          table,
        );
        await this.snowflakeQueryService.closeConnection(snowflakeConn);
        return columns.map((col: any) => ({
          name: col.name || col.NAME,
          type: col.type || col.TYPE,
        }));
      } else if ((connection as any).serverType === 'mysql') {
        const decryptedCreds = this.snowflakeService.decryptCredentials(
          (connection as any).credentials,
        );
        const mysqlConn = await mysql.createConnection({
          host: (connection as any).host,
          port: (connection as any).port,
          user: decryptedCreds.username,
          password: decryptedCreds.password,
          database: database,
        });

        const [rows] = (await mysqlConn.query(`SHOW COLUMNS FROM ${table}`)) as unknown as [
          any[],
          any,
        ];
        await mysqlConn.end();
        return (rows as any[]).map((row: any) => ({
          name: row.Field,
          type: row.Type,
        }));
      } else if ((connection as any).serverType === 'postgresql') {
        const decryptedCreds = this.snowflakeService.decryptCredentials(
          (connection as any).credentials,
        );
        const client = new Client({
          host: (connection as any).host,
          port: (connection as any).port,
          user: decryptedCreds.username,
          password: decryptedCreds.password,
          database: database,
        });

        await client.connect();
        const result = await client.query(
          `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2`,
          [schema, table],
        );
        await client.end();
        return result.rows.map((row: any) => ({
          name: row.column_name,
          type: row.data_type,
        }));
      }

      throw new BadRequestException(
        `Database type ${(connection as any).serverType} not supported`,
      );
    } catch (error) {
      this.logger.error(`Failed to get columns: ${error.message}`);
      throw new BadRequestException(`Failed to get columns: ${error.message}`);
    }
  }

  /**
   * Get distinct values from a column
   */
  async getColumnData(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    column: string,
    userId: string,
  ): Promise<string[]> {
    let connection;

    // Handle system default connection
    if (connectionId === 'hierarchy-builder-core') {
      const useRsa =
        this.snowflakeService['configService'].get<string>('SNOWFLAKE_USE_RSA') === 'true';
      connection = {
        id: connectionId,
        serverType: 'Snowflake',
        snowflakeAccount: this.snowflakeService['configService'].get<string>('SNOWFLAKE_ACCOUNT'),
        snowflakeUser: this.snowflakeService['configService'].get<string>('SNOWFLAKE_USER'),
        snowflakeWarehouse:
          this.snowflakeService['configService'].get<string>('SNOWFLAKE_WAREHOUSE'),
        snowflakeDatabase: this.snowflakeService['configService'].get<string>('SNOWFLAKE_DATABASE'),
        snowflakeSchema: this.snowflakeService['configService'].get<string>('SNOWFLAKE_SCHEMA'),
        connectionType: useRsa ? 'rsa' : 'password',
        credentials: this.snowflakeService.encryptCredentials({
          password: useRsa
            ? undefined
            : this.snowflakeService['configService'].get<string>('SNOWFLAKE_PASSWORD'),
          useRsa: useRsa,
          rsaPrivateKeyPath: useRsa
            ? this.snowflakeService['configService'].get<string>('SNOWFLAKE_RSA_PRIVATE_KEY_PATH')
            : undefined,
        }),
      };
    } else {
      connection = await (this.prisma as any).connection.findFirst({
        where: { id: connectionId, userId },
      });
    }

    if (!connection) {
      throw new NotFoundException('Connection not found');
    }

    try {
      if (
        (connection as any).serverType === 'Snowflake' ||
        (connection as any).serverType === 'Snowflake'
      ) {
        const snowflakeConn = await this.createSnowflakeConnectionId(connection as any);
        const result = await this.snowflakeQueryService.executeQuery(
          snowflakeConn,
          `SELECT DISTINCT ${column} FROM ${database}.${schema}.${table} WHERE ${column} IS NOT NULL ORDER BY ${column} LIMIT 1000`,
        );
        await this.snowflakeQueryService.closeConnection(snowflakeConn);
        return result.rows.map((row: any) =>
          String(row[column] || row[column.toUpperCase()] || row[column.toLowerCase()]),
        );
      } else if ((connection as any).serverType === 'mysql') {
        const decryptedCreds = this.snowflakeService.decryptCredentials(
          (connection as any).credentials,
        );
        const mysqlConn = await mysql.createConnection({
          host: (connection as any).host,
          port: (connection as any).port,
          user: decryptedCreds.username,
          password: decryptedCreds.password,
          database: database,
        });

        const [rows] = (await mysqlConn.query(
          `SELECT DISTINCT \`${column}\` FROM \`${table}\` WHERE \`${column}\` IS NOT NULL ORDER BY \`${column}\` LIMIT 1000`,
        )) as unknown as [any[], any];
        await mysqlConn.end();
        return (rows as any[]).map((row: any) => String(row[column]));
      } else if ((connection as any).serverType === 'postgresql') {
        const decryptedCreds = this.snowflakeService.decryptCredentials(
          (connection as any).credentials,
        );
        const client = new Client({
          host: (connection as any).host,
          port: (connection as any).port,
          user: decryptedCreds.username,
          password: decryptedCreds.password,
          database: database,
        });

        await client.connect();
        const result = await client.query(
          `SELECT DISTINCT "${column}" FROM "${schema}"."${table}" WHERE "${column}" IS NOT NULL ORDER BY "${column}" LIMIT 1000`,
        );
        await client.end();
        return result.rows.map((row: any) => String(row[column]));
      }

      throw new BadRequestException(
        `Database type ${(connection as any).serverType} not supported`,
      );
    } catch (error) {
      this.logger.error(`Failed to get column data: ${error.message}`);
      throw new BadRequestException(`Failed to get column data: ${error.message}`);
    }
  }

  /**
   * Helper to create Snowflake connection from stored connection and return connection ID
   */
  private async createSnowflakeConnectionId(connection: any): Promise<string> {
    const decryptedCreds = this.snowflakeService.decryptCredentials(connection.credentials);

    if (connection.connectionType === 'oauth') {
      const conn = await this.snowflakeService.createOAuthConnection({
        account: connection.snowflakeAccount,
        username: connection.snowflakeUser,
        token: decryptedCreds.token,
        warehouse: connection.snowflakeWarehouse,
        database: connection.snowflakeDatabase,
        schema: connection.snowflakeSchema,
      });

      // Store connection with generated ID
      const connId = `temp_${Date.now()}`;
      await this.snowflakeQueryService.createConnection(
        {
          account: connection.snowflakeAccount,
          username: connection.snowflakeUser,
          authenticator: 'oauth',
          token: decryptedCreds.token,
          warehouse: connection.snowflakeWarehouse,
          database: connection.snowflakeDatabase,
          schema: connection.snowflakeSchema,
        },
        connId,
      );

      return connId;
    } else if (connection.connectionType === 'rsa') {
      // RSA key authentication
      const connId = `temp_${Date.now()}`;
      await this.snowflakeQueryService.createConnection(
        {
          account: connection.snowflakeAccount,
          username: connection.snowflakeUser,
          privateKeyPath: decryptedCreds.rsaPrivateKeyPath,
          warehouse: connection.snowflakeWarehouse,
          database: connection.snowflakeDatabase,
          schema: connection.snowflakeSchema,
        },
        connId,
      );

      return connId;
    } else {
      // Password authentication
      const connId = `temp_${Date.now()}`;
      await this.snowflakeQueryService.createConnection(
        {
          account: connection.snowflakeAccount,
          username: connection.snowflakeUser,
          password: decryptedCreds.password,
          warehouse: connection.snowflakeWarehouse,
          database: connection.snowflakeDatabase,
          schema: connection.snowflakeSchema,
        },
        connId,
      );

      return connId;
    }
  }
}
