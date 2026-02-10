import { Injectable, BadRequestException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../database/prisma/prisma.service';
import { PushToSnowflakeDto } from '../dto/smart-hierarchy.dto';
import { ScriptGeneratorService } from './script-generator.service';
import { ConnectionsService } from '../../connections/connections.service';
import { SnowflakeService } from '../../connections/services/snowflake.service';
import * as snowflake from 'snowflake-sdk';
import * as fs from 'fs';

@Injectable()
export class SnowflakeDeploymentService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly scriptGenerator: ScriptGeneratorService,
    private readonly config: ConfigService,
    private readonly connectionsService: ConnectionsService,
    private readonly snowflakeService: SnowflakeService,
  ) {}

  async pushToSnowflake(dto: PushToSnowflakeDto): Promise<any> {
    const startTime = Date.now();

    // Get connection with proper credentials
    let connection: any;
    let credentials: any;

    console.log('[pushToSnowflake] Connection ID:', dto.connectionId);

    if (dto.connectionId === 'hierarchy-builder-core') {
      // Build system connection directly from environment variables
      const useRsa = this.config.get<string>('SNOWFLAKE_USE_RSA') === 'true';

      connection = {
        id: 'hierarchy-builder-core',
        serverType: 'Snowflake',
        snowflakeAccount: this.config.get<string>('SNOWFLAKE_ACCOUNT'),
        snowflakeUser: this.config.get<string>('SNOWFLAKE_USER'),
        snowflakeWarehouse: this.config.get<string>('SNOWFLAKE_WAREHOUSE'),
        snowflakeDatabase: this.config.get<string>('SNOWFLAKE_DATABASE'),
        snowflakeSchema: this.config.get<string>('SNOWFLAKE_SCHEMA'),
      };

      // Build credentials directly without encryption/decryption
      credentials = {
        password: this.config.get<string>('SNOWFLAKE_PASSWORD'),
        useRsa: useRsa,
        rsaPrivateKeyPath: this.config.get<string>('SNOWFLAKE_RSA_PRIVATE_KEY_PATH'),
      };

      console.log('[pushToSnowflake] SBAIG Connection built from env:', {
        account: connection.snowflakeAccount,
        user: connection.snowflakeUser,
        warehouse: connection.snowflakeWarehouse,
        database: connection.snowflakeDatabase,
        schema: connection.snowflakeSchema,
        hasPassword: !!credentials.password,
        useRsa: credentials.useRsa,
        rsaPath: credentials.rsaPrivateKeyPath,
      });
    } else {
      connection = await this.prisma.connection.findUnique({
        where: { id: dto.connectionId },
      });
      if (!connection) {
        throw new BadRequestException('Connection not found');
      }
      credentials = this.snowflakeService.decryptCredentials(connection.credentials);
    }

    // Generate all scripts ONLY for selected hierarchies
    // NOTE: During deployment, we pass the ACTUAL database/schema from deployment DTO
    // This ensures scripts are executed in the correct target location
    const { scripts } = await this.scriptGenerator.generateScripts({
      projectId: dto.projectId,
      hierarchyIds: dto.hierarchyIds, // Use hierarchyIds array from frontend
      scriptType: 'all',
      databaseType: dto.databaseType,
      deployedBy: dto.deployedBy, // Pass user email for UPDATED_BY field
      database: dto.database, // ACTUAL deployment target database
      schema: dto.schema, // ACTUAL deployment target schema
    });

    // Get hierarchy names for display
    const hierarchies = await this.prisma.smartHierarchyMaster.findMany({
      where: {
        projectId: dto.projectId,
        hierarchyId: { in: dto.hierarchyIds },
      },
      select: { hierarchyId: true, hierarchyName: true },
    });

    const hierarchyNamesMap = Object.fromEntries(
      hierarchies.map((h) => [h.hierarchyId, h.hierarchyName]),
    );

    // Separate scripts by type
    const insertScripts: string[] = [];
    const viewScripts: string[] = [];
    const mappingScripts: string[] = [];
    const dtScripts: string[] = [];

    for (const scriptData of scripts) {
      if (scriptData.scriptType === 'insert' && dto.createTables) {
        insertScripts.push(scriptData.script);
      } else if (scriptData.scriptType === 'view' && dto.createViews) {
        viewScripts.push(scriptData.script);
      } else if (scriptData.scriptType === 'mapping' && dto.createViews) {
        mappingScripts.push(scriptData.script);
      } else if (scriptData.scriptType === 'dt' && dto.createDynamicTables) {
        dtScripts.push(scriptData.script);
      }
    }

    // Execute scripts on database
    const results: any[] = [];
    let successCount = 0;
    let failedCount = 0;
    let errorMessage: string | null = null;

    try {
      const dbConnection = await this.createSnowflakeConnection(
        connection,
        credentials,
        dto.database,
        dto.schema,
      );

      for (const scriptData of scripts) {
        // Skip non-relevant script types based on dto flags
        if (scriptData.scriptType === 'insert' && !dto.createTables) continue;
        if (
          (scriptData.scriptType === 'view' || scriptData.scriptType === 'mapping') &&
          !dto.createViews
        )
          continue;
        if (scriptData.scriptType === 'dt' && !dto.createDynamicTables) continue;

        try {
          await this.executeScript(dbConnection, scriptData.script);
          successCount++;
          results.push({
            hierarchyId: scriptData.hierarchyId,
            scriptType: scriptData.scriptType,
            status: 'success',
          });
        } catch (error: any) {
          failedCount++;
          errorMessage = error.message;
          results.push({
            hierarchyId: scriptData.hierarchyId,
            scriptType: scriptData.scriptType,
            status: 'failed',
            error: error.message,
          });
        }
      }

      await this.destroySnowflakeConnection(dbConnection);
    } catch (error: any) {
      errorMessage = error.message;
      throw new BadRequestException(`Failed to push to database: ${error.message}`);
    }

    const executionTime = Date.now() - startTime;

    // Save deployment history
    const deploymentHistory = await this.prisma.deploymentHistory.create({
      data: {
        projectId: dto.projectId,
        connectionId: dto.connectionId,
        database: dto.database,
        schema: dto.schema,
        masterTableName: dto.masterTableName,
        masterViewName: dto.masterViewName,
        databaseType: dto.databaseType,
        hierarchyIds: dto.hierarchyIds,
        hierarchyNames: dto.hierarchyIds.map((id) => hierarchyNamesMap[id] || id),
        deployedBy: dto.deployedBy,
        insertScript: insertScripts.join('\n\n'),
        viewScript: viewScripts.join('\n\n'),
        mappingScript: mappingScripts.join('\n\n'),
        dynamicTableScript: dtScripts.join('\n\n'),
        status: failedCount > 0 ? (successCount > 0 ? 'partial' : 'failed') : 'success',
        successCount,
        failedCount,
        errorMessage,
        executionTime,
      },
    });

    // Save/update deployment config if requested
    if (dto.saveAsDeploymentConfig) {
      await this.prisma.hierarchyProject.update({
        where: { id: dto.projectId },
        data: {
          deploymentConfig: {
            connectionId: dto.connectionId,
            database: dto.database,
            schema: dto.schema,
            masterTableName: dto.masterTableName,
            masterViewName: dto.masterViewName,
            databaseType: dto.databaseType,
            createTables: dto.createTables,
            createViews: dto.createViews,
            createDynamicTables: dto.createDynamicTables,
          },
        },
      });
    }

    return {
      success: successCount,
      failed: failedCount,
      results,
      deploymentId: deploymentHistory.id,
    };
  }

  private async createSnowflakeConnection(
    connection: any,
    credentials: any,
    database: string,
    schema: string,
  ): Promise<snowflake.Connection> {
    return new Promise((resolve, reject) => {
      console.log('[createSnowflakeConnection] Creating connection with:', {
        account: connection.snowflakeAccount,
        username: connection.snowflakeUser,
        warehouse: connection.snowflakeWarehouse,
        database,
        schema,
        useRsa: credentials.useRsa,
      });

      const connectionOptions: any = {
        account: connection.snowflakeAccount,
        username: connection.snowflakeUser,
        warehouse: connection.snowflakeWarehouse,
        database: database,
        schema: schema,
        role: connection.snowflakeRole || this.config.get('SNOWFLAKE_ROLE'),
      };

      // Handle RSA key authentication or password
      if (credentials.useRsa && credentials.rsaPrivateKeyPath) {
        try {
          console.log(
            '[createSnowflakeConnection] Using RSA authentication, reading key from:',
            credentials.rsaPrivateKeyPath,
          );
          const privateKey = fs.readFileSync(credentials.rsaPrivateKeyPath, 'utf8');
          connectionOptions.privateKey = privateKey;
          connectionOptions.authenticator = 'SNOWFLAKE_JWT';
          console.log('[createSnowflakeConnection] RSA key loaded successfully');
        } catch (error) {
          console.error('[createSnowflakeConnection] Failed to read RSA key:', error);
          reject(new Error(`Failed to read RSA private key: ${error.message}`));
          return;
        }
      } else if (credentials.password) {
        console.log('[createSnowflakeConnection] Using password authentication');
        connectionOptions.password = credentials.password;
      } else {
        console.error('[createSnowflakeConnection] No valid credentials found');
        reject(new Error('No valid authentication credentials found'));
        return;
      }

      const sfConnection = snowflake.createConnection(connectionOptions);

      sfConnection.connect((err, conn) => {
        if (err) {
          console.error('[createSnowflakeConnection] Connection failed:', err);
          reject(err);
        } else {
          console.log('[createSnowflakeConnection] Connection successful');
          resolve(conn);
        }
      });
    });
  }

  private async executeScript(connection: snowflake.Connection, script: string): Promise<any> {
    return new Promise((resolve, reject) => {
      connection.execute({
        sqlText: script,
        complete: (err, stmt, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve({ rows, stmt });
          }
        },
      });
    });
  }

  private async destroySnowflakeConnection(connection: snowflake.Connection): Promise<void> {
    return new Promise((resolve, reject) => {
      connection.destroy((err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }
}
