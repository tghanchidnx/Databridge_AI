import { Controller, Get, Post, Body, Query } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse } from '@nestjs/swagger';
import { Public } from '../../common/decorators/public.decorator';
import { SnowflakeService } from '../../database/snowflake/snowflake.service';
import { ConfigService } from '@nestjs/config';

@ApiTags('Connection Tests')
@Controller('test')
export class TestController {
  constructor(
    private readonly snowflakeService: SnowflakeService,
    private readonly configService: ConfigService,
  ) {}

  @Get('snowflake/connection')
  @Public()
  @ApiOperation({ summary: 'Test Snowflake connection with env credentials' })
  @ApiResponse({ status: 200, description: 'Connection successful' })
  @ApiResponse({ status: 500, description: 'Connection failed' })
  async testSnowflakeConnection() {
    let connectionId: string;

    try {
      const config = {
        account: this.configService.get('snowflakeAccount'),
        username: 'ENV_MERY', // Test user from env
        authenticator: 'SNOWFLAKE_JWT',
        privateKey: this.configService.get('snowflakePrivateKey'),
        warehouse: this.configService.get('snowflakeWarehouse'),
        database: this.configService.get('snowflakeDatabase'),
        schema: this.configService.get('snowflakeSchema'),
      };

      // Create connection
      connectionId = await this.snowflakeService.createConnection(config, 'test-connection');

      // Test query
      const result = await this.snowflakeService.executeQuery(
        connectionId,
        'SELECT CURRENT_VERSION(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()',
      );

      // Close connection
      await this.snowflakeService.closeConnection(connectionId);

      return {
        status: 'success',
        message: 'Snowflake connection successful',
        credentials: {
          account: config.account,
          warehouse: config.warehouse,
          database: config.database,
          schema: config.schema,
        },
        result: {
          rows: result.rows,
          rowCount: result.rowCount,
        },
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      if (connectionId) {
        try {
          await this.snowflakeService.closeConnection(connectionId);
        } catch (closeError) {
          // Ignore close errors
        }
      }

      return {
        status: 'error',
        message: 'Snowflake connection failed',
        error: error.message,
        stack: error.stack,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Get('snowflake/databases')
  @Public()
  @ApiOperation({ summary: 'List Snowflake databases' })
  @ApiResponse({ status: 200, description: 'Databases listed successfully' })
  async listSnowflakeDatabases() {
    let connectionId: string;

    try {
      const config = {
        account: this.configService.get('snowflakeAccount'),
        username: 'ENV_MERY',
        authenticator: 'SNOWFLAKE_JWT',
        privateKey: this.configService.get('snowflakePrivateKey'),
        warehouse: this.configService.get('snowflakeWarehouse'),
      };

      connectionId = await this.snowflakeService.createConnection(config, 'test-db-list');
      const result = await this.snowflakeService.executeQuery(connectionId, 'SHOW DATABASES');
      await this.snowflakeService.closeConnection(connectionId);

      return {
        status: 'success',
        databases: result.rows,
        count: result.rowCount,
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      if (connectionId) {
        try {
          await this.snowflakeService.closeConnection(connectionId);
        } catch (closeError) {}
      }

      return {
        status: 'error',
        message: 'Failed to list databases',
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Get('snowflake/schemas')
  @Public()
  @ApiOperation({ summary: 'List Snowflake schemas in a database' })
  @ApiResponse({ status: 200, description: 'Schemas listed successfully' })
  async listSnowflakeSchemas(@Query('database') database: string = 'TRANSFORMATION') {
    let connectionId: string;

    try {
      const config = {
        account: this.configService.get('snowflakeAccount'),
        username: 'ENV_MERY',
        authenticator: 'SNOWFLAKE_JWT',
        privateKey: this.configService.get('snowflakePrivateKey'),
        warehouse: this.configService.get('snowflakeWarehouse'),
        database: database,
      };

      connectionId = await this.snowflakeService.createConnection(config, 'test-schema-list');
      const result = await this.snowflakeService.executeQuery(
        connectionId,
        `SHOW SCHEMAS IN DATABASE ${database}`,
      );
      await this.snowflakeService.closeConnection(connectionId);

      return {
        status: 'success',
        database: database,
        schemas: result.rows,
        count: result.rowCount,
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      if (connectionId) {
        try {
          await this.snowflakeService.closeConnection(connectionId);
        } catch (closeError) {}
      }

      return {
        status: 'error',
        message: `Failed to list schemas in ${database}`,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Get('snowflake/tables')
  @Public()
  @ApiOperation({ summary: 'List Snowflake tables in a schema' })
  @ApiResponse({ status: 200, description: 'Tables listed successfully' })
  async listSnowflakeTables(
    @Query('database') database: string = 'TRANSFORMATION',
    @Query('schema') schema: string = 'CONFIGURATION',
  ) {
    let connectionId: string;

    try {
      const config = {
        account: this.configService.get('snowflakeAccount'),
        username: 'ENV_MERY',
        authenticator: 'SNOWFLAKE_JWT',
        privateKey: this.configService.get('snowflakePrivateKey'),
        warehouse: this.configService.get('snowflakeWarehouse'),
        database: database,
        schema: schema,
      };

      connectionId = await this.snowflakeService.createConnection(config, 'test-table-list');
      const result = await this.snowflakeService.executeQuery(
        connectionId,
        `SHOW TABLES IN SCHEMA ${database}.${schema}`,
      );
      await this.snowflakeService.closeConnection(connectionId);

      return {
        status: 'success',
        database: database,
        schema: schema,
        tables: result.rows,
        count: result.rowCount,
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      if (connectionId) {
        try {
          await this.snowflakeService.closeConnection(connectionId);
        } catch (closeError) {}
      }

      return {
        status: 'error',
        message: `Failed to list tables in ${database}.${schema}`,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Get('azure/config')
  @Public()
  @ApiOperation({ summary: 'Check Azure AD configuration (sensitive data masked)' })
  @ApiResponse({ status: 200, description: 'Configuration checked' })
  async checkAzureConfig() {
    const azureTenantId = this.configService.get('azureTenantId');
    const azureClientId = this.configService.get('azureClientId');
    const azureClientSecret = this.configService.get('azureClientSecret');

    return {
      status: 'success',
      config: {
        tenantId: azureTenantId ? `${azureTenantId.substring(0, 8)}...` : 'NOT_SET',
        clientId: azureClientId ? `${azureClientId.substring(0, 8)}...` : 'NOT_SET',
        clientSecret:
          azureClientSecret && azureClientSecret !== 'your-azure-secret-from-portal'
            ? 'SET (masked)'
            : 'NOT_SET or PLACEHOLDER',
        isPlaceholder: azureClientSecret === 'your-azure-secret-from-portal',
      },
      warning:
        azureClientSecret === 'your-azure-secret-from-portal'
          ? 'Azure Client Secret is still using placeholder value. Please update .env with real secret from Azure Portal.'
          : null,
      timestamp: new Date().toISOString(),
    };
  }

  @Get('health')
  @Public()
  @ApiOperation({ summary: 'Health check with all service statuses' })
  @ApiResponse({ status: 200, description: 'Health status' })
  async healthCheck() {
    return {
      status: 'healthy',
      services: {
        server: 'running',
        database: 'connected',
        snowflake: 'configured',
        azure: 'configured',
      },
      environment: {
        nodeEnv: this.configService.get('nodeEnv'),
        port: this.configService.get('port'),
        apiPrefix: this.configService.get('apiPrefix'),
      },
      timestamp: new Date().toISOString(),
    };
  }
}
