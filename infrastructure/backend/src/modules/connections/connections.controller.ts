import { Controller, Get, Post, Body, Patch, Param, Delete, UseGuards } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiBearerAuth } from '@nestjs/swagger';
import { ConnectionsService } from './connections.service';
import { CreateConnectionDto } from './dto/create-connection.dto';
import { UpdateConnectionDto } from './dto/update-connection.dto';
import {
  SnowflakeOAuthCallbackDto,
  SnowflakePasswordConnectionDto,
  RefreshSnowflakeTokenDto,
} from './dto/snowflake-oauth-connection.dto';
import { JwtAuthGuard } from '../../common/guards/jwt-auth.guard';
import { CurrentUser } from '../../common/decorators/current-user.decorator';

@ApiTags('Connections')
@ApiBearerAuth()
@UseGuards(JwtAuthGuard)
@Controller('connections')
export class ConnectionsController {
  constructor(private readonly connectionsService: ConnectionsService) {}

  @Post()
  @ApiOperation({ summary: 'Create a new database connection' })
  @ApiResponse({ status: 201, description: 'Connection created successfully' })
  create(@CurrentUser('id') userId: string, @Body() createConnectionDto: CreateConnectionDto) {
    return this.connectionsService.create(userId, createConnectionDto);
  }

  @Get()
  @ApiOperation({ summary: 'Get all connections for current user' })
  @ApiResponse({ status: 200, description: 'Connections retrieved successfully' })
  findAll(@CurrentUser('id') userId: string, @CurrentUser('email') userEmail: string) {
    return this.connectionsService.findAll(userId, userEmail);
  }

  @Get(':id')
  @ApiOperation({ summary: 'Get connection by ID' })
  @ApiResponse({ status: 200, description: 'Connection found' })
  @ApiResponse({ status: 404, description: 'Connection not found' })
  findOne(@Param('id') id: string, @CurrentUser('id') userId: string) {
    return this.connectionsService.findOne(id, userId);
  }

  @Get(':id/test')
  @ApiOperation({ summary: 'Test database connection' })
  @ApiResponse({ status: 200, description: 'Connection test completed' })
  testConnection(@Param('id') id: string, @CurrentUser('id') userId: string) {
    return this.connectionsService.testConnection(id, userId);
  }

  @Post('databases')
  @ApiOperation({ summary: 'Get all databases for a connection' })
  @ApiResponse({ status: 200, description: 'Databases retrieved successfully' })
  getDatabases(@Body('connectionId') connectionId: string, @CurrentUser('id') userId: string) {
    return this.connectionsService.getDatabases(connectionId, userId);
  }

  @Post('schemas')
  @ApiOperation({ summary: 'Get all schemas for a database' })
  @ApiResponse({ status: 200, description: 'Schemas retrieved successfully' })
  getSchemas(
    @Body('connectionId') connectionId: string,
    @Body('database') database: string,
    @CurrentUser('id') userId: string,
  ) {
    return this.connectionsService.getSchemas(connectionId, database, userId);
  }

  @Post('tables')
  @ApiOperation({ summary: 'Get all tables for a schema' })
  @ApiResponse({ status: 200, description: 'Tables retrieved successfully' })
  getTables(
    @Body('connectionId') connectionId: string,
    @Body('database') database: string,
    @Body('schema') schema: string,
    @CurrentUser('id') userId: string,
  ) {
    return this.connectionsService.getTables(connectionId, database, schema, userId);
  }

  @Post('columns')
  @ApiOperation({ summary: 'Get all columns for a table' })
  @ApiResponse({ status: 200, description: 'Columns retrieved successfully' })
  getColumns(
    @Body('connectionId') connectionId: string,
    @Body('database') database: string,
    @Body('schema') schema: string,
    @Body('table') table: string,
    @CurrentUser('id') userId: string,
  ) {
    return this.connectionsService.getColumns(connectionId, database, schema, table, userId);
  }

  @Post('column-data')
  @ApiOperation({ summary: 'Get distinct values from a column' })
  @ApiResponse({ status: 200, description: 'Column data retrieved successfully' })
  getColumnData(
    @Body('connectionId') connectionId: string,
    @Body('database') database: string,
    @Body('schema') schema: string,
    @Body('table') table: string,
    @Body('column') column: string,
    @CurrentUser('id') userId: string,
  ) {
    return this.connectionsService.getColumnData(
      connectionId,
      database,
      schema,
      table,
      column,
      userId,
    );
  }

  @Patch(':id')
  @ApiOperation({ summary: 'Update connection' })
  @ApiResponse({ status: 200, description: 'Connection updated successfully' })
  @ApiResponse({ status: 404, description: 'Connection not found' })
  update(
    @Param('id') id: string,
    @CurrentUser('id') userId: string,
    @Body() updateConnectionDto: UpdateConnectionDto,
  ) {
    return this.connectionsService.update(id, userId, updateConnectionDto);
  }

  @Delete(':id')
  @ApiOperation({ summary: 'Delete connection' })
  @ApiResponse({ status: 200, description: 'Connection deleted successfully' })
  @ApiResponse({ status: 404, description: 'Connection not found' })
  remove(@Param('id') id: string, @CurrentUser('id') userId: string) {
    return this.connectionsService.remove(id, userId);
  }

  @Post('snowflake/oauth')
  @ApiOperation({ summary: 'Create Snowflake OAuth connection from callback' })
  @ApiResponse({ status: 201, description: 'Snowflake OAuth connection created successfully' })
  createSnowflakeOAuth(
    @CurrentUser('id') userId: string,
    @Body() callbackDto: SnowflakeOAuthCallbackDto,
  ) {
    return this.connectionsService.createSnowflakeOAuthConnection(userId, callbackDto);
  }

  @Post('snowflake/password')
  @ApiOperation({ summary: 'Create Snowflake password-based connection' })
  @ApiResponse({ status: 201, description: 'Snowflake password connection created successfully' })
  createSnowflakePassword(
    @CurrentUser('id') userId: string,
    @Body() passwordDto: SnowflakePasswordConnectionDto,
  ) {
    return this.connectionsService.createSnowflakePasswordConnection(userId, passwordDto);
  }

  @Post('snowflake/refresh')
  @ApiOperation({ summary: 'Refresh Snowflake OAuth token' })
  @ApiResponse({ status: 200, description: 'Token refreshed successfully' })
  refreshSnowflakeToken(
    @CurrentUser('id') userId: string,
    @Body() refreshDto: RefreshSnowflakeTokenDto,
  ) {
    return this.connectionsService.refreshSnowflakeToken(userId, refreshDto.connectionId);
  }

  @Post('mysql')
  @ApiOperation({ summary: 'Create MySQL database connection' })
  @ApiResponse({ status: 201, description: 'MySQL connection created successfully' })
  createMySQLConnection(
    @CurrentUser('id') userId: string,
    @Body()
    mysqlDto: {
      connectionName: string;
      host: string;
      port: number;
      database: string;
      username: string;
      password: string;
    },
  ) {
    return this.connectionsService.createMySQLConnection(userId, mysqlDto);
  }

  @Post('postgresql')
  @ApiOperation({ summary: 'Create PostgreSQL database connection' })
  @ApiResponse({ status: 201, description: 'PostgreSQL connection created successfully' })
  createPostgreSQLConnection(
    @CurrentUser('id') userId: string,
    @Body()
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
    return this.connectionsService.createPostgreSQLConnection(userId, pgDto);
  }
}
