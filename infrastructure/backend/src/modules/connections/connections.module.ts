import { Module } from '@nestjs/common';
import { ConnectionsService } from './connections.service';
import { ConnectionsController } from './connections.controller';
import { SnowflakeService } from './services/snowflake.service';
import { SnowflakeService as SnowflakeQueryService } from '../../database/snowflake/snowflake.service';

@Module({
  controllers: [ConnectionsController],
  providers: [ConnectionsService, SnowflakeService, SnowflakeQueryService],
  exports: [ConnectionsService, SnowflakeService],
})
export class ConnectionsModule {}
