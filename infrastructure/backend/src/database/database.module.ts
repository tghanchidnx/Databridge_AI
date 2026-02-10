import { Module, Global } from '@nestjs/common';
import { PrismaService } from './prisma/prisma.service';
import { SnowflakeService } from './snowflake/snowflake.service';

@Global()
@Module({
  providers: [PrismaService, SnowflakeService],
  exports: [PrismaService, SnowflakeService],
})
export class DatabaseModule {}
