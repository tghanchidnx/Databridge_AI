import { Module } from '@nestjs/common';
import { TestController } from './test.controller';
import { SnowflakeService } from '../../database/snowflake/snowflake.service';
import { PrismaService } from '../../database/prisma/prisma.service';

@Module({
  controllers: [TestController],
  providers: [SnowflakeService, PrismaService],
})
export class TestModule {}
