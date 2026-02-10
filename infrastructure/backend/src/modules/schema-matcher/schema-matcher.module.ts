import { Module } from '@nestjs/common';
import { CacheModule } from '@nestjs/cache-manager';
import { SchemaMatcherService } from './schema-matcher.service';
import { SchemaMatcherController } from './schema-matcher.controller';
import { ScriptGeneratorService } from './script-generator.service';
import { SnowflakeService } from '../connections/services/snowflake.service';
import { EncryptionService } from '../../common/services/encryption.service';

@Module({
  imports: [
    CacheModule.register({
      ttl: 60000, // 60 seconds (matches Python 1-minute cache)
      max: 100, // Maximum items in cache
    }),
  ],
  controllers: [SchemaMatcherController],
  providers: [SchemaMatcherService, ScriptGeneratorService, SnowflakeService, EncryptionService],
})
export class SchemaMatcherModule {}
