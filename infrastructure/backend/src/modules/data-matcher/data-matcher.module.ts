import { Module } from '@nestjs/common';
import { CacheModule } from '@nestjs/cache-manager';
import { DataMatcherService } from './data-matcher.service';
import { DataMatcherController } from './data-matcher.controller';

@Module({
  imports: [
    CacheModule.register({
      ttl: 60000, // 60 seconds
      max: 100,
    }),
  ],
  controllers: [DataMatcherController],
  providers: [DataMatcherService],
})
export class DataMatcherModule {}
