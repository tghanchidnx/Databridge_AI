import { Module } from '@nestjs/common';
import { AiController } from './ai.controller';
import { AiService } from './ai.service';
import { MappingSuggesterService } from './mapping-suggester.service';
import { FormulaSuggesterService } from './formula-suggester.service';
import { HierarchyChatService } from './chat.service';
import { AnomalyDetectorService } from './anomaly-detector.service';
import { NlHierarchyBuilderService } from './nl-hierarchy-builder.service';
import { NLQueryService } from './nl-query.service';
import { AutoMapperService } from './auto-mapper.service';
import { SchemaMatcherModule } from '../schema-matcher/schema-matcher.module';
import { ConnectionsModule } from '../connections/connections.module';

@Module({
  imports: [SchemaMatcherModule, ConnectionsModule],
  controllers: [AiController],
  providers: [
    AiService,
    MappingSuggesterService,
    FormulaSuggesterService,
    HierarchyChatService,
    AnomalyDetectorService,
    NlHierarchyBuilderService,
    NLQueryService,
    AutoMapperService,
  ],
  exports: [
    AiService,
    MappingSuggesterService,
    FormulaSuggesterService,
    HierarchyChatService,
    AnomalyDetectorService,
    NlHierarchyBuilderService,
    NLQueryService,
    AutoMapperService,
  ],
})
export class AiModule {}
