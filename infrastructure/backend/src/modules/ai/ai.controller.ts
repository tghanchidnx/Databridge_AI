import { Controller, Post, Body, UseGuards, Get, Param, Delete } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiBearerAuth } from '@nestjs/swagger';
import { AiService } from './ai.service';
import { MappingSuggesterService } from './mapping-suggester.service';
import { FormulaSuggesterService } from './formula-suggester.service';
import { HierarchyChatService, ChatContext } from './chat.service';
import { AnomalyDetectorService, HierarchyNode, AnomalyDetectionConfig } from './anomaly-detector.service';
import { NlHierarchyBuilderService, GenerationRequest } from './nl-hierarchy-builder.service';
import { NLQueryService, ProjectContext } from './nl-query.service';
import { AutoMapperService, ExcelParseResult } from './auto-mapper.service';
import { ChatMessageDto, GenerateSqlDto, ExplainQueryDto } from './dto/chat.dto';
import { JwtAuthGuard } from '../../common/guards/jwt-auth.guard';

@ApiTags('ai')
@Controller('ai')
@UseGuards(JwtAuthGuard)
@ApiBearerAuth()
export class AiController {
  constructor(
    private readonly aiService: AiService,
    private readonly mappingSuggester: MappingSuggesterService,
    private readonly formulaSuggester: FormulaSuggesterService,
    private readonly chatService: HierarchyChatService,
    private readonly anomalyDetector: AnomalyDetectorService,
    private readonly nlBuilder: NlHierarchyBuilderService,
    private readonly nlQueryService: NLQueryService,
    private readonly autoMapperService: AutoMapperService,
  ) {}

  @Post('chat')
  @ApiOperation({ summary: 'Chat with AI assistant' })
  @ApiResponse({ status: 200, description: 'AI response generated' })
  @ApiResponse({ status: 400, description: 'Invalid request' })
  async chat(@Body() chatDto: ChatMessageDto) {
    return this.aiService.processChat(chatDto.message, chatDto.context);
  }

  @Post('generate-sql')
  @ApiOperation({ summary: 'Generate SQL query from natural language' })
  @ApiResponse({ status: 200, description: 'SQL query generated' })
  @ApiResponse({ status: 400, description: 'Invalid request' })
  async generateSql(@Body() generateDto: GenerateSqlDto) {
    return this.aiService.generateSql(
      generateDto.query,
      generateDto.databaseType,
      generateDto.tableSchemas,
    );
  }

  @Post('explain-query')
  @ApiOperation({ summary: 'Explain SQL query' })
  @ApiResponse({ status: 200, description: 'Query explanation generated' })
  @ApiResponse({ status: 400, description: 'Invalid request' })
  async explainQuery(@Body() explainDto: ExplainQueryDto) {
    return this.aiService.explainQuery(explainDto.query);
  }

  @Get('health')
  @ApiOperation({ summary: 'Check AI service health' })
  @ApiResponse({ status: 200, description: 'AI service is healthy' })
  async health() {
    return {
      status: 'healthy',
      service: 'ai',
      timestamp: new Date().toISOString(),
    };
  }

  // ==================== Mapping Suggestions ====================

  @Post('suggest-mappings')
  @ApiOperation({ summary: 'Get AI-powered mapping suggestions for a hierarchy' })
  @ApiResponse({ status: 200, description: 'Mapping suggestions generated' })
  async suggestMappings(
    @Body() body: {
      hierarchy: {
        id: string;
        name: string;
        hierarchyId: string;
        parentName?: string;
        level: number;
        existingMappings?: Array<{
          sourceDatabase: string;
          sourceTable: string;
          sourceColumn: string;
        }>;
      };
      availableSources: Array<{
        database: string;
        schema: string;
        table: string;
        columns: Array<{ name: string; dataType: string; description?: string }>;
      }>;
      projectMappingHistory?: Array<{
        hierarchyName: string;
        mapping: {
          sourceDatabase: string;
          sourceSchema: string;
          sourceTable: string;
          sourceColumn: string;
        };
      }>;
    },
  ) {
    return this.mappingSuggester.suggestMappings(
      body.hierarchy,
      body.availableSources,
      body.projectMappingHistory,
    );
  }

  @Post('learn-mapping')
  @ApiOperation({ summary: 'Learn from user-accepted mapping' })
  @ApiResponse({ status: 200, description: 'Mapping pattern learned' })
  async learnMapping(
    @Body() body: {
      hierarchyName: string;
      mapping: {
        sourceDatabase: string;
        sourceSchema: string;
        sourceTable: string;
        sourceColumn: string;
      };
    },
  ) {
    this.mappingSuggester.learnFromAcceptedMapping(body.hierarchyName, body.mapping);
    return { success: true };
  }

  // ==================== Formula Suggestions ====================

  @Post('suggest-formulas')
  @ApiOperation({ summary: 'Get formula suggestions for a hierarchy node' })
  @ApiResponse({ status: 200, description: 'Formula suggestions generated' })
  async suggestFormulas(
    @Body() body: {
      name: string;
      hierarchyId: string;
      parentName?: string;
      siblingNames: string[];
      childNames: string[];
      level: number;
    },
  ) {
    return this.formulaSuggester.suggestFormulas(body);
  }

  @Post('validate-formula')
  @ApiOperation({ summary: 'Validate formula syntax' })
  @ApiResponse({ status: 200, description: 'Formula validation result' })
  async validateFormula(
    @Body() body: { formula: string; availableVariables: string[] },
  ) {
    return this.formulaSuggester.validateFormula(body.formula, body.availableVariables);
  }

  @Post('generate-formula-sql')
  @ApiOperation({ summary: 'Generate SQL from formula' })
  @ApiResponse({ status: 200, description: 'SQL generated from formula' })
  async generateFormulaSql(
    @Body() body: { formula: string; variableToColumnMap: Record<string, string> },
  ) {
    return {
      sql: this.formulaSuggester.generateFormulaSQL(body.formula, body.variableToColumnMap),
    };
  }

  // ==================== Hierarchy Chat ====================

  @Post('hierarchy-chat')
  @ApiOperation({ summary: 'Chat with AI about hierarchies' })
  @ApiResponse({ status: 200, description: 'Chat response generated' })
  async hierarchyChat(
    @Body() body: {
      sessionId: string;
      message: string;
      context: ChatContext;
    },
  ) {
    return this.chatService.processMessage(body.sessionId, body.message, body.context);
  }

  @Delete('hierarchy-chat/:sessionId')
  @ApiOperation({ summary: 'Clear chat history for a session' })
  @ApiResponse({ status: 200, description: 'Chat history cleared' })
  async clearChatHistory(@Param('sessionId') sessionId: string) {
    this.chatService.clearHistory(sessionId);
    return { success: true };
  }

  // ==================== Anomaly Detection ====================

  @Post('detect-anomalies')
  @ApiOperation({ summary: 'Detect anomalies in hierarchy structure' })
  @ApiResponse({ status: 200, description: 'Anomalies detected' })
  async detectAnomalies(
    @Body() body: {
      nodes: HierarchyNode[];
      config?: Partial<AnomalyDetectionConfig>;
    },
  ) {
    const anomalies = this.anomalyDetector.detectAnomalies(body.nodes, body.config);
    const summary = this.anomalyDetector.getAnomalySummary(anomalies);
    return { anomalies, summary };
  }

  @Post('check-mapping-change')
  @ApiOperation({ summary: 'Check for anomalies on mapping change' })
  @ApiResponse({ status: 200, description: 'Mapping change anomalies checked' })
  async checkMappingChange(
    @Body() body: {
      node: HierarchyNode;
      newMapping: {
        sourceDatabase: string;
        sourceTable: string;
        sourceColumn: string;
        dataType?: string;
      };
      allNodes: HierarchyNode[];
    },
  ) {
    return this.anomalyDetector.checkMappingChange(body.node, body.newMapping, body.allNodes);
  }

  // ==================== NL Hierarchy Builder ====================

  @Post('generate-hierarchy')
  @ApiOperation({ summary: 'Generate hierarchy from natural language description' })
  @ApiResponse({ status: 200, description: 'Hierarchy generated' })
  async generateHierarchy(@Body() request: GenerationRequest) {
    return this.nlBuilder.generateHierarchy(request);
  }

  @Post('refine-hierarchy')
  @ApiOperation({ summary: 'Refine existing hierarchy with feedback' })
  @ApiResponse({ status: 200, description: 'Hierarchy refined' })
  async refineHierarchy(
    @Body() body: {
      conversationId: string;
      currentHierarchy: any;
      feedback: string;
    },
  ) {
    return this.nlBuilder.refineHierarchy(
      body.conversationId,
      body.currentHierarchy,
      body.feedback,
    );
  }

  // ==================== Natural Language Query ====================

  @Post('natural-language-query')
  @ApiOperation({ summary: 'Translate natural language query to SQL' })
  @ApiResponse({ status: 200, description: 'SQL query generated from natural language' })
  async naturalLanguageQuery(
    @Body() body: {
      query: string;
      context: ProjectContext;
    },
  ) {
    return this.nlQueryService.translateToSQL(body.query, body.context);
  }

  // ==================== Auto-Mapper ====================

  @Post('auto-map-excel')
  @ApiOperation({ summary: 'Auto-map Excel columns to hierarchy fields' })
  @ApiResponse({ status: 200, description: 'Column mappings generated' })
  async autoMapExcel(
    @Body() body: {
      excelData: ExcelParseResult;
      sheetName?: string;
    },
  ) {
    return this.autoMapperService.autoMapExcelToHierarchy(
      body.excelData,
      body.sheetName,
    );
  }
}
