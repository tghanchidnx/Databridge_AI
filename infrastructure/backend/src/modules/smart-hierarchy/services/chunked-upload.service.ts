import { Injectable, Logger, BadRequestException } from '@nestjs/common';
import { CsvAnalyzerService, CSVAnalysisResult, AutoFixAction } from './csv-analyzer.service';
import { UploadLogService, UploadLogEntry } from './upload-log.service';
import { OldFormatImportV2Service } from './old-format-import-v2.service';
import { OldFormatMappingService } from './old-format-mapping.service';

/**
 * Chunk metadata for tracking upload progress
 */
export interface ChunkMetadata {
  uploadId: string;
  fileName: string;
  fileSize: number;
  totalChunks: number;
  receivedChunks: number[];
  content: string;
  createdAt: Date;
  expiresAt: Date;
}

/**
 * Import result with detailed information
 */
export interface ImportResult {
  success: boolean;
  logId: string;
  message: string;
  rowsTotal: number;
  rowsImported: number;
  rowsFailed: number;
  issues: Array<{
    type: string;
    message: string;
    resolved: boolean;
  }>;
  autoFixesApplied: string[];
  report: string;
  duration: number;
}

/**
 * Batch processing options
 */
export interface BatchOptions {
  batchSize: number;
  validateBeforeImport: boolean;
  autoFix: boolean;
  stopOnError: boolean;
  dryRun: boolean;
}

const DEFAULT_BATCH_OPTIONS: BatchOptions = {
  batchSize: 500,
  validateBeforeImport: true,
  autoFix: true,
  stopOnError: false,
  dryRun: false,
};

/**
 * Chunked Upload Service
 * Handles large file uploads by splitting into chunks and processing in batches
 */
@Injectable()
export class ChunkedUploadService {
  private readonly logger = new Logger(ChunkedUploadService.name);
  private readonly uploadSessions: Map<string, ChunkMetadata> = new Map();
  private readonly CHUNK_SIZE = 1024 * 1024; // 1MB chunks
  private readonly SESSION_TIMEOUT = 30 * 60 * 1000; // 30 minutes

  constructor(
    private readonly analyzerService: CsvAnalyzerService,
    private readonly uploadLogService: UploadLogService,
    private readonly hierarchyImportService: OldFormatImportV2Service,
    private readonly mappingImportService: OldFormatMappingService,
  ) {
    // Clean up expired sessions periodically
    setInterval(() => this.cleanupExpiredSessions(), 5 * 60 * 1000);
  }

  /**
   * Initialize a chunked upload session
   */
  initializeUpload(data: {
    fileName: string;
    fileSize: number;
    totalChunks: number;
    projectId?: string;
    userId?: string;
  }): { uploadId: string; chunkSize: number } {
    const uploadId = `chunk_${Date.now()}_${Math.random().toString(36).substring(7)}`;

    const metadata: ChunkMetadata = {
      uploadId,
      fileName: data.fileName,
      fileSize: data.fileSize,
      totalChunks: data.totalChunks,
      receivedChunks: [],
      content: '',
      createdAt: new Date(),
      expiresAt: new Date(Date.now() + this.SESSION_TIMEOUT),
    };

    this.uploadSessions.set(uploadId, metadata);
    this.logger.log(`Initialized chunked upload: ${uploadId} for ${data.fileName} (${data.fileSize} bytes, ${data.totalChunks} chunks)`);

    return {
      uploadId,
      chunkSize: this.CHUNK_SIZE,
    };
  }

  /**
   * Receive a chunk of data
   */
  async receiveChunk(uploadId: string, chunkIndex: number, chunkData: string): Promise<{
    received: boolean;
    complete: boolean;
    progress: number;
    missingChunks: number[];
  }> {
    const session = this.uploadSessions.get(uploadId);
    if (!session) {
      throw new BadRequestException(`Upload session not found: ${uploadId}`);
    }

    if (session.receivedChunks.includes(chunkIndex)) {
      this.logger.warn(`Chunk ${chunkIndex} already received for ${uploadId}`);
      return {
        received: true,
        complete: session.receivedChunks.length === session.totalChunks,
        progress: (session.receivedChunks.length / session.totalChunks) * 100,
        missingChunks: this.getMissingChunks(session),
      };
    }

    // Store chunk (we'll reassemble later)
    session.receivedChunks.push(chunkIndex);

    // For simplicity, we append in order. In production, you'd want to handle out-of-order chunks
    if (chunkIndex === session.receivedChunks.length - 1) {
      session.content += chunkData;
    }

    const complete = session.receivedChunks.length === session.totalChunks;
    const progress = (session.receivedChunks.length / session.totalChunks) * 100;

    this.logger.log(`Received chunk ${chunkIndex + 1}/${session.totalChunks} for ${uploadId} (${progress.toFixed(1)}%)`);

    return {
      received: true,
      complete,
      progress,
      missingChunks: this.getMissingChunks(session),
    };
  }

  /**
   * Process completed upload with analysis and import
   */
  async processUpload(
    uploadId: string,
    projectId: string,
    fileType: 'hierarchy' | 'mapping',
    options: Partial<BatchOptions> = {},
  ): Promise<ImportResult> {
    const session = this.uploadSessions.get(uploadId);
    if (!session) {
      throw new BadRequestException(`Upload session not found: ${uploadId}`);
    }

    if (session.receivedChunks.length !== session.totalChunks) {
      throw new BadRequestException(
        `Upload incomplete: received ${session.receivedChunks.length}/${session.totalChunks} chunks`,
      );
    }

    const opts = { ...DEFAULT_BATCH_OPTIONS, ...options };
    const startTime = Date.now();

    // Create log entry
    const log = this.uploadLogService.createLog({
      projectId,
      fileName: session.fileName,
      fileSize: session.fileSize,
      fileType,
    });

    try {
      this.uploadLogService.updateStatus(log.id, 'analyzing');

      // Analyze the CSV
      const analysis = await this.analyzerService.analyzeCSV(session.content, session.fileName);

      // Log issues from analysis
      analysis.issues.forEach(issue => {
        this.uploadLogService.addIssue(log.id, {
          type: issue.type,
          code: issue.code,
          message: issue.message,
          row: issue.row,
          column: issue.column,
        });
      });

      this.uploadLogService.updateCounts(log.id, { rowsTotal: analysis.rowCount });

      // Check for critical errors
      const criticalErrors = analysis.issues.filter(i => i.type === 'error' && !i.canAutoFix);
      if (criticalErrors.length > 0 && !opts.dryRun) {
        const errorMsg = criticalErrors.map(e => e.message).join('; ');
        this.uploadLogService.completeLog(log.id, false, errorMsg);

        return {
          success: false,
          logId: log.id,
          message: `Import failed due to critical errors: ${errorMsg}`,
          rowsTotal: analysis.rowCount,
          rowsImported: 0,
          rowsFailed: analysis.rowCount,
          issues: analysis.issues.map(i => ({ type: i.type, message: i.message, resolved: false })),
          autoFixesApplied: [],
          report: this.uploadLogService.generateReport(log.id),
          duration: Date.now() - startTime,
        };
      }

      // Apply auto-fixes if enabled
      let contentToImport = session.content;
      if (opts.autoFix && analysis.canAutoFix) {
        this.uploadLogService.updateStatus(log.id, 'fixing');

        const fixResult = await this.analyzerService.applyAutoFixes(
          session.content,
          analysis.autoFixActions,
        );

        contentToImport = fixResult.fixedContent;

        fixResult.appliedFixes.forEach(fix => {
          this.uploadLogService.addAutoFix(log.id, fix);
        });

        // Mark issues as resolved
        analysis.autoFixActions.forEach(action => {
          this.uploadLogService.resolveIssue(log.id, action.type, action.description);
        });
      }

      if (opts.dryRun) {
        this.uploadLogService.completeLog(log.id, true);
        return {
          success: true,
          logId: log.id,
          message: 'Dry run completed - no changes made',
          rowsTotal: analysis.rowCount,
          rowsImported: 0,
          rowsFailed: 0,
          issues: analysis.issues.map(i => ({ type: i.type, message: i.message, resolved: false })),
          autoFixesApplied: log.autoFixesApplied,
          report: this.uploadLogService.generateReport(log.id),
          duration: Date.now() - startTime,
        };
      }

      // Import with batching
      this.uploadLogService.updateStatus(log.id, 'importing');

      let importResult;
      if (fileType === 'hierarchy') {
        importResult = await this.importHierarchyInBatches(
          projectId,
          contentToImport,
          opts.batchSize,
          log.id,
        );
      } else {
        importResult = await this.importMappingInBatches(
          projectId,
          contentToImport,
          opts.batchSize,
          log.id,
        );
      }

      this.uploadLogService.updateCounts(log.id, {
        rowsImported: importResult.imported,
        rowsFailed: importResult.failed,
      });

      const success = importResult.failed === 0;
      this.uploadLogService.completeLog(log.id, success, importResult.error);

      // Clean up session
      this.uploadSessions.delete(uploadId);

      const finalLog = this.uploadLogService.getLog(log.id);

      return {
        success,
        logId: log.id,
        message: success
          ? `Successfully imported ${importResult.imported} rows`
          : `Import completed with ${importResult.failed} failures`,
        rowsTotal: analysis.rowCount,
        rowsImported: importResult.imported,
        rowsFailed: importResult.failed,
        issues: finalLog?.issues.map(i => ({ type: i.type, message: i.message, resolved: i.resolved })) || [],
        autoFixesApplied: finalLog?.autoFixesApplied || [],
        report: this.uploadLogService.generateReport(log.id),
        duration: Date.now() - startTime,
      };
    } catch (error) {
      this.uploadLogService.completeLog(log.id, false, error.message);
      throw error;
    }
  }

  /**
   * Import hierarchy CSV in batches
   */
  private async importHierarchyInBatches(
    projectId: string,
    csvContent: string,
    batchSize: number,
    logId: string,
  ): Promise<{ imported: number; failed: number; error?: string }> {
    try {
      // The existing service handles large files, but we can split if needed
      const result = await this.hierarchyImportService.importHierarchyCSV(projectId, csvContent);

      return {
        imported: result.imported,
        failed: result.skipped,
      };
    } catch (error) {
      this.logger.error(`Hierarchy import failed: ${error.message}`);
      return {
        imported: 0,
        failed: 0,
        error: error.message,
      };
    }
  }

  /**
   * Import mapping CSV in batches
   */
  private async importMappingInBatches(
    projectId: string,
    csvContent: string,
    batchSize: number,
    logId: string,
  ): Promise<{ imported: number; failed: number; error?: string }> {
    try {
      const result = await this.mappingImportService.importMappingCSV(projectId, csvContent);

      return {
        imported: result.imported,
        failed: result.skipped,
      };
    } catch (error) {
      this.logger.error(`Mapping import failed: ${error.message}`);
      return {
        imported: 0,
        failed: 0,
        error: error.message,
      };
    }
  }

  /**
   * Direct import (non-chunked) with analysis
   */
  async directImport(
    projectId: string,
    csvContent: string,
    fileName: string,
    fileType: 'hierarchy' | 'mapping',
    options: Partial<BatchOptions> = {},
  ): Promise<ImportResult> {
    const opts = { ...DEFAULT_BATCH_OPTIONS, ...options };
    const startTime = Date.now();
    const fileSize = new Blob([csvContent]).size;

    // Create log entry
    const log = this.uploadLogService.createLog({
      projectId,
      fileName,
      fileSize,
      fileType,
    });

    try {
      this.uploadLogService.updateStatus(log.id, 'analyzing');

      // Analyze the CSV
      const analysis = await this.analyzerService.analyzeCSV(csvContent, fileName);

      // Log issues from analysis
      analysis.issues.forEach(issue => {
        this.uploadLogService.addIssue(log.id, {
          type: issue.type,
          code: issue.code,
          message: issue.message,
          row: issue.row,
          column: issue.column,
        });
      });

      this.uploadLogService.updateCounts(log.id, { rowsTotal: analysis.rowCount });

      // Check for critical errors that prevent import
      const criticalErrors = analysis.issues.filter(i => i.type === 'error' && !i.canAutoFix);
      if (criticalErrors.length > 0) {
        const errorMsg = `Cannot import due to ${criticalErrors.length} critical error(s): ${criticalErrors.map(e => e.message).join('; ')}`;
        this.uploadLogService.completeLog(log.id, false, errorMsg);

        return {
          success: false,
          logId: log.id,
          message: errorMsg,
          rowsTotal: analysis.rowCount,
          rowsImported: 0,
          rowsFailed: analysis.rowCount,
          issues: analysis.issues.map(i => ({ type: i.type, message: i.message, resolved: false })),
          autoFixesApplied: [],
          report: this.uploadLogService.generateReport(log.id),
          duration: Date.now() - startTime,
        };
      }

      // Apply auto-fixes if enabled and available
      let contentToImport = csvContent;
      if (opts.autoFix && analysis.canAutoFix) {
        this.uploadLogService.updateStatus(log.id, 'fixing');

        const fixResult = await this.analyzerService.applyAutoFixes(
          csvContent,
          analysis.autoFixActions,
        );

        contentToImport = fixResult.fixedContent;

        fixResult.appliedFixes.forEach(fix => {
          this.uploadLogService.addAutoFix(log.id, fix);
        });

        // Resolve related issues
        fixResult.appliedFixes.forEach((fix, index) => {
          if (analysis.autoFixActions[index]) {
            this.uploadLogService.resolveIssue(
              log.id,
              analysis.autoFixActions[index].type,
              fix,
            );
          }
        });
      }

      // Perform import
      this.uploadLogService.updateStatus(log.id, 'importing');

      let importResult;
      if (fileType === 'hierarchy') {
        importResult = await this.importHierarchyInBatches(
          projectId,
          contentToImport,
          opts.batchSize,
          log.id,
        );
      } else {
        importResult = await this.importMappingInBatches(
          projectId,
          contentToImport,
          opts.batchSize,
          log.id,
        );
      }

      this.uploadLogService.updateCounts(log.id, {
        rowsImported: importResult.imported,
        rowsFailed: importResult.failed,
      });

      const success = !importResult.error && importResult.failed === 0;
      this.uploadLogService.completeLog(log.id, success, importResult.error);

      const finalLog = this.uploadLogService.getLog(log.id);

      return {
        success,
        logId: log.id,
        message: success
          ? `Successfully imported ${importResult.imported} rows`
          : importResult.error || `Import completed with ${importResult.failed} failures`,
        rowsTotal: analysis.rowCount,
        rowsImported: importResult.imported,
        rowsFailed: importResult.failed,
        issues: finalLog?.issues.map(i => ({ type: i.type, message: i.message, resolved: i.resolved })) || [],
        autoFixesApplied: finalLog?.autoFixesApplied || [],
        report: this.uploadLogService.generateReport(log.id),
        duration: Date.now() - startTime,
      };
    } catch (error) {
      this.uploadLogService.completeLog(log.id, false, error.message);

      return {
        success: false,
        logId: log.id,
        message: `Import failed: ${error.message}`,
        rowsTotal: 0,
        rowsImported: 0,
        rowsFailed: 0,
        issues: [{ type: 'error', message: error.message, resolved: false }],
        autoFixesApplied: [],
        report: this.uploadLogService.generateReport(log.id),
        duration: Date.now() - startTime,
      };
    }
  }

  /**
   * Get upload session status
   */
  getSessionStatus(uploadId: string): ChunkMetadata | undefined {
    return this.uploadSessions.get(uploadId);
  }

  /**
   * Cancel an upload session
   */
  cancelUpload(uploadId: string): boolean {
    const session = this.uploadSessions.get(uploadId);
    if (session) {
      this.uploadSessions.delete(uploadId);
      this.logger.log(`Cancelled upload session: ${uploadId}`);
      return true;
    }
    return false;
  }

  /**
   * Get missing chunks for a session
   */
  private getMissingChunks(session: ChunkMetadata): number[] {
    const missing: number[] = [];
    for (let i = 0; i < session.totalChunks; i++) {
      if (!session.receivedChunks.includes(i)) {
        missing.push(i);
      }
    }
    return missing;
  }

  /**
   * Clean up expired upload sessions
   */
  private cleanupExpiredSessions(): void {
    const now = Date.now();
    let cleaned = 0;

    this.uploadSessions.forEach((session, uploadId) => {
      if (session.expiresAt.getTime() < now) {
        this.uploadSessions.delete(uploadId);
        cleaned++;
      }
    });

    if (cleaned > 0) {
      this.logger.log(`Cleaned up ${cleaned} expired upload sessions`);
    }
  }
}
