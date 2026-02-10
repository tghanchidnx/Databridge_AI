import { Injectable, Logger } from '@nestjs/common';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Upload Log Entry Interface
 */
export interface UploadLogEntry {
  id: string;
  timestamp: Date;
  projectId?: string;
  userId?: string;
  fileName: string;
  fileSize: number;
  fileType: 'hierarchy' | 'mapping' | 'unknown';
  status: 'started' | 'analyzing' | 'fixing' | 'importing' | 'success' | 'failed' | 'partial';
  rowsTotal: number;
  rowsImported: number;
  rowsFailed: number;
  issues: UploadIssue[];
  autoFixesApplied: string[];
  errorMessage?: string;
  duration?: number; // milliseconds
  metadata?: Record<string, any>;
}

export interface UploadIssue {
  type: 'error' | 'warning' | 'info';
  code: string;
  message: string;
  row?: number;
  column?: string;
  resolved: boolean;
  resolution?: string;
}

/**
 * Upload Log Service
 * Tracks all CSV upload attempts, issues, and resolutions
 */
@Injectable()
export class UploadLogService {
  private readonly logger = new Logger(UploadLogService.name);
  private readonly logDir: string;
  private logs: Map<string, UploadLogEntry> = new Map();

  constructor() {
    // Create log directory
    this.logDir = path.join(process.cwd(), 'uploads', 'logs');
    if (!fs.existsSync(this.logDir)) {
      fs.mkdirSync(this.logDir, { recursive: true });
    }
  }

  /**
   * Create a new upload log entry
   */
  createLog(data: {
    projectId?: string;
    userId?: string;
    fileName: string;
    fileSize: number;
    fileType: UploadLogEntry['fileType'];
  }): UploadLogEntry {
    const id = `upload_${Date.now()}_${Math.random().toString(36).substring(7)}`;

    const entry: UploadLogEntry = {
      id,
      timestamp: new Date(),
      projectId: data.projectId,
      userId: data.userId,
      fileName: data.fileName,
      fileSize: data.fileSize,
      fileType: data.fileType,
      status: 'started',
      rowsTotal: 0,
      rowsImported: 0,
      rowsFailed: 0,
      issues: [],
      autoFixesApplied: [],
    };

    this.logs.set(id, entry);
    this.logger.log(`Created upload log: ${id} for file ${data.fileName}`);

    return entry;
  }

  /**
   * Update log status
   */
  updateStatus(logId: string, status: UploadLogEntry['status'], metadata?: Record<string, any>): void {
    const entry = this.logs.get(logId);
    if (entry) {
      entry.status = status;
      if (metadata) {
        entry.metadata = { ...entry.metadata, ...metadata };
      }
      this.logger.log(`Updated log ${logId} status to: ${status}`);
    }
  }

  /**
   * Update row counts
   */
  updateCounts(logId: string, counts: {
    rowsTotal?: number;
    rowsImported?: number;
    rowsFailed?: number;
  }): void {
    const entry = this.logs.get(logId);
    if (entry) {
      if (counts.rowsTotal !== undefined) entry.rowsTotal = counts.rowsTotal;
      if (counts.rowsImported !== undefined) entry.rowsImported = counts.rowsImported;
      if (counts.rowsFailed !== undefined) entry.rowsFailed = counts.rowsFailed;
    }
  }

  /**
   * Add an issue to the log
   */
  addIssue(logId: string, issue: Omit<UploadIssue, 'resolved'>): void {
    const entry = this.logs.get(logId);
    if (entry) {
      entry.issues.push({
        ...issue,
        resolved: false,
      });
    }
  }

  /**
   * Mark an issue as resolved
   */
  resolveIssue(logId: string, issueCode: string, resolution: string): void {
    const entry = this.logs.get(logId);
    if (entry) {
      const issue = entry.issues.find(i => i.code === issueCode && !i.resolved);
      if (issue) {
        issue.resolved = true;
        issue.resolution = resolution;
      }
    }
  }

  /**
   * Add auto-fix to the log
   */
  addAutoFix(logId: string, fix: string): void {
    const entry = this.logs.get(logId);
    if (entry) {
      entry.autoFixesApplied.push(fix);
      this.logger.log(`Applied auto-fix to ${logId}: ${fix}`);
    }
  }

  /**
   * Complete the log (success or failure)
   */
  completeLog(logId: string, success: boolean, errorMessage?: string): UploadLogEntry | undefined {
    const entry = this.logs.get(logId);
    if (entry) {
      entry.status = success ? 'success' : 'failed';
      entry.duration = Date.now() - entry.timestamp.getTime();
      if (errorMessage) {
        entry.errorMessage = errorMessage;
      }

      // Persist to file
      this.persistLog(entry);

      this.logger.log(
        `Completed log ${logId}: ${entry.status} - ${entry.rowsImported}/${entry.rowsTotal} rows imported in ${entry.duration}ms`,
      );

      return entry;
    }
    return undefined;
  }

  /**
   * Get a log entry by ID
   */
  getLog(logId: string): UploadLogEntry | undefined {
    return this.logs.get(logId);
  }

  /**
   * Get all logs for a project
   */
  async getProjectLogs(projectId: string, limit = 50): Promise<UploadLogEntry[]> {
    // First check in-memory logs
    const memoryLogs = Array.from(this.logs.values())
      .filter(log => log.projectId === projectId)
      .sort((a, b) => b.timestamp.getTime() - a.timestamp.getTime())
      .slice(0, limit);

    // Then check persisted logs
    const logFiles = await this.getLogFiles(projectId);
    const persistedLogs: UploadLogEntry[] = [];

    for (const file of logFiles.slice(0, limit - memoryLogs.length)) {
      try {
        const content = fs.readFileSync(path.join(this.logDir, file), 'utf-8');
        const log = JSON.parse(content) as UploadLogEntry;
        if (log.projectId === projectId) {
          persistedLogs.push(log);
        }
      } catch (error) {
        this.logger.warn(`Failed to read log file ${file}: ${error.message}`);
      }
    }

    return [...memoryLogs, ...persistedLogs]
      .sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime())
      .slice(0, limit);
  }

  /**
   * Get recent logs
   */
  async getRecentLogs(limit = 20): Promise<UploadLogEntry[]> {
    const memoryLogs = Array.from(this.logs.values())
      .sort((a, b) => b.timestamp.getTime() - a.timestamp.getTime())
      .slice(0, limit);

    return memoryLogs;
  }

  /**
   * Persist log to file
   */
  private persistLog(entry: UploadLogEntry): void {
    try {
      const fileName = `${entry.id}.json`;
      const filePath = path.join(this.logDir, fileName);
      fs.writeFileSync(filePath, JSON.stringify(entry, null, 2));
      this.logger.debug(`Persisted log to ${filePath}`);
    } catch (error) {
      this.logger.error(`Failed to persist log: ${error.message}`);
    }
  }

  /**
   * Get list of log files for a project
   */
  private async getLogFiles(projectId?: string): Promise<string[]> {
    try {
      const files = fs.readdirSync(this.logDir);
      return files
        .filter(f => f.endsWith('.json'))
        .sort()
        .reverse();
    } catch (error) {
      this.logger.warn(`Failed to list log files: ${error.message}`);
      return [];
    }
  }

  /**
   * Generate a summary report for a log
   */
  generateReport(logId: string): string {
    const entry = this.logs.get(logId);
    if (!entry) {
      return 'Log not found';
    }

    const lines: string[] = [
      `=== Upload Report ===`,
      `File: ${entry.fileName}`,
      `Size: ${(entry.fileSize / 1024).toFixed(2)} KB`,
      `Type: ${entry.fileType}`,
      `Status: ${entry.status}`,
      `Duration: ${entry.duration ? (entry.duration / 1000).toFixed(2) + 's' : 'N/A'}`,
      ``,
      `=== Results ===`,
      `Total Rows: ${entry.rowsTotal}`,
      `Imported: ${entry.rowsImported}`,
      `Failed: ${entry.rowsFailed}`,
      `Success Rate: ${entry.rowsTotal > 0 ? ((entry.rowsImported / entry.rowsTotal) * 100).toFixed(1) : 0}%`,
    ];

    if (entry.issues.length > 0) {
      lines.push(``, `=== Issues (${entry.issues.length}) ===`);
      entry.issues.forEach((issue, i) => {
        const status = issue.resolved ? '[RESOLVED]' : `[${issue.type.toUpperCase()}]`;
        lines.push(`${i + 1}. ${status} ${issue.message}`);
        if (issue.resolution) {
          lines.push(`   Resolution: ${issue.resolution}`);
        }
      });
    }

    if (entry.autoFixesApplied.length > 0) {
      lines.push(``, `=== Auto-Fixes Applied ===`);
      entry.autoFixesApplied.forEach((fix, i) => {
        lines.push(`${i + 1}. ${fix}`);
      });
    }

    if (entry.errorMessage) {
      lines.push(``, `=== Error ===`, entry.errorMessage);
    }

    return lines.join('\n');
  }
}
