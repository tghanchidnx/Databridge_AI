/**
 * Excel Service
 *
 * Office.js abstractions for working with Excel workbooks, sheets, and ranges.
 * Provides helper methods for common operations.
 */

/* global Excel */

export interface SheetInfo {
  id: string;
  name: string;
  position: number;
}

export interface RangeData {
  address: string;
  values: any[][];
  rowCount: number;
  columnCount: number;
  headers?: string[];
}

export interface CellFormat {
  fill?: { color: string };
  font?: { color?: string; bold?: boolean };
  borders?: { color: string; style: string };
}

class ExcelService {
  /**
   * Check if Office.js is ready
   */
  async waitForOffice(): Promise<void> {
    return new Promise((resolve) => {
      if (typeof Office !== 'undefined' && Office.context) {
        resolve();
      } else {
        Office.onReady(() => resolve());
      }
    });
  }

  // =========================================================================
  // Sheet Operations
  // =========================================================================

  /**
   * Get all sheets in the workbook
   */
  async getSheets(): Promise<SheetInfo[]> {
    return Excel.run(async (context) => {
      const sheets = context.workbook.worksheets;
      sheets.load('items/id,items/name,items/position');
      await context.sync();

      return sheets.items.map((sheet) => ({
        id: sheet.id,
        name: sheet.name,
        position: sheet.position,
      }));
    });
  }

  /**
   * Get the active sheet
   */
  async getActiveSheet(): Promise<SheetInfo> {
    return Excel.run(async (context) => {
      const sheet = context.workbook.worksheets.getActiveWorksheet();
      sheet.load('id,name,position');
      await context.sync();

      return {
        id: sheet.id,
        name: sheet.name,
        position: sheet.position,
      };
    });
  }

  /**
   * Create a new sheet
   */
  async createSheet(name: string): Promise<SheetInfo> {
    return Excel.run(async (context) => {
      const sheet = context.workbook.worksheets.add(name);
      sheet.load('id,name,position');
      await context.sync();

      return {
        id: sheet.id,
        name: sheet.name,
        position: sheet.position,
      };
    });
  }

  // =========================================================================
  // Selection & Range Operations
  // =========================================================================

  /**
   * Get currently selected range
   */
  async getSelectedRange(): Promise<RangeData> {
    return Excel.run(async (context) => {
      const range = context.workbook.getSelectedRange();
      range.load('address,values,rowCount,columnCount');
      await context.sync();

      const values = range.values as any[][];
      const headers = values.length > 0 ? values[0].map(String) : [];

      return {
        address: range.address,
        values,
        rowCount: range.rowCount,
        columnCount: range.columnCount,
        headers,
      };
    });
  }

  /**
   * Get data from a specific range
   */
  async getRange(sheetName: string, address: string): Promise<RangeData> {
    return Excel.run(async (context) => {
      const sheet = context.workbook.worksheets.getItem(sheetName);
      const range = sheet.getRange(address);
      range.load('address,values,rowCount,columnCount');
      await context.sync();

      const values = range.values as any[][];
      const headers = values.length > 0 ? values[0].map(String) : [];

      return {
        address: range.address,
        values,
        rowCount: range.rowCount,
        columnCount: range.columnCount,
        headers,
      };
    });
  }

  /**
   * Get used range (all data) from a sheet
   */
  async getUsedRange(sheetName: string): Promise<RangeData> {
    return Excel.run(async (context) => {
      const sheet = context.workbook.worksheets.getItem(sheetName);
      const range = sheet.getUsedRange();
      range.load('address,values,rowCount,columnCount');
      await context.sync();

      const values = range.values as any[][];
      const headers = values.length > 0 ? values[0].map(String) : [];

      return {
        address: range.address,
        values,
        rowCount: range.rowCount,
        columnCount: range.columnCount,
        headers,
      };
    });
  }

  // =========================================================================
  // Write Operations
  // =========================================================================

  /**
   * Write data to a range
   */
  async writeToRange(
    sheetName: string,
    startCell: string,
    data: any[][]
  ): Promise<string> {
    return Excel.run(async (context) => {
      const sheet = context.workbook.worksheets.getItem(sheetName);
      const startRange = sheet.getRange(startCell);

      // Calculate end range based on data dimensions
      const rowCount = data.length;
      const colCount = data[0]?.length || 0;

      if (rowCount === 0 || colCount === 0) {
        return startCell;
      }

      const range = startRange.getResizedRange(rowCount - 1, colCount - 1);
      range.values = data;

      await context.sync();

      return range.address;
    });
  }

  /**
   * Write data to a new sheet
   */
  async writeToNewSheet(sheetName: string, data: any[][]): Promise<SheetInfo> {
    return Excel.run(async (context) => {
      // Create sheet
      const sheet = context.workbook.worksheets.add(sheetName);

      // Write data starting at A1
      if (data.length > 0) {
        const range = sheet.getRange('A1').getResizedRange(
          data.length - 1,
          (data[0]?.length || 1) - 1
        );
        range.values = data;
      }

      // Format as table
      if (data.length > 1) {
        const tableRange = sheet.getRange('A1').getResizedRange(
          data.length - 1,
          (data[0]?.length || 1) - 1
        );
        sheet.tables.add(tableRange, true);
      }

      sheet.load('id,name,position');
      await context.sync();

      return {
        id: sheet.id,
        name: sheet.name,
        position: sheet.position,
      };
    });
  }

  // =========================================================================
  // Formatting Operations
  // =========================================================================

  /**
   * Apply conditional formatting for variances
   */
  async highlightVariances(
    sheetName: string,
    address: string,
    threshold: number = 0.1
  ): Promise<void> {
    return Excel.run(async (context) => {
      const sheet = context.workbook.worksheets.getItem(sheetName);
      const range = sheet.getRange(address);

      // Add conditional formatting for negative variances (red)
      const negativeFormat = range.conditionalFormats.add(
        Excel.ConditionalFormatType.cellValue
      );
      negativeFormat.cellValue.format.fill.color = '#FFC7CE';
      negativeFormat.cellValue.format.font.color = '#9C0006';
      negativeFormat.cellValue.rule = {
        formula1: `-${threshold}`,
        operator: Excel.ConditionalCellValueOperator.lessThan,
      };

      // Add conditional formatting for positive variances (green)
      const positiveFormat = range.conditionalFormats.add(
        Excel.ConditionalFormatType.cellValue
      );
      positiveFormat.cellValue.format.fill.color = '#C6EFCE';
      positiveFormat.cellValue.format.font.color = '#006100';
      positiveFormat.cellValue.rule = {
        formula1: `${threshold}`,
        operator: Excel.ConditionalCellValueOperator.greaterThan,
      };

      await context.sync();
    });
  }

  /**
   * Highlight specific cells (e.g., for reconciliation differences)
   */
  async highlightCells(
    sheetName: string,
    addresses: string[],
    format: CellFormat
  ): Promise<void> {
    return Excel.run(async (context) => {
      const sheet = context.workbook.worksheets.getItem(sheetName);

      for (const address of addresses) {
        const range = sheet.getRange(address);

        if (format.fill?.color) {
          range.format.fill.color = format.fill.color;
        }
        if (format.font?.color) {
          range.format.font.color = format.font.color;
        }
        if (format.font?.bold !== undefined) {
          range.format.font.bold = format.font.bold;
        }
      }

      await context.sync();
    });
  }

  /**
   * Clear formatting from a range
   */
  async clearFormatting(sheetName: string, address: string): Promise<void> {
    return Excel.run(async (context) => {
      const sheet = context.workbook.worksheets.getItem(sheetName);
      const range = sheet.getRange(address);
      range.format.fill.clear();
      range.format.font.color = '#000000';
      range.format.font.bold = false;

      await context.sync();
    });
  }

  // =========================================================================
  // Table Operations
  // =========================================================================

  /**
   * Convert range to Excel table
   */
  async createTable(
    sheetName: string,
    address: string,
    hasHeaders: boolean = true
  ): Promise<string> {
    return Excel.run(async (context) => {
      const sheet = context.workbook.worksheets.getItem(sheetName);
      const range = sheet.getRange(address);
      const table = sheet.tables.add(range, hasHeaders);
      table.load('name');
      await context.sync();

      return table.name;
    });
  }

  // =========================================================================
  // Utility Methods
  // =========================================================================

  /**
   * Parse a 2D array into objects using first row as headers
   */
  parseToObjects(data: any[][]): Record<string, any>[] {
    if (data.length < 2) return [];

    const headers = data[0].map(String);
    return data.slice(1).map((row) => {
      const obj: Record<string, any> = {};
      headers.forEach((header, index) => {
        obj[header] = row[index];
      });
      return obj;
    });
  }

  /**
   * Convert objects to 2D array with headers
   */
  objectsToArray(objects: Record<string, any>[]): any[][] {
    if (objects.length === 0) return [];

    const headers = Object.keys(objects[0]);
    const rows = objects.map((obj) => headers.map((h) => obj[h]));

    return [headers, ...rows];
  }

  /**
   * Get column letter from index (0 = A, 1 = B, etc.)
   */
  getColumnLetter(index: number): string {
    let letter = '';
    while (index >= 0) {
      letter = String.fromCharCode((index % 26) + 65) + letter;
      index = Math.floor(index / 26) - 1;
    }
    return letter;
  }

  /**
   * Get column index from letter (A = 0, B = 1, etc.)
   */
  getColumnIndex(letter: string): number {
    let index = 0;
    for (let i = 0; i < letter.length; i++) {
      index = index * 26 + (letter.charCodeAt(i) - 64);
    }
    return index - 1;
  }
}

// Export singleton instance
export const excelService = new ExcelService();
export default excelService;
