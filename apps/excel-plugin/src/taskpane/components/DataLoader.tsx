/**
 * Data Loader Component
 *
 * Load data from connections into Excel sheets,
 * profile data quality, and reconcile sheets.
 */

import React, { useState } from 'react';
import {
  Button,
  Dropdown,
  Option,
  Spinner,
  ProgressBar,
  Badge,
} from '@fluentui/react-components';
import {
  DocumentSearch24Regular,
  ArrowDownload24Regular,
  TableSimple24Regular,
  CheckmarkCircle24Regular,
  Warning24Regular,
} from '@fluentui/react-icons';
import { useDataBridge } from '../providers/DataBridgeProvider';
import { excelService, RangeData } from '../../services/excel.service';
import { apiService, DataProfile } from '../../services/api.service';

export function DataLoader(): JSX.Element {
  return (
    <div className="data-loader">
      <DataProfiler />
      <SheetReconciler />
    </div>
  );
}

function DataProfiler(): JSX.Element {
  const [selectedData, setSelectedData] = useState<RangeData | null>(null);
  const [profile, setProfile] = useState<DataProfile | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSelectData = async () => {
    setError(null);
    try {
      const data = await excelService.getSelectedRange();
      if (data.rowCount < 2) {
        setError('Please select at least 2 rows (headers + data)');
        return;
      }
      setSelectedData(data);
      setProfile(null);
    } catch (e: any) {
      setError(e.message);
    }
  };

  const handleProfileData = async () => {
    if (!selectedData) return;

    setIsLoading(true);
    setError(null);

    try {
      // Simple client-side profiling
      const values = selectedData.values;
      const headers = values[0].map(String);
      const dataRows = values.slice(1);

      const columns = headers.map((name, colIdx) => {
        const columnValues = dataRows.map((row) => row[colIdx]);
        const nullCount = columnValues.filter((v) => v === null || v === '' || v === undefined).length;
        const uniqueValues = new Set(columnValues.filter((v) => v != null && v !== ''));

        return {
          name,
          type: inferType(columnValues),
          nullCount,
          nullPercentage: Math.round((nullCount / dataRows.length) * 100),
          uniqueCount: uniqueValues.size,
          cardinality: Math.round((uniqueValues.size / dataRows.length) * 100),
          sampleValues: Array.from(uniqueValues).slice(0, 5),
        };
      });

      // Count duplicate rows
      const rowStrings = dataRows.map((row) => JSON.stringify(row));
      const uniqueRows = new Set(rowStrings);
      const duplicateRows = dataRows.length - uniqueRows.size;

      const profileResult: DataProfile = {
        rowCount: dataRows.length,
        columnCount: headers.length,
        columns,
        duplicateRows,
        nullPercentage: Math.round(
          (columns.reduce((sum, c) => sum + c.nullCount, 0) / (dataRows.length * headers.length)) * 100
        ),
      };

      setProfile(profileResult);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="card">
      <div className="card-header">
        <h2 className="card-title">
          <DocumentSearch24Regular />
          Data Profiler
        </h2>
      </div>

      <p className="form-hint">
        Select a data range in Excel to analyze its quality and structure.
      </p>

      <div className="button-group">
        <Button appearance="secondary" onClick={handleSelectData}>
          Select Data Range
        </Button>
        {selectedData && (
          <Button
            appearance="primary"
            onClick={handleProfileData}
            disabled={isLoading}
          >
            {isLoading ? <Spinner size="tiny" /> : 'Profile Data'}
          </Button>
        )}
      </div>

      {selectedData && (
        <div className="selected-info">
          <Badge appearance="outline">
            {selectedData.address} • {selectedData.rowCount} rows × {selectedData.columnCount} columns
          </Badge>
        </div>
      )}

      {error && <div className="error-message">{error}</div>}

      {profile && (
        <div className="profile-results">
          <h3>Profile Results</h3>

          <div className="profile-summary">
            <div className="summary-item">
              <span className="summary-value">{profile.rowCount}</span>
              <span className="summary-label">Rows</span>
            </div>
            <div className="summary-item">
              <span className="summary-value">{profile.columnCount}</span>
              <span className="summary-label">Columns</span>
            </div>
            <div className="summary-item">
              <span className="summary-value">{profile.duplicateRows}</span>
              <span className="summary-label">Duplicates</span>
            </div>
            <div className="summary-item">
              <span className="summary-value">{profile.nullPercentage}%</span>
              <span className="summary-label">Null</span>
            </div>
          </div>

          <h4>Column Details</h4>
          <div className="column-profiles">
            {profile.columns.map((col, idx) => (
              <div key={idx} className="column-profile">
                <div className="column-header">
                  <span className="column-name">{col.name}</span>
                  <Badge size="small" appearance="outline">
                    {col.type}
                  </Badge>
                </div>
                <div className="column-stats">
                  <span>Unique: {col.uniqueCount} ({col.cardinality}%)</span>
                  <span>Null: {col.nullCount} ({col.nullPercentage}%)</span>
                </div>
                {col.nullPercentage > 20 && (
                  <Badge color="warning" icon={<Warning24Regular />} size="small">
                    High null rate
                  </Badge>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      <style>{`
        .selected-info {
          margin-top: 12px;
        }

        .error-message {
          margin-top: 12px;
          padding: 8px 12px;
          background: #fed9cc;
          border-radius: 4px;
          color: #d13438;
          font-size: 13px;
        }

        .profile-results {
          margin-top: 16px;
        }

        .profile-results h3 {
          margin: 0 0 12px 0;
          font-size: 16px;
        }

        .profile-results h4 {
          margin: 16px 0 8px 0;
          font-size: 14px;
          color: #605e5c;
        }

        .profile-summary {
          display: grid;
          grid-template-columns: repeat(4, 1fr);
          gap: 8px;
        }

        .summary-item {
          text-align: center;
          padding: 12px;
          background: #f3f2f1;
          border-radius: 8px;
        }

        .summary-value {
          display: block;
          font-size: 24px;
          font-weight: 600;
          color: #0078d4;
        }

        .summary-label {
          font-size: 12px;
          color: #605e5c;
        }

        .column-profiles {
          display: flex;
          flex-direction: column;
          gap: 8px;
        }

        .column-profile {
          padding: 8px 12px;
          background: #f9f9f9;
          border-radius: 4px;
          border-left: 3px solid #0078d4;
        }

        .column-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: 4px;
        }

        .column-name {
          font-weight: 500;
        }

        .column-stats {
          display: flex;
          gap: 16px;
          font-size: 12px;
          color: #605e5c;
        }
      `}</style>
    </div>
  );
}

function SheetReconciler(): JSX.Element {
  const [sheets, setSheets] = useState<Array<{ id: string; name: string }>>([]);
  const [sourceSheet, setSourceSheet] = useState<string>('');
  const [targetSheet, setTargetSheet] = useState<string>('');
  const [isLoading, setIsLoading] = useState(false);

  const loadSheets = async () => {
    try {
      const sheetList = await excelService.getSheets();
      setSheets(sheetList);
    } catch (e) {
      console.error('Failed to load sheets:', e);
    }
  };

  React.useEffect(() => {
    loadSheets();
  }, []);

  const handleReconcile = async () => {
    if (!sourceSheet || !targetSheet || sourceSheet === targetSheet) return;

    setIsLoading(true);
    try {
      // Get data from both sheets
      const sourceData = await excelService.getUsedRange(sourceSheet);
      const targetData = await excelService.getUsedRange(targetSheet);

      // For now, just show a comparison summary
      const sourceRows = sourceData.rowCount - 1; // minus header
      const targetRows = targetData.rowCount - 1;

      alert(
        `Reconciliation Preview:\n\n` +
          `Source (${sourceSheet}): ${sourceRows} rows\n` +
          `Target (${targetSheet}): ${targetRows} rows\n\n` +
          `Full reconciliation with highlighting coming soon!`
      );
    } catch (e: any) {
      alert(`Error: ${e.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="card">
      <div className="card-header">
        <h2 className="card-title">
          <TableSimple24Regular />
          Sheet Reconciler
        </h2>
      </div>

      <p className="form-hint">
        Compare two sheets and highlight differences.
      </p>

      <div className="form-group">
        <label className="form-label">Source Sheet</label>
        <Dropdown
          value={sourceSheet}
          onOptionSelect={(_, data) => setSourceSheet(data.optionValue as string)}
          placeholder="Select source sheet"
        >
          {sheets.map((sheet) => (
            <Option key={sheet.id} value={sheet.name}>
              {sheet.name}
            </Option>
          ))}
        </Dropdown>
      </div>

      <div className="form-group">
        <label className="form-label">Target Sheet</label>
        <Dropdown
          value={targetSheet}
          onOptionSelect={(_, data) => setTargetSheet(data.optionValue as string)}
          placeholder="Select target sheet"
        >
          {sheets.filter((s) => s.name !== sourceSheet).map((sheet) => (
            <Option key={sheet.id} value={sheet.name}>
              {sheet.name}
            </Option>
          ))}
        </Dropdown>
      </div>

      <div className="button-group right">
        <Button
          appearance="primary"
          disabled={!sourceSheet || !targetSheet || isLoading}
          onClick={handleReconcile}
        >
          {isLoading ? <Spinner size="tiny" /> : 'Reconcile'}
        </Button>
      </div>
    </div>
  );
}

function inferType(values: any[]): string {
  const nonNull = values.filter((v) => v != null && v !== '');
  if (nonNull.length === 0) return 'unknown';

  const sample = nonNull[0];

  if (typeof sample === 'number') return 'number';
  if (typeof sample === 'boolean') return 'boolean';
  if (sample instanceof Date) return 'date';

  // Check if string values are dates or numbers
  const strSample = String(sample);
  if (!isNaN(Number(strSample))) return 'number';
  if (!isNaN(Date.parse(strSample))) return 'date';

  return 'text';
}

export default DataLoader;
