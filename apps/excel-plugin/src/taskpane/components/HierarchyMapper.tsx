/**
 * Hierarchy Mapper Component (FP&A Feature)
 *
 * Map Excel data ranges to hierarchies using AI-powered suggestions.
 */

import React, { useState, useEffect } from 'react';
import {
  Button,
  Dropdown,
  Option,
  Spinner,
  Badge,
  Checkbox,
  ProgressBar,
} from '@fluentui/react-components';
import {
  TableSimple24Regular,
  Lightbulb24Regular,
  CheckmarkCircle24Regular,
  DismissCircle24Regular,
  ArrowRight24Regular,
} from '@fluentui/react-icons';
import { useDataBridge } from '../providers/DataBridgeProvider';
import { excelService, RangeData } from '../../services/excel.service';
import { apiService, MappingSuggestion, Hierarchy } from '../../services/api.service';

interface MappingItem {
  sourceValue: string;
  rowIndex: number;
  suggestion: MappingSuggestion | null;
  approved: boolean;
  manualHierarchyId?: string;
}

export function HierarchyMapper(): JSX.Element {
  const { projects, selectedProject, hierarchies, selectProject, refreshHierarchies } = useDataBridge();

  const [selectedData, setSelectedData] = useState<RangeData | null>(null);
  const [selectedColumn, setSelectedColumn] = useState<string>('');
  const [mappings, setMappings] = useState<MappingItem[]>([]);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [isApplying, setIsApplying] = useState(false);

  // Load hierarchies when project changes
  useEffect(() => {
    if (selectedProject) {
      refreshHierarchies();
    }
  }, [selectedProject]);

  const handleSelectData = async () => {
    try {
      const data = await excelService.getSelectedRange();
      if (data.rowCount < 2) {
        alert('Please select at least 2 rows (headers + data)');
        return;
      }
      setSelectedData(data);
      setSelectedColumn('');
      setMappings([]);
    } catch (e: any) {
      alert(`Error: ${e.message}`);
    }
  };

  const handleAnalyze = async () => {
    if (!selectedData || !selectedColumn || !selectedProject) return;

    setIsAnalyzing(true);
    setMappings([]);

    try {
      // Get column index
      const columnIndex = selectedData.headers?.indexOf(selectedColumn) ?? -1;
      if (columnIndex < 0) {
        alert('Column not found');
        return;
      }

      // Extract unique values from the column
      const dataRows = selectedData.values.slice(1);
      const uniqueValues = new Map<string, number[]>();

      dataRows.forEach((row, idx) => {
        const value = String(row[columnIndex] ?? '').trim();
        if (value) {
          if (!uniqueValues.has(value)) {
            uniqueValues.set(value, []);
          }
          uniqueValues.get(value)!.push(idx + 1); // +1 for header offset
        }
      });

      // Create initial mappings
      const initialMappings: MappingItem[] = Array.from(uniqueValues.entries()).map(
        ([value, rows]) => ({
          sourceValue: value,
          rowIndex: rows[0],
          suggestion: null,
          approved: false,
        })
      );

      // Try to get AI suggestions
      try {
        const result = await apiService.suggestMappings(
          selectedProject.id,
          Array.from(uniqueValues.keys()),
          selectedColumn
        );

        if (result.success && result.data) {
          // Merge suggestions with mappings
          result.data.forEach((suggestion) => {
            const mapping = initialMappings.find(
              (m) => m.sourceValue === suggestion.sourceValue
            );
            if (mapping) {
              mapping.suggestion = suggestion;
              mapping.approved = suggestion.confidence >= 0.8;
            }
          });
        }
      } catch {
        // Suggestions failed, continue with manual mapping
        console.warn('AI suggestions not available');
      }

      // If no AI suggestions, do basic fuzzy matching locally
      if (!initialMappings.some((m) => m.suggestion)) {
        initialMappings.forEach((mapping) => {
          const hierarchy = findBestMatch(mapping.sourceValue, hierarchies);
          if (hierarchy) {
            mapping.suggestion = {
              sourceValue: mapping.sourceValue,
              hierarchyId: hierarchy.hierarchyId,
              hierarchyName: hierarchy.hierarchyName,
              confidence: 0.7,
              matchType: 'fuzzy',
            };
          }
        });
      }

      setMappings(initialMappings);
    } catch (e: any) {
      alert(`Error: ${e.message}`);
    } finally {
      setIsAnalyzing(false);
    }
  };

  const handleApproveMapping = (index: number, approved: boolean) => {
    setMappings((prev) =>
      prev.map((m, i) => (i === index ? { ...m, approved } : m))
    );
  };

  const handleManualMapping = (index: number, hierarchyId: string) => {
    const hierarchy = hierarchies.find((h) => h.hierarchyId === hierarchyId);
    setMappings((prev) =>
      prev.map((m, i) =>
        i === index
          ? {
              ...m,
              manualHierarchyId: hierarchyId,
              suggestion: hierarchy
                ? {
                    sourceValue: m.sourceValue,
                    hierarchyId: hierarchy.hierarchyId,
                    hierarchyName: hierarchy.hierarchyName,
                    confidence: 1.0,
                    matchType: 'exact',
                  }
                : null,
              approved: true,
            }
          : m
      )
    );
  };

  const handleApplyMappings = async () => {
    const approvedMappings = mappings.filter((m) => m.approved && m.suggestion);
    if (approvedMappings.length === 0) {
      alert('No mappings approved');
      return;
    }

    setIsApplying(true);

    try {
      // Add a new column with hierarchy names
      if (selectedData) {
        const columnIndex = selectedData.headers?.indexOf(selectedColumn) ?? 0;
        const newColumnData: any[][] = [];

        // Header
        newColumnData.push(['Hierarchy Mapping']);

        // Data rows
        const dataRows = selectedData.values.slice(1);
        dataRows.forEach((row) => {
          const value = String(row[columnIndex] ?? '').trim();
          const mapping = approvedMappings.find((m) => m.sourceValue === value);
          newColumnData.push([mapping?.suggestion?.hierarchyName ?? '']);
        });

        // Write to Excel (next column after selection)
        const lastCol = selectedData.address.match(/[A-Z]+/g)?.pop() || 'A';
        const nextCol = excelService.getColumnLetter(
          excelService.getColumnIndex(lastCol) + 1
        );
        const startRow = selectedData.address.match(/\d+/)?.[0] || '1';
        const sheetName = selectedData.address.split('!')[0].replace(/'/g, '');

        await excelService.writeToRange(
          sheetName || (await excelService.getActiveSheet()).name,
          `${nextCol}${startRow}`,
          newColumnData
        );

        alert(`Applied ${approvedMappings.length} mappings to column ${nextCol}`);
      }
    } catch (e: any) {
      alert(`Error: ${e.message}`);
    } finally {
      setIsApplying(false);
    }
  };

  const approvedCount = mappings.filter((m) => m.approved).length;
  const highConfidenceCount = mappings.filter(
    (m) => m.suggestion && m.suggestion.confidence >= 0.8
  ).length;

  return (
    <div className="hierarchy-mapper">
      {/* Project Selection */}
      <div className="card">
        <div className="card-header">
          <h2 className="card-title">
            <TableSimple24Regular />
            Hierarchy Mapper
          </h2>
        </div>

        <div className="form-group">
          <label className="form-label">Project</label>
          <Dropdown
            value={selectedProject?.id || ''}
            onOptionSelect={(_, data) => selectProject(data.optionValue as string)}
            placeholder="Select a project"
          >
            {projects.map((project) => (
              <Option key={project.id} value={project.id}>
                {project.name}
              </Option>
            ))}
          </Dropdown>
        </div>

        {selectedProject && (
          <>
            <div className="button-group">
              <Button appearance="secondary" onClick={handleSelectData}>
                Select Data Range
              </Button>
            </div>

            {selectedData && (
              <div className="selected-info">
                <Badge appearance="outline">
                  {selectedData.address} • {selectedData.rowCount - 1} data rows
                </Badge>
              </div>
            )}
          </>
        )}
      </div>

      {/* Column Selection & Analysis */}
      {selectedData && selectedData.headers && (
        <div className="card">
          <h3>Select Column to Map</h3>

          <div className="form-group">
            <label className="form-label">Column</label>
            <Dropdown
              value={selectedColumn}
              onOptionSelect={(_, data) => setSelectedColumn(data.optionValue as string)}
              placeholder="Select column"
            >
              {selectedData.headers.map((header, idx) => (
                <Option key={idx} value={header}>
                  {header}
                </Option>
              ))}
            </Dropdown>
          </div>

          <div className="button-group right">
            <Button
              appearance="primary"
              icon={<Lightbulb24Regular />}
              disabled={!selectedColumn || isAnalyzing}
              onClick={handleAnalyze}
            >
              {isAnalyzing ? <Spinner size="tiny" /> : 'Analyze & Suggest'}
            </Button>
          </div>
        </div>
      )}

      {/* Mapping Results */}
      {mappings.length > 0 && (
        <div className="card">
          <div className="card-header">
            <h3>Mapping Suggestions</h3>
            <Badge appearance="filled" color="brand">
              {approvedCount}/{mappings.length} approved
            </Badge>
          </div>

          <p className="form-hint">
            Review suggestions and approve or manually select hierarchies.
            {highConfidenceCount > 0 &&
              ` ${highConfidenceCount} high-confidence matches auto-approved.`}
          </p>

          <div className="mapping-list">
            {mappings.map((mapping, idx) => (
              <div key={idx} className="suggestion-item">
                <div className="mapping-source">
                  <Checkbox
                    checked={mapping.approved}
                    onChange={(_, data) =>
                      handleApproveMapping(idx, data.checked as boolean)
                    }
                  />
                  <span className="suggestion-source">{mapping.sourceValue}</span>
                </div>

                <ArrowRight24Regular className="mapping-arrow" />

                <div className="suggestion-target">
                  {mapping.suggestion ? (
                    <>
                      <span>{mapping.suggestion.hierarchyName}</span>
                      <span
                        className={`suggestion-confidence ${
                          mapping.suggestion.confidence >= 0.8
                            ? 'high'
                            : mapping.suggestion.confidence >= 0.5
                            ? 'medium'
                            : 'low'
                        }`}
                      >
                        {Math.round(mapping.suggestion.confidence * 100)}%
                      </span>
                    </>
                  ) : (
                    <Dropdown
                      placeholder="Select hierarchy"
                      value={mapping.manualHierarchyId || ''}
                      onOptionSelect={(_, data) =>
                        handleManualMapping(idx, data.optionValue as string)
                      }
                      style={{ minWidth: 150 }}
                    >
                      {hierarchies.map((h) => (
                        <Option key={h.hierarchyId} value={h.hierarchyId}>
                          {h.hierarchyName}
                        </Option>
                      ))}
                    </Dropdown>
                  )}
                </div>
              </div>
            ))}
          </div>

          <div className="button-group right">
            <Button
              appearance="primary"
              disabled={approvedCount === 0 || isApplying}
              onClick={handleApplyMappings}
            >
              {isApplying ? (
                <Spinner size="tiny" />
              ) : (
                `Apply ${approvedCount} Mappings`
              )}
            </Button>
          </div>
        </div>
      )}

      <style>{`
        .selected-info {
          margin-top: 12px;
        }

        .mapping-list {
          display: flex;
          flex-direction: column;
          gap: 8px;
          margin: 16px 0;
          max-height: 300px;
          overflow-y: auto;
        }

        .mapping-source {
          display: flex;
          align-items: center;
          gap: 8px;
          flex: 1;
        }

        .mapping-arrow {
          color: #605e5c;
          flex-shrink: 0;
        }

        .suggestion-item {
          display: flex;
          align-items: center;
          gap: 12px;
          padding: 8px 12px;
          background: #f9f9f9;
          border-radius: 4px;
        }

        .suggestion-target {
          display: flex;
          align-items: center;
          gap: 8px;
          flex: 1;
          justify-content: flex-end;
        }
      `}</style>
    </div>
  );
}

/**
 * Simple fuzzy matching for local suggestions
 */
function findBestMatch(
  value: string,
  hierarchies: Hierarchy[]
): Hierarchy | null {
  const normalized = value.toLowerCase().trim();

  // Exact match first
  let match = hierarchies.find(
    (h) => h.hierarchyName.toLowerCase() === normalized
  );
  if (match) return match;

  // Contains match
  match = hierarchies.find((h) =>
    h.hierarchyName.toLowerCase().includes(normalized)
  );
  if (match) return match;

  // Reverse contains
  match = hierarchies.find((h) =>
    normalized.includes(h.hierarchyName.toLowerCase())
  );

  return match || null;
}

export default HierarchyMapper;
