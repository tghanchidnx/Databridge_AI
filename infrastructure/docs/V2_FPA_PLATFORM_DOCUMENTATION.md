# DataBridge AI V2 - FP&A Platform Documentation

## Complete Guide to the Financial Planning & Analysis Platform

---

## Table of Contents

1. [Introduction](#introduction)
2. [V2 Architecture](#v2-architecture)
3. [Quick Start Guide](#quick-start-guide)
4. [Phase 11: Multi-System Mapping](#phase-11-multi-system-mapping)
5. [Phase 12: System Mapping UI](#phase-12-system-mapping-ui)
6. [Phase 13: Live Data Preview](#phase-13-live-data-preview)
7. [Phase 14: Fact Table Generation](#phase-14-fact-table-generation)
8. [Phase 15: Excel Integration](#phase-15-excel-integration)
9. [Phase 16: AI Enhancements](#phase-16-ai-enhancements)
10. [API Reference](#api-reference)
11. [Component Reference](#component-reference)
12. [Troubleshooting](#troubleshooting)

---

## Introduction

DataBridge AI V2 transforms the Hierarchy Knowledge Base into a complete **Financial Planning & Analysis (FP&A) Platform** with:

- **Multi-System Mapping**: Map hierarchies to ACTUALS, BUDGET, FORECAST, and PRIOR_YEAR systems
- **Live Data Preview**: Real-time query execution with data visualization
- **Fact Table Generation**: Automated DDL and INSERT script generation with variance calculations
- **Excel Integration**: Import/export with AI-powered column detection
- **Natural Language Queries**: Ask questions in plain English, get SQL back
- **AI Auto-Mapping**: Intelligent column-to-field mapping for Excel imports

### What's New in V2

| Feature | V1 | V2 |
|---------|----|----|
| System Types | Single source | ACTUALS, BUDGET, FORECAST, PRIOR_YEAR |
| Join Types | Not supported | INNER, LEFT, RIGHT, FULL |
| Dimension Roles | Not supported | PRIMARY, SECONDARY, OPTIONAL |
| Variance Calculations | Manual | Automatic with percentages |
| Live Preview | Not available | Real-time query execution |
| Excel Import | Basic | AI-powered with column detection |
| Natural Language | Not available | Full NL to SQL translation |

---

## V2 Architecture

### Port Configuration

| Service | V1 Port | V2 Port | Purpose |
|---------|---------|---------|---------|
| MySQL | 3307 | **3308** | Database |
| NestJS Backend | 3001 | **3002** | REST API |
| React Frontend | 5173 | **5174** | Web UI |
| Redis | 6380 | **6381** | Caching |

### Folder Structure

```
T:\Users\telha\Databridge_AI_Source\infrastructure\
├── frontend/                    # React + Vite + TailwindCSS
│   └── src/
│       └── components/
│           └── hierarchy-knowledge-base/
│               ├── components/  # UI Components
│               │   ├── SystemMappingPanel.tsx
│               │   ├── VarianceConfigPanel.tsx
│               │   ├── LiveDataPreview.tsx
│               │   └── ...
│               └── dialogs/     # Modal Dialogs
│                   ├── ExcelImportDialog.tsx
│                   └── ...
├── backend/                     # NestJS + Prisma + MySQL
│   └── src/
│       └── modules/
│           ├── smart-hierarchy/
│           │   └── services/
│           │       ├── query-executor.service.ts
│           │       └── fact-table.service.ts
│           ├── excel/
│           │   ├── excel-import.service.ts
│           │   └── excel-export.service.ts
│           └── ai/
│               ├── nl-query.service.ts
│               └── auto-mapper.service.ts
└── docker-compose.yml           # Container orchestration
```

---

## Quick Start Guide

### Prerequisites

- Node.js 18+
- Docker Desktop (for MySQL/Redis)
- Git

### Step 1: Start Docker Services

```bash
cd T:\Users\telha\Databridge_AI_Source\infrastructure
docker-compose up -d
```

### Step 2: Start Backend

```bash
cd T:\Users\telha\Databridge_AI_Source\infrastructure\backend
npm install   # First time only
npm run start:dev
```

Wait for: `Nest application successfully started on port 3002`

### Step 3: Start Frontend

```bash
cd T:\Users\telha\Databridge_AI_Source\infrastructure\frontend
npm install   # First time only
npm run dev
```

Wait for: `VITE v5.x.x ready in xxx ms`

### Step 4: Access Application

Open http://localhost:5174 in your browser.

---

## Phase 11: Multi-System Mapping

### Overview

Phase 11 extends the source mapping schema to support FP&A workflows with multiple financial systems.

### New Data Types

#### System Types

```typescript
type SystemType = 'ACTUALS' | 'BUDGET' | 'FORECAST' | 'PRIOR_YEAR' | 'CUSTOM';
```

| System | Description | Common Use |
|--------|-------------|------------|
| ACTUALS | Real transaction data | General Ledger, ERP |
| BUDGET | Annual budget data | Planning system |
| FORECAST | Rolling forecasts | FP&A projections |
| PRIOR_YEAR | Historical comparisons | YoY analysis |
| CUSTOM | User-defined | Special scenarios |

#### Join Types

```typescript
type JoinType = 'INNER' | 'LEFT' | 'RIGHT' | 'FULL';
```

| Join Type | When to Use |
|-----------|-------------|
| INNER | Required dimension - row must exist |
| LEFT | Optional dimension - include nulls |
| RIGHT | Rare - include unmatched targets |
| FULL | Complete reconciliation views |

#### Dimension Roles

```typescript
type DimensionRole = 'PRIMARY' | 'SECONDARY' | 'OPTIONAL';
```

| Role | Join Type | Description |
|------|-----------|-------------|
| PRIMARY | INNER | Main dimension - required for aggregation |
| SECONDARY | LEFT | Supporting dimension - enhance detail |
| OPTIONAL | LEFT + COALESCE | Nice-to-have with defaults |

### Extended Source Mapping

```typescript
interface SourceMapping {
  // Existing fields
  mapping_index: number;
  source_database: string;
  source_schema: string;
  source_table: string;
  source_column: string;
  source_uid?: string;

  // NEW FP&A Fields
  join_type?: JoinType;           // Default: 'LEFT'
  system_type?: SystemType;        // Default: 'ACTUALS'
  dimension_role?: DimensionRole;  // Default: 'SECONDARY'
  fact_table_ref?: string;         // Reference to fact table
  join_keys?: JoinKeyConfig[];     // Multi-column joins

  flags: SourceMappingFlags;
}
```

### Variance Configuration

```typescript
interface VarianceConfig {
  enabled: boolean;
  comparisons: Array<{
    name: string;            // "Actual vs Budget"
    minuend: SystemType;     // ACTUALS (what we have)
    subtrahend: SystemType;  // BUDGET (what we expected)
    includePercent: boolean; // Calculate variance %
  }>;
}
```

#### Default Variance Comparisons

1. **Actual vs Budget** - ACTUALS - BUDGET
2. **Actual vs Forecast** - ACTUALS - FORECAST
3. **YoY Variance** - ACTUALS - PRIOR_YEAR
4. **Budget vs Forecast** - BUDGET - FORECAST

---

## Phase 12: System Mapping UI

### SystemMappingPanel Component

Location: `frontend/src/components/hierarchy-knowledge-base/components/SystemMappingPanel.tsx`

#### Features

- **Side-by-side columns** for ACTUALS, BUDGET, FORECAST
- **Visual indicators** for missing mappings (red highlight)
- **Join type selector** with tooltips
- **Dimension role badges** with color coding
- **Bulk operations** across all systems
- **Coverage statistics** per system

#### Usage

```tsx
import { SystemMappingPanel } from './components';

<SystemMappingPanel
  hierarchyId="REVENUE_001"
  mappings={currentMappings}
  onMappingChange={handleMappingChange}
  systemTypes={['ACTUALS', 'BUDGET', 'FORECAST']}
/>
```

### VarianceConfigPanel Component

Location: `frontend/src/components/hierarchy-knowledge-base/components/VarianceConfigPanel.tsx`

#### Features

- **Preset comparisons** with one-click setup
- **Custom comparison builder**
- **Toggle percentage calculations**
- **Preview variance SQL**

#### Usage

```tsx
import { VarianceConfigPanel } from './components';

<VarianceConfigPanel
  config={varianceConfig}
  onChange={handleVarianceChange}
  systemTypes={availableSystemTypes}
/>
```

---

## Phase 13: Live Data Preview

### Overview

Execute queries in real-time as mappings change, with instant data visualization.

### LiveDataPreview Component

Location: `frontend/src/components/hierarchy-knowledge-base/components/LiveDataPreview.tsx`

#### Features

- **Real-time SQL generation** from mappings
- **Debounced execution** (500ms delay)
- **Data table** with sorting and pagination
- **Column statistics** (min, max, distinct count)
- **Query timing** display
- **Export to CSV/Excel**
- **Error handling** with user-friendly messages

#### Usage

```tsx
import { LiveDataPreview } from './components';

<LiveDataPreview
  hierarchyId="REVENUE_001"
  mappings={currentMappings}
  connectionId="snowflake-prod"
  autoExecute={true}
  maxRows={100}
/>
```

### Backend Service: QueryExecutorService

Location: `backend/src/modules/smart-hierarchy/services/query-executor.service.ts`

#### Methods

```typescript
// Execute preview query with caching
async executePreviewQuery(dto: PreviewQueryDto): Promise<PreviewResult>

// Get row count estimation
async estimateRowCount(connectionId: string, query: string): Promise<number>

// Generate preview SQL from mappings
generatePreviewSQL(mappings: SourceMapping[]): string
```

#### Caching

- **Cache Key**: SHA-256 hash of connection + query
- **TTL**: 5 minutes
- **Auto-invalidation**: On mapping changes

---

## Phase 14: Fact Table Generation

### Overview

Generate complete DDL and INSERT scripts for fact tables with proper joins and variance calculations.

### FactTableService

Location: `backend/src/modules/smart-hierarchy/services/fact-table.service.ts`

#### Generate Fact Table Script

```typescript
async generateFactTableScript(dto: GenerateFactTableDto): Promise<FactTableScript>
```

**Output includes:**
- CREATE TABLE DDL with proper data types
- INSERT/MERGE statements with joins
- Variance column calculations
- Index recommendations

#### Join Logic

```sql
-- PRIMARY Dimension (INNER JOIN - Required)
INNER JOIN dim_account da ON f.account_key = da.account_key

-- SECONDARY Dimension (LEFT JOIN - Optional)
LEFT JOIN dim_product dp ON f.product_key = dp.product_key

-- OPTIONAL Dimension (LEFT JOIN with COALESCE)
LEFT JOIN dim_region dr ON f.region_key = dr.region_key
```

#### Variance Column Generation

```sql
-- Variance Amount
(actuals_amount - budget_amount) AS variance_avb_amount,

-- Variance Percentage (with null handling)
CASE
  WHEN budget_amount != 0
  THEN ((actuals_amount - budget_amount) / budget_amount * 100)
  ELSE NULL
END AS variance_avb_percent
```

### Supported Databases

| Database | Dialect | Features |
|----------|---------|----------|
| Snowflake | snowflake | MERGE, VARIANT, CLUSTER BY |
| PostgreSQL | postgresql | UPSERT, JSONB |
| MySQL | mysql | ON DUPLICATE KEY |
| SQL Server | mssql | MERGE, IDENTITY |

---

## Phase 15: Excel Integration

### Overview

Import and export hierarchies with AI-powered column detection and mapping.

### Excel Import

#### ExcelImportDialog

Location: `frontend/src/components/hierarchy-knowledge-base/dialogs/ExcelImportDialog.tsx`

**Features:**
- Drag-and-drop file upload
- Sheet selection with tabs
- AI-powered column mapping
- Preview table with sample data
- Conflict resolution options
- Progress tracking

**Conflict Resolution Options:**
- **Merge**: Update existing, add new
- **Replace**: Delete existing, import all
- **Skip**: Only add new, skip conflicts

#### Backend Service

Location: `backend/src/modules/excel/excel-import.service.ts`

```typescript
// Parse Excel file
async parseExcelFile(buffer: Buffer, fileName: string): Promise<ExcelParseResult>

// Import to project
async importExcelToProject(
  projectId: string,
  parseResult: ExcelParseResult,
  sheetName: string,
  columnMappings: ColumnMapping[],
  conflictResolution?: 'merge' | 'replace' | 'skip'
): Promise<ImportResult>
```

### Excel Export

#### Backend Service

Location: `backend/src/modules/excel/excel-export.service.ts`

**Exported Worksheets:**
1. **Hierarchies** - Structure with all levels
2. **Mappings** - Source mappings with system types
3. **Formulas** - Formula configurations
4. **Variance Config** - Variance comparison setup
5. **Instructions** - Import guide

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/excel/preview` | Upload and preview Excel |
| POST | `/excel/import` | Import Excel to project |
| GET | `/excel/export/:projectId` | Export project to Excel |
| GET | `/excel/template` | Download import template |
| POST | `/excel/validate` | Validate import data |

---

## Phase 16: AI Enhancements

### Natural Language Query Service

Location: `backend/src/modules/ai/nl-query.service.ts`

#### Features

Translate plain English questions to SQL:

```
"Show Q3 actuals vs budget by department"
↓
SELECT department, actuals_amount, budget_amount,
       (actuals_amount - budget_amount) AS variance
FROM fact_hierarchy_data
WHERE period_date BETWEEN '2024-07-01' AND '2024-09-30'
GROUP BY department
```

#### Supported Query Types

| Query Type | Example |
|------------|---------|
| Comparison | "actuals vs budget" |
| Trend | "monthly revenue trend" |
| Filter | "top 10 accounts by variance" |
| Aggregation | "total expenses by department" |
| Time Range | "YTD", "Q3", "last 12 months" |

#### API Endpoint

```bash
POST /ai/natural-language-query
Content-Type: application/json

{
  "query": "Show YoY variance for Operating Expenses",
  "context": {
    "projectId": "uuid",
    "hierarchies": [...],
    "availableSystems": ["ACTUALS", "BUDGET", "PRIOR_YEAR"]
  }
}
```

**Response:**
```json
{
  "sql": "SELECT ... FROM ... WHERE ...",
  "explanation": "This query will compare ACTUALS vs PRIOR_YEAR for Operating Expenses (YoY).",
  "tables": ["fact_hierarchy_data"],
  "columns": ["hierarchy_name", "actuals_amount", "prior_year_amount", "variance"],
  "confidence": 0.85
}
```

### Auto-Mapper Service

Location: `backend/src/modules/ai/auto-mapper.service.ts`

#### Features

Automatically map Excel columns to hierarchy fields:

- **Pattern Matching**: Recognizes common column names
- **Fuzzy Matching**: Handles abbreviations and typos
- **AI Enhancement**: Uses Claude/OpenAI for complex mappings
- **Format Detection**: Standard, legacy, or custom
- **System Type Detection**: Identifies ACTUALS, BUDGET columns

#### API Endpoint

```bash
POST /ai/auto-map-excel
Content-Type: application/json

{
  "excelData": {
    "fileName": "hierarchy_import.xlsx",
    "sheets": [...]
  },
  "sheetName": "Hierarchies"
}
```

**Response:**
```json
{
  "sheetName": "Hierarchies",
  "columnMappings": [
    {
      "excelColumn": "HIER_ID",
      "hierarchyField": "HIERARCHY_ID",
      "confidence": 0.95,
      "reasoning": "Exact pattern match"
    },
    {
      "excelColumn": "Account Name",
      "hierarchyField": "HIERARCHY_NAME",
      "confidence": 0.82,
      "reasoning": "AI suggestion"
    }
  ],
  "detectedFormat": "legacy",
  "systemTypeDetection": {
    "hasActuals": true,
    "hasBudget": true,
    "detectedColumns": {
      "ACTUAL_AMT": "ACTUALS",
      "BUDGET_AMT": "BUDGET"
    }
  }
}
```

---

## API Reference

### Smart Hierarchy Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/smart-hierarchy/projects` | List projects |
| POST | `/smart-hierarchy/projects` | Create project |
| GET | `/smart-hierarchy/project/:id/tree` | Get hierarchy tree |
| POST | `/smart-hierarchy` | Create hierarchy |
| PUT | `/smart-hierarchy/project/:pid/:hid` | Update hierarchy |

### Query & Preview Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/smart-hierarchy/preview-query` | Execute preview query |
| POST | `/smart-hierarchy/estimate-row-count` | Get row count |
| POST | `/smart-hierarchy/generate-fact-table` | Generate DDL |

### Excel Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/excel/preview` | Upload and preview |
| POST | `/excel/import` | Import to project |
| GET | `/excel/export/:projectId` | Export to Excel |
| GET | `/excel/template` | Download template |

### AI Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/ai/natural-language-query` | NL to SQL |
| POST | `/ai/auto-map-excel` | Auto-map columns |
| POST | `/ai/suggest-mappings` | Get mapping suggestions |
| POST | `/ai/detect-anomalies` | Find anomalies |

---

## Component Reference

### Frontend Components

#### Hierarchy Knowledge Base

| Component | Location | Purpose |
|-----------|----------|---------|
| SystemMappingPanel | components/ | Multi-system mapping UI |
| VarianceConfigPanel | components/ | Variance configuration |
| LiveDataPreview | components/ | Real-time query preview |
| MappingCoverageHeatmap | components/ | Coverage visualization |
| ProjectHealthDashboard | components/ | Project metrics |

#### Dialogs

| Dialog | Location | Purpose |
|--------|----------|---------|
| ExcelImportDialog | dialogs/ | Excel import wizard |
| SmartImportDialog | dialogs/ | AI-powered import |
| TemplateGalleryDialog | dialogs/ | Browse templates |

### Backend Services

#### Smart Hierarchy Module

| Service | File | Purpose |
|---------|------|---------|
| QueryExecutorService | query-executor.service.ts | Execute queries |
| FactTableService | fact-table.service.ts | Generate DDL |
| ScriptGeneratorService | script-generator.service.ts | SQL generation |

#### Excel Module

| Service | File | Purpose |
|---------|------|---------|
| ExcelImportService | excel-import.service.ts | Parse & import |
| ExcelExportService | excel-export.service.ts | Generate Excel |

#### AI Module

| Service | File | Purpose |
|---------|------|---------|
| NLQueryService | nl-query.service.ts | NL to SQL |
| AutoMapperService | auto-mapper.service.ts | Column mapping |
| MappingSuggesterService | mapping-suggester.service.ts | Suggest mappings |
| AnomalyDetectorService | anomaly-detector.service.ts | Find issues |

---

## Troubleshooting

### Common Issues

#### Backend Won't Start

**Error**: `Cannot find module 'xlsx'`

**Solution**:
```bash
cd v2/backend
npm install xlsx
```

#### Excel Import Fails

**Error**: `Only Excel files (.xlsx, .xls) are allowed`

**Solution**: Ensure file has correct extension and is a valid Excel file.

#### Query Preview Times Out

**Error**: `Query execution timeout`

**Solutions**:
1. Reduce `maxRows` parameter
2. Add WHERE clause filters
3. Check database connection
4. Increase timeout in configuration

#### AI Mapping Returns Empty

**Error**: No column mappings returned

**Solutions**:
1. Ensure ANTHROPIC_API_KEY or OPENAI_API_KEY is set
2. Check column names match expected patterns
3. Verify Excel file has headers in first row

### Logs

```bash
# Backend logs
tail -f v2/backend/logs/combined.log

# Frontend console
Open browser DevTools (F12) → Console tab
```

### Health Checks

```bash
# Backend health
curl http://localhost:3002/api/health

# Excel module
curl -H "X-API-Key: dev-key-1" http://localhost:3002/api/excel/template -o template.xlsx

# AI module
curl http://localhost:3002/api/ai/health
```

---

## Quick Reference

### System Type Values

| Value | Description |
|-------|-------------|
| ACTUALS | Real/actual data |
| BUDGET | Budget/plan data |
| FORECAST | Forecast data |
| PRIOR_YEAR | Prior year comparison |
| CUSTOM | Custom system |

### Join Type Rules

| Dimension Role | Default Join | Override Allowed |
|----------------|--------------|------------------|
| PRIMARY | INNER | No |
| SECONDARY | LEFT | Yes |
| OPTIONAL | LEFT | Yes |

### Variance Formula

```
Variance Amount = Minuend - Subtrahend
Variance Percent = (Minuend - Subtrahend) / Subtrahend × 100
```

---

*DataBridge AI V2 - FP&A Platform Documentation*
*Last Updated: January 2026*
