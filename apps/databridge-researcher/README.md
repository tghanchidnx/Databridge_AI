# DataBridge Researcher

## Headless FP&A Analytics Engine

Researcher is the analytics companion to Librarian (Hierarchy Builder). Together they form a complete Financial Planning & Analysis platform.

### Relationship to Librarian

| Librarian: Hierarchy Builder | Researcher: Analytics Engine |
|----------------------|---------------------|
| Build P&L hierarchies | Query fact data against hierarchies |
| Define cost center structures | Analyze GL by cost center |
| Map GL accounts | Calculate variances automatically |
| Deploy to Snowflake | Run analysis on data warehouse |

### Key Capabilities

- **Multi-Source Connectivity**: Snowflake, Databricks, SQL Server, PostgreSQL
- **Compute Pushdown**: Execute analysis on the data warehouse, not locally
- **Librarian Integration**: Use Librarian hierarchies as dimensional context
- **NL-to-SQL**: Natural language queries without SQL knowledge
- **FP&A Workflows**: Close, variance, forecast, reporting automation

---

## Quick Start

### 1. Start Docker Sample Environment

```bash
cd researcher/docker
docker-compose up -d

# Wait for database to initialize
docker-compose logs -f postgres
```

### 2. Verify Sample Data

```bash
# Connect to database
docker exec -it databridge-analytics-dw psql -U dw_admin -d financial_dw

# Check tables
\dt dimensions.*
\dt facts.*

# Query sample data
SELECT COUNT(*) FROM facts.fact_gl_balance;
```

### 3. Configure Analytics CLI

```bash
# Copy environment template
cp .env.example .env

# Edit configuration
# DATABASE_URL=postgresql://dw_admin:dw_secure_pass_2024@localhost:5434/financial_dw
```

### 4. Run Analytics

```bash
# Install dependencies
pip install -r requirements.txt

# Add connection
databridge researcher connect add postgresql \
    --name sample-dw \
    --host localhost \
    --port 5434 \
    --database financial_dw \
    --user dw_admin

# Sync metadata catalog
databridge researcher catalog sync sample-dw

# Run analysis
databridge researcher query ask sample-dw "What was total revenue by quarter in 2024?"
```

---

## Docker Environment

| Service | Port | Purpose |
|---------|------|---------|
| PostgreSQL | 5434 | Sample data warehouse |
| pgAdmin | 5050 | Database UI (admin@databridge.local / admin123) |
| ChromaDB | 8001 | Vector store for semantic search |
| Redis | 6382 | Query cache |

### Sample Data Included

| Table | Rows | Description |
|-------|------|-------------|
| dim_date | 1,461 | 4 years (2022-2025) |
| dim_period | 48 | Monthly periods |
| dim_account | 40 | Chart of Accounts (P&L + BS) |
| dim_cost_center | 17 | Departments and regions |
| dim_entity | 6 | Legal entities |
| dim_customer | 200 | Sample customers |
| dim_product | 10 | Products and services |
| fact_gl_balance | ~50K | Monthly GL balances |
| fact_budget | ~20K | Budget by account/cost center |

---

## MCP Server

### Configure Claude Desktop

```json
{
  "mcpServers": {
    "databridge-researcher": {
      "command": "python",
      "args": ["-m", "databridge_researcher.mcp.server"],
      "cwd": "C:\\Users\\telha\\Databridge_AI\\researcher"
    }
  }
}
```

### Available Tools (52)

- **Catalog** (15): Connection, schema, table, column metadata
- **Analysis** (12): Schema detection, profiling, quality scoring
- **Query** (10): SQL execution, NL-to-SQL, query building
- **Insights** (8): Anomalies, trends, comparisons, summaries
- **Knowledgebase** (7): Business glossary, metrics, Librarian integration

---

## FP&A Skills (7)

| Skill | Focus |
|-------|-------|
| FP&A Close Analyst | Month-end close, reconciliation |
| Variance Analyst | BvA analysis, driver identification |
| Financial Forecaster | Rolling forecasts, scenarios |
| Operational Finance | Ops-to-finance translation |
| Management Reporter | Reports, board packages |
| Data Integration | Source connectivity, quality |
| NL Query Assistant | Plain English queries |

---

## Documentation

- [PLAN.md](./PLAN.md) - Full implementation plan
- [FPA_WORKFLOW.md](./FPA_WORKFLOW.md) - End-to-end FP&A workflow
- [skills/README.md](./skills/README.md) - Skill usage guide

---

*Part of the DataBridge AI Platform*
- Librarian: Hierarchy Builder
- Researcher: Analytics Engine (this component)
