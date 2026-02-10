# DataBridge AI - User Guide

## Overview

DataBridge AI is a comprehensive data management platform that combines:
- **Data Reconciliation** - Compare and match data from multiple sources
- **Hierarchy Knowledge Base** - Build and manage financial hierarchies
- **AI Configuration** - Configure templates, skills, and knowledge bases for AI-powered features

---

## Getting Started

### 1. Login & Authentication
- Sign in with email/password or Microsoft SSO
- Join an organization or create a new one during onboarding

### 2. Dashboard
The dashboard provides an overview of:
- Recent projects and activities
- Quick access to common features
- System status and notifications

---

## Core Features

### Schema Matcher
Compare and match database schemas across different connections.

**How to use:**
1. Navigate to **Schema Matcher** from the sidebar
2. Select source and target database connections
3. Choose tables to compare
4. Run comparison to identify differences
5. Review and resolve schema mismatches

### Report Matcher
Compare data reports and identify discrepancies at the cell level.

**Features:**
- Support for CSV, Excel, and database query comparison
- Cell-by-cell difference highlighting
- Configurable matching rules and tolerance thresholds
- Export comparison results

### Hierarchy Knowledge Base
Build and manage hierarchical data structures for financial reporting.

**How to use:**
1. Navigate to **Hierarchy KnowledgeBase**
2. Create a new project or use a template
3. Build your hierarchy structure (P&L, Balance Sheet, Cost Centers, etc.)
4. Map source data to hierarchy nodes
5. Apply formulas and calculations
6. Export or deploy to target systems

---

## Import/Export CSV Files

### Overview
DataBridge AI uses TWO CSV files for complete hierarchy import/export:
1. **Hierarchy CSV** (`_HIERARCHY.CSV`) - Contains the hierarchy structure and sort orders
2. **Mapping CSV** (`_HIERARCHY_MAPPING.CSV`) - Contains source database mappings

### Hierarchy CSV Format

The hierarchy CSV contains the structure, levels, and **sort orders** for your hierarchies.

| Column | Type | Description |
|--------|------|-------------|
| HIERARCHY_ID | String | Unique identifier for the hierarchy |
| HIERARCHY_NAME | String | Display name |
| PARENT_ID | String | Parent hierarchy ID (empty for root nodes) |
| DESCRIPTION | String | Optional description |
| LEVEL_1 - LEVEL_10 | String | Hierarchy level values |
| **LEVEL_1_SORT - LEVEL_10_SORT** | Integer | **Sort order for each level** |
| INCLUDE_FLAG | Boolean | Include in calculations |
| EXCLUDE_FLAG | Boolean | Exclude from calculations |
| TRANSFORM_FLAG | Boolean | Apply transformations |
| CALCULATION_FLAG | Boolean | Is a calculated field |
| ACTIVE_FLAG | Boolean | Is active |
| IS_LEAF_NODE | Boolean | Is a leaf node (no children) |
| FORMULA_GROUP | String | Associated formula group name |
| SORT_ORDER | Integer | Overall display order |

**Important:** Sort orders (`LEVEL_X_SORT`) control the display order of hierarchies within each level. These values come from the hierarchy CSV, NOT the mapping CSV.

### Mapping CSV Format

The mapping CSV contains source database references for each hierarchy.

| Column | Type | Description |
|--------|------|-------------|
| HIERARCHY_ID | String | Links to hierarchy (must match existing) |
| MAPPING_INDEX | Integer | Order of this mapping (1, 2, 3...) |
| SOURCE_DATABASE | String | Source database name |
| SOURCE_SCHEMA | String | Source schema name |
| SOURCE_TABLE | String | Source table name |
| SOURCE_COLUMN | String | Source column name |
| SOURCE_UID | String | Specific value filter (optional) |
| PRECEDENCE_GROUP | String | Precedence grouping (default: "1") |
| INCLUDE_FLAG | Boolean | Include this mapping |
| EXCLUDE_FLAG | Boolean | Exclude this mapping |
| TRANSFORM_FLAG | Boolean | Apply transformation |
| ACTIVE_FLAG | Boolean | Is active |

### Importing CSV Files

**Step-by-step:**
1. Navigate to **Project Details** > **Import/Export** tab
2. Check the **Legacy/Older CSV Format** checkbox if importing from an older version
3. Upload both CSV files:
   - Select the `_HIERARCHY.CSV` file in the Hierarchy section
   - Select the `_HIERARCHY_MAPPING.CSV` file in the Mapping section
4. Click **Import Both** (or import individually)

**Import Order:**
- Hierarchy CSV is always imported first (creates the structure)
- Mapping CSV is imported second (links to existing hierarchies)

### Exporting CSV Files

**Step-by-step:**
1. Navigate to **Project Details** > **Import/Export** tab
2. Click **Export Hierarchy CSV** to download the structure
3. Click **Export Mapping CSV** to download the mappings

**File naming:**
- Hierarchy: `{PROJECT_NAME}_HIERARCHY.csv`
- Mapping: `{PROJECT_NAME}_HIERARCHY_MAPPING.csv`

### Mapping Inheritance

Mappings can be inherited from child hierarchies to parent levels:
- When a child hierarchy has a mapping, parent hierarchies can see it as "inherited"
- Inheritance flows UP (child → parent), never down
- Use the **Mapping Summary** view to see inherited mappings
- Mappings are grouped by **Precedence Group** for organization

### Precedence Groups

Precedence groups allow you to segregate mappings into logical groupings:
- Each unique precedence value creates a separate mapping context
- Precedence "1" is the default group
- Use different precedence values for different data sources or scenarios
- The UI displays mappings grouped vertically by precedence

---

## AI Configuration

The **AI Configuration** page allows you to configure and customize the AI-powered features of DataBridge AI.

### Templates

Templates are pre-built financial statement and organizational structures that you can use as starting points for new projects.

**Available Template Domains:**
- **Accounting** - P&L statements, balance sheets, chart of accounts
- **Finance** - Cost centers, profit centers, budgets
- **Operations** - Geographic hierarchies, department structures, asset classifications

**Available Industries:**
- General (industry-agnostic)
- Oil & Gas (Upstream, Midstream, Services)
- Manufacturing
- Industrial Services
- SaaS / Technology
- Transportation & Logistics

**How to use templates:**
1. Navigate to **AI Configuration** > **Templates** tab
2. Filter by domain or industry
3. Click **View** to see template details
4. Click **Create Project from Template** to start a new project

### Skills

Skills are specialized AI expertise definitions that help the system understand and work with different types of financial data.

**Available Skills:**
| Skill | Domain | Description |
|-------|--------|-------------|
| Financial Analyst | Accounting | GL reconciliation, trial balance, bank rec, COA design |
| Manufacturing Analyst | Accounting | Standard costing, COGS, variances, inventory |
| FP&A Oil & Gas Analyst | Finance | LOS analysis, JIB, reserves, hedge accounting |
| FP&A Cost Analyst | Finance | Cost centers, budgets, allocations |
| SaaS Metrics Analyst | Finance | ARR/MRR, cohorts, CAC/LTV, unit economics |
| Operations Analyst | Operations | Geographic, department, asset hierarchies |
| Transportation Analyst | Operations | Operating ratio, fleet, lanes, driver metrics |

**How to use skills:**
1. Navigate to **AI Configuration** > **Skills** tab
2. Browse skills by domain
3. Click **View Details** to see capabilities and system prompt
4. Skills are automatically applied based on project context

### Knowledge Base

The Knowledge Base stores client-specific configurations, custom prompts, and learned mappings.

**Client Profile Features:**
- Industry classification
- ERP system identification
- GL pattern mappings
- Custom prompts and instructions
- Preferred templates and skills

**How to use the Knowledge Base:**
1. Navigate to **AI Configuration** > **Knowledge Base** tab
2. Click **Add Client** to create a new client profile
3. Configure client settings (industry, ERP, etc.)
4. Add custom prompts for client-specific instructions
5. Define GL pattern mappings

---

## Connections

Manage database connections to your data sources.

**Supported Connection Types:**
- Snowflake
- PostgreSQL
- MySQL
- Oracle
- SQL Server

**How to add a connection:**
1. Navigate to **Connections** from the sidebar
2. Click **Add Connection**
3. Select connection type
4. Enter connection credentials
5. Test and save

---

## Version Control

Track changes to your hierarchies and configurations.

**Features:**
- Commit history
- Version comparison
- Rollback capability
- Branch management

---

## Settings

### Profile
- Update your name, email, and bio
- Upload a profile avatar

### Workspace
- Manage organization settings
- View and invite team members
- Organization invitation key and URL

### Integrations
- GitHub integration for version control
- OpenAI API key configuration
- SMTP configuration for email notifications

### Billing
- View current plan (Free/Pro/Enterprise)
- Billing history and invoices
- Change plan

### Notifications
- Email notification preferences
- Schema comparison alerts
- Connection status changes
- Team activity notifications

### Appearance
- Theme (Light/Dark)
- Font size
- Compact mode

### Security
- Change password
- Two-factor authentication
- Active sessions management

---

## AI Assistant

The AI Assistant provides context-aware help for database operations.

**Capabilities:**
- Natural language query generation
- Schema-aware suggestions
- Self-healing recommendations for mismatches
- Project-specific chat history
- Code generation for data transformations

---

## Tips & Best Practices

### Building Hierarchies
1. **Start with a template** - Templates provide industry-standard structures
2. **Use appropriate skills** - Select skills matching your industry
3. **Map carefully** - Verify source data mappings before calculations
4. **Test formulas** - Validate calculations with sample data

### Managing Connections
1. **Use secure credentials** - Never share connection passwords
2. **Test connections regularly** - Verify connectivity before operations
3. **Monitor status** - Enable connection status change notifications

### Collaborating with Teams
1. **Use the invitation link** - Share organization invitation URL with team members
2. **Assign appropriate roles** - Set member roles based on responsibilities
3. **Track changes** - Use version control for change history

---

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl/Cmd + K` | Open command palette |
| `Ctrl/Cmd + S` | Save current work |
| `Ctrl/Cmd + Z` | Undo |
| `Ctrl/Cmd + Shift + Z` | Redo |
| `Escape` | Close dialogs |

---

## Troubleshooting

### Connection Issues
- Verify credentials are correct
- Check network connectivity
- Ensure firewall allows database access

### Performance
- Close unused browser tabs
- Clear browser cache if issues persist
- Contact support for large dataset handling

### Data Mismatches
- Verify source data quality
- Check mapping configurations
- Review formula calculations

---

## Support

For additional help:
- Visit the documentation at `/docs`
- Contact support through the AI Assistant
- Check the GitHub repository for updates

---

*DataBridge AI v2.0 - Built for financial data excellence*
