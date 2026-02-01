# DataBridge AI - Development Changelog & Reference Guide

> **Purpose:** Track all changes, lessons learned, and requirements clarifications to avoid repeating mistakes and improve future development.

---

## Session: January 23, 2026

### Changes Made

#### 1. Mapping Inheritance System
**Files Modified:**
- `src/hierarchy/service.py` - Added inheritance methods
- `src/hierarchy/mcp_tools.py` - Added 3 new tools

**What was implemented:**
- Mappings propagate from **child → parent** (NOT parent → child)
- Added `get_child_hierarchies()` method
- Added `get_all_descendants()` method
- Added `get_inherited_mappings()` method
- Added `get_mapping_summary()` method

**MCP Tools Added:**
| Tool | Purpose |
|------|---------|
| `get_inherited_mappings` | Get all mappings inherited from child hierarchies |
| `get_mapping_summary` | Get summary of own vs inherited mappings |
| `get_mappings_by_precedence` | Get mappings grouped by precedence value |

**Key Decision:** Inheritance flows UP only (child to parent), never down. This matches financial reporting logic where leaf nodes have actual data mappings and parent nodes aggregate them.

---

#### 2. Precedence Groups for Mapping Segregation
**What was implemented:**
- Mappings can be segregated by `precedence_group` field
- UI displays mappings grouped vertically by precedence
- Default precedence is "1"

**Use Case:** Different data sources or scenarios can have separate mapping contexts (e.g., "Budget" vs "Actual", "Source A" vs "Source B")

---

#### 3. CSV Import/Export Updates
**Files Modified:**
- `src/hierarchy/types.py` - Added level sort fields
- `src/hierarchy/service.py` - Updated import/export functions
- `src/hierarchy/mcp_tools.py` - Added `import_mapping_csv` tool

**Critical Clarification:**
| Data | Source File | Columns |
|------|-------------|---------|
| Sort orders | `_HIERARCHY.CSV` | `LEVEL_1_SORT` through `LEVEL_10_SORT` |
| Mappings | `_HIERARCHY_MAPPING.CSV` | `MAPPING_INDEX`, `SOURCE_*` columns |

**CSV Column Reference - Hierarchy File:**
```
HIERARCHY_ID, HIERARCHY_NAME, PARENT_ID, DESCRIPTION,
LEVEL_1...LEVEL_10, LEVEL_1_SORT...LEVEL_10_SORT,
INCLUDE_FLAG, EXCLUDE_FLAG, TRANSFORM_FLAG, CALCULATION_FLAG,
ACTIVE_FLAG, IS_LEAF_NODE, FORMULA_GROUP, SORT_ORDER
```

**CSV Column Reference - Mapping File:**
```
HIERARCHY_ID, MAPPING_INDEX, SOURCE_DATABASE, SOURCE_SCHEMA,
SOURCE_TABLE, SOURCE_COLUMN, SOURCE_UID, PRECEDENCE_GROUP,
INCLUDE_FLAG, EXCLUDE_FLAG, TRANSFORM_FLAG, ACTIVE_FLAG
```

**Lesson Learned:** Always confirm which CSV file contains which data. Sort orders are structural (hierarchy file), not relational (mapping file).

---

#### 4. Legacy CSV Format Support
**Files Modified:**
- `ImportDialog.tsx` - Added checkbox UI
- `ProjectDetailsDialog.tsx` - Added state management
- `hierarchy.service.ts` - Added parameter to API call

**Implementation:**
- Checkbox labeled "Legacy/Older CSV Format"
- **Checked by default** (backward compatibility)
- When checked, uses legacy parsing logic

---

#### 5. UI Component: InheritedMappingsView
**Files Created:**
- `src/components/hierarchy-knowledge-base/components/InheritedMappingsView.tsx`

**Files Modified:**
- `SmartHierarchyEditor.tsx` - Added import and usage
- `index.ts` - Added export

**Features:**
- Shows mapping counts (own vs inherited)
- Expandable sections for child hierarchy mappings
- Grouped by precedence
- Collapsible UI using shadcn Collapsible component

---

#### 6. InheritedMappingsView Enhancement (Fixed)
**Files Modified:**
- `src/components/hierarchy-knowledge-base/components/InheritedMappingsView.tsx`

**Issue:** Mappings were not properly traversing up to parent levels. When clicking on a level_2 item, the total count of all descendant mappings was not showing correctly.

**What was fixed:**
- Properly builds a tree structure of ALL descendants (children, grandchildren, etc.)
- Shows **total aggregated count** of mappings from all descendants
- Displays **full hierarchy path** showing which child the mapping is attached to
- Path format: `Parent / Child / Grandchild` (breadcrumb style)
- Two views provided:
  1. **Tree View:** Expandable hierarchy showing "X direct" and "+Y below" counts
  2. **Precedence View:** All mappings grouped by precedence with hierarchy paths

**Key Data Structures Added:**
```typescript
interface HierarchyTreeNode {
  id: string;
  hierarchyName: string;
  ownMappings: SourceMapping[];
  ownCount: number;           // Mappings directly on this node
  descendantCount: number;    // Sum of all children's total counts
  totalCount: number;         // ownCount + descendantCount
  children: HierarchyTreeNode[];
  path: string[];             // Full path from current view root
  depth: number;
}

interface MappingWithPath {
  mapping: SourceMapping;
  sourcePath: string[];       // e.g., ["Revenue", "Product Revenue", "Hardware"]
  sourceHierarchyName: string;
  depth: number;
}
```

**UI Features:**
- Badges show "X direct" (blue) and "+Y below" (gray) for each node
- Blue highlighted box shows "Mappings attached here:" with pin icon
- Precedence Group 1 expanded by default
- Max height with scroll for large hierarchies

---

### Bug Fixes

#### 1. CSV Import 404 Error
**Location:** `hierarchy.service.ts` - `importHierarchyCSV()`
**Problem:** Frontend was calling `/import-hierarchy-csv-old-format` which doesn't exist in backend
**Root Cause:** Frontend had conditional endpoint based on `isLegacyFormat` flag, but backend only has one endpoint that auto-detects format

**Backend endpoints (NestJS):**
- `/import-hierarchy-csv` - Auto-detects format (this is the only endpoint)
- `/import-mapping-csv` - For mapping CSV
- `/import-both-csv` - For both at once

**Fix:** Changed frontend to always use `/import-hierarchy-csv`:
```typescript
// Before (WRONG)
const endpoint = isLegacyFormat
  ? `/import-hierarchy-csv-old-format`  // 404!
  : `/import-hierarchy-csv`;

// After (CORRECT)
const endpoint = `/import-hierarchy-csv`;  // Backend auto-detects
```

**Lesson Learned:** Always verify backend endpoints exist before adding conditional routing in frontend.

---

#### 2. CSV Import Sort Order - Formula Lines at Bottom
**Location:** `old-format-import-v2.service.ts` - `phase2_createHierarchies()`
**Problem:** Formula/calculated rows were always appearing at the bottom instead of their correct sort order position
**Root Cause:** The import was sorting hierarchies only by `depth`, not by `sortOrder` within each level

**Fix:**
```typescript
// Before (WRONG - only sorts by depth)
const sorted = [...hierarchies].sort((a, b) => a.depth - b.depth);

// After (CORRECT - sorts by depth, then sortOrder)
const sorted = [...hierarchies].sort((a, b) => {
  if (a.depth !== b.depth) return a.depth - b.depth;
  return (a.sortOrder || 0) - (b.sortOrder || 0);
});
```

**Lesson Learned:** When importing hierarchies with parent-child relationships, sort by depth first (parents must exist before children can reference them), THEN by sortOrder within each depth level.

---

#### 3. `formula_config.get()` on NoneType
**Location:** `src/hierarchy/service.py` - `export_hierarchy_csv()`
**Problem:** `formula_config` could be `None`, causing AttributeError
**Fix:** Added null safety check:
```python
formula_config = h.get("formula_config") or {}
```

**Lesson Learned:** Always handle `None` cases when accessing nested dictionary fields.

---

### Tool Count History
| Date | Count | Change |
|------|-------|--------|
| Before session | 88 | - |
| After inheritance tools | 91 | +3 |
| After import_mapping_csv | 92 | +1 |

---

## Requirements Clarification Questions

### Questions to Ask Before Implementing

**For CSV/Data Import:**
1. Which file should contain this data - hierarchy or mapping?
2. Is this for legacy format compatibility or new format only?
3. Should this overwrite existing data or merge with it?

**For UI Components:**
1. Should this be a new component or extend an existing one?
2. What's the default state (checked/unchecked, expanded/collapsed)?
3. Where exactly should this appear in the UI hierarchy?

**For Inheritance/Relationships:**
1. Which direction should data flow? (parent→child, child→parent, or both)
2. Should inherited data be editable or read-only?
3. How should conflicts be handled?

**For MCP Tools:**
1. Should this be a read-only query or allow modifications?
2. What's the expected output format (JSON, CSV, formatted text)?
3. Should this sync to backend automatically?

---

## Common Mistakes to Avoid

### 1. Assuming Data Location
**Wrong:** "Sort orders probably go in the mapping file"
**Right:** Ask: "Which file should contain sort orders - hierarchy or mapping?"

### 2. Null Safety
**Wrong:** `config.get("field").get("subfield")`
**Right:** `(config.get("field") or {}).get("subfield")`

### 3. Default States
**Wrong:** Leaving checkbox unchecked by default
**Right:** Ask: "What should the default state be? Is backward compatibility needed?"

### 4. Inheritance Direction
**Wrong:** Assuming bidirectional inheritance
**Right:** Ask: "Should this propagate up, down, or both?"

### 5. MCP Server Behavior
**Note:** MCP servers with stdio transport exit when no client is connected. This is **expected behavior**, not an error.

### 6. Inheritance Display
**Wrong:** Only showing immediate children's mappings
**Right:** Recursively traverse ALL descendants (children, grandchildren, great-grandchildren, etc.) and aggregate total counts

**Wrong:** Just showing "inherited from: Child Name"
**Right:** Show the FULL PATH: "Parent / Child / Grandchild / GreatGrandchild"

**Key Insight:** When user clicks on Level 2, they want to see:
- Total count of ALL mappings in ALL descendants (not just immediate children)
- The exact hierarchy path showing where each mapping is attached

### 7. Parent-Child ID Mismatch
**Wrong:** Comparing `h.parentId === hierarchy.id` (UUID)
**Right:** Comparing `h.parentId === hierarchy.hierarchyId` (slug)

**Reason:** The backend's `toSmartHierarchyMaster()` converts `parentId` from UUID to the parent's `hierarchyId` (slug like "VOLUME_1"). So children's `parentId` field contains the parent's hierarchyId, NOT the UUID.

**Data structure:**
```typescript
// When you load "Volume" hierarchy:
hierarchy.id = "abc123-uuid..."        // UUID
hierarchy.hierarchyId = "VOLUME_1"     // Slug

// Its children have:
child.parentId = "VOLUME_1"            // Parent's hierarchyId (NOT UUID!)
```

---

## Architecture Notes

### File Structure
```
C:\Users\telha\Databridge_AI\
├── src/
│   ├── hierarchy/           # Hierarchy KB module
│   │   ├── types.py         # Pydantic models
│   │   ├── service.py       # Business logic
│   │   └── mcp_tools.py     # MCP tool definitions
│   ├── templates/           # Templates, Skills, Knowledge Base
│   └── server.py            # Main MCP server
├── templates/               # Financial statement templates
├── skills/                  # AI expertise definitions
├── knowledge_base/          # Client-specific knowledge
└── current application/     # Frontend React app
    └── extracted_app/HIERARCHY_BUILDER_APP/dataamp-ai/
        └── src/
            ├── components/  # React components
            ├── services/    # API services
            └── types/       # TypeScript types
```

### Service Ports
| Service | Port | Notes |
|---------|------|-------|
| Frontend (Vite) | 5173 | Hot reload enabled |
| NestJS Backend | 3001 | API server |
| MCP Server | stdio | Exits without client (normal) |

---

#### 7. Help Tooltip System (In-App Documentation)
**Files Created:**
- `src/components/ui/help-tooltip.tsx` - HelpTooltip, HelpLabel, HelpSection components
- `src/lib/help-content.ts` - All help topic content and documentation

**Files Modified:**
- `SmartHierarchyEditor.tsx` - Added tooltips to tabs, flags section, parent/sort order fields
- `MappingArrayEditor.tsx` - Added tooltips to Source Mappings, Database Connection, Precedence Group, Mapping Flags
- `InheritedMappingsView.tsx` - Added tooltips to Mapping Summary, Child Hierarchy Mappings, Precedence Groups
- `HierarchyKnowledgeBaseView.tsx` - Added tooltips to main title, Projects, Import/Export, Manage Formulas buttons

**Features:**
- **HelpTooltip Component**: Displays help icon that shows tooltip on hover and opens detailed documentation dialog on click
- **HelpLabel Component**: Form label with integrated help tooltip
- **HelpSection Component**: Section header with help tooltip
- Uses `react-markdown` for rendering detailed help content

**Help Topics Defined:**
| Topic ID | Title | Description |
|----------|-------|-------------|
| `hierarchyTree` | Hierarchy Tree | Visual tree structure, navigation, visual indicators |
| `hierarchyLevels` | Hierarchy Levels | Up to 15 nested levels (LEVEL_1 through LEVEL_15) |
| `parentChild` | Parent-Child Relationships | Creating tree structures, inheritance rules |
| `sortOrder` | Sort Order | Display sequence control within levels |
| `sourceMapping` | Source Mappings | Connecting hierarchies to data sources |
| `mappingInheritance` | Mapping Inheritance | Mappings flow from child → parent |
| `precedenceGroups` | Precedence Groups | Segregating mappings by context |
| `mappingEditor` | Mapping Editor | Add, edit, remove source mappings |
| `hierarchyFlags` | Hierarchy Flags | Boolean flags (active, include, exclude, etc.) |
| `formulaBuilder` | Formula Builder | Creating calculations from other hierarchies |
| `csvImport` | CSV Import | Two-file system for importing |
| `csvExport` | CSV Export | Exporting hierarchies and mappings |
| `mappingSummary` | Mapping Summary | Overview of own vs inherited mappings |
| `hierarchyEditor` | Hierarchy Editor | Edit panel tabs and functionality |
| `projectManagement` | Project Management | Creating and organizing projects |

**Dependencies Added:**
- `react-markdown` - For rendering markdown help content in dialogs

---

## Pending/Future Work

### From Plan File (pure-imagining-crescent.md)
- [ ] Templates system (5 MCP tools)
- [ ] Skills system MCP integration (3 MCP tools)
- [ ] Knowledge Base system (4 MCP tools)
- [ ] Create initial financial statement templates

### Known Issues
- Pydantic deprecation warnings for class-based config (use ConfigDict)
- dateutil deprecation warning for utcfromtimestamp()

---

## Version Info
- **FastMCP:** 2.14.3 (update available: 2.14.4)
- **Python:** 3.12
- **Tool Count:** 92

---

*Last Updated: January 23, 2026*
