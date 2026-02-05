# Use Case 6: Apple's Money Tree

## The Story

You're an intern at Apple's finance department. Your boss the CFO says: "I don't want
to look at a flat list of numbers. I want a **tree** - organize our accounts so I can
see the big picture and drill down into the details!"

Right now, Apple's accounts are just a flat list in a spreadsheet. Your job is to turn
them into a **hierarchy** - a tree structure where related accounts are grouped together.
Think of it like organizing your school folders: Math homework goes under Math, which
goes under School.

---

## What You Will Learn

- How to **create a hierarchy project** in DataBridge
- How to use **Tier 1 import** (the simplest way to build a hierarchy)
- How to **view the hierarchy tree** to see your organized accounts
- How to **browse financial templates** to compare your work to a standard P&L
- The difference between a **flat list** and a **tree structure**

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `load_csv` | Load the Tier 1 CSV file |
| `detect_hierarchy_format` | Figure out what format the CSV is in |
| `create_hierarchy_project` | Create a new project to hold our hierarchy |
| `import_flexible_hierarchy` | Import the CSV and build the tree automatically |
| `get_hierarchy_tree` | See the tree structure we built |
| `list_financial_templates` | Browse pre-built hierarchy templates |
| `get_template_details` | Look at a specific template |

**Components used:** Books (hierarchy builder), Librarian (templates)

---

## Step-by-Step Instructions

### Step 1: Look at the Tier 1 CSV

First, let's see what our simple input looks like:

```
Load the CSV file at data/apple_income_statement_tier1.csv
```

**What happens:** You'll see a tiny 2-column CSV with just `source_value` and
`group_name`. This is the **Tier 1 format** - the simplest way to create a hierarchy.
Each account code (like 4000) is mapped to a group (like "Revenue").

### Step 2: Detect the Format

Let DataBridge figure out what kind of CSV this is:

```
Read the file data/apple_income_statement_tier1.csv and detect the hierarchy format
```

**What happens:** DataBridge analyzes the CSV and tells you it's **Tier 1** -
the simplest format with just 2 columns.

### Step 3: Create a Hierarchy Project

Now let's create a project to hold our hierarchy:

```
Create a hierarchy project called "Apple P&L" with description "Apple Inc. Income Statement Hierarchy from SEC EDGAR data"
```

**What happens:** DataBridge creates a new project. Remember the **project ID** it gives
you - you'll need it for the next step!

### Step 4: Import the Hierarchy

Now import the Tier 1 CSV into your project. Replace `YOUR_PROJECT_ID` with the
actual ID from Step 3:

```
Import the flexible hierarchy from data/apple_income_statement_tier1.csv into project YOUR_PROJECT_ID
```

**What happens:** DataBridge reads the simple 2-column CSV and automatically builds
a tree structure! It creates parent nodes for each group and links the account codes
underneath.

### Step 5: View Your Tree

Let's see what we built:

```
Show me the hierarchy tree for project YOUR_PROJECT_ID
```

**What happens:** You'll see your accounts organized as a tree:

```
Apple P&L
├── Revenue
│   └── 4000
├── Cost of Goods Sold
│   └── 5000
├── Gross Profit
│   └── 5500
├── Operating Expenses
│   ├── 6100 (R&D)
│   └── 6200 (SG&A)
├── Operating Income
│   └── 7000
├── Income Tax
│   └── 8000
└── Net Income
    └── 9000
```

### Step 6: Browse Financial Templates

Now let's see how the pros organize a P&L:

```
List all available financial templates
```

**What happens:** DataBridge shows you pre-built templates for different industries -
Standard P&L, Oil & Gas LOS, SaaS P&L, Manufacturing P&L, and more!

### Step 7: Compare to a Standard P&L Template

Look at the standard template to see how a typical P&L is structured:

```
Show me the details of the standard_pl template
```

**What happens:** You'll see a professional P&L template with more detail than our
simple tree - it has sub-categories, formulas, and a deeper structure.

---

## What Did We Find?

### Our Tree vs. the Template

| Our Apple P&L | Standard P&L Template |
|---------------|----------------------|
| Simple 2-level tree | Multi-level with sub-groups |
| 8 accounts | Many more categories |
| No formulas | SUM/SUBTRACT formulas |
| Good for a quick view | Good for detailed reporting |

### Key Insight

We started with a **flat list** (just rows in a spreadsheet) and turned it into a
**tree structure** in seconds! The Tier 1 import is perfect for quick groupings.
For more detail, you'd use Tier 2 or Tier 3 formats, or start from a template.

---

## Understanding Hierarchy Tiers

| Tier | Columns | When to Use |
|------|---------|-------------|
| **Tier 1** | 2 (source_value, group_name) | Quick grouping, non-technical users |
| **Tier 2** | 5-7 (with parent names, sort order) | Basic parent-child hierarchies |
| **Tier 3** | 10-12 (explicit IDs, source info) | Full control |
| **Tier 4** | 28+ (all flags, formulas) | Enterprise / full detail |

We used Tier 1 because it's the easiest - just two columns!

---

## Bonus Challenge

Try creating a hierarchy from a template instead:

```
Create a new project from the standard_pl template called "Standard Apple P&L"
```

Then compare the template's tree to the one you built:

```
Show me the hierarchy tree for the new project
```

How is the template tree different from yours? (Hint: it has more levels and
includes formula calculations!)

---

## What's Next?

Awesome work! You turned a flat list into an organized tree.
Now try [Use Case 7: Apple vs Microsoft Showdown](../07_apple_vs_microsoft/README.md)
to compare two tech giants' finances!
