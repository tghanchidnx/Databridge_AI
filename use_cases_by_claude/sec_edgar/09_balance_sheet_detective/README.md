# Use Case 9: Balance Sheet Detective

## The Story

A friend asks you: "Is Apple rich or broke?" You say "Rich, obviously!" But they
push back: "Sure, they make a lot of money. But what if they OWE even more than
they OWN?"

That's actually a great question! To answer it, you need to look at Apple's
**Balance Sheet** - a financial snapshot showing:
- **Assets** = what Apple OWNS (cash, buildings, patents)
- **Liabilities** = what Apple OWES (loans, bills)
- **Equity** = Assets minus Liabilities (the company's actual "net worth")

This is the **accounting equation**: **A = L + E** (Assets = Liabilities + Equity)

Let's investigate!

---

## What You Will Learn

- How to read a **Balance Sheet** (what companies own vs. owe)
- How to use **smart recommendations** to get AI-powered suggestions
- How to create a project **from a template** (pre-built structure)
- How to verify the **accounting equation** (A = L + E)
- The difference between **current** and **non-current** items

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `load_csv` | Load Apple's balance sheet |
| `profile_data` | Get a health report on the balance sheet data |
| `get_smart_recommendations` | Get AI-powered suggestions for how to work with this data |
| `create_project_from_template` | Create a hierarchy from a pre-built template |
| `get_hierarchy_tree` | View the template's tree structure |
| `import_flexible_hierarchy` | Import balance sheet data into a hierarchy |

**Components used:** Books (hierarchy), Librarian (templates, project from template), Smart Recommendations

---

## Step-by-Step Instructions

### Step 1: Load the Balance Sheet

```
Load the CSV file at data/apple_balance_sheet.csv and call it "apple_bs"
```

**What happens:** Apple's balance sheet is loaded with about 12 rows showing
assets, liabilities, and equity accounts.

### Step 2: Profile the Data

```
Profile the data quality of the apple_bs balance sheet data
```

**What happens:** You'll see the range of amounts - from millions (Inventories)
to hundreds of billions (Total Assets).

### Step 3: Get Smart Recommendations

This is where AI helps us figure out the best way to work with this data:

```
Get smart recommendations for the file data/apple_balance_sheet.csv with user intent "Build a balance sheet hierarchy for Apple"
```

**What happens:** The Smart Recommendation Engine analyzes the CSV and suggests:
- Which **import tier** to use (probably Tier 1 since it's simple)
- Which **skill** would help (Financial Analyst)
- Which **template** matches best (Standard Balance Sheet)
- Industry-specific recommendations

### Step 4: Create a Project from a Template

Let's use the Standard Balance Sheet template as a starting point:

```
Create a new project from the standard_bs template called "Apple Balance Sheet FY2024"
```

**What happens:** DataBridge creates a full balance sheet hierarchy based on the
template, with pre-built categories for Assets, Liabilities, and Equity.

### Step 5: View the Template Tree

```
Show me the hierarchy tree for the project you just created
```

**What happens:** You'll see a professional balance sheet structure:

```
Apple Balance Sheet FY2024
├── Assets
│   ├── Current Assets
│   │   ├── Cash & Equivalents
│   │   ├── Accounts Receivable
│   │   └── Inventory
│   └── Non-Current Assets
│       ├── Property, Plant & Equipment
│       └── Intangible Assets
├── Liabilities
│   ├── Current Liabilities
│   │   ├── Accounts Payable
│   │   └── Short-Term Debt
│   └── Non-Current Liabilities
│       └── Long-Term Debt
└── Stockholders' Equity
    ├── Common Stock
    └── Retained Earnings
```

### Step 6: Import the Balance Sheet Data

Now import Apple's actual data using the Tier 1 format:

```
Create a hierarchy project called "Apple BS Imported" with description "Apple Balance Sheet from Tier 1 import"
```

Then import:

```
Import the flexible hierarchy from data/apple_balance_sheet_tier1.csv into the new project
```

### Step 7: View Your Imported Tree

```
Show me the hierarchy tree for the Apple BS Imported project
```

---

## What Did We Find?

### Apple's Balance Sheet (FY2024)

| Category | Account | Amount |
|----------|---------|--------|
| **ASSETS** | | |
| Current Asset | Cash & Cash Equivalents | $29.9B |
| Current Asset | Accounts Receivable | $66.2B |
| Current Asset | Inventories | $7.3B |
| Current Asset | **Total Current Assets** | **$153.0B** |
| Non-Current Asset | Property, Plant & Equipment | $44.9B |
| | **Total Assets** | **$365.0B** |
| **LIABILITIES** | | |
| Current Liability | Accounts Payable | $69.0B |
| Current Liability | **Total Current Liabilities** | **$176.4B** |
| Non-Current Liability | Long-Term Debt | $85.8B |
| | **Total Liabilities** | **$308.0B** |
| **EQUITY** | | |
| Equity | Retained Earnings | -$19.2B |
| | **Total Stockholders' Equity** | **$57.0B** |

### The Accounting Equation Check

**A = L + E**

$365.0B = $308.0B + $57.0B = **$365.0B**

It balances! (It always should - that's why it's called a "balance" sheet.)

### Key Insights

1. **Apple has $29.9B in CASH** just sitting in the bank! That's enough to buy
   most countries' entire GDP.

2. **Negative Retained Earnings (-$19.2B):** This looks scary, but it's actually
   because Apple spent MORE money buying back its own stock than it kept as
   retained profits. Apple returns money to shareholders aggressively.

3. **More Liabilities than Equity:** Apple owes $308B but equity is only $57B.
   This is normal for Apple - they use debt strategically because interest rates
   on their loans are low, and they generate so much cash they can easily pay it.

4. **Accounts Receivable ($66.2B):** People owe Apple $66 billion! These are
   mostly payments from carriers (AT&T, Verizon) for iPhones sold.

5. **Low Inventory ($7.3B):** Apple is famous for lean manufacturing. $7.3B
   in inventory is tiny compared to $416B in annual revenue.

---

## Understanding Current vs. Non-Current

| Type | Meaning | Time Frame | Examples |
|------|---------|------------|---------|
| **Current Asset** | Can be turned into cash quickly | Within 1 year | Cash, receivables, inventory |
| **Non-Current Asset** | Long-term investments | More than 1 year | Buildings, equipment, patents |
| **Current Liability** | Must be paid soon | Within 1 year | Bills, short-term loans |
| **Non-Current Liability** | Long-term debt | More than 1 year | Bonds, long-term loans |

---

## Bonus Challenge

Calculate Apple's **current ratio** (a measure of short-term financial health):

```
What is Apple's current ratio? Calculate Total Current Assets divided by Total Current Liabilities.
```

**Answer:**
- Current Ratio = $153.0B / $176.4B = **0.87**
- A ratio below 1.0 means current liabilities exceed current assets
- This is actually fine for Apple because they generate so much cash from operations
  (over $100B per year) that they can easily cover their short-term obligations

---

## What's Next?

You cracked the balance sheet mystery!
Now try [Use Case 10: Full Financial Pipeline](../10_full_financial_pipeline/README.md)
to build a complete data pipeline from start to finish - like a real data engineer!
