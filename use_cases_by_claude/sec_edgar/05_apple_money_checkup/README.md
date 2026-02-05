# Use Case 5: Apple's Money Checkup

## The Story

You just found out that **Apple makes $416 BILLION dollars a year**. That's
$416,000,000,000 - a number so big it's hard to even imagine! But where does all
that money come from? And where does it go?

Lucky for us, the U.S. government makes every big company share their financial
numbers on a website called **SEC EDGAR**. We downloaded Apple's real numbers
and turned them into a CSV file. Let's check it out!

---

## What You Will Learn

- How to load **real financial data** from a Fortune 500 company
- How to **profile** an income statement to understand what each number means
- What Revenue, COGS, Operating Expenses, and Net Income actually are
- How to read numbers in the **billions** (hint: count the zeros!)

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `load_csv` | Reads the Apple income statement CSV into memory |
| `profile_data` | Scans every column and gives us a data quality report |

**Components used:** Books (data engine)

---

## Step-by-Step Instructions

### Step 1: Load Apple's Income Statement

Copy and paste this into Claude Desktop:

```
Load the CSV file at data/apple_income_statement.csv
```

**What happens:** DataBridge reads Apple's income statement with about 8 rows -
one for each line on Apple's Profit & Loss report. You should see columns like:
- `account_code` - A number code for each account (like 4000 for Revenue)
- `account_name` - The name (Revenue, Cost of Goods Sold, etc.)
- `account_type` - What category it belongs to
- `amount` - The actual dollar amount (these are BIG numbers!)
- `fiscal_year` - 2025 (Apple's fiscal year ends in September)

### Step 2: Profile the Data

Now copy and paste this:

```
Profile the data quality of the Apple income statement data
```

**What happens:** DataBridge scans every column and creates a health report.

### Step 3: Ask About the Numbers

Now let's understand what we're looking at:

```
Look at the Apple income statement data. What is Apple's revenue, and how much
profit did they keep after paying all their expenses? Show me the key numbers.
```

---

## What Did We Find?

Here's what Apple's income statement looks like (FY2025 - ending September 2025):

| Account | Amount | What It Means |
|---------|--------|--------------|
| **Revenue** | $416.2 billion | Total money from selling iPhones, Macs, iPads, Services |
| **Cost of Goods Sold** | $221.0 billion | Cost to manufacture all those products |
| **Gross Profit** | $195.2 billion | Revenue minus COGS (money left after making products) |
| **Research & Development** | $34.6 billion | Spent on inventing new products |
| **Selling, General & Admin** | $27.6 billion | Marketing, salaries, offices, lawyers |
| **Operating Income** | $133.1 billion | Profit from running the business |
| **Income Tax** | $22.1 billion | Taxes paid to the government |
| **Net Income** | $112.0 billion | Final profit after EVERYTHING |

### Mind-Blowing Facts

- Apple makes about **$1.14 billion PER DAY** in revenue
- After ALL expenses, Apple keeps about **$307 million PER DAY** in profit
- Apple spends **$94.8 million PER DAY** on research (that's how you get new iPhones!)
- Apple's **profit margin** is about 27% - for every dollar they earn, they keep 27 cents

### The Profile Report

The profile should show you:
- **No missing values** - SEC filings are carefully audited, so the data is clean!
- **amount column** - All numbers are large (billions range)
- **account_type column** - Mix of Revenue, COGS, Operating Expense, Tax, Net Income
- **8 unique accounts** - One row per income statement line item

---

## Understanding an Income Statement

Think of it like a waterfall - money flows down from the top:

```
Revenue (all money coming in)              $416.2B
  minus Cost of Goods Sold                 -$221.0B
  ─────────────────────────────────────────
= Gross Profit                             $195.2B
  minus R&D Expenses                       -$34.6B
  minus SG&A Expenses                      -$27.6B
  ─────────────────────────────────────────
= Operating Income                         $133.1B
  minus Income Tax                         -$22.1B
  ─────────────────────────────────────────
= Net Income (final profit)                $112.0B
```

Each step removes more expenses until you get to the **bottom line** (Net Income).
That's why people call profit "the bottom line" - it's literally at the bottom!

---

## Bonus Challenge

Try this on your own:

```
What percentage of Apple's revenue goes to Research & Development?
And what percentage goes to taxes?
```

**Answers:**
- **R&D:** $34.6B / $416.2B = about **8.3%** of revenue goes to R&D
- **Taxes:** $22.1B / $416.2B = about **5.3%** of revenue goes to taxes

---

## What's Next?

Great job! You just analyzed a real Fortune 500 company's finances!
Now try [Use Case 6: Apple's Money Tree](../06_apple_money_tree/README.md) to
organize these numbers into a hierarchy - just like a CFO would!
