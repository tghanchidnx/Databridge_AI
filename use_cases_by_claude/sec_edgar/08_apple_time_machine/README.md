# Use Case 8: Apple Time Machine

## The Story

You've been hired as a **financial analyst** at a big investment firm. Your boss says:
"I need to know - is Apple growing or shrinking? Look at the last few years and
tell me what's happening."

To answer this, we need to **compare Apple's income statement across multiple years**.
Did revenue go up or down? Are expenses growing faster than revenue? Is Apple making
more money each year?

Time to fire up the time machine and look at the data!

---

## What You Will Learn

- How to **compare the same company across different years**
- How to use **hash comparison** for year-over-year analysis
- How to **detect schema drift** (changes in data structure over time)
- How to spot **growth trends** and **warning signs** in financial data
- How to profile multi-year data for patterns

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `load_csv` | Load income statement files for different years |
| `compare_hashes` | Compare FY2023 vs FY2024 to find what changed |
| `get_conflict_details` | See the dollar differences year-over-year |
| `detect_schema_drift` | Check if the data format changed between years |
| `profile_data` | Profile the multi-year data |

**Components used:** Books (comparison, profiling, schema drift detection)

---

## Step-by-Step Instructions

### Step 1: Load FY2023 Data

```
Load the CSV file at data/apple_income_2023.csv and call it "apple_2023"
```

**What happens:** Apple's FY2023 income statement is loaded (fiscal year ending
September 2023).

### Step 2: Load FY2024 Data

```
Load the CSV file at data/apple_income_2024.csv and call it "apple_2024"
```

**What happens:** Apple's FY2024 income statement is loaded (fiscal year ending
September 2024).

### Step 3: Compare Year-Over-Year

```
Compare the apple_2023 and apple_2024 datasets using account_name as the key column
```

**What happens:** DataBridge compares the two years and finds:
- **Matches** - Same accounts appear in both years (all of them should match!)
- **Conflicts** - The dollar amounts changed between years (this is what we want to see)
- **Orphans** - Any accounts that appeared or disappeared (hopefully none)

### Step 4: See What Changed

```
Show me the conflict details from the comparison. For each account, show how much it changed from 2023 to 2024.
```

**What happens:** You'll see a table showing how each account changed. For example:
- Revenue: $383.3B (2023) → $391.0B (2024) = **+$7.7B growth!**
- Net Income: $97.0B (2023) → $93.7B (2024) = **-$3.3B decline!** (Wait, why?)

### Step 5: Check for Schema Drift

Let's make sure the data format is consistent between years:

```
Detect schema drift between the apple_2023 and apple_2024 datasets
```

**What happens:** DataBridge checks if the columns, data types, or structure changed
between years. For well-formatted SEC data, there should be minimal drift.

### Step 6: Load the Multi-Year View

Now let's look at ALL four years together:

```
Load the CSV file at data/apple_multiyear.csv and call it "apple_trend"
```

```
Profile the data quality of the apple_trend data
```

**What happens:** You'll see Apple's numbers across FY2022-FY2025, making it
easy to spot the trends.

---

## What Did We Find?

### Apple's Income Statement Over Time

| Account | FY2022 | FY2023 | FY2024 | FY2025 | Trend |
|---------|--------|--------|--------|--------|-------|
| Revenue | $394.3B | $383.3B | $391.0B | $416.2B | Down then UP |
| COGS | $223.5B | $214.1B | $210.4B | $221.0B | Down then UP |
| Gross Profit | $170.8B | $169.1B | $180.7B | $195.2B | Mostly UP |
| R&D | $26.3B | $29.9B | $31.4B | $34.6B | Always UP |
| SG&A | $25.1B | $24.9B | $26.1B | $27.6B | Mostly UP |
| Operating Income | $119.4B | $114.3B | $123.2B | $133.1B | Down then UP |
| Net Income | $99.8B | $97.0B | $93.7B | $112.0B | Down then UP |

### Key Insights

1. **The FY2023 Dip:** Revenue dropped from $394B to $383B! This was caused by
   a global slowdown in consumer electronics spending. Apple's first revenue decline
   in years.

2. **The Recovery:** By FY2025, revenue bounced back to $416B - a new all-time high!
   iPhone 16 and services growth drove the recovery.

3. **R&D Never Stops:** Apple increased R&D spending EVERY year, from $26.3B to $34.6B.
   Even when revenue fell, they kept investing in the future. That's how great
   companies stay great.

4. **Net Income Surprise:** Net Income dropped from $99.8B (FY2022) to $93.7B (FY2024)
   before surging to $112.0B in FY2025. The FY2024 dip was partly due to a one-time
   tax charge.

5. **Costs Went Down Then Up:** COGS dropped from $223.5B to $210.4B (FY2024) as
   Apple made products more efficiently, then rose to $221.0B as volume increased.

### Growth Rates (FY2024 to FY2025)

| Account | Change | Growth Rate |
|---------|--------|-------------|
| Revenue | +$25.1B | +6.4% |
| Gross Profit | +$14.5B | +8.0% |
| R&D | +$3.2B | +10.2% |
| Operating Income | +$9.8B | +8.0% |
| Net Income | +$18.3B | +19.5% |

Net Income grew **19.5%** while Revenue only grew 6.4%! That means Apple got more
efficient - more of each dollar in revenue turned into profit.

---

## Bonus Challenge

Calculate Apple's **gross margin** for each year and see the trend:

```
Using the apple_trend data, calculate the gross margin (Gross Profit / Revenue) for each year from FY2022 to FY2025
```

**Expected answers:**
- FY2022: $170.8B / $394.3B = **43.3%**
- FY2023: $169.1B / $383.3B = **44.1%**
- FY2024: $180.7B / $391.0B = **46.2%**
- FY2025: $195.2B / $416.2B = **46.9%**

The gross margin improved every year! Even when revenue dipped, Apple was getting
better at controlling costs. That's a sign of strong management.

---

## What's Next?

You just tracked Apple's performance through time like a real analyst!
Now try [Use Case 9: Balance Sheet Detective](../09_balance_sheet_detective/README.md)
to investigate what Apple OWNS versus what they OWE.
