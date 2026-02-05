# Use Case 7: Apple vs Microsoft Showdown

## The Story

You're a **tech reporter** writing an article: "Apple vs Microsoft - Who's the
Bigger Money Machine?" Both companies are worth over $3 TRILLION dollars, but
their finances are very different.

Apple makes hardware (iPhones, Macs) and services (App Store, iCloud).
Microsoft makes software (Windows, Office) and cloud services (Azure).

Let's compare their real financial numbers and see what we discover!

---

## What You Will Learn

- How to **load two datasets** and compare them
- How to use **hash comparison** to find matching and different accounts
- How to find **orphans** (accounts that exist in one company but not the other)
- How to find **conflicts** (same account name, different amounts)
- How to use **fuzzy matching** to match accounts with slightly different names

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `load_csv` | Load the income statement files |
| `compare_hashes` | Compare two datasets to find matches, orphans, and conflicts |
| `get_orphan_details` | See which accounts exist in only one company |
| `get_conflict_details` | See where amounts differ for matching accounts |
| `fuzzy_match_columns` | Match account names that are spelled slightly differently |

**Components used:** Books (data reconciliation), Researcher concept (fuzzy matching)

---

## Step-by-Step Instructions

### Step 1: Load Apple's Numbers

```
Load the CSV file at data/apple_income_statement.csv and call it "apple"
```

**What happens:** Apple's FY2025 income statement is loaded - about 8 rows with
revenue of $416 billion.

### Step 2: Load Microsoft's Numbers

```
Load the CSV file at data/microsoft_income_statement.csv and call it "microsoft"
```

**What happens:** Microsoft's FY2024 income statement is loaded - about 9 rows with
revenue of $245 billion.

### Step 3: Compare the Two Companies

Now let's see how they stack up:

```
Compare the apple and microsoft datasets using account_name as the key column
```

**What happens:** DataBridge hashes both datasets and finds:
- **Matches** - Accounts that exist in BOTH companies (like Revenue, Net Income)
- **Orphans** - Accounts that exist in only ONE company
- **Conflicts** - Same account name but different dollar amounts

### Step 4: Look at the Orphans

```
Show me the orphan details from the comparison
```

**What happens:** You'll discover accounts that don't match between companies!

For example:
- Apple has **"Selling, General & Administrative"** (one combined line)
- Microsoft splits this into **"Sales & Marketing"** AND **"General & Administrative"**
- These show up as orphans because the names don't match exactly

### Step 5: Look at the Conflicts

```
Show me the conflict details from the comparison
```

**What happens:** For accounts that DO match (like Revenue, Net Income), you'll see
the dollar differences. Revenue conflict: Apple has $416B vs Microsoft's $245B!

### Step 6: Try Fuzzy Matching

The orphans happened because Apple and Microsoft use slightly different names.
Let's try fuzzy matching to find the connections:

```
Fuzzy match the account_name column between the apple and microsoft datasets with a threshold of 60
```

**What happens:** DataBridge uses smart string matching to find similar names.
It might match:
- "Selling, General & Administrative" with "General & Administrative" (similar words)
- "Cost of Goods Sold" with "Cost of Revenue" (same concept, different label)

---

## What Did We Find?

### The Big Numbers

| Account | Apple (FY2025) | Microsoft (FY2024) | Difference |
|---------|---------------|-------------------|------------|
| Revenue | $416.2B | $245.1B | Apple +$171.1B |
| Cost of Goods/Revenue | $221.0B | $74.1B | Apple +$146.9B |
| Gross Profit | $195.2B | $171.0B | Apple +$24.2B |
| R&D | $34.6B | $29.5B | Apple +$5.1B |
| Operating Income | $133.1B | $109.4B | Apple +$23.7B |
| Net Income | $112.0B | $88.1B | Apple +$23.9B |

### Key Insights

1. **Apple has higher revenue** ($416B vs $245B) because hardware sales are huge
2. **Microsoft has MUCH higher gross margins** - Microsoft keeps 70% of revenue as
   gross profit, Apple keeps 47%. Software is cheaper to "make" than hardware!
3. **Both spend billions on R&D** - Apple $34.6B, Microsoft $29.5B
4. **Different naming conventions** - Companies use different names for similar accounts,
   which is why fuzzy matching is so important in real-world data work

### Why Names Don't Match

This is a REAL problem that financial analysts deal with every day:
- Apple says **"Cost of Goods Sold"** → Microsoft says **"Cost of Revenue"** (same thing!)
- Apple combines **"Selling, General & Administrative"** into one line
- Microsoft splits it into **"Sales & Marketing"** + **"General & Administrative"**

DataBridge's fuzzy matching helps bridge these naming differences automatically.

---

## Bonus Challenge

Load the side-by-side comparison file and profile it:

```
Load the CSV file at data/apple_vs_microsoft_comparison.csv
```

```
Profile the data quality of the Apple vs Microsoft comparison data
```

Can you calculate each company's **profit margin** (Net Income / Revenue)?
- Apple: $112.0B / $416.2B = **26.9%**
- Microsoft: $88.1B / $245.1B = **36.0%**

Microsoft has a higher profit margin! Even though Apple makes more total dollars,
Microsoft keeps a bigger *percentage* of each dollar earned.

---

## What's Next?

You just compared two of the world's biggest companies!
Now try [Use Case 8: Apple Time Machine](../08_apple_time_machine/README.md) to
travel through time and see how Apple's numbers changed over the years.
