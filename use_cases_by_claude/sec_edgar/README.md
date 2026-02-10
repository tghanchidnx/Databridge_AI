# SEC EDGAR Tutorials - Real Wall Street Data

Welcome to the **advanced tutorials**! These 7 use cases use **real financial data**
from Apple and Microsoft - the same numbers that Wall Street analysts look at every day.

The data comes from the **SEC EDGAR** database, which is the U.S. government website
where every public company (Apple, Microsoft, Tesla, etc.) must report their finances.
It's completely free and public!

---

## What is SEC EDGAR?

**SEC** = Securities and Exchange Commission (the U.S. government agency that watches over
the stock market)

**EDGAR** = Electronic Data Gathering, Analysis, and Retrieval (their database of company filings)

Every public company must file a **10-K report** once a year. This report tells everyone:
- How much money the company made (**Revenue**)
- How much they spent (**Expenses**)
- How much profit they kept (**Net Income**)
- What they own (**Assets**) and what they owe (**Liabilities**)

We use SEC's free API to download these real numbers and turn them into CSV files
that DataBridge can analyze.

---

## Before You Start (One-Time Setup)

### Step 1: Generate the financial data CSV files

Open your command prompt and run:

```
cd T:\Users\telha\Databridge_AI_Source
python use_cases_by_claude/sec_edgar/download_sec_data.py
```

This creates 10 CSV files with real financial data from Apple and Microsoft.
The script tries to fetch fresh data from SEC.GOV, but also has bundled data
so it works even without internet!

### Step 2: Copy files to the data folder

```
python use_cases_by_claude/sec_edgar/setup.py
```

### Step 3: Make sure the MCP server is running

```
cd T:\Users\telha\Databridge_AI_Source
python -m src.server
```

Leave this window open.

### Step 4: Open Claude Desktop

Open the Claude Desktop app. You're ready to go!

---

## The 7 Tutorials

| # | Tutorial | What You Learn | Difficulty |
|---|----------|---------------|------------|
| 5 | [Apple's Money Checkup](05_apple_money_checkup/README.md) | Load real financial data, profile Apple's income | Easy |
| 6 | [Apple's Money Tree](06_apple_money_tree/README.md) | Build a P&L hierarchy, use templates | Medium |
| 7 | [Apple vs Microsoft Showdown](07_apple_vs_microsoft/README.md) | Compare two companies, find differences | Medium |
| 8 | [Apple Time Machine](08_apple_time_machine/README.md) | Multi-year comparison, detect changes over time | Medium |
| 9 | [Balance Sheet Detective](09_balance_sheet_detective/README.md) | Assets vs liabilities, smart recommendations | Medium-Hard |
| 10 | [Full Financial Pipeline](10_full_financial_pipeline/README.md) | End-to-end pipeline with workflow recording | Hard |
| 11 | [Wall Street Analyst](11_wall_street_analyst/README.md) | Capstone: skills, recommendations, full analysis | Hard |

**Start with Tutorial 5** and work your way up! If you haven't done Tutorials 1-4 yet,
go back to the [beginner tutorials](../README.md) first.

---

## What Companies Are We Analyzing?

### Apple Inc. (AAPL)
- **Revenue:** $416 billion per year (FY2025)
- **Net Income:** $112 billion profit
- That's about **$1.14 billion per day** in revenue!
- Apple's fiscal year ends in **September** (not December)

### Microsoft Corp. (MSFT)
- **Revenue:** $245 billion per year (FY2024)
- **Net Income:** $88 billion profit
- Microsoft's fiscal year ends in **June** (not December)

### Fun Fact
If you stacked $416 billion in $100 bills, the pile would be about **282 miles high** -
that's higher than the International Space Station orbits Earth!

---

## Files Used in These Tutorials

| File | What's In It | Used In |
|------|-------------|---------|
| `apple_income_statement.csv` | Apple's income (FY2025) | UC5, UC6, UC10, UC11 |
| `apple_income_statement_tier1.csv` | Simple 2-column format | UC6 |
| `microsoft_income_statement.csv` | Microsoft's income (FY2024) | UC7, UC11 |
| `apple_vs_microsoft_comparison.csv` | Side-by-side comparison | UC7 |
| `apple_multiyear.csv` | Apple FY2022-FY2025 | UC8 |
| `apple_income_2023.csv` | Apple FY2023 only | UC8 |
| `apple_income_2024.csv` | Apple FY2024 only | UC8 |
| `apple_balance_sheet.csv` | Apple's assets & liabilities | UC9, UC10, UC11 |
| `apple_balance_sheet_tier1.csv` | Simple 2-column format | UC9 |
| `apple_full_chart_of_accounts.csv` | Everything combined | UC10, UC11 |

---

## Glossary (Financial Terms)

| Term | What It Means | Example |
|------|-------------- |---------|
| **Revenue** | Money coming IN from selling products/services | Apple sells iPhones = revenue |
| **COGS** | Cost Of Goods Sold - what it costs to MAKE the product | Parts + factory costs for iPhones |
| **Gross Profit** | Revenue minus COGS | Money left after paying for products |
| **Operating Expenses** | Costs of RUNNING the business (not making products) | Salaries, rent, advertising |
| **R&D** | Research & Development - spending on new inventions | Designing the next iPhone |
| **SG&A** | Selling, General & Administrative | Marketing, lawyers, office supplies |
| **Operating Income** | Gross Profit minus Operating Expenses | Profit from actual business |
| **Net Income** | The FINAL profit after everything (including taxes) | What's left at the end |
| **Assets** | Things a company OWNS | Cash, buildings, patents |
| **Liabilities** | Things a company OWES | Loans, bills to pay |
| **Equity** | Assets minus Liabilities = what belongs to shareholders | The company's "net worth" |
| **10-K** | Annual report filed with the SEC | Like a yearly report card |
| **Fiscal Year (FY)** | A company's 12-month reporting period | Apple's FY ends in September |
