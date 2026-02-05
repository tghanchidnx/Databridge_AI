# Use Case 11: Wall Street Analyst (Capstone)

## The Story

Congratulations - you've been promoted to **Junior Wall Street Analyst**!

Your managing director just walked up to your desk: "I need a full financial
analysis of Apple and Microsoft on my desk by end of day. Compare their income
statements, build hierarchies for both, and tell me which company is in better
financial shape. Use every tool we have."

This is the **capstone challenge** - you'll use EVERYTHING from the previous
tutorials: data loading, profiling, hierarchy building, templates, comparisons,
fuzzy matching, recommendations, skills, and diff analysis.

Time to show them what you've learned!

---

## What You Will Learn

- How to use **smart recommendations** to plan your analysis approach
- How to explore the **skills system** (AI expertise for specific industries)
- How to build **hierarchies for multiple companies** and compare them
- How to use **diff tools** to get character-level comparison explanations
- How to combine ALL DataBridge components into a comprehensive analysis

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `get_smart_recommendations` | Get AI suggestions for analyzing the data |
| `list_available_skills` | See what AI expertise is available |
| `get_skill_details` | Learn about a specific skill's capabilities |
| `create_hierarchy_project` | Create projects for Apple and Microsoft |
| `import_flexible_hierarchy` | Build hierarchy trees |
| `get_hierarchy_tree` | View the tree structures |
| `load_csv` | Load financial data |
| `compare_hashes` | Compare the two companies |
| `get_conflict_details` | See where amounts differ |
| `explain_diff` | Get human-readable explanations of differences |

**Components used:** ALL THREE - Books (data + hierarchy), Librarian (projects, templates, skills), Researcher concept (recommendations, validation, diff)

---

## Step-by-Step Instructions

### Part A: Research & Planning

#### Step 1: Get Smart Recommendations

Before diving in, let's ask DataBridge what it recommends:

```
Get smart recommendations for the file data/apple_income_statement.csv with user intent "Comprehensive financial analysis comparing Apple and Microsoft for investment report"
```

**What happens:** The recommendation engine analyzes the data and suggests:
- Best skill to use (probably Financial Analyst or FP&A)
- Relevant templates
- Recommended import tier
- Industry detection (Technology/General)

#### Step 2: Explore Available Skills

```
List all available skills
```

**What happens:** You'll see skills like Financial Analyst, FP&A Oil & Gas Analyst,
Manufacturing Analyst, SaaS Metrics Analyst, etc.

#### Step 3: Learn About the Financial Analyst Skill

```
Show me the details of the financial-analyst skill
```

**What happens:** You'll learn what the Financial Analyst skill can do - GL
reconciliation, trial balance analysis, chart of accounts design, and more.
These skills guide the AI in how to analyze your data.

---

### Part B: Build Apple's Hierarchy

#### Step 4: Create Apple's Project

```
Create a hierarchy project called "Apple P&L Analysis" with description "Apple Inc. income statement hierarchy for Wall Street comparison report"
```

#### Step 5: Import Apple's Data

Use the project ID from Step 4:

```
Import the flexible hierarchy from data/apple_income_statement_tier1.csv into project YOUR_APPLE_PROJECT_ID
```

#### Step 6: View Apple's Tree

```
Show me the hierarchy tree for project YOUR_APPLE_PROJECT_ID
```

---

### Part C: Build Microsoft's Hierarchy

#### Step 7: Load and Prepare Microsoft Data

First, load Microsoft's income statement:

```
Load the CSV file at data/microsoft_income_statement.csv and call it "msft_is"
```

#### Step 8: Create Microsoft's Project

```
Create a hierarchy project called "Microsoft P&L Analysis" with description "Microsoft Corp. income statement hierarchy for Wall Street comparison report"
```

#### Step 9: View Microsoft's Data Structure

Since Microsoft's data doesn't have a pre-built Tier 1 file, let's ask DataBridge
to detect the format:

```
Read the file data/microsoft_income_statement.csv and detect the hierarchy format
```

---

### Part D: Cross-Company Comparison

#### Step 10: Load Both Datasets

```
Load the CSV file at data/apple_income_statement.csv and call it "apple_is"
```

(Microsoft should already be loaded from Step 7)

#### Step 11: Compare the Companies

```
Compare the apple_is and msft_is datasets using account_name as the key column
```

#### Step 12: See the Differences

```
Show me the conflict details from the comparison
```

#### Step 13: Get Diff Explanations

For a human-readable explanation of the key differences:

```
Explain the differences between Apple and Microsoft's financial data. Use the diff explanation tool to describe the key conflicts found in the comparison.
```

**What happens:** DataBridge provides a plain-English explanation of each difference,
with similarity scores and character-level analysis where applicable.

---

### Part E: The Final Report

#### Step 14: Compile Your Analysis

Ask Claude to put it all together:

```
Based on everything we've analyzed, write a brief Wall Street analyst report comparing Apple and Microsoft. Include:
1. Revenue comparison
2. Profitability comparison (margins)
3. Key differences in cost structure
4. Which company has stronger financial metrics
5. Any concerns or red flags
```

---

## What Did We Find?

### The Wall Street Report Summary

#### Revenue Champion: Apple ($416B vs $245B)
Apple generates 70% more revenue, driven by massive iPhone sales globally.

#### Profit Margin Champion: Microsoft (36.0% vs 26.9%)
Microsoft keeps a higher percentage of each dollar earned. Software and cloud
services have lower costs than manufacturing hardware.

#### Cost Structure Differences

| Metric | Apple | Microsoft | Winner |
|--------|-------|-----------|--------|
| Revenue | $416.2B | $245.1B | Apple (bigger) |
| Gross Margin | 46.9% | 69.8% | Microsoft (more efficient) |
| R&D % of Revenue | 8.3% | 12.0% | Microsoft (invests more %) |
| Net Margin | 26.9% | 36.0% | Microsoft (more profitable %) |
| Net Income | $112.0B | $88.1B | Apple (more total dollars) |

#### Key Insights

1. **Apple = Volume Play:** Makes money by selling a LOT of products. High revenue,
   but hardware costs eat into margins.

2. **Microsoft = Margin Play:** Makes money by selling software with near-zero
   marginal cost. Every extra Office 365 subscription is almost pure profit.

3. **Both Invest Heavily in R&D:** Apple $34.6B, Microsoft $29.5B. But as a
   percentage of revenue, Microsoft invests more (12% vs 8.3%).

4. **Different But Both Strong:** Neither company has red flags. Both generate
   enormous free cash flow and dominate their markets.

---

## Scorecard: Tools & Components Used

| Component | Tools Used | What We Accomplished |
|-----------|-----------|---------------------|
| **Books** (Data Engine) | load_csv, compare_hashes, get_conflict_details | Loaded and compared financial data |
| **Books** (Hierarchy) | create_hierarchy_project, import_flexible_hierarchy, get_hierarchy_tree | Built P&L trees for both companies |
| **Librarian** (Templates) | Referenced in recommendations | Used standard P&L as comparison |
| **Librarian** (Skills) | list_available_skills, get_skill_details | Explored financial analyst capabilities |
| **Smart Recs** | get_smart_recommendations | Got AI-powered analysis suggestions |
| **Diff Tools** | explain_diff | Human-readable difference explanations |

---

## Bonus Challenge: The Full Monty

If you really want to impress your managing director, add the balance sheet analysis:

```
Load the CSV file at data/apple_balance_sheet.csv and call it "apple_bs"
```

```
Profile the apple_bs balance sheet data and tell me: What is Apple's current ratio? What is their debt-to-equity ratio? Are there any concerns?
```

**Expected findings:**
- **Current Ratio:** 0.87 (below 1.0, but fine for Apple's cash generation)
- **Debt-to-Equity Ratio:** $308B / $57B = 5.4x (high, but Apple uses leverage strategically)
- **Cash Position:** $29.9B in cash, plenty for operations

---

## Congratulations!

You've completed ALL 7 SEC EDGAR tutorials! You now know how to:

- Load and profile real financial data
- Build hierarchy trees from flat data
- Compare companies using hash comparison and fuzzy matching
- Track changes over time and detect schema drift
- Use smart recommendations and the skills system
- Build complete data pipelines with audit trails
- Create Wall Street-quality financial analysis

**You've gone from beginner to Wall Street analyst!**

---

## What's Next?

- Try building hierarchies for other companies (Tesla, Amazon, Google)
- Explore more templates (`list_financial_templates`)
- Look at the Oil & Gas or SaaS templates for industry-specific analysis
- Try the full pipeline with deployment to Snowflake (if you have access)
- Create your own CSV data and run it through the pipeline!

Go back to the [SEC EDGAR tutorials home](../README.md) or the
[main tutorials page](../../README.md).
