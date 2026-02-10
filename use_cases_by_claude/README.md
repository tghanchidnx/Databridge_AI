# DataBridge AI - Fun Use Cases for Beginners

Welcome! This folder has **11 fun tutorials** that teach you how to use DataBridge AI.
Start with the 4 beginner tutorials using everyday data (pizza orders, sports teams),
then advance to 7 tutorials using **real financial data** from Apple and Microsoft!

**You do NOT need to be a programmer.** If you can copy and paste, you can do this!

---

## What is DataBridge AI?

DataBridge AI is a tool that helps you:
- **Check your data** for missing or wrong information
- **Compare two lists** to find differences
- **Organize things into groups** (like sorting school subjects into categories)
- **Find spelling mistakes** by comparing names that look almost the same

It works inside **Claude Desktop** (a chat app), so you just type what you want in plain English!

---

## Before You Start (One-Time Setup)

### Step 1: Make sure these programs are installed

You need these on your computer:
- **Python** (version 3.10 or newer) - [Download Python](https://www.python.org/downloads/)
- **Claude Desktop** - [Download Claude Desktop](https://claude.ai/download)

To check if Python is installed, open your command prompt and type:
```
python --version
```
You should see something like `Python 3.10.x` or higher.

### Step 2: Copy the sample data files

Open your command prompt (search "cmd" in Windows Start menu) and run:

```
cd T:\Users\telha\Databridge_AI_Source
python use_cases_by_claude/setup.py
```

This copies all the sample CSV files into the `data/` folder where DataBridge can find them.

### Step 3: Start the DataBridge MCP Server

In your command prompt, type:

```
cd T:\Users\telha\Databridge_AI_Source
python -m src.server
```

Leave this window open! The server needs to keep running while you use Claude Desktop.

### Step 4: Open Claude Desktop

Open the Claude Desktop app. DataBridge AI tools should already be connected.
You will see a small hammer icon or tool indicator showing the MCP tools are available.

---

## Beginner Tutorials (Everyday Data)

| # | Tutorial | What You Learn | Difficulty |
|---|----------|---------------|------------|
| 1 | [Pizza Shop Sales Check](01_pizza_shop_sales_check/README.md) | Load data and find missing info | Easy |
| 2 | [Find My Friends](02_find_my_friends/README.md) | Match misspelled names between two lists | Easy-Medium |
| 3 | [School Report Card Hierarchy](03_school_report_card_hierarchy/README.md) | Organize subjects into groups | Medium |
| 4 | [Sports League Comparison](04_sports_league_comparison/README.md) | Compare two score sheets and find errors | Medium |

**Start with Tutorial 1** and work your way up!

---

## Advanced Tutorials (Real Wall Street Data)

These 7 tutorials use **real financial data** from Apple ($416B revenue) and Microsoft
($245B revenue), downloaded from the SEC EDGAR government database.

| # | Tutorial | What You Learn | Difficulty |
|---|----------|---------------|------------|
| 5 | [Apple's Money Checkup](sec_edgar/05_apple_money_checkup/README.md) | Load real financial data, profile Apple's income | Easy |
| 6 | [Apple's Money Tree](sec_edgar/06_apple_money_tree/README.md) | Build a P&L hierarchy, use templates | Medium |
| 7 | [Apple vs Microsoft Showdown](sec_edgar/07_apple_vs_microsoft/README.md) | Compare two companies, find differences | Medium |
| 8 | [Apple Time Machine](sec_edgar/08_apple_time_machine/README.md) | Multi-year comparison, detect changes | Medium |
| 9 | [Balance Sheet Detective](sec_edgar/09_balance_sheet_detective/README.md) | Assets vs liabilities, smart recommendations | Medium-Hard |
| 10 | [Full Financial Pipeline](sec_edgar/10_full_financial_pipeline/README.md) | End-to-end pipeline with workflow recording | Hard |
| 11 | [Wall Street Analyst](sec_edgar/11_wall_street_analyst/README.md) | Capstone: skills, recommendations, full analysis | Hard |

**Setup for SEC tutorials:** See [SEC EDGAR Tutorials](sec_edgar/README.md) for setup instructions.

---

## Troubleshooting

### "Tool not found" error in Claude Desktop
Make sure the MCP server is running (Step 3 above). Check the command prompt window -
it should say something like "Server started" or "MCP server running."

### "File not found" error
Make sure you ran the setup script (Step 2 above). The CSV files need to be in the
`data/` folder.

### "rapidfuzz not installed" error (Tutorial 2 only)
Open command prompt and run:
```
pip install rapidfuzz
```

### Server crashes or won't start
Try installing the required packages:
```
cd T:\Users\telha\Databridge_AI_Source
pip install -r requirements.txt
```

---

## How Each Tutorial Works

1. **Read the story** - Each tutorial starts with a fun story about why you need to check data
2. **Copy the prompts** - Each step has an exact prompt you can copy and paste into Claude Desktop
3. **Read the results** - The tutorial explains what each result means
4. **Try the bonus challenge** - At the end, there is an extra challenge to try on your own!

Have fun! Data detective work is like solving a puzzle.
