# Use Case 4: Sports League Comparison

## The Story

You are a **newspaper editor** covering the local sports league. You have two
spreadsheets with team statistics:

1. **Official stats** from the league office (this is the truth!)
2. **Newspaper stats** that your reporter typed up from watching the games

Your reporter is good, but they made some mistakes. Some numbers are wrong, and
there are even teams that appear on one list but not the other!

Your job: **find every single mistake** before the newspaper goes to print tomorrow morning!

---

## What You Will Learn

- How to **compare two datasets** using hash-based comparison
- What **orphans** are (items in one list but not the other)
- What **conflicts** are (same item, different values)
- How to read a **match rate** (what percentage of data matches perfectly)

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `load_csv` | Reads a spreadsheet file into memory |
| `compare_hashes` | Compares two datasets row by row using a key column |
| `get_orphan_details` | Shows items that exist in only one dataset |
| `get_conflict_details` | Shows items where values don't match between datasets |

---

## Step-by-Step Instructions

### Step 1: Make sure the server is running

If you haven't already, open a command prompt and run:
```
cd C:\Users\telha\Databridge_AI
python -m src.server
```

### Step 2: Open Claude Desktop

Open the Claude Desktop app on your computer.

### Step 3: Load the official stats

Copy and paste this into Claude Desktop:

```
Load the CSV file at data/league_stats_official.csv
```

**What happens:** You should see 10 teams loaded with columns: team_name, wins,
losses, points_scored, coach.

### Step 4: Load the newspaper stats

Copy and paste this:

```
Load the CSV file at data/league_stats_newspaper.csv
```

**What happens:** Another 10 teams loaded. They look similar, but some data is different!

### Step 5: Compare the two datasets

Now let's find the differences! Copy and paste this:

```
Compare league_stats_official and league_stats_newspaper using team_name as the key column
```

**What happens:** DataBridge goes through every team and checks if all the numbers
match between the two files. You'll see a summary showing:
- How many teams **matched perfectly**
- How many teams have **conflicts** (different values)
- How many teams are **orphans** (only in one file)

### Step 6: Find the orphans

Copy and paste this:

```
Show me the orphan details from the comparison
```

**What happens:** DataBridge shows you teams that exist in only ONE of the two files:

| Team | Found In | Missing From |
|------|----------|-------------|
| **Rabbits** | Official stats | Newspaper stats |
| **Dragons** | Newspaper stats | Official stats |

This means:
- The **Rabbits** are a real team (they're in the official stats) but your reporter
  completely missed them!
- The **Dragons** appear in the newspaper but NOT in the official stats - your
  reporter might have made them up, or they're from a different league!

### Step 7: Find the conflicts

Copy and paste this:

```
Show me the conflict details from the comparison
```

**What happens:** DataBridge shows you teams where the data doesn't match:

| Team | Column | Official (Correct) | Newspaper (Wrong) |
|------|--------|-------------------|-------------------|
| **Eagles** | points_scored | 1280 | 1290 |
| **Wolves** | coach | Coach Wilson | Coach Williams |
| **Hawks** | wins | 11 | 10 |

Your reporter got three things wrong:
1. **Eagles' points:** Wrote 1290 instead of 1280 (off by 10 points!)
2. **Wolves' coach:** Wrote "Coach Williams" instead of "Coach Wilson" (wrong name!)
3. **Hawks' wins:** Wrote 10 instead of 11 (missed a win!)

---

## What Did We Find?

Here's the full report card for your reporter:

| Metric | Count |
|--------|-------|
| Total teams in official stats | 10 |
| Total teams in newspaper stats | 10 |
| Perfect matches | 6 |
| Teams with wrong data (conflicts) | 3 |
| Teams missing from newspaper (orphans) | 1 |
| Teams only in newspaper (orphans) | 1 |
| **Overall match rate** | **~60%** |

### The 6 Perfect Matches
These teams had identical data in both files:
Tigers, Bears, Sharks, Panthers, Foxes, Turtles

### Summary for the Reporter
"Hey reporter, before we print tomorrow's paper, you need to fix these things:
1. Eagles scored 1280 points, not 1290
2. The Wolves coach is Coach Wilson, not Coach Williams
3. The Hawks won 11 games, not 10
4. You forgot to include the Rabbits (4 wins, 16 losses)
5. Please remove the Dragons - they're not in this league!"

---

## Understanding Data Comparison Terms

| Term | What It Means | Real-World Example |
|------|--------------|-------------------|
| **Key Column** | The column used to match rows between files | `team_name` - we match teams by name |
| **Match** | A row where ALL values are identical in both files | Tigers: same wins, losses, points, coach |
| **Conflict** | A row where the key matches but other values differ | Eagles: same name, different points |
| **Orphan** | A row that exists in only one file | Rabbits: only in official stats |
| **Match Rate** | Percentage of rows that matched perfectly | 60% = 6 out of 10 teams matched |

---

## Bonus Challenge

Try comparing just specific columns! Paste this into Claude Desktop:

```
Compare only the wins and losses columns between the two league stats files using team_name as the key
```

When you compare fewer columns, your match rate goes up! That's because some
conflicts are only in other columns (like the coach name for Wolves).

You can also try:

```
Profile the data quality of the official league stats
```

This will tell you interesting things like the average number of wins, the highest
scoring team, and more!

---

## Congratulations!

You've completed all 4 tutorials! Here's what you learned:

| Tutorial | Skill |
|----------|-------|
| 1. Pizza Shop | Loading data and finding missing values |
| 2. Find My Friends | Fuzzy matching to find similar names |
| 3. Report Card | Building hierarchies (tree structures) |
| 4. Sports League | Comparing datasets to find errors |

These are the same skills that real data analysts use every day at big companies -
they just work with bigger spreadsheets! Now you know the basics of **data quality**
and **data reconciliation**.

---

## What Can You Try Next?

- **Make your own CSV files** and try loading, profiling, and comparing them
- **Try different thresholds** for fuzzy matching (what happens at 50? at 95?)
- **Build bigger hierarchies** with 3 or 4 levels instead of 2
- **Compare your own data** - maybe two versions of a contact list or inventory?

Happy data exploring!
