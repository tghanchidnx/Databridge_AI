# Use Case 2: Find My Friends

## The Story

You are a **5th grade teacher**. You have two class rosters (lists of students):
- The **morning roster** from the school office (the official one)
- The **afternoon roster** that a student volunteer typed up

The problem? The student volunteer made some **spelling mistakes** when typing names!
You need to figure out: **which students are the same person, even though the names
are spelled a little differently?**

For example, is "Emily Jonson" the same person as "Emily Johnson"? (Spoiler: yes!)

---

## What You Will Learn

- How to **load two CSV files** and compare them
- How to use **fuzzy matching** - a smart way to find names that are *almost* the same
- How to read **similarity scores** (a number from 0-100 that tells you how close two names are)

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `load_csv` | Reads a spreadsheet file into memory |
| `fuzzy_match_columns` | Compares two columns and finds similar values (even with typos!) |

---

## Before You Start

This tutorial needs a special package called `rapidfuzz`. Open a command prompt and run:
```
pip install rapidfuzz
```
You only need to do this once.

---

## Step-by-Step Instructions

### Step 1: Make sure the server is running

If you haven't already, open a command prompt and run:
```
cd T:\Users\telha\Databridge_AI_Source
python -m src.server
```

### Step 2: Open Claude Desktop

Open the Claude Desktop app on your computer.

### Step 3: Load the morning roster (the correct list)

Copy and paste this into Claude Desktop:

```
Load the CSV file at data/class_roster_morning.csv
```

**What happens:** You should see 12 students loaded with columns: student_id,
student_name, grade, favorite_subject.

### Step 4: Load the afternoon roster (the one with typos)

Copy and paste this:

```
Load the CSV file at data/class_roster_afternoon.csv
```

**What happens:** Another 12 students loaded. If you look carefully, some names
look slightly different from the morning list!

### Step 5: Find matching students with fuzzy matching

Now the fun part! Copy and paste this:

```
Use fuzzy matching to compare the student_name column between class_roster_morning and class_roster_afternoon with a threshold of 70
```

**What happens:** DataBridge compares every name from the morning list against every
name in the afternoon list. It gives each pair a **similarity score** from 0 to 100.

---

## What Did We Find?

Here are the matches DataBridge should find:

| Morning Roster (Correct) | Afternoon Roster (Typos) | Score | What Changed? |
|--------------------------|--------------------------|-------|---------------|
| Emily Johnson | Emily Jonson | ~93 | "h" missing from Johnson |
| Liam Martinez | Liam Martinz | ~93 | "e" missing from Martinez |
| Sophia Williams | Sofia Williams | ~96 | "ph" became "f" in Sofia |
| Noah Brown | Noah Browne | ~95 | Extra "e" at the end |
| Olivia Davis | Olivia Davies | ~93 | "s" became "es" |
| Aiden Wilson | Aidan Wilson | ~95 | "e" and "a" swapped |
| Mia Anderson | Mia Andreson | ~93 | "o" and "e" swapped in Anderson |
| Lucas Thomas | Luke Thomas | ~91 | "Lucas" became "Luke" |
| Isabella Garcia | Isabela Garcia | ~97 | One "l" missing |
| Ethan Taylor | Ethan Tayler | ~93 | "o" became "e" in Taylor |
| Charlotte Lee | Charlie Lee | ~82 | Nickname used instead |
| Benjamin Moore | Tommy Jackson | ~30 | Completely different! |

### The Big Discovery

**11 out of 12 students** matched between the two lists! But look at the last row:
- **Benjamin Moore** (morning) does NOT match **Tommy Jackson** (afternoon)
- Their similarity score is very low (around 30)
- These are different people! The afternoon roster has Tommy Jackson instead of Benjamin Moore

This means either:
- Benjamin Moore wasn't in the afternoon class
- Tommy Jackson is a new student who joined later
- Someone made a mistake on one of the rosters

---

## Understanding Similarity Scores

| Score | What It Means |
|-------|--------------|
| **100** | Perfect match - exactly the same |
| **90-99** | Very close - probably the same person with a small typo |
| **80-89** | Pretty close - likely the same person with a bigger typo or nickname |
| **70-79** | Maybe the same - worth checking manually |
| **Below 70** | Probably different people |

The **threshold of 70** we used means: "Only show me matches where the names are
at least 70% similar."

---

## Bonus Challenge

Try changing the threshold! Paste this into Claude Desktop:

```
Use fuzzy matching to compare student_name between class_roster_morning and class_roster_afternoon with a threshold of 90
```

What happens when you raise the threshold to 90? You should see **fewer matches**
because now DataBridge is being pickier - it only shows names that are at least
90% similar. "Charlotte Lee" and "Charlie Lee" might drop off because they are
only about 82% similar!

---

## What's Next?

Awesome work! You just learned how to find people across two lists even when names
are misspelled. Now try [Use Case 3: School Report Card Hierarchy](../03_school_report_card_hierarchy/README.md)
to learn how to organize things into groups!
