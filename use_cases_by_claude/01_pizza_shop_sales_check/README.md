# Use Case 1: Pizza Shop Sales Check

## The Story

You own a pizza shop called **"Perfect Slice Pizza."** Your cashier writes down every
order in a spreadsheet. But sometimes they get busy and forget to type in all the
information. You want to check: **is any data missing?**

---

## What You Will Learn

- How to **load a CSV file** (a spreadsheet file) into DataBridge
- How to **profile your data** to find missing values and understand your data quality
- These are the 2 most basic DataBridge tools - a great starting point!

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `load_csv` | Reads a spreadsheet file and loads it into memory |
| `profile_data` | Scans every column and tells you about missing values, unique values, and data types |

---

## Step-by-Step Instructions

### Step 1: Make sure the server is running

If you haven't already, open a command prompt and run:
```
cd T:\Users\telha\Databridge_AI_Source
python -m src.server
```
Leave this window open.

### Step 2: Open Claude Desktop

Open the Claude Desktop app on your computer.

### Step 3: Load the pizza orders

Copy and paste this into Claude Desktop:

```
Load the CSV file at data/pizza_orders.csv
```

**What happens:** DataBridge reads all 15 pizza orders from the file. You should see a
summary showing:
- 15 rows of data
- 7 columns: order_id, customer_name, pizza_type, size, quantity, price, date
- A preview of the first few rows

### Step 4: Profile the data quality

Now copy and paste this:

```
Profile the data quality of the pizza orders data
```

**What happens:** DataBridge scans every column and creates a "health report" for your data.

---

## What Did We Find?

When you profile the data, you should discover these problems:

### Problem 1: Missing Customer Name
- **Row 6** has no customer name! Someone ordered a Veggie Supreme but the cashier
  forgot to write down who ordered it.
- The profile will show `customer_name` has **1 null value** (null means empty/missing).

### Problem 2: Missing Pizza Size
- **Row 12** (Ivy's Hawaiian pizza) has no size listed! We don't know if it was
  Small, Medium, or Large.
- The profile will show `size` has **1 null value**.

### Other Interesting Things You'll See

- **Alice** is the most frequent customer (she ordered 4 times!)
- **Pepperoni** is the most popular pizza type
- Prices range from $7.99 to $28.99
- Orders span from March 1 to March 5, 2024

---

## Understanding the Profile Results

The profile report shows you this for each column:

| Metric | What It Means |
|--------|--------------|
| **count** | How many rows have data in this column |
| **nulls** | How many rows are MISSING data (this is what we're looking for!) |
| **unique** | How many different values exist |
| **top** | The most common value |
| **type** | What kind of data it is (text, number, date) |

---

## Bonus Challenge

Try this on your own! Type into Claude Desktop:

```
How many unique customers ordered pizza? And which pizza type was ordered the most?
```

Can you figure out the answers before Claude tells you?

- **Unique customers:** There are 9 different customers (Alice, Bob, Charlie, Diana, Frank, Grace, Hank, Ivy, Jack) - plus 1 unknown!
- **Most ordered pizza:** Pepperoni (5 orders)

---

## What's Next?

Great job! You just learned how to load data and check its quality.
Now try [Use Case 2: Find My Friends](../02_find_my_friends/README.md) to learn
how to compare two lists and find misspelled names!
