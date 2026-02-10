# Use Case 3: School Report Card Hierarchy

## The Story

You are a **school principal** and report card day is coming! Your school has
15 different subjects (Algebra, Biology, Painting, etc.) but on the report card,
you want them **organized into groups**:

- **Math** - Algebra, Geometry, Statistics
- **Science** - Biology, Chemistry, Physics
- **Language Arts** - English Literature, Creative Writing, Spanish
- **Social Studies** - US History, World Geography
- **Art and Music** - Painting, Sculpture, Band, Chorus

Instead of listing all 15 subjects in a random order, you want a nice **tree structure**
(like a family tree, but for school subjects!).

---

## What You Will Learn

- How to **create a hierarchy project** (a container for your tree)
- How to **import a simple CSV** to build a tree automatically
- How to **view the tree** to see your organized groups
- What **Tier 1 format** means (the simplest way to build a hierarchy)

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `create_hierarchy_project` | Creates a new project to hold your hierarchy |
| `detect_hierarchy_format` | Looks at your CSV and figures out what format it's in |
| `import_flexible_hierarchy` | Reads your CSV and builds the tree structure |
| `get_hierarchy_tree` | Shows you the tree in a visual format |

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

### Step 3: Create a new project

Every hierarchy needs a "home" - a project to live in. Copy and paste this:

```
Create a hierarchy project called "School Subjects"
```

**What happens:** DataBridge creates a new empty project. You'll see a project ID
(something like "school-subjects") that we'll use in the next steps.

### Step 4: Check the format of our CSV

Before importing, let's have DataBridge figure out what kind of CSV we have:

```
Detect the hierarchy format of the file at data/report_card.csv
```

**What happens:** DataBridge reads the CSV and tells you:
- It's a **Tier 1** format (the simplest kind!)
- It has 2 columns: `source_value` and `group_name`
- It found 15 items in 5 groups

#### What is Tier 1?

Tier 1 is the simplest way to build a hierarchy. Your CSV only needs 2 columns:

| source_value | group_name |
|-------------|------------|
| Algebra | Math |
| Geometry | Math |
| Biology | Science |

That's it! DataBridge figures out the rest automatically.

### Step 5: Import the CSV into your project

Now let's build the tree! Copy and paste this:

```
Import the hierarchy from data/report_card.csv into the School Subjects project using flexible import
```

**What happens:** DataBridge reads all 15 subjects, creates the 5 groups (Math,
Science, Language Arts, Social Studies, Art and Music), and puts each subject
under the right group.

### Step 6: View your tree

Let's see the result! Copy and paste this:

```
Show me the hierarchy tree for the School Subjects project
```

**What happens:** You should see a tree that looks something like this:

```
School Subjects
├── Math
│   ├── Algebra
│   ├── Geometry
│   └── Statistics
├── Science
│   ├── Biology
│   ├── Chemistry
│   └── Physics
├── Language Arts
│   ├── English Literature
│   ├── Creative Writing
│   └── Spanish
├── Social Studies
│   ├── US History
│   └── World Geography
└── Art and Music
    ├── Painting
    ├── Sculpture
    ├── Band
    └── Chorus
```

---

## What Did We Find?

- **5 groups** were created automatically from the `group_name` column
- **15 subjects** were placed under their correct groups
- The tree has **2 levels**: Level 1 is the group name, Level 2 is the subject
- All of this was built from a simple 2-column CSV file!

---

## Understanding Hierarchies

A **hierarchy** is just a way of organizing things into levels, like:

```
Big Category
├── Smaller Category
│   ├── Specific Item
│   └── Specific Item
└── Smaller Category
    └── Specific Item
```

Real-world examples:
- **Your school:** District > School > Grade > Classroom
- **A store:** Department > Aisle > Shelf > Product
- **Your computer:** Drive > Folder > Subfolder > File

DataBridge can build these trees automatically from simple spreadsheets!

---

## Bonus Challenge

Try creating your own hierarchy! Make a CSV file with your favorite things:

```
source_value,group_name
Basketball,Sports
Soccer,Sports
Swimming,Sports
Pizza,Food
Tacos,Food
Ice Cream,Food
Minecraft,Video Games
Fortnite,Video Games
```

Save it as `data/my_favorites.csv` and try importing it:

```
Create a hierarchy project called "My Favorites"
```

Then:

```
Import the hierarchy from data/my_favorites.csv into the My Favorites project using flexible import
```

Then:

```
Show me the hierarchy tree for the My Favorites project
```

---

## A Note About Docker

You might see a warning message about "auto-sync" or "backend connection" when
creating projects. That's okay! The hierarchy builder works perfectly fine without
Docker. The sync feature is optional and only needed if you want to save your
hierarchies to a database.

---

## What's Next?

You're getting really good at this! Now try the final challenge:
[Use Case 4: Sports League Comparison](../04_sports_league_comparison/README.md)
where you'll compare two score sheets and find all the errors!
