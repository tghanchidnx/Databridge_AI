# Use Case 2: Planning Your Birthday Party!

Planning a party can be a lot of work, but with the `Book` library, we can create a smart to-do list that's easy to organize and track.

## What We're Going to Do

We're going to turn a simple to-do list from a CSV file into a hierarchical `Book` that shows all the tasks and subtasks for planning a birthday party.

## Step 1: Your To-Do List

First, we need a list of all the things we need to do. We've created a file for you called `todo_list.csv`. It looks like this:

```csv
id,task,parent_id,status
1,Birthday Party,
2,Guests,1,
...
5,Invite Friends,2,Pending
...
```

The `parent_id` column groups our tasks together. For example, "Invite Friends" is a subtask of "Guests."

## Step 2: Open Your Command Terminal

Just like before, open a **terminal** or **command prompt** and start a Python session by typing `python`.

## Step 3: Create Your Party Plan Book

Now, let's type in the Python code to create our party plan.

```python
# Import the tools we need
from book import Book, from_list
import csv

# Load our todo_list.csv file
with open('todo_list.csv', 'r') as f:
    todo_data = list(csv.DictReader(f))

# Create the hierarchy
root_nodes = from_list(todo_data, parent_col="parent_id", child_col="id", name_col="task")

# Create our Party Plan Book!
party_plan_book = Book(name="My Birthday Party Plan", root_nodes=root_nodes)

# Let's create a function to print our plan
def print_plan(nodes, indent=""):
    for node in nodes:
        # We can get the status from the node's properties!
        status = node.properties.get('status', '')
        print(f"{indent}- {node.name} [{status}]")
        print_plan(node.children, indent + "  ")

# Now, let's see our plan!
print_plan(party_plan_book.root_nodes)
```

## What You Should See

You should see an output like this:

```
- Birthday Party []
  - Guests []
    - Invite Friends [Pending]
    - Send Reminders [Pending]
  - Decorations []
    - Balloons [Done]
    - Streamers [Done]
  - Food []
    - Cake [Pending]
    - Pizza [Pending]
    - Drinks [Done]
```

Now you have a perfectly organized to-do list for your party! You can see which tasks are done and which are still pending. This is a great way to use the `Book` library to manage projects and stay organized.
