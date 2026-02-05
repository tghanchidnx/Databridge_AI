# Use Case 3: Building a Recipe Book

Let's use the `Book` library to create a digital recipe book! We'll start with a recipe for a delicious pizza.

## What We're Going to Do

We'll take a list of ingredients and steps from a CSV file and organize them into a `Book` that's easy to read and follow.

## Step 1: Your Recipe Data

We've created a file for you called `pizza_recipe.csv`. It contains all the ingredients and steps for making a pizza:

```csv
id,item,parent_id,quantity,unit
1,Pizza Recipe,
2,Ingredients,1,
3,Steps,1,
4,Dough,2,1,ball
...
```

The `parent_id` tells us whether an item is an ingredient (parent_id 2) or a step (parent_id 3).

## Step 2: Open Your Command Terminal

Open a **terminal** or **command prompt** and start a Python session by typing `python`.

## Step 3: Create Your Recipe Book

Now, let's type in the Python code to create our recipe `Book`.

```python
# Import the tools we need
from book import Book, from_list
import csv

# Load our pizza_recipe.csv file
with open('pizza_recipe.csv', 'r') as f:
    recipe_data = list(csv.DictReader(f))

# Create the hierarchy
root_nodes = from_list(recipe_data, parent_col="parent_id", child_col="id", name_col="item")

# Create our Recipe Book!
recipe_book = Book(name="My Pizza Recipe", root_nodes=root_nodes)

# Let's create a function to print our recipe
def print_recipe(nodes, indent=""):
    for node in nodes:
        quantity = node.properties.get('quantity', '')
        unit = node.properties.get('unit', '')
        print(f"{indent}- {node.name} {quantity} {unit}")
        print_recipe(node.children, indent + "  ")

# Now, let's see our recipe!
print_recipe(recipe_book.root_nodes)
```

## What You Should See

You should see an output like this:

```
- Pizza Recipe  
  - Ingredients  
    - Dough 1 ball
    - Tomato Sauce 0.5 cup
    - Cheese 1 cup
    - Pepperoni 10 slices
  - Steps  
    - Preheat Oven 400 F
    - Roll out Dough  
    - Add Toppings  
    - Bake 15 minutes
```

Now you have a perfectly organized recipe! You can use this to create a whole cookbook of your favorite meals.
