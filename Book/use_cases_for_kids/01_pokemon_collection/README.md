# Use Case 1: Organizing Your Pokémon Collection!

Have you ever wanted to organize your Pokémon collection in a cool, new way? With the `Book` library, you can create a "Pokédex Book" that shows you how your Pokémon are related!

## What We're Going to Do

We're going to take a simple list of Pokémon and turn it into a cool, hierarchical Pokédex that shows the evolution chain for each starter Pokémon.

## Step 1: Your Pokémon Data

First, you need a list of your Pokémon. We've created a file for you called `pokemon.csv`. It looks like this:

```csv
id,name,parent_id,type,hp
1,Pokemon,
2,Fire,1,
...
5,Charmander,2,Fire,39
6,Charmeleon,5,Fire,58
...
```

The `parent_id` column tells us how the Pokémon are related. For example, Charmeleon's `parent_id` is 5, which is Charmander's `id`. This means Charmeleon evolves from Charmander!

## Step 2: Open Your Command Terminal

Ask a grown-up to help you open a **terminal** or **command prompt**. This is where we'll type our magic Python commands.

Once it's open, type `python` and press Enter to start a Python session.

## Step 3: Create Your Pokédex Book

Now, let's type in some Python code to create our Pokédex. Type each command and press Enter.

```python
# First, we need to import the tools from our Book library
from book import Book, from_list
import csv

# Now, let's load our pokemon.csv file
with open('pokemon.csv', 'r') as f:
    pokemon_data = list(csv.DictReader(f))

# Next, we'll use from_list to create our hierarchy
# This tells the Book to use the 'parent_id' and 'id' columns to link the Pokemon
root_nodes = from_list(pokemon_data, parent_col="parent_id", child_col="id", name_col="name")

# Finally, let's create our Pokedex Book!
pokedex_book = Book(name="My Awesome Pokedex", root_nodes=root_nodes)

# Let's create a function to print our Pokedex
def print_pokedex(nodes, indent=""):
    for node in nodes:
        print(f"{indent}- {node.name}")
        print_pokedex(node.children, indent + "  ")

# Now, let's see our Pokedex!
print_pokedex(pokedex_book.root_nodes)
```

## What You Should See

You should see an output like this:

```
- Pokemon
  - Fire
    - Charmander
      - Charmeleon
        - Charizard
  - Water
    - Squirtle
      - Wartortle
        - Blastoise
  - Grass
    - Bulbasaur
      - Ivysaur
        - Venusaur
```

How cool is that? You've just created a hierarchical Pokédex that shows the evolution chains for the classic starter Pokémon. You can use this same idea to organize all sorts of things, from your favorite movies to your school subjects. Happy coding!
