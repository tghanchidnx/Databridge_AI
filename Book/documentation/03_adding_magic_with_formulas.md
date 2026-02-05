# Adding Magic with Formulas

`Book`s are not just for storing information; they can also perform calculations using **Formulas**.

## What's a Formula?

A formula is like a math equation that you can attach to a `Node`. It can use the properties of other nodes to calculate a new value.

Let's imagine we have a `Book` for a simple video game character:

*   **Node:** "Hero"
*   **Properties:**
    *   `strength`: 10
    *   `bonus_attack`: 5

We can add a formula to this node to calculate the hero's `total_attack`.

## Creating and Using a Formula

Let's continue with our "Hero" example in a Python interactive session:

```python
# Import the tools we need
from book import Book, Node, Formula, execute_formulas, add_property

# Create our hero
hero_node = Node(name="Hero")
add_property(hero_node, "strength", 10)
add_property(hero_node, "bonus_attack", 5)

# Create a book to hold our hero
game_book = Book(name="Game Characters", root_nodes=[hero_node])

# Create the formula
attack_formula = Formula(
    name="total_attack",
    expression="strength + bonus_attack",
    operands=["strength", "bonus_attack"]
)

# Attach the formula to the hero node
hero_node.formulas.append(attack_formula)

# Execute the formulas for the hero node
execute_formulas(hero_node, game_book)

# See the result!
print(hero_node.properties["total_attack"])
```

You should see this output:

```
15
```

The `execute_formulas` function calculated `10 + 5` and stored the result in a new property called `total_attack` on the `hero_node`.

This is a simple example, but you can use formulas to perform all sorts of calculations in your `Book`s, from adding up numbers in a financial report to calculating game scores.
