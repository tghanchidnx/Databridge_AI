# Saving and Loading Your 'Books'

Once you've created a `Book`, you'll want to save it so you can use it later. The `Book` library provides several ways to save and load your `Book`s.

## Saving Your 'Book'

You can save your `Book` in different formats, but the most common are JSON and GML (a special format for graphs).

Let's continue with the `my_first_book` example from the "Creating Your First 'Book'" tutorial.

In a Python interactive session:

```python
# Import the copy_book tool
from book import copy_book

# Save the book to a JSON file
copy_book(my_first_book, "my_foods.json", "json")

# Save the book to a GML file
copy_book(my_first_book, "my_foods.gml", "gml")
```

You will now have two new files in your directory: `my_foods.json` and `my_foods.gml`.

## Loading Your 'Book'

You can load a `Book` from a JSON or GML file using the `load_book` function.

```python
# Import the load_book tool
from book import load_book

# Load the book from the JSON file
loaded_from_json = load_book("my_foods.json", "json")

# Load the book from the GML file
loaded_from_gml = load_book("my_foods.gml", "gml")

# Print the name of the loaded book to verify
print(loaded_from_json.name)
```

You should see this output:

```
My Favorite Foods
```

This shows that you have successfully loaded your `Book` from the file.

Saving and loading `Book`s is essential for sharing your work with others and for building more complex applications.
