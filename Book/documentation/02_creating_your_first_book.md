# Creating Your First 'Book'

Let's create your very first `Book`! We'll make a simple `Book` to organize your favorite foods.

## 1. Get Your Tools Ready

First, you'll need a special tool called a **terminal** or **command prompt**. This is where you'll type your commands.

You also need to have Python and the `Book` library installed. If you haven't done this yet, ask a grown-up to help you with the setup.

## 2. Create Your Data File

Let's start with a simple CSV file. CSV stands for "Comma-Separated Values," and it's just a plain text file that looks like a spreadsheet.

Create a file named `favorite_foods.csv` and put this text inside it:

```csv
id,name,parent_id,category
1,Foods,
2,Fruits,1,Healthy
3,Vegetables,1,Healthy
4,Junk Food,1,Treat
5,Apple,2,
6,Banana,2,
7,Broccoli,3,
8,Pizza,4,
```

This file describes a hierarchy of foods. For example, "Apple" is a child of "Fruits" (parent_id 2).

## 3. Create Your 'Book' with Python

Now, let's use Python to turn this CSV file into a `Book`. Open your terminal and start a Python interactive session by typing `python` and pressing Enter.

Now, type these commands one by one:

```python
# Import the tools we need from the Book library
from book import from_list, Book
import csv

# Load the data from our CSV file
with open('favorite_foods.csv', 'r') as f:
    data = list(csv.DictReader(f))

# Create the hierarchy
root_nodes = from_list(data, parent_col="parent_id", child_col="id", name_col="name")

# Create the Book
my_first_book = Book(name="My Favorite Foods", root_nodes=root_nodes)

# Print the name of your book
print(my_first_book.name)
```

You should see this output:

```
My Favorite Foods
```

Congratulations! You have just created your first `Book` object. In the next section, we'll learn how to see what's inside it.
