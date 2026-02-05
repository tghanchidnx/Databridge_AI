# What is a 'Book'?

Imagine you have a big, magical book. But instead of pages, it's made of **Nodes**.

## What's a Node?

A **Node** is like a single entry or a sticky note in your book. It has a **name** and can hold all sorts of information in its **properties**.

For example, you could have a Node for a character in a story:

*   **Name:** "Gandalf"
*   **Properties:**
    *   `race`: "Wizard"
    *   `weapon`: "Staff"
    *   `has_beard`: True

## The Magic of Hierarchy

Here's where it gets cool. Nodes can have **children**. This means you can create a tree-like structure, or a **hierarchy**.

Imagine a `Book` about the animal kingdom:

```
- Animal Kingdom (Root Node)
  - Mammals (Child Node)
    - Lions (Grandchild Node)
    - Elephants (Grandchild Node)
  - Reptiles (Child Node)
    - Snakes
    - Lizards
```

In this example, "Animal Kingdom" is the main `Node`, and "Mammals" and "Reptiles" are its children. "Lions" and "Elephants" are the children of "Mammals."

This is the core idea of the `Book` library: it allows you to organize your information in a hierarchical way, just like a family tree or a table of contents in a real book.
