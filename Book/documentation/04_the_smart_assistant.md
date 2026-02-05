# The Smart Assistant (AI Agent)

The `Book` library has a built-in smart assistant called the **AI Agent**. This agent can help you by analyzing your `Book`s and suggesting improvements or providing insights.

## What is the AI Agent?

The `AIAgent` is like having a helpful robot friend who knows a lot about different topics. You can ask it a question, and it will try to find the most relevant "skill" it has to help you.

## How Does it Work?

The `AIAgent` has a collection of **skills**. Each skill is like a small instruction book on a specific topic (e.g., "Financial Analyst," "Pricing Analyst").

When you ask the agent a question, it uses a special technique called **vector similarity search** to find the skill that is the best match for your query. It then uses the rules and information from that skill to give you a suggestion.

## Using the AI Agent

Let's see the `AIAgent` in action! Imagine you have a `Book` about products and you want to know how to price them.

In a Python interactive session:

```python
# Import the tools we need
from book import Book, AIAgent

# Create a dummy book (the agent needs a book to analyze)
my_book = Book(name="My Products")

# Create the agent
# The agent needs to know where to find the skills. In this case, they
# are in the '../skills' directory relative to where the use cases are run.
agent = AIAgent(databridge_project_path="..")

# Ask the agent a question
my_question = "How should I price my products?"
suggestions = agent.suggest_enhancements(my_book, my_question)

# See what the agent suggests
for suggestion in suggestions:
    print(suggestion)
```

You should see an output like this:

```
- Using skill: Pricing Analyst
- Suggestion: Increase price by 10%
- Suggestion: Maintain current price
- Suggestion: Decrease price by 5%
```

The agent correctly identified that the "Pricing Analyst" skill was the most relevant for your question and provided suggestions based on the rules in that skill.

This is a powerful feature that can help you make smarter decisions and get more out of your data.
