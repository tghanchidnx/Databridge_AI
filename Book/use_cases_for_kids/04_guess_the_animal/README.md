# Use Case 4: A Simple "Guess the Animal" Game

Let's build a fun "Guess the Animal" game using the `Book` library's `AIAgent`! The computer will think of an animal, and it will give you clues to help you guess.

## What We're Going to Do

We'll use the `AIAgent` and a special "Animal Expert" skill to create a simple, interactive guessing game.

## Step 1: Your Animal Skill

We've created a special skill file for our `AIAgent` called `animal-expert.json`. It's like a little cheat sheet for the agent that contains clues for different animals.

## Step 2: Open Your Command Terminal

Open a **terminal** or **command prompt** and start a Python session by typing `python`.

## Step 3: Let's Play the Game!

Now, let's type in the Python code to play our game.

```python
# Import the tools we need
from book import AIAgent, AIAgentConfig
import random

# Initialize our AI Agent and tell it to use our "Animal Expert" skill
agent_config = AIAgentConfig(skills_to_use=["animal-expert"])
agent = AIAgent(databridge_project_path="../..", config=agent_config)

# Get the list of animals from the agent's skill
animals = agent.skills["animal-expert"]["animals"]
animal_names = list(animals.keys())

# The computer secretly picks an animal
secret_animal = random.choice(animal_names)
clues = animals[secret_animal]

print("I'm thinking of an animal...")
print("Here is your first clue:")
print(f"- {clues[0]}")

# Now it's your turn to guess!
guess = input("What is your guess? ")

if guess.lower() == secret_animal.lower():
    print("You got it! You are an Animal Expert!")
else:
    print(f"Sorry, the correct answer was {secret_animal}.")
    print("Here are all the clues:")
    for clue in clues:
        print(f"- {clue}")

```

## What Happens When You Play

1.  **The agent gets ready:** The `AIAgent` is created and loads the `animal-expert` skill.
2.  **The computer picks an animal:** The script randomly selects an animal from the skill's list.
3.  **You get a clue:** The script prints the first clue for the secret animal.
4.  **You guess:** You type in your guess and press Enter.
5.  **Did you win?:** The script checks if your guess is correct and lets you know if you won.

This is a simple example, but it shows how you can use the `AIAgent` and its skills to create fun and interactive games!
