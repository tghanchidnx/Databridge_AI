# Use Case 21: Vector-Powered AI Agent for Skill Discovery

This use case demonstrates the enhanced `AIAgent`, which now uses vector embeddings to dynamically find the most relevant "skill" for a user's query and provide context-aware suggestions.

## Features Highlighted

*   **Vector Similarity Search:** The `AIAgent` uses vector embeddings to match a user's query with the most relevant skill from its knowledge base.
*   **Dynamic Skill Selection:** Instead of being pre-configured, the agent dynamically selects the best skill for the task at hand.
*   **Intelligent Suggestions:** The suggestions provided by the agent are now directly related to the user's query.

## Components Involved

*   **`Book` Library:** Provides the `AIAgent` and the underlying data structures.
*   **`sentence-transformers`:** Used to create the vector embeddings.
*   **`TinyDB`:** Used as a simple, file-based vector database to store the skill embeddings.

## Files

*   `run_ai_suggestions.py`: The Python script that runs the use case.
*   `../skills/skill_embeddings.json`: The database of skill embeddings.
*   `../skills/*.json`: The skill definition files.

## Step-by-Step Instructions

### 1. Set up the Environment & Index Skills

First, make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

Next, you need to create the skill embeddings database. Navigate to the `Book/skills` directory and run the `index_skills.py` script:

```bash
cd ../../skills
python index_skills.py
cd ../use_cases/21_vector_powered_agent
```

### 2. Run the AI Suggestions Script

Now, run the `run_ai_suggestions.py` script:

```bash
python run_ai_suggestions.py
```

### 3. What's Happening?

1.  **Initialize AI Agent:** The `AIAgent` is initialized. It loads the skill definitions and the skill embeddings database (`skill_embeddings.json`).
2.  **Find Best Skill:** When the `suggest_enhancements` method is called with a user query, the agent:
    *   Creates a vector embedding of the user's query.
    *   Performs a vector similarity search against the skill embeddings in its database.
    *   Identifies the skill with the highest similarity score as the "best skill" for the query.
3.  **Generate Suggestions:** The agent then uses the rules and mappings from the selected skill to generate relevant suggestions.
4.  **Print Suggestions:** The script prints the suggestions, which are now tailored to the user's query.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting vector-powered AI agent use case...
INFO:__main__:Initializing AI Agent...
INFO:__main__:Getting suggestions for query: 'How should I adjust my product prices?'

--- AI Agent Suggestions ---
- Using skill: Pricing Analyst
- Suggestion: Increase price by 10%
- Suggestion: Maintain current price
- Suggestion: Decrease price by 5%

INFO:__main__:
Vector-powered AI agent use case completed.
```

This use case demonstrates a significant step forward in the intelligence of the `AIAgent`. By using vector embeddings, the agent can now understand the user's intent and dynamically select the most appropriate tool for the job, making it a much more powerful and flexible assistant.
