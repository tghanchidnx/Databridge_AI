# Use Case 12: Creating a Knowledge Graph from Unstructured Data

This use case demonstrates how to use the `Book` library and the `AIAgent` to create a simple knowledge graph from unstructured text. It highlights how to:

*   Use an AI Agent with a specific skill to extract entities from text.
*   Represent the extracted entities and their relationships as a hierarchical `Book`.
*   Build a graph-like structure by nesting `Node` objects.

## Components Involved

*   **`Book` Library:** Used to structure the knowledge graph, with `Node` objects representing entities and their relationships.
*   **`AIAgent`:** The AI agent is used to identify and extract relevant entities (in this case, companies) from the unstructured text, based on the rules defined in its skill.

## Files

*   `news_article.txt`: A simple text file containing a short news article about a corporate acquisition.
*   `../skills/entity-extractor.json`: A dummy AI skill that defines the entities to be extracted.
*   `build_knowledge_graph.py`: The Python script that implements the use case.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Knowledge Graph Script

Navigate to the `Book/use_cases/12_knowledge_graph` directory and run the `build_knowledge_graph.py` script:

```bash
python build_knowledge_graph.py
```

### 3. What's Happening?

1.  **Read Text:** The script reads the content of the `news_article.txt` file.
2.  **Initialize AI Agent:** An `AIAgent` is initialized with the `entity-extractor` skill. This skill provides the agent with a list of company names to look for in the text.
3.  **Entity Extraction (Simulated):** The script simulates the AI agent's entity extraction process. It searches the text for the company names defined in the skill and creates a list of the entities found.
4.  **Build Knowledge Graph:** A `Book` object is created to represent the knowledge graph. A `Node` is created for each extracted company.
5.  **Establish Relationships:** The script then establishes relationships between the nodes. For example, it makes "Beta LLC" a child of "Alpha Corp" and adds a "relationship" property to indicate that it was "acquired\_by" Alpha Corp. This demonstrates how the flexible `Book` structure can represent more than just simple parent-child hierarchies.
6.  **Print Graph:** Finally, the script prints the resulting knowledge graph, showing the hierarchical relationships between the extracted entities.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting knowledge graph creation use case...
INFO:__main__:Initializing AI Agent with 'entity-extractor' skill...
INFO:__main__:Extracting entities from text...
INFO:__main__:Extracted entities: {'company': ['Alpha Corp', 'Beta LLC', 'Gamma Inc']}
INFO:__main__:Building knowledge graph...

--- Knowledge Graph ---
Alpha Corp (Relationship: )
  Beta LLC (Relationship: acquired_by)
  Gamma Inc (Relationship: advised_by)

INFO:__main__:Knowledge graph creation use case completed.
```

This use case illustrates how the `Book` library, in combination with the `AIAgent`, can be a powerful tool for knowledge extraction and representation, turning unstructured data into structured, actionable insights.
