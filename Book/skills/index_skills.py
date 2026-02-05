import os
import json
from sentence_transformers import SentenceTransformer
from tinydb import TinyDB, Query

def main():
    """
    This script creates vector embeddings for the skills and stores them in a TinyDB database.
    """
    print("Starting skill indexing...")

    # Initialize the sentence transformer model
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # Initialize the TinyDB database
    db = TinyDB('skill_embeddings.json')
    db.truncate()  # Clear the database before indexing

    # Iterate through the skill files
    for filename in os.listdir('.'):
        if filename.endswith(".json") and filename != "skill_embeddings.json":
            with open(filename, 'r') as f:
                skill_data = json.load(f)
                skill_name = skill_data.get("name", os.path.splitext(filename)[0])
                skill_description = skill_data.get("description", "")
                
                # Create a text representation of the skill
                skill_text = f"Skill: {skill_name}\nDescription: {skill_description}" # Corrected: \n is the correct escape for newline
                
                # Create the embedding
                embedding = model.encode(skill_text).tolist()
                
                # Store the skill and its embedding in the database
                db.insert({
                    "name": skill_name,
                    "description": skill_description,
                    "embedding": embedding
                })
                
                print(f"Indexed skill: {skill_name}")

    print("\nSkill indexing completed.")
    print(f"Skill embeddings have been stored in 'skill_embeddings.json'.")

if __name__ == "__main__":
    main()
