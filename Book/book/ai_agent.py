import os
import json
from typing import List, Dict, Any, Optional
from .models import Book, Node
from .ai_agent_config import AIAgentConfig
from sentence_transformers import SentenceTransformer
from tinydb import TinyDB, Query
import numpy as np

class AIAgent:
    """
    An AI agent that suggests enhancements for a Book.
    """
    def __init__(self, databridge_project_path: str, config: Optional[AIAgentConfig] = None):
        self.databridge_project_path = databridge_project_path
        self.config = config if config else AIAgentConfig()
        self.skills: Dict[str, Any] = self._load_skills()
        self.knowledge_base: Dict[str, Any] = self._load_knowledge_base()
        self.skill_embeddings_db = self._load_skill_embeddings()
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')

    def _load_skills(self) -> Dict[str, Any]:
        """
        Loads skills from the Databridge AI project.
        """
        skills_path = os.path.join(self.databridge_project_path, "skills")
        skills = {}
        if os.path.exists(skills_path):
            for filename in os.listdir(skills_path):
                if filename.endswith(".json") and "embeddings" not in filename:
                    skill_name = os.path.splitext(filename)[0]
                    if not self.config.skills_to_use or skill_name in self.config.skills_to_use:
                        with open(os.path.join(skills_path, filename), 'r') as f:
                            skills[skill_name] = json.load(f)
        return skills

    def _load_knowledge_base(self) -> Dict[str, Any]:
        """
        Loads the knowledge base from the Databridge AI project.
        """
        kb_path = os.path.join(self.databridge_project_path, "knowledge_base", "index.json")
        if os.path.exists(kb_path):
            with open(kb_path, 'r') as f:
                return json.load(f)
        return {}

    def _load_skill_embeddings(self) -> TinyDB:
        """
        Loads the skill embeddings database.
        """
        db_path = os.path.join(self.databridge_project_path, "skills", "skill_embeddings.json")
        if os.path.exists(db_path):
            return TinyDB(db_path)
        return None

    def find_best_skill(self, query: str) -> Optional[str]:
        """
        Finds the most relevant skill for a given query using vector similarity search.
        """
        if not self.skill_embeddings_db:
            return None

        query_embedding = self.embedding_model.encode(query)
        
        best_skill = None
        highest_similarity = -1

        for item in self.skill_embeddings_db.all():
            skill_embedding = np.array(item['embedding'])
            similarity = np.dot(query_embedding, skill_embedding) / (np.linalg.norm(query_embedding) * np.linalg.norm(skill_embedding))
            
            if similarity > highest_similarity:
                highest_similarity = similarity
                best_skill = item['name']
        
        return best_skill

    def analyze_validation_results(self, book: Book) -> List[str]:
        """
        Analyzes the Great Expectations validation results in a Book and provides suggestions.
        """
        suggestions = []
        for node in book.traverse():
            if "validation_results" in node.properties:
                results = node.properties["validation_results"]
                if not results.get("success"):
                    for result in results.get("results", []):
                        if not result.get("success"):
                            expectation_config = result.get("expectation_config", {})
                            column = expectation_config.get("kwargs", {}).get("column")
                            expectation_type = expectation_config.get("expectation_type")
                            
                            suggestion = f"Data Quality Issue Found in column '{column}': Expectation '{expectation_type}' failed."
                            suggestions.append(suggestion)
        return suggestions

    def suggest_enhancements(self, book: Book, query: str) -> List[str]:
        """
        Analyzes a Book and suggests enhancements based on the most relevant skill for the query,
        and also analyzes data quality issues.
        """
        suggestions = []
        
        best_skill_name = self.find_best_skill(query)
        
        if best_skill_name:
            suggestions.append(f"Using skill: {best_skill_name}")
            skill = self.skills.get(best_skill_name.lower().replace(' ', '-'))
            
            if skill and "rules" in skill:
                for rule in skill["rules"]:
                    suggestions.append(f"Suggestion: {rule['suggestion']}")
            elif skill and "mappings" in skill:
                suggestions.append(f"Consider using the following mappings: {skill['mappings']}")
            else:
                suggestions.append("No specific rules or mappings found in the selected skill.")
        else:
            suggestions.append("No relevant skills found for your query.")

        # Analyze data quality
        validation_suggestions = self.analyze_validation_results(book)
        if validation_suggestions:
            suggestions.append("\n--- Data Quality Analysis ---")
            suggestions.extend(validation_suggestions)

        return suggestions

