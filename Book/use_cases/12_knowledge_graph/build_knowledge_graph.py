from book import (
    Book,
    Node,
    get_logger,
    add_property,
    AIAgent,
    AIAgentConfig,
)
import re

logger = get_logger(__name__)

def main():
    """
    This script demonstrates creating a knowledge graph from unstructured text.
    """
    logger.info("Starting knowledge graph creation use case...")

    # 1. Read unstructured text
    with open("news_article.txt", "r") as f:
        text = f.read()

    # 2. Configure and initialize AI Agent
    logger.info("Initializing AI Agent with 'entity-extractor' skill...")
    agent_config = AIAgentConfig(skills_to_use=["entity-extractor"])
    agent = AIAgent(databridge_project_path="..", config=agent_config)

    # 3. Use AI Agent to extract entities (simulated)
    logger.info("Extracting entities from text...")
    extracted_entities = {}
    if "entity-extractor" in agent.skills:
        for entity_type, entity_list in agent.skills["entity-extractor"]["entity_patterns"].items():
            for entity in entity_list:
                if re.search(r'\b' + entity + r'\b', text):
                    if entity_type not in extracted_entities:
                        extracted_entities[entity_type] = []
                    extracted_entities[entity_type].append(entity)

    logger.info(f"Extracted entities: {extracted_entities}")

    # 4. Build the knowledge graph Book
    logger.info("Building knowledge graph...")
    knowledge_graph = Book(name="Acquisition Knowledge Graph")
    
    nodes = {name: Node(name=name) for name in extracted_entities.get("company", [])}

    # 5. Establish relationships (simplified for demonstration)
    if "Alpha Corp" in nodes and "Beta LLC" in nodes:
        nodes["Alpha Corp"].children.append(nodes["Beta LLC"])
        add_property(nodes["Beta LLC"], "relationship", "acquired_by")

    if "Alpha Corp" in nodes and "Gamma Inc" in nodes:
        nodes["Alpha Corp"].children.append(nodes["Gamma Inc"])
        add_property(nodes["Gamma Inc"], "relationship", "advised_by")
        
    if "Alpha Corp" in nodes:
        knowledge_graph.root_nodes.append(nodes["Alpha Corp"])

    # 6. Print the knowledge graph
    logger.info("\n--- Knowledge Graph ---")
    def print_hierarchy(nodes, indent=""):
        for node in nodes:
            relationship = node.properties.get("relationship", "")
            print(f"{indent}{node.name} (Relationship: {relationship})")
            print_hierarchy(node.children, indent + "  ")
    
    print_hierarchy(knowledge_graph.root_nodes)
    
    logger.info("\nKnowledge graph creation use case completed.")

if __name__ == "__main__":
    main()
