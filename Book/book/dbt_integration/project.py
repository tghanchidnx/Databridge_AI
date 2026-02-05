from book import Book, Node, get_logger
from jinja2 import Template
import os

logger = get_logger(__name__)

DBT_PROJECT_YML_TEMPLATE = """
name: '{{ project_name }}'
version: '1.0.0'
config-version: 2

profile: 'default'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"
"""

DBT_MODEL_SQL_TEMPLATE = """
-- This model is auto-generated from a Book hierarchy.
-- It unnests a hierarchical structure into a flat dimension table.

WITH base_hierarchy AS (
    -- In a real scenario, this would be a {{ source(...) }} call
    -- For demonstration, we'll use a series of UNION ALL statements
    {% for node in all_nodes %}
    SELECT '{{ node.id }}' AS id, '{{ node.name }}' AS name, {% if node.properties.get('parent_id') %}'{{ node.properties.get('parent_id') }}'{% else %}NULL{% endif %} AS parent_id
    {% if not loop.last %}UNION ALL {% endif %}{% endfor %}
),

-- Recursive CTE to build the full path for each node
hierarchy_paths AS (
    SELECT
        id,
        name,
        parent_id,
        name AS path
    FROM base_hierarchy
    WHERE parent_id IS NULL

    UNION ALL

    SELECT
        c.id,
        c.name,
        c.parent_id,
        p.path || ' -> ' || c.name
    FROM base_hierarchy c
    JOIN hierarchy_paths p ON c.parent_id = p.id
)

SELECT
    id,
    name,
    path
FROM hierarchy_paths
"""

class DbtProject:
    """
    A class to manage a simple dbt project structure.
    """
    def __init__(self, project_name: str, project_dir: str = "."):
        self.project_name = project_name
        self.project_dir = os.path.join(project_dir, project_name)
        self.models_dir = os.path.join(self.project_dir, "models")

    def create_project_structure(self):
        """
        Creates the basic directory structure for a dbt project.
        """
        os.makedirs(self.models_dir, exist_ok=True)
        
        # Create dbt_project.yml
        dbt_project_yml_path = os.path.join(self.project_dir, "dbt_project.yml")
        with open(dbt_project_yml_path, "w") as f:
            template = Template(DBT_PROJECT_YML_TEMPLATE)
            f.write(template.render(project_name=self.project_name))

def generate_dbt_project_from_book(book: Book, project_dir: str = "."):
    """
    Generates a dbt project from a Book object.
    """
    logger.info(f"Generating dbt project for Book: {book.name}...")

    # Create the dbt project structure
    dbt_project = DbtProject(book.name, project_dir)
    dbt_project.create_project_structure()

    # Generate the model SQL
    all_nodes = []
    def get_all_nodes(nodes, parent_id=None):
        for node in nodes:
            node.properties["parent_id"] = parent_id
            all_nodes.append(node)
            get_all_nodes(node.children, node.id)
    
    get_all_nodes(book.root_nodes)

    template = Template(DBT_MODEL_SQL_TEMPLATE)
    model_sql = template.render(all_nodes=all_nodes)
    
    # Save the model SQL to a file
    model_path = os.path.join(dbt_project.models_dir, f"{book.name.lower().replace(' ', '_')}.sql")
    with open(model_path, "w") as f:
        f.write(model_sql)

    logger.info(f"dbt project generated at: {dbt_project.project_dir}")
