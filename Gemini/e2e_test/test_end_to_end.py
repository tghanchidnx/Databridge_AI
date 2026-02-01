import subprocess
import pytest
import os
import re
from pathlib import Path

# Define paths relative to the project root
GEMINI_DIR = Path(__file__).parent
E2E_DATA_DIR = GEMINI_DIR / "data"
V4_MAIN_PATH = Path(__file__).parent.parent.parent / "v4" / "src" / "main.py"

# --- Helper Functions ---

def run_command(command, **kwargs):
    """Runs a command and returns its output."""
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            **kwargs
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {' '.join(command)}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        raise

def extract_project_id(output):
    """Extracts the project ID from the 'project create' output."""
    match = re.search(r"Created project:\s*([a-f0-9-]+)", output, re.IGNORECASE)
    if not match:
        raise ValueError("Could not find project ID in the output.")
    return match.group(1)

# --- Pytest Fixtures ---

@pytest.fixture(scope="function")
def v3_project(tmp_path):
    """
    A pytest fixture that sets up a V3 project for the test module.
    It creates a project and yields the project ID.
    It uses a temporary SQLite database for isolation and cleans up afterwards.
    """
    print("--- Setting up V3 Project ---")
    
    # Create a temporary directory for V3's data and database
    v3_test_dir = tmp_path / "v3_test_data"
    v3_test_dir.mkdir()
    
    # Set environment variables for V3 to use the temporary database
    env = os.environ.copy()
    env["DATABRIDGE_DB_PATH"] = str(v3_test_dir / "databridge.db")
    env["DATABRIDGE_DATA_DIR"] = str(v3_test_dir)
    env["DATABRIDGE_DISABLE_AUDIT"] = "True" # Disable audit logging to prevent potential locks
    
    command = [
        "python", "-m", "src.cli.app", "project", "create",
        "E2ETestProject", "--description", "Automated E2E Test"
    ]
    output = run_command(command, cwd=Path(__file__).parent.parent.parent / "v3", env=env)
    project_id = extract_project_id(output)
    print(f"Created project with ID: {project_id}")
    
    yield project_id, env
    
    # No explicit cleanup of the database file needed as tmp_path handles its removal.
    # print(f"--- Tearing down V3 Project {project_id} and temporary files ---")


# --- Test Cases ---

@pytest.mark.end_to_end
def test_v3_hierarchy_import(v3_project):
    """Tests the import of the hierarchy CSV."""
    v3_project_id, v3_env = v3_project
    print(f"--- Testing Hierarchy Import for project {v3_project_id} ---")
    hierarchy_csv = E2E_DATA_DIR / "e2e_hierarchy.csv"
    command = [
        "python", "-m", "src.cli.app", "csv", "import", "hierarchy",
        v3_project_id, str(hierarchy_csv)
    ]
    output = run_command(command, cwd=Path(__file__).parent.parent.parent / "v3", env=v3_env)
    assert "Implementation pending in Phase 2" in output # TODO: Update this assertion once V3 hierarchy import is fully implemented



@pytest.mark.end_to_end
def test_v3_mapping_import(v3_project):
    """Tests the import of the mapping CSV."""
    v3_project_id, v3_env = v3_project
    print(f"--- Testing Mapping Import for project {v3_project_id} ---")
    mapping_csv = E2E_DATA_DIR / "e2e_mapping.csv"
    command = [
        "python", "-m", "src.cli.app", "csv", "import", "mapping",
        v3_project_id, str(mapping_csv)
    ]
    output = run_command(command, cwd=Path(__file__).parent.parent.parent / "v3", env=v3_env)
    assert "Implementation pending in Phase 2" in output # TODO: Update this assertion once V3 mapping import is fully implemented



@pytest.mark.end_to_end
@pytest.mark.skip(reason="V4 analytics test requires a configured live data connection.")
def test_v4_analytical_query(v3_project):
    """
    Tests the V4 analytical query functionality.

    NOTE: This test is skipped by default because it requires significant setup.
    To run this test, you would need to:
    1. Ensure the `e2e_transactions.csv` data is loaded into a database that V4 can connect to.
    2. Configure a connection in V4 named 'e2e_db' that points to this database.
    3. Ensure the V4 knowledge base is synced with the V3 project created in this test.
    """
    print("--- Testing V4 Analytical Query ---")
    
    # This is the question we want to ask the V4 engine
    question = "What was our total Product Revenue in January 2024?"
    
    # The command to run the query
    command = [
        "python", str(V4_MAIN_PATH), "query", "ask", "e2e_db", question
    ]
    
    # Run the command and capture the output
    output = run_command(command)
    
    # Assert that the output contains the correct calculated value
    # Based on the sample data, the sum of product revenue in Jan 2024 is 15000 + 8500 = 23500
    assert "23500" in output
    print("V4 analytics test passed with expected result.")

# --- How to Run This Test ---
#
# 1. Make sure you have pytest and pytest-cov installed:
#    pip install pytest pytest-cov
#
# 2. Navigate to the root of the Databridge_AI project.
#
# 3. Run pytest, pointing it to this test file:
#    pytest Gemini/e2e_test/test_end_to_end.py -v
#
# 4. To run the skipped V4 test, you would first need to set up the environment
#    as described in the test's docstring, and then run pytest with:
#    pytest Gemini/e2e_test/test_end_to_end.py -v -k "not test_v4_analytical_query" to run all but the skipped test
#    or by removing the @pytest.mark.skip decorator.
#
