"""Pytest fixtures for DataBridge AI tests."""
import pytest
import pandas as pd
import tempfile
import os
import sys
from pathlib import Path

# Add tests directory to path so test_helpers can be imported
sys.path.insert(0, os.path.dirname(__file__))
# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def sample_csv_a(temp_dir):
    """Create a sample CSV file A (source of truth)."""
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "amount": [100.0, 200.0, 300.0, 400.0, 500.0],
        "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
    })
    path = os.path.join(temp_dir, "source_a.csv")
    df.to_csv(path, index=False)
    return path


@pytest.fixture
def sample_csv_b(temp_dir):
    """Create a sample CSV file B (target with differences)."""
    df = pd.DataFrame({
        "id": [1, 2, 3, 6, 7],  # 4,5 missing; 6,7 added
        "name": ["Alice", "Bobby", "Charlie", "Frank", "Grace"],  # Bob -> Bobby (conflict)
        "amount": [100.0, 250.0, 300.0, 600.0, 700.0],  # 200 -> 250 (conflict)
        "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-06", "2024-01-07"]
    })
    path = os.path.join(temp_dir, "source_b.csv")
    df.to_csv(path, index=False)
    return path


@pytest.fixture
def sample_json(temp_dir):
    """Create a sample JSON file."""
    import json
    data = [
        {"id": 1, "name": "Alice", "value": 100},
        {"id": 2, "name": "Bob", "value": 200},
        {"id": 3, "name": "Charlie", "value": 300}
    ]
    path = os.path.join(temp_dir, "data.json")
    with open(path, "w") as f:
        json.dump(data, f)
    return path


@pytest.fixture
def fuzzy_csv(temp_dir):
    """Create CSV with fuzzy-matchable data."""
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "company": [
            "Microsoft Corporation",
            "Apple Inc.",
            "Google LLC",
            "Amazon.com Inc.",
            "Meta Platforms"
        ]
    })
    path = os.path.join(temp_dir, "fuzzy.csv")
    df.to_csv(path, index=False)
    return path


@pytest.fixture
def fuzzy_csv_variants(temp_dir):
    """Create CSV with name variants for fuzzy matching."""
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "company": [
            "Microsoft Corp",
            "Apple Incorporated",
            "Alphabet/Google",
            "Amazon Inc",
            "Facebook/Meta"
        ]
    })
    path = os.path.join(temp_dir, "fuzzy_variants.csv")
    df.to_csv(path, index=False)
    return path
