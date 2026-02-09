"""Shared test fixtures for DataBridge AI examples test suite.

Provides common fixtures used across CE and Pro test modules.
"""
import pytest
from pathlib import Path


@pytest.fixture
def sample_data_dir():
    """Path to sample data files shipped with the examples package."""
    return Path(__file__).parent.parent / "use_cases"


@pytest.fixture
def temp_project(tmp_path):
    """Create a temporary project directory for test isolation."""
    project_dir = tmp_path / "test_project"
    project_dir.mkdir()
    return project_dir
