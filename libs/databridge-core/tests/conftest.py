"""
Pytest configuration and fixtures for databridge-core tests.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock


@pytest.fixture
def tmp_git_repo(tmp_path):
    """Create a temporary directory with a .git folder to simulate a git repo."""
    git_dir = tmp_path / ".git"
    git_dir.mkdir()
    return tmp_path


@pytest.fixture
def mock_subprocess_success():
    """Create a mock for successful subprocess.run calls."""
    import subprocess

    def _make_mock(stdout="", stderr=""):
        return subprocess.CompletedProcess(
            args=["git", "test"],
            returncode=0,
            stdout=stdout,
            stderr=stderr,
        )

    return _make_mock


@pytest.fixture
def mock_subprocess_failure():
    """Create a mock for failed subprocess.run calls."""
    import subprocess

    def _make_mock(stderr="error"):
        return subprocess.CompletedProcess(
            args=["git", "test"],
            returncode=1,
            stdout="",
            stderr=stderr,
        )

    return _make_mock
