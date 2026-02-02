"""Pytest configuration for DataBridge AI Researcher Analytics Engine tests."""

import sys
from pathlib import Path

import pytest

# Add the researcher src directory to the path
researcher_root = Path(__file__).parent.parent
sys.path.insert(0, str(researcher_root))
