"""Pytest configuration for DataBridge AI V4 Analytics Engine tests."""

import sys
from pathlib import Path

import pytest

# Add the v4 src directory to the path
v4_root = Path(__file__).parent.parent
sys.path.insert(0, str(v4_root))
