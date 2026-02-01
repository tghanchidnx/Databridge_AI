# DataBridge AI Tests
"""Test package initialization - sets up Python path for test imports."""
import os
import sys

# Add tests directory to path so test_helpers can be imported
_tests_dir = os.path.dirname(__file__)
if _tests_dir not in sys.path:
    sys.path.insert(0, _tests_dir)

# Add src directory to path for imports
_src_dir = os.path.join(os.path.dirname(__file__), "..", "src")
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)
