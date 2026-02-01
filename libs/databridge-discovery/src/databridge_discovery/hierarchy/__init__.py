"""
Hierarchy extraction and processing modules.

This package provides tools for:
- Converting CASE statements to hierarchies
- Detecting hierarchy levels
- Inferring sort orders
- Merging with existing Librarian hierarchies
"""

from databridge_discovery.hierarchy.case_to_hierarchy import CaseToHierarchyConverter
from databridge_discovery.hierarchy.level_detector import LevelDetector
from databridge_discovery.hierarchy.sort_order_inferrer import SortOrderInferrer
from databridge_discovery.hierarchy.hierarchy_merger import HierarchyMerger

__all__ = [
    "CaseToHierarchyConverter",
    "LevelDetector",
    "SortOrderInferrer",
    "HierarchyMerger",
]
