"""DataBridge AI Community Edition.

An open-source, MCP-native data reconciliation engine.

This package provides essential tools for:
- Data Reconciliation (load, compare, profile)
- Fuzzy Matching
- PDF/OCR Extraction
- dbt Integration (basic)
- Data Quality

For advanced features like Cortex AI, Wright Pipeline, GraphRAG, and more,
upgrade to DataBridge AI Pro: https://databridge.ai/pro
"""

__version__ = "0.39.0"
__edition__ = "Community"

from .server import mcp
from .config import settings

__all__ = [
    '__version__',
    '__edition__',
    'mcp',
    'settings',
]
