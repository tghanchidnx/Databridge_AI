"""DataBridge AI Pro - Advanced features for enterprise data management.

This package extends DataBridge AI Community Edition with:
- Cortex AI Agent (Snowflake Cortex integration)
- Wright Pipeline (4-object data mart factory)
- GraphRAG Engine (anti-hallucination layer)
- Data Observability (metrics, alerting, health monitoring)
- Full Data Catalog (metadata registry with lineage)
- Advanced Hierarchy Management
- AI Orchestrator (multi-agent coordination)

License: Proprietary - Requires valid license key
"""
import os
from typing import Any, Optional

__version__ = "0.40.0"
__edition__ = "Pro"

# Module availability flags
CORTEX_AVAILABLE = False
WRIGHT_AVAILABLE = False
GRAPHRAG_AVAILABLE = False
OBSERVABILITY_AVAILABLE = False
CATALOG_AVAILABLE = False
LINEAGE_AVAILABLE = False
ORCHESTRATOR_AVAILABLE = False


def validate_license() -> bool:
    """Validate the Pro license key.

    Returns:
        True if license is valid for Pro features
    """
    try:
        # Import from CE package
        from src.plugins import get_license_manager
        mgr = get_license_manager()
        return mgr.is_pro()
    except ImportError:
        # Fallback: check environment variable directly
        license_key = os.environ.get('DATABRIDGE_LICENSE_KEY', '')
        if not license_key:
            return False

        # Basic validation - key should start with DB-PRO or DB-ENTERPRISE
        parts = license_key.split('-')
        if len(parts) >= 2:
            return parts[1] in ('PRO', 'ENTERPRISE')

        return False


def register_pro_tools(mcp_instance: Any) -> bool:
    """Register all Pro tools with the MCP server.

    Args:
        mcp_instance: FastMCP instance to register tools with

    Returns:
        True if Pro tools were registered successfully
    """
    if not validate_license():
        print("[PRO] License validation failed - Pro features disabled")
        return False

    registered = 0

    # Register Cortex AI tools
    try:
        from .cortex import register_cortex_tools
        register_cortex_tools(mcp_instance)
        global CORTEX_AVAILABLE
        CORTEX_AVAILABLE = True
        registered += 1
        print("[PRO] Cortex AI Agent loaded")
    except ImportError as e:
        print(f"[PRO] Cortex AI Agent not available: {e}")

    # Register Wright Pipeline tools
    try:
        from .wright import register_wright_tools
        register_wright_tools(mcp_instance)
        global WRIGHT_AVAILABLE
        WRIGHT_AVAILABLE = True
        registered += 1
        print("[PRO] Wright Pipeline loaded")
    except ImportError as e:
        print(f"[PRO] Wright Pipeline not available: {e}")

    # Register GraphRAG tools
    try:
        from .graphrag import register_graphrag_tools
        register_graphrag_tools(mcp_instance)
        global GRAPHRAG_AVAILABLE
        GRAPHRAG_AVAILABLE = True
        registered += 1
        print("[PRO] GraphRAG Engine loaded")
    except ImportError as e:
        print(f"[PRO] GraphRAG Engine not available: {e}")

    # Register Observability tools
    try:
        from .observability import register_observability_tools
        register_observability_tools(mcp_instance)
        global OBSERVABILITY_AVAILABLE
        OBSERVABILITY_AVAILABLE = True
        registered += 1
        print("[PRO] Data Observability loaded")
    except ImportError as e:
        print(f"[PRO] Data Observability not available: {e}")

    # Register Data Catalog tools
    try:
        from .catalog import register_catalog_tools
        register_catalog_tools(mcp_instance)
        global CATALOG_AVAILABLE
        CATALOG_AVAILABLE = True
        registered += 1
        print("[PRO] Data Catalog loaded")
    except ImportError as e:
        print(f"[PRO] Data Catalog not available: {e}")

    # Register Lineage tools
    try:
        from .lineage import register_lineage_tools
        register_lineage_tools(mcp_instance)
        global LINEAGE_AVAILABLE
        LINEAGE_AVAILABLE = True
        registered += 1
        print("[PRO] Column Lineage loaded")
    except ImportError as e:
        print(f"[PRO] Column Lineage not available: {e}")

    # Register Orchestrator tools
    try:
        from .orchestrator import register_orchestrator_tools
        register_orchestrator_tools(mcp_instance)
        global ORCHESTRATOR_AVAILABLE
        ORCHESTRATOR_AVAILABLE = True
        registered += 1
        print("[PRO] AI Orchestrator loaded")
    except ImportError as e:
        print(f"[PRO] AI Orchestrator not available: {e}")

    # Register Hierarchy (advanced) tools
    try:
        from .hierarchy import register_hierarchy_tools
        register_hierarchy_tools(mcp_instance)
        registered += 1
        print("[PRO] Advanced Hierarchy loaded")
    except ImportError as e:
        print(f"[PRO] Advanced Hierarchy not available: {e}")

    # Register Connections tools
    try:
        from .connections import register_connections_tools
        register_connections_tools(mcp_instance)
        registered += 1
        print("[PRO] Backend Connections loaded")
    except ImportError as e:
        print(f"[PRO] Backend Connections not available: {e}")

    print(f"[PRO] Loaded {registered} Pro modules")
    return registered > 0


def get_pro_status() -> dict:
    """Get status of Pro features.

    Returns:
        Dict with Pro feature availability
    """
    return {
        "version": __version__,
        "edition": __edition__,
        "license_valid": validate_license(),
        "features": {
            "cortex_ai": CORTEX_AVAILABLE,
            "wright_pipeline": WRIGHT_AVAILABLE,
            "graphrag": GRAPHRAG_AVAILABLE,
            "observability": OBSERVABILITY_AVAILABLE,
            "data_catalog": CATALOG_AVAILABLE,
            "lineage": LINEAGE_AVAILABLE,
            "orchestrator": ORCHESTRATOR_AVAILABLE,
        }
    }


# Public API
__all__ = [
    '__version__',
    '__edition__',
    'validate_license',
    'register_pro_tools',
    'get_pro_status',
    'CORTEX_AVAILABLE',
    'WRIGHT_AVAILABLE',
    'GRAPHRAG_AVAILABLE',
    'OBSERVABILITY_AVAILABLE',
    'CATALOG_AVAILABLE',
    'LINEAGE_AVAILABLE',
    'ORCHESTRATOR_AVAILABLE',
]
