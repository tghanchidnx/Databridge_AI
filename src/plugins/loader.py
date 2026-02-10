"""Universal Plugin Loader for DataBridge AI.

Replaces hardcoded Phase blocks in server.py with dynamic plugin discovery.
Each plugin is a directory with a plugin.json manifest and a mcp_tools.py module.

Discovery order:
1. src/ — built-in modules (each src/<name>/mcp_tools.py)
2. plugins/ — community plugins
3. private_plugins/ — proprietary extensions
4. ~/.databridge/plugins/ — user-installed plugins
5. $DATABRIDGE_PLUGINS_PATH — env override
"""
import importlib
import importlib.util
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


# Argument pattern types for register functions
ARG_PATTERN_MCP_ONLY = "mcp_only"
ARG_PATTERN_MCP_SETTINGS = "mcp_settings"
ARG_PATTERN_MCP_DATA_DIR = "mcp_data_dir"
ARG_PATTERN_MCP_BACKEND = "mcp_backend"
ARG_PATTERN_TEMPLATES = "templates"


def _read_manifest(plugin_dir: Path) -> Optional[Dict]:
    """Read and parse a plugin.json manifest from a directory.

    Args:
        plugin_dir: Directory containing plugin.json

    Returns:
        Parsed manifest dict or None if not found/invalid.
    """
    manifest_path = plugin_dir / "plugin.json"
    if not manifest_path.exists():
        return None
    try:
        with open(manifest_path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _import_module(module_dir: Path, module_file: str = "mcp_tools.py"):
    """Import a Python module from a file path, handling dual-import patterns.

    Tries src.X.mcp_tools first, then X.mcp_tools, then direct file import.

    Args:
        module_dir: Directory containing the module file
        module_file: Python file to import (default: mcp_tools.py)

    Returns:
        The imported module object.

    Raises:
        ImportError: If module cannot be imported.
    """
    file_path = module_dir / module_file
    if not file_path.exists():
        raise ImportError(f"Module file not found: {file_path}")

    # Determine the module name for import
    dir_name = module_dir.name
    parent_name = module_dir.parent.name if module_dir.parent.name != "src" else None
    stem = Path(module_file).stem

    # Build potential import paths
    if parent_name and parent_name != "src":
        # Nested module like src/agents/unified_agent or src/cortex_agent/analyst_tools
        import_paths = [
            f"src.{parent_name}.{dir_name}.{stem}",
            f"{parent_name}.{dir_name}.{stem}",
        ]
    else:
        import_paths = [
            f"src.{dir_name}.{stem}",
            f"{dir_name}.{stem}",
        ]

    # Try standard imports first
    for import_path in import_paths:
        try:
            return importlib.import_module(import_path)
        except (ImportError, ModuleNotFoundError):
            continue

    # Fallback: direct file import
    spec = importlib.util.spec_from_file_location(
        f"databridge_plugin_{dir_name}_{stem}", str(file_path)
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot create module spec for {file_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _call_register_function(module, func_name: str, mcp: Any,
                             settings: Any, arg_pattern: str,
                             context: Dict[str, Any]) -> Any:
    """Call a register function with the correct arguments based on its pattern.

    Args:
        module: The imported module
        func_name: Name of the registration function
        mcp: FastMCP instance
        settings: Application settings object
        arg_pattern: One of the ARG_PATTERN_* constants
        context: Extra context (hierarchy_service, etc.)

    Returns:
        Whatever the register function returns.
    """
    func = getattr(module, func_name)

    if arg_pattern == ARG_PATTERN_MCP_ONLY:
        return func(mcp)

    elif arg_pattern == ARG_PATTERN_MCP_SETTINGS:
        return func(mcp, settings)

    elif arg_pattern == ARG_PATTERN_MCP_DATA_DIR:
        return func(mcp, str(settings.data_dir))

    elif arg_pattern == ARG_PATTERN_MCP_BACKEND:
        return func(mcp, settings.nestjs_backend_url, settings.nestjs_api_key)

    elif arg_pattern == ARG_PATTERN_TEMPLATES:
        hierarchy_service = context.get("hierarchy_service")
        return func(
            mcp,
            templates_dir="templates",
            skills_dir="skills",
            kb_dir="knowledge_base",
            hierarchy_service=hierarchy_service,
        )

    else:
        # Default: try (mcp, settings), fallback to (mcp)
        try:
            return func(mcp, settings)
        except TypeError:
            return func(mcp)


def discover_plugins(plugin_dirs: List[Path]) -> List[Dict]:
    """Discover all plugins with manifests from given directories.

    Args:
        plugin_dirs: List of directories to scan for plugins.

    Returns:
        List of manifest dicts, sorted by phase number.
    """
    discovered = []
    seen_names = set()

    for base_dir in plugin_dirs:
        if not base_dir.exists():
            continue

        for item in sorted(base_dir.iterdir()):
            if not item.is_dir():
                continue
            if item.name.startswith("_") or item.name.startswith("."):
                continue

            manifest = _read_manifest(item)
            if manifest is None:
                continue

            name = manifest.get("name", item.name)
            if name in seen_names:
                continue
            seen_names.add(name)

            # Attach the directory path to the manifest
            manifest["_dir"] = item
            discovered.append(manifest)

            # Handle sub_plugins (e.g. cortex_analyst inside cortex_agent dir)
            for sub in manifest.get("sub_plugins", []):
                sub_name = sub.get("name", "")
                if sub_name and sub_name not in seen_names:
                    seen_names.add(sub_name)
                    # Sub-plugins inherit parent dir and tier
                    sub["_dir"] = item
                    sub.setdefault("tier", manifest.get("tier", "CE"))
                    discovered.append(sub)

            # Also check subdirectories for nested plugins (e.g. agents/unified_agent/)
            for sub_item in sorted(item.iterdir()):
                if not sub_item.is_dir():
                    continue
                if sub_item.name.startswith("_") or sub_item.name.startswith("."):
                    continue
                sub_manifest = _read_manifest(sub_item)
                if sub_manifest is None:
                    continue
                sub_name = sub_manifest.get("name", sub_item.name)
                if sub_name in seen_names:
                    continue
                seen_names.add(sub_name)
                sub_manifest["_dir"] = sub_item
                discovered.append(sub_manifest)

    # Sort by phase number for deterministic load order
    discovered.sort(key=lambda m: m.get("phase", 999))
    return discovered


def load_all_plugins(
    mcp: Any,
    settings: Any,
    plugin_dirs: Optional[List[Path]] = None,
    context: Optional[Dict[str, Any]] = None,
    license_manager: Any = "AUTO",
) -> Dict[str, Dict[str, Any]]:
    """Discover and load all plugins from given directories.

    1. Scan each plugin_dir for subdirectories with plugin.json
    2. Read plugin.json for metadata (tier, name, etc.)
    3. Check license tier before loading
    4. Call register_*_tools(mcp, ...) with correct arguments
    5. Return results dict

    Args:
        mcp: FastMCP instance
        settings: Application settings object
        plugin_dirs: Directories to scan. Defaults to standard locations.
        context: Extra context dict (e.g. hierarchy_service for templates).

    Returns:
        Dict mapping plugin names to result dicts with keys:
        - loaded (bool)
        - tools (int) — declared tool count
        - skipped (bool) — True if license-gated
        - tier (str) — required tier
        - error (str) — error message if failed
    """
    if context is None:
        context = {}

    if plugin_dirs is None:
        src_dir = Path(__file__).parent.parent
        project_root = src_dir.parent
        plugin_dirs = [
            src_dir,
            project_root / "plugins",
            project_root / "private_plugins",
            Path.home() / ".databridge" / "plugins",
        ]
        # Environment override
        env_path = os.environ.get("DATABRIDGE_PLUGINS_PATH")
        if env_path:
            plugin_dirs.append(Path(env_path))

    # Resolve license manager
    if license_manager == "AUTO":
        try:
            from src.plugins import get_license_manager
        except ImportError:
            from plugins import get_license_manager
        license_mgr = get_license_manager()
    else:
        license_mgr = license_manager

    # Discover all plugins
    plugins = discover_plugins(plugin_dirs)
    results = {}

    for manifest in plugins:
        name = manifest.get("name", "unknown")
        tier = manifest.get("tier", "CE").upper()
        phase = manifest.get("phase", 999)
        tool_count = manifest.get("tools", 0)
        register_func_name = manifest.get("register_function", f"register_{name}_tools")
        arg_pattern = manifest.get("arg_pattern", ARG_PATTERN_MCP_SETTINGS)
        module_file = manifest.get("module_file", "mcp_tools.py")
        plugin_dir = manifest["_dir"]

        # Check license tier
        tier_levels = {"CE": 0, "PRO": 1, "ENTERPRISE": 2}
        required_level = tier_levels.get(tier, 0)

        if license_mgr is None:
            # No license manager = full mode (backward compat)
            can_load = True
        elif license_mgr.tier == "FULL":
            can_load = True
        else:
            can_load = license_mgr.tier_level >= required_level

        if not can_load:
            results[name] = {
                "loaded": False,
                "skipped": True,
                "tier": tier,
                "tools": tool_count,
                "phase": phase,
            }
            continue

        # Try to load the plugin
        try:
            module = _import_module(plugin_dir, module_file)
            if not hasattr(module, register_func_name):
                results[name] = {
                    "loaded": False,
                    "error": f"Function {register_func_name} not found in {module_file}",
                    "tools": 0,
                    "phase": phase,
                }
                continue

            return_value = _call_register_function(
                module, register_func_name, mcp, settings, arg_pattern, context
            )

            # Store return value in context for downstream plugins
            # (e.g. hierarchy_service used by templates)
            if name == "hierarchy" and return_value is not None:
                context["hierarchy_service"] = return_value

            results[name] = {
                "loaded": True,
                "tools": tool_count,
                "tier": tier,
                "phase": phase,
            }

        except ImportError as e:
            results[name] = {
                "loaded": False,
                "error": f"ImportError: {e}",
                "tools": 0,
                "phase": phase,
            }
        except Exception as e:
            results[name] = {
                "loaded": False,
                "error": f"{type(e).__name__}: {e}",
                "tools": 0,
                "phase": phase,
            }

    return results
