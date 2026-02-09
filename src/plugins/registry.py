"""Plugin Registry for DataBridge AI.

Handles dynamic discovery, loading, and management of tool plugins.
Supports both public (CE) and private (Pro/Enterprise) plugin directories.
"""
import importlib
import importlib.util
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any, Set
from dataclasses import dataclass, field

from . import get_license_manager, CE_MODULES, PRO_MODULES, ENTERPRISE_MODULES


@dataclass
class PluginInfo:
    """Information about a registered plugin."""
    name: str
    module_path: str
    tier: str  # CE, PRO, ENTERPRISE
    tools: List[str] = field(default_factory=list)
    loaded: bool = False
    error: Optional[str] = None


class PluginRegistry:
    """Registry for discovering and managing DataBridge AI plugins.

    Plugins are organized by tier:
    - CE plugins: Always loaded (Community Edition)
    - PRO plugins: Loaded only with valid PRO+ license
    - ENTERPRISE plugins: Loaded only with ENTERPRISE license

    Plugin Structure:
        plugins/
        ├── ce/                    # Community Edition plugins
        │   ├── reconciliation/
        │   │   └── mcp_tools.py  # Must have register_tools(mcp)
        │   └── ...
        └── pro/                   # Pro plugins (loaded conditionally)
            ├── cortex/
            └── ...
    """

    def __init__(self, base_path: Optional[Path] = None):
        """Initialize the plugin registry.

        Args:
            base_path: Base path for plugin discovery. Defaults to src/plugins/
        """
        self.base_path = base_path or Path(__file__).parent
        self.plugins: Dict[str, PluginInfo] = {}
        self._registered_tools: Set[str] = set()
        self._license_mgr = get_license_manager()

    def discover_plugins(self) -> Dict[str, PluginInfo]:
        """Discover all available plugins in the plugin directories.

        Returns:
            Dict mapping plugin names to PluginInfo objects
        """
        discovered = {}

        # CE plugins directory
        ce_path = self.base_path / 'ce'
        if ce_path.exists():
            for plugin in self._scan_directory(ce_path, 'CE'):
                discovered[plugin.name] = plugin

        # PRO plugins directory
        pro_path = self.base_path / 'pro'
        if pro_path.exists():
            for plugin in self._scan_directory(pro_path, 'PRO'):
                discovered[plugin.name] = plugin

        # Enterprise plugins directory
        enterprise_path = self.base_path / 'enterprise'
        if enterprise_path.exists():
            for plugin in self._scan_directory(enterprise_path, 'ENTERPRISE'):
                discovered[plugin.name] = plugin

        # Also check external plugin directories
        external_paths = [
            Path(os.environ.get('DATABRIDGE_PLUGINS_PATH', '')),
            Path.home() / '.databridge' / 'plugins',
        ]
        for ext_path in external_paths:
            if ext_path.exists():
                for plugin in self._scan_directory(ext_path, 'EXTERNAL'):
                    discovered[plugin.name] = plugin

        self.plugins = discovered
        return discovered

    def _scan_directory(self, directory: Path, tier: str) -> List[PluginInfo]:
        """Scan a directory for plugin modules.

        Args:
            directory: Directory to scan
            tier: Tier assignment for discovered plugins

        Returns:
            List of discovered PluginInfo objects
        """
        plugins = []

        for item in directory.iterdir():
            if item.is_dir() and not item.name.startswith('_'):
                # Look for mcp_tools.py in the plugin directory
                mcp_tools_file = item / 'mcp_tools.py'
                if mcp_tools_file.exists():
                    plugins.append(PluginInfo(
                        name=item.name,
                        module_path=str(mcp_tools_file),
                        tier=tier,
                    ))

        return plugins

    def load_plugin(self, plugin_name: str, mcp_instance: Any) -> bool:
        """Load a specific plugin by name.

        Args:
            plugin_name: Name of the plugin to load
            mcp_instance: FastMCP instance to register tools with

        Returns:
            True if plugin loaded successfully
        """
        if plugin_name not in self.plugins:
            return False

        plugin = self.plugins[plugin_name]

        # Check license tier
        if not self._can_load_plugin(plugin):
            plugin.error = f'Requires {plugin.tier} license (current: {self._license_mgr.tier})'
            return False

        try:
            # Load the module dynamically
            spec = importlib.util.spec_from_file_location(
                f"databridge_plugins.{plugin_name}",
                plugin.module_path
            )
            if spec is None or spec.loader is None:
                plugin.error = 'Failed to create module spec'
                return False

            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)

            # Call register_tools if it exists
            if hasattr(module, 'register_tools'):
                tool_count_before = len(self._registered_tools)
                module.register_tools(mcp_instance)
                # Track new tools (approximate)
                plugin.loaded = True
                return True
            else:
                plugin.error = 'Module missing register_tools() function'
                return False

        except Exception as e:
            plugin.error = f'Load error: {str(e)}'
            return False

    def _can_load_plugin(self, plugin: PluginInfo) -> bool:
        """Check if current license allows loading a plugin.

        Args:
            plugin: Plugin to check

        Returns:
            True if plugin can be loaded
        """
        tier_levels = {'CE': 0, 'PRO': 1, 'ENTERPRISE': 2, 'EXTERNAL': 0}
        required_level = tier_levels.get(plugin.tier, 0)
        return self._license_mgr.tier_level >= required_level

    def load_all_plugins(self, mcp_instance: Any) -> Dict[str, bool]:
        """Load all discovered plugins that are available at current tier.

        Args:
            mcp_instance: FastMCP instance to register tools with

        Returns:
            Dict mapping plugin names to load success status
        """
        if not self.plugins:
            self.discover_plugins()

        results = {}
        for plugin_name in self.plugins:
            results[plugin_name] = self.load_plugin(plugin_name, mcp_instance)

        return results

    def get_loaded_plugins(self) -> List[str]:
        """Get list of successfully loaded plugin names."""
        return [name for name, info in self.plugins.items() if info.loaded]

    def get_available_plugins(self) -> List[str]:
        """Get list of plugins available at current license tier."""
        return [
            name for name, info in self.plugins.items()
            if self._can_load_plugin(info)
        ]

    def get_status(self) -> Dict[str, Any]:
        """Get full registry status."""
        return {
            'license_tier': self._license_mgr.tier,
            'plugins_discovered': len(self.plugins),
            'plugins_loaded': len(self.get_loaded_plugins()),
            'plugins_available': len(self.get_available_plugins()),
            'plugins': {
                name: {
                    'tier': info.tier,
                    'loaded': info.loaded,
                    'error': info.error,
                }
                for name, info in self.plugins.items()
            }
        }


def create_tier_gated_loader(tier: str) -> Callable:
    """Create a registration function that checks tier before loading.

    This is used to wrap module registration functions with tier checks.

    Args:
        tier: Required tier for the module

    Returns:
        A loader function that checks tier before calling register_tools
    """
    def loader(register_func: Callable) -> Callable:
        def gated_register(mcp_instance: Any, *args, **kwargs):
            mgr = get_license_manager()
            tier_levels = {'CE': 0, 'PRO': 1, 'ENTERPRISE': 2}

            if mgr.tier_level < tier_levels.get(tier, 0):
                print(f"[License] {tier} module skipped (current tier: {mgr.tier})")
                return None

            print(f"[License] Loading {tier} module...")
            return register_func(mcp_instance, *args, **kwargs)

        return gated_register
    return loader


# Convenience decorators
ce_module = create_tier_gated_loader('CE')
pro_module = create_tier_gated_loader('PRO')
enterprise_module = create_tier_gated_loader('ENTERPRISE')


# Global registry instance
_registry: Optional[PluginRegistry] = None


def get_registry() -> PluginRegistry:
    """Get or create the global plugin registry."""
    global _registry
    if _registry is None:
        _registry = PluginRegistry()
    return _registry


__all__ = [
    'PluginRegistry',
    'PluginInfo',
    'get_registry',
    'create_tier_gated_loader',
    'ce_module',
    'pro_module',
    'enterprise_module',
]
