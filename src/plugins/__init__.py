"""DataBridge AI Plugin System with License Management.

This module provides:
1. LicenseManager - Offline hash-based license validation
2. PluginRegistry - Dynamic plugin discovery and loading
3. Tier-based feature gating (CE, PRO, PRO_EXAMPLES, ENTERPRISE)

License Key Format: DB-{TIER}-{CUSTOMER_ID}-{EXPIRY}-{SIGNATURE}
Example: DB-PRO-ACME001-20270209-a1b2c3d4e5f6
"""
import os
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Callable, Any
from functools import wraps

# License tiers in order of capability
TIERS = {
    'CE': 0,        # Community Edition (free)
    'PRO': 1,       # Professional (licensed)
    'ENTERPRISE': 2 # Enterprise (custom)
}

# Tool tier assignments - modules classified by tier
CE_MODULES = {
    'file_discovery',
    'data_loading',
    'data_profiling',
    'hashing',
    'fuzzy_matching',
    'pdf_ocr',
    'workflow',
    'transform',
    'documentation',
    'templates_basic',
    'diff_utilities',
    'dbt_basic',
    'data_quality',
}

PRO_MODULES = {
    'hierarchy',
    'connections',
    'schema_matcher',
    'data_matcher',
    'orchestrator',
    'cortex_agent',
    'cortex_analyst',
    'wright',
    'lineage',
    'data_catalog',
    'versioning',
    'graphrag',
    'observability',
    'console_dashboard',
    'faux_objects',
    'git_integration',
    'templates_advanced',
    'datashield',
}

ENTERPRISE_MODULES = {
    'custom_agents',
    'white_label',
    'sla_support',
}

# Pro Examples sub-tier: tests & use-case tutorials (separate pip package)
PRO_EXAMPLES_MODULES = {
    'examples_beginner',       # Use cases 01-04 (pizza, friends, school, sports)
    'examples_financial',      # Use cases 05-11 (SEC EDGAR / Apple / Microsoft)
    'examples_faux_objects',   # Use cases 12-19 (domain personas)
    'tests_ce',                # Tests for CE modules
    'tests_pro',               # Tests for Pro modules
    'test_fixtures',           # Shared conftest.py, sample data
}


class LicenseManager:
    """Manages license validation for DataBridge AI tiers.

    Uses offline hash-based validation - no external server required.
    License keys are validated against a secret salt that must be kept private.
    """

    # IMPORTANT: Change this in production and keep it secret!
    # This should be loaded from a secure location, not hardcoded
    _SECRET = os.environ.get('DATABRIDGE_LICENSE_SECRET', 'databridge-ai-2024-secret-salt')

    def __init__(self):
        """Initialize LicenseManager and validate any existing license."""
        self._license_key = os.environ.get('DATABRIDGE_LICENSE_KEY', '')
        self._tier = 'CE'
        self._customer_id = ''
        self._expiry_date: Optional[datetime] = None
        self._valid = False
        self._validation_message = ''

        # Auto-validate on init
        if self._license_key:
            self._validate()
        else:
            self._validation_message = 'No license key provided - running in Community Edition mode'

    def _validate(self) -> bool:
        """Validate the license key using offline hash verification.

        Key format: DB-{TIER}-{CUSTOMER_ID}-{EXPIRY}-{SIGNATURE}
        - TIER: CE, PRO, or ENTERPRISE
        - CUSTOMER_ID: 4-12 alphanumeric chars
        - EXPIRY: YYYYMMDD format
        - SIGNATURE: First 12 chars of SHA256 hash

        Returns:
            bool: True if license is valid
        """
        try:
            # Parse key format
            parts = self._license_key.split('-')
            if len(parts) != 5:
                self._validation_message = 'Invalid license key format'
                return False

            prefix, tier, customer_id, expiry, signature = parts

            # Validate prefix
            if prefix != 'DB':
                self._validation_message = 'Invalid license key prefix'
                return False

            # Validate tier
            if tier not in TIERS:
                self._validation_message = f'Invalid tier: {tier}'
                return False

            # Validate expiry date
            try:
                expiry_date = datetime.strptime(expiry, '%Y%m%d')
            except ValueError:
                self._validation_message = 'Invalid expiry date format'
                return False

            # Check if expired
            if datetime.now() > expiry_date:
                self._validation_message = f'License expired on {expiry_date.strftime("%Y-%m-%d")}'
                self._tier = 'CE'  # Fall back to CE
                return False

            # Verify signature
            payload = f"{tier}-{customer_id}-{expiry}-{self._SECRET}"
            expected_sig = hashlib.sha256(payload.encode()).hexdigest()[:12]

            if signature != expected_sig:
                self._validation_message = 'Invalid license signature'
                return False

            # License is valid!
            self._tier = tier
            self._customer_id = customer_id
            self._expiry_date = expiry_date
            self._valid = True
            self._validation_message = f'Valid {tier} license for {customer_id} (expires {expiry_date.strftime("%Y-%m-%d")})'

            return True

        except Exception as e:
            self._validation_message = f'License validation error: {str(e)}'
            return False

    @property
    def tier(self) -> str:
        """Get the current license tier."""
        return self._tier

    @property
    def tier_level(self) -> int:
        """Get the numeric tier level (0=CE, 1=PRO, 2=ENTERPRISE)."""
        return TIERS.get(self._tier, 0)

    @property
    def customer_id(self) -> str:
        """Get the customer ID from the license."""
        return self._customer_id

    @property
    def expiry_date(self) -> Optional[datetime]:
        """Get the license expiry date."""
        return self._expiry_date

    @property
    def is_valid(self) -> bool:
        """Check if a valid license is loaded."""
        return self._valid

    @property
    def validation_message(self) -> str:
        """Get the validation result message."""
        return self._validation_message

    def is_ce(self) -> bool:
        """Check if running in Community Edition mode."""
        return self._tier == 'CE'

    def is_pro(self) -> bool:
        """Check if Pro features are available."""
        return TIERS.get(self._tier, 0) >= TIERS['PRO']

    def is_enterprise(self) -> bool:
        """Check if Enterprise features are available."""
        return self._tier == 'ENTERPRISE'

    def is_pro_examples(self) -> bool:
        """Check if Pro Examples (tests & use cases) are available.

        Requires a Pro or higher license.
        """
        return self.is_pro()

    def can_use_module(self, module_name: str) -> bool:
        """Check if the current license allows using a specific module.

        Args:
            module_name: The module identifier to check

        Returns:
            bool: True if the module is available at current tier
        """
        # CE modules always available
        if module_name in CE_MODULES:
            return True

        # PRO modules require PRO or higher
        if module_name in PRO_MODULES:
            return self.is_pro()

        # PRO_EXAMPLES modules require PRO or higher
        if module_name in PRO_EXAMPLES_MODULES:
            return self.is_pro_examples()

        # ENTERPRISE modules require ENTERPRISE
        if module_name in ENTERPRISE_MODULES:
            return self.is_enterprise()

        # Unknown modules default to CE (available)
        return True

    def get_available_modules(self) -> List[str]:
        """Get list of modules available at current tier."""
        available = list(CE_MODULES)

        if self.is_pro():
            available.extend(PRO_MODULES)

        if self.is_pro_examples():
            available.extend(PRO_EXAMPLES_MODULES)

        if self.is_enterprise():
            available.extend(ENTERPRISE_MODULES)

        return sorted(available)

    def get_status(self) -> Dict[str, Any]:
        """Get full license status as a dictionary."""
        return {
            'tier': self._tier,
            'tier_level': self.tier_level,
            'customer_id': self._customer_id,
            'expiry_date': self._expiry_date.isoformat() if self._expiry_date else None,
            'is_valid': self._valid,
            'message': self._validation_message,
            'available_modules': self.get_available_modules(),
        }


def require_tier(required_tier: str):
    """Decorator to gate tool functions by license tier.

    Usage:
        @require_tier('PRO')
        def my_pro_tool():
            ...

    Args:
        required_tier: Minimum tier required ('CE', 'PRO', 'ENTERPRISE')
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get the global license manager
            mgr = get_license_manager()
            required_level = TIERS.get(required_tier, 0)

            if mgr.tier_level < required_level:
                return {
                    'error': f'This feature requires {required_tier} license',
                    'current_tier': mgr.tier,
                    'upgrade_info': 'Contact sales@databridge.ai to upgrade'
                }

            return func(*args, **kwargs)

        # Mark the function with its tier requirement
        wrapper._required_tier = required_tier
        return wrapper

    return decorator


# Global license manager instance
_license_manager: Optional[LicenseManager] = None


def get_license_manager() -> LicenseManager:
    """Get or create the global LicenseManager instance."""
    global _license_manager
    if _license_manager is None:
        _license_manager = LicenseManager()
    return _license_manager


def reset_license_manager() -> LicenseManager:
    """Reset and reinitialize the license manager (useful for testing)."""
    global _license_manager
    _license_manager = LicenseManager()
    return _license_manager


# Plugin loader - lazy import to avoid circular dependencies
def load_all_plugins(mcp, settings, plugin_dirs=None, context=None, license_manager="AUTO"):
    """Load all plugins via the universal plugin loader.

    See src/plugins/loader.py for full documentation.
    """
    from .loader import load_all_plugins as _load
    return _load(mcp, settings, plugin_dirs=plugin_dirs, context=context,
                 license_manager=license_manager)


# Export public API
__all__ = [
    'LicenseManager',
    'get_license_manager',
    'reset_license_manager',
    'require_tier',
    'load_all_plugins',
    'TIERS',
    'CE_MODULES',
    'PRO_MODULES',
    'PRO_EXAMPLES_MODULES',
    'ENTERPRISE_MODULES',
]
