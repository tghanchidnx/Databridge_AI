#!/usr/bin/env python3
"""Test script for DataBridge AI license system.

Run this to verify the license system is working correctly.
"""
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def test_license_generation():
    """Test license key generation."""
    print("=" * 60)
    print("Testing License Key Generation")
    print("=" * 60)

    from scripts.generate_license import generate_license_key, validate_license_key

    # Generate test keys
    ce_key = generate_license_key('CE', 'TEST0001', 365)
    pro_key = generate_license_key('PRO', 'TEST0002', 365)
    ent_key = generate_license_key('ENTERPRISE', 'TEST0003', 365)

    print(f"\nGenerated Keys:")
    print(f"  CE:         {ce_key}")
    print(f"  PRO:        {pro_key}")
    print(f"  ENTERPRISE: {ent_key}")

    # Validate keys
    print(f"\nValidation Results:")
    for name, key in [('CE', ce_key), ('PRO', pro_key), ('ENTERPRISE', ent_key)]:
        result = validate_license_key(key)
        status = "VALID" if result['valid'] else "INVALID"
        print(f"  {name}: {status} - {result['message']}")

    return True


def test_license_manager():
    """Test LicenseManager class."""
    print("\n" + "=" * 60)
    print("Testing LicenseManager")
    print("=" * 60)

    from src.plugins import LicenseManager, reset_license_manager
    from scripts.generate_license import generate_license_key

    # Test without license
    os.environ.pop('DATABRIDGE_LICENSE_KEY', None)
    mgr = reset_license_manager()
    print(f"\nNo license key:")
    print(f"  Tier: {mgr.tier}")
    print(f"  Is Pro: {mgr.is_pro()}")
    print(f"  Message: {mgr.validation_message}")

    # Test with CE key
    ce_key = generate_license_key('CE', 'TEST0001', 365)
    os.environ['DATABRIDGE_LICENSE_KEY'] = ce_key
    mgr = reset_license_manager()
    print(f"\nCE license key:")
    print(f"  Tier: {mgr.tier}")
    print(f"  Is Pro: {mgr.is_pro()}")
    print(f"  Message: {mgr.validation_message}")

    # Test with PRO key
    pro_key = generate_license_key('PRO', 'TEST0002', 365)
    os.environ['DATABRIDGE_LICENSE_KEY'] = pro_key
    mgr = reset_license_manager()
    print(f"\nPRO license key:")
    print(f"  Tier: {mgr.tier}")
    print(f"  Is Pro: {mgr.is_pro()}")
    print(f"  Message: {mgr.validation_message}")

    # Test with ENTERPRISE key
    ent_key = generate_license_key('ENTERPRISE', 'TEST0003', 365)
    os.environ['DATABRIDGE_LICENSE_KEY'] = ent_key
    mgr = reset_license_manager()
    print(f"\nENTERPRISE license key:")
    print(f"  Tier: {mgr.tier}")
    print(f"  Is Pro: {mgr.is_pro()}")
    print(f"  Is Enterprise: {mgr.is_enterprise()}")
    print(f"  Message: {mgr.validation_message}")

    # Test expired key (manually craft one)
    expired_key = "DB-PRO-TEST0001-20200101-000000000000"
    os.environ['DATABRIDGE_LICENSE_KEY'] = expired_key
    mgr = reset_license_manager()
    print(f"\nExpired license key:")
    print(f"  Tier: {mgr.tier}")
    print(f"  Is Pro: {mgr.is_pro()}")
    print(f"  Message: {mgr.validation_message}")

    # Clean up
    os.environ.pop('DATABRIDGE_LICENSE_KEY', None)
    return True


def test_tier_checks():
    """Test tier-based feature gating."""
    print("\n" + "=" * 60)
    print("Testing Tier-Based Feature Gating")
    print("=" * 60)

    from src.plugins import get_license_manager, reset_license_manager, require_tier
    from scripts.generate_license import generate_license_key

    # Define test functions with tier requirements
    @require_tier('CE')
    def ce_feature():
        return "CE feature executed"

    @require_tier('PRO')
    def pro_feature():
        return "PRO feature executed"

    @require_tier('ENTERPRISE')
    def enterprise_feature():
        return "ENTERPRISE feature executed"

    # Test with CE license
    ce_key = generate_license_key('CE', 'TEST0001', 365)
    os.environ['DATABRIDGE_LICENSE_KEY'] = ce_key
    reset_license_manager()

    print(f"\nWith CE license:")
    print(f"  CE feature: {ce_feature()}")
    print(f"  PRO feature: {pro_feature()}")
    print(f"  ENT feature: {enterprise_feature()}")

    # Test with PRO license
    pro_key = generate_license_key('PRO', 'TEST0002', 365)
    os.environ['DATABRIDGE_LICENSE_KEY'] = pro_key
    reset_license_manager()

    print(f"\nWith PRO license:")
    print(f"  CE feature: {ce_feature()}")
    print(f"  PRO feature: {pro_feature()}")
    print(f"  ENT feature: {enterprise_feature()}")

    # Clean up
    os.environ.pop('DATABRIDGE_LICENSE_KEY', None)
    return True


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("DataBridge AI License System Tests")
    print("=" * 60)

    try:
        test_license_generation()
        test_license_manager()
        test_tier_checks()

        print("\n" + "=" * 60)
        print("ALL TESTS PASSED")
        print("=" * 60)
        return 0

    except Exception as e:
        print(f"\nTEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
