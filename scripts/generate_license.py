#!/usr/bin/env python3
"""License Key Generator for DataBridge AI.

This script generates offline-validated license keys for DataBridge AI.
Keep this script and the SECRET value private - do not distribute!

Usage:
    python generate_license.py PRO ACME001 365
    python generate_license.py ENTERPRISE BIGCORP 730 --output keys.txt

License Key Format: DB-{TIER}-{CUSTOMER_ID}-{EXPIRY}-{SIGNATURE}
"""
import argparse
import hashlib
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# CRITICAL: This secret must match the one in src/plugins/__init__.py
# Change this to your own secret and keep it private!
SECRET = os.environ.get('DATABRIDGE_LICENSE_SECRET', 'databridge-ai-2024-secret-salt')

VALID_TIERS = ['CE', 'PRO', 'ENTERPRISE']


def generate_license_key(
    tier: str,
    customer_id: str,
    days: int = 365,
    secret: str = SECRET
) -> str:
    """Generate a license key for a customer.

    Args:
        tier: License tier (CE, PRO, ENTERPRISE)
        customer_id: Customer identifier (4-12 alphanumeric chars)
        days: Number of days until expiry (default 365)
        secret: Secret salt for signature generation

    Returns:
        License key string in format: DB-{TIER}-{CUSTOMER_ID}-{EXPIRY}-{SIGNATURE}

    Raises:
        ValueError: If parameters are invalid
    """
    # Validate tier
    tier = tier.upper()
    if tier not in VALID_TIERS:
        raise ValueError(f"Invalid tier: {tier}. Must be one of {VALID_TIERS}")

    # Validate customer_id
    customer_id = customer_id.upper()
    if not customer_id.isalnum():
        raise ValueError("Customer ID must be alphanumeric")
    if len(customer_id) < 4 or len(customer_id) > 12:
        raise ValueError("Customer ID must be 4-12 characters")

    # Calculate expiry date
    if days < 1:
        raise ValueError("Days must be at least 1")
    if days > 3650:  # Max 10 years
        raise ValueError("Days cannot exceed 3650 (10 years)")

    expiry_date = datetime.now() + timedelta(days=days)
    expiry = expiry_date.strftime('%Y%m%d')

    # Generate signature
    payload = f"{tier}-{customer_id}-{expiry}-{secret}"
    signature = hashlib.sha256(payload.encode()).hexdigest()[:12]

    # Assemble key
    license_key = f"DB-{tier}-{customer_id}-{expiry}-{signature}"

    return license_key


def validate_license_key(license_key: str, secret: str = SECRET) -> dict:
    """Validate a license key and return its details.

    Args:
        license_key: The license key to validate
        secret: Secret salt for signature verification

    Returns:
        Dict with validation results
    """
    result = {
        'valid': False,
        'tier': None,
        'customer_id': None,
        'expiry_date': None,
        'expired': False,
        'message': ''
    }

    try:
        parts = license_key.split('-')
        if len(parts) != 5:
            result['message'] = 'Invalid format (expected 5 parts)'
            return result

        prefix, tier, customer_id, expiry, signature = parts

        if prefix != 'DB':
            result['message'] = 'Invalid prefix'
            return result

        if tier not in VALID_TIERS:
            result['message'] = f'Invalid tier: {tier}'
            return result

        # Parse expiry
        try:
            expiry_date = datetime.strptime(expiry, '%Y%m%d')
            result['expiry_date'] = expiry_date.strftime('%Y-%m-%d')
        except ValueError:
            result['message'] = 'Invalid expiry date format'
            return result

        # Check expiry
        if datetime.now() > expiry_date:
            result['expired'] = True
            result['message'] = f'License expired on {result["expiry_date"]}'
            result['tier'] = tier
            result['customer_id'] = customer_id
            return result

        # Verify signature
        payload = f"{tier}-{customer_id}-{expiry}-{secret}"
        expected_sig = hashlib.sha256(payload.encode()).hexdigest()[:12]

        if signature != expected_sig:
            result['message'] = 'Invalid signature'
            return result

        # Valid!
        result['valid'] = True
        result['tier'] = tier
        result['customer_id'] = customer_id
        result['message'] = 'License is valid'

        return result

    except Exception as e:
        result['message'] = f'Validation error: {str(e)}'
        return result


def main():
    parser = argparse.ArgumentParser(
        description='Generate DataBridge AI license keys',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Generate a 1-year PRO license:
    python generate_license.py PRO ACME001

  Generate a 2-year ENTERPRISE license:
    python generate_license.py ENTERPRISE BIGCORP 730

  Validate an existing license:
    python generate_license.py --validate DB-PRO-ACME001-20260209-abc123def456

  Generate multiple licenses:
    python generate_license.py PRO CUSTOMER1 365 --output licenses.txt
    python generate_license.py PRO CUSTOMER2 365 --output licenses.txt --append
        """
    )

    parser.add_argument('tier', nargs='?', help='License tier: CE, PRO, or ENTERPRISE')
    parser.add_argument('customer_id', nargs='?', help='Customer identifier (4-12 alphanumeric)')
    parser.add_argument('days', nargs='?', type=int, default=365,
                        help='Days until expiry (default: 365)')
    parser.add_argument('--output', '-o', help='Output file for license key')
    parser.add_argument('--append', '-a', action='store_true',
                        help='Append to output file instead of overwriting')
    parser.add_argument('--validate', '-v', help='Validate an existing license key')
    parser.add_argument('--secret', '-s', help='Override the secret (for testing)')
    parser.add_argument('--quiet', '-q', action='store_true',
                        help='Only output the license key')

    args = parser.parse_args()

    # Use custom secret if provided
    secret = args.secret or SECRET

    # Validate mode
    if args.validate:
        result = validate_license_key(args.validate, secret)
        if args.quiet:
            print('VALID' if result['valid'] else 'INVALID')
        else:
            print(f"\nLicense Validation Results")
            print("=" * 40)
            print(f"Key:         {args.validate}")
            print(f"Valid:       {'Yes' if result['valid'] else 'No'}")
            print(f"Tier:        {result['tier'] or 'N/A'}")
            print(f"Customer:    {result['customer_id'] or 'N/A'}")
            print(f"Expiry:      {result['expiry_date'] or 'N/A'}")
            print(f"Expired:     {'Yes' if result['expired'] else 'No'}")
            print(f"Message:     {result['message']}")
        sys.exit(0 if result['valid'] else 1)

    # Generate mode - require tier and customer_id
    if not args.tier or not args.customer_id:
        parser.print_help()
        print("\nError: tier and customer_id are required for generation")
        sys.exit(1)

    try:
        license_key = generate_license_key(
            tier=args.tier,
            customer_id=args.customer_id,
            days=args.days,
            secret=secret
        )

        if args.quiet:
            print(license_key)
        else:
            expiry = (datetime.now() + timedelta(days=args.days)).strftime('%Y-%m-%d')
            print(f"\nDataBridge AI License Generated")
            print("=" * 50)
            print(f"Tier:        {args.tier.upper()}")
            print(f"Customer:    {args.customer_id.upper()}")
            print(f"Expires:     {expiry} ({args.days} days)")
            print(f"\nLicense Key:")
            print(f"  {license_key}")
            print(f"\nTo activate, set environment variable:")
            print(f"  export DATABRIDGE_LICENSE_KEY=\"{license_key}\"")
            print(f"\nOr add to .env file:")
            print(f"  DATABRIDGE_LICENSE_KEY={license_key}")

        # Write to file if requested
        if args.output:
            mode = 'a' if args.append else 'w'
            with open(args.output, mode) as f:
                timestamp = datetime.now().isoformat()
                f.write(f"# Generated: {timestamp}\n")
                f.write(f"# Customer: {args.customer_id.upper()}, Tier: {args.tier.upper()}, Days: {args.days}\n")
                f.write(f"{license_key}\n\n")
            if not args.quiet:
                print(f"\nKey {'appended to' if args.append else 'written to'}: {args.output}")

    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
