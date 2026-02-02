"""
Expand Hierarchy Mappings with Chart of Accounts (COA).

This script takes mapping files with ILIKE patterns and IN conditions,
and expands them to actual account codes from the DIM_ACCOUNT.csv COA.

For example:
- ILIKE '501%' -> expands to 501-100, 501-110, 501-120, etc.
- IN ('205-102', '205-106') -> keeps as-is but adds COA metadata
"""

import csv
import os
import re
import fnmatch
from pathlib import Path
from collections import defaultdict
from datetime import datetime


def load_coa(coa_path: str) -> dict:
    """Load Chart of Accounts into a dictionary keyed by ACCOUNT_CODE."""
    coa = {}
    with open(coa_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get('ACCOUNT_CODE', '').strip()
            if code:
                coa[code] = {
                    'ACCOUNT_CODE': code,
                    'ACCOUNT_ID': row.get('ACCOUNT_ID', ''),
                    'ACCOUNT_NAME': row.get('ACCOUNT_NAME', ''),
                    'ACCOUNT_CLASS': row.get('ACCOUNT_CLASS', ''),
                    'ACCOUNT_CLASS_CODE': row.get('ACCOUNT_CLASS_CODE', ''),
                    'ACCOUNT_MAJOR': row.get('ACCOUNT_MAJOR', ''),
                    'ACCOUNT_MAJOR_CODE': row.get('ACCOUNT_MAJOR_CODE', ''),
                    'ACCOUNT_MINOR': row.get('ACCOUNT_MINOR', ''),
                    'ACCOUNT_MINOR_CODE': row.get('ACCOUNT_MINOR_CODE', ''),
                    'ACCOUNT_HOLDER': row.get('ACCOUNT_HOLDER', ''),
                    'ACCOUNT_FIN_CLASS': row.get('ACCOUNT_FIN_CLASS', ''),
                    'ACCOUNT_BILLING_CATEGORY_CODE': row.get('ACCOUNT_BILLING_CATEGORY_CODE', ''),
                    'ACCOUNT_BILLING_CATEGORY_DESCRIPTION': row.get('ACCOUNT_BILLING_CATEGORY_DESCRIPTION', ''),
                }
    return coa


def match_pattern_to_codes(pattern: str, coa: dict, condition_type: str) -> list:
    """
    Match a pattern to actual account codes from COA.

    Args:
        pattern: The pattern (e.g., '501%', '11%', '205-102')
        coa: Dictionary of COA codes
        condition_type: ILIKE, IN, =, etc.

    Returns:
        List of matching account codes with COA metadata
    """
    matches = []
    pattern = pattern.strip()

    if condition_type.upper() in ('ILIKE', 'LIKE'):
        # Convert SQL LIKE pattern to regex
        # '501%' -> matches codes starting with '501'
        # '%990' -> matches codes ending with '990'
        # '%gas%' -> matches codes containing 'gas'

        if pattern.endswith('%') and not pattern.startswith('%'):
            # Prefix match: '501%' -> startswith('501')
            prefix = pattern[:-1]
            for code, data in coa.items():
                if code.lower().startswith(prefix.lower()):
                    matches.append(data)
        elif pattern.startswith('%') and not pattern.endswith('%'):
            # Suffix match: '%990' -> endswith('990')
            suffix = pattern[1:]
            for code, data in coa.items():
                if code.lower().endswith(suffix.lower()):
                    matches.append(data)
        elif pattern.startswith('%') and pattern.endswith('%'):
            # Contains match: '%gas%' -> contains('gas')
            contains = pattern[1:-1]
            for code, data in coa.items():
                if contains.lower() in code.lower():
                    matches.append(data)
        else:
            # Exact match
            if pattern in coa:
                matches.append(coa[pattern])

    elif condition_type.upper() in ('IN', '='):
        # Exact match
        if pattern in coa:
            matches.append(coa[pattern])

    return matches


def expand_mapping_file(mapping_path: str, coa: dict, output_dir: str) -> dict:
    """
    Expand a mapping file with COA details.

    Args:
        mapping_path: Path to the mapping CSV
        coa: Chart of accounts dictionary
        output_dir: Output directory for expanded file

    Returns:
        Statistics about the expansion
    """
    stats = {
        'original_rows': 0,
        'expanded_rows': 0,
        'matched_accounts': 0,
        'unmatched_patterns': [],
        'hierarchies_processed': set(),
    }

    expanded_rows = []

    with open(mapping_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        original_fieldnames = reader.fieldnames or []

        for row in reader:
            stats['original_rows'] += 1

            hierarchy_id = row.get('HIERARCHY_ID', '')
            hierarchy_name = row.get('HIERARCHY_NAME', '')
            source_column = row.get('SOURCE_COLUMN', '').lower()
            condition_type = row.get('CONDITION_TYPE', '')
            condition_value = row.get('CONDITION_VALUE', '')

            stats['hierarchies_processed'].add(hierarchy_name)

            # Only expand account-related mappings
            is_account_mapping = any(kw in source_column for kw in [
                'account', 'acct', 'code', 'gl'
            ])

            if is_account_mapping and condition_type.upper() in ('ILIKE', 'LIKE', 'IN', '='):
                # Expand pattern to actual codes
                matches = match_pattern_to_codes(condition_value, coa, condition_type)

                if matches:
                    stats['matched_accounts'] += len(matches)

                    for match in matches:
                        expanded_row = row.copy()
                        expanded_row['EXPANDED_ACCOUNT_CODE'] = match['ACCOUNT_CODE']
                        expanded_row['EXPANDED_ACCOUNT_ID'] = match['ACCOUNT_ID']
                        expanded_row['EXPANDED_ACCOUNT_NAME'] = match['ACCOUNT_NAME']
                        expanded_row['EXPANDED_ACCOUNT_CLASS'] = match['ACCOUNT_CLASS']
                        expanded_row['EXPANDED_ACCOUNT_MAJOR'] = match['ACCOUNT_MAJOR']
                        expanded_row['EXPANDED_ACCOUNT_MINOR'] = match['ACCOUNT_MINOR']
                        expanded_row['EXPANDED_ACCOUNT_HOLDER'] = match['ACCOUNT_HOLDER']
                        expanded_row['EXPANDED_BILLING_CATEGORY_CODE'] = match['ACCOUNT_BILLING_CATEGORY_CODE']
                        expanded_row['MATCH_TYPE'] = 'COA_MATCH'
                        expanded_rows.append(expanded_row)
                else:
                    # No matches found - keep original row
                    stats['unmatched_patterns'].append(f"{hierarchy_name}: {condition_value}")
                    expanded_row = row.copy()
                    expanded_row['EXPANDED_ACCOUNT_CODE'] = ''
                    expanded_row['EXPANDED_ACCOUNT_ID'] = ''
                    expanded_row['EXPANDED_ACCOUNT_NAME'] = ''
                    expanded_row['EXPANDED_ACCOUNT_CLASS'] = ''
                    expanded_row['EXPANDED_ACCOUNT_MAJOR'] = ''
                    expanded_row['EXPANDED_ACCOUNT_MINOR'] = ''
                    expanded_row['EXPANDED_ACCOUNT_HOLDER'] = ''
                    expanded_row['EXPANDED_BILLING_CATEGORY_CODE'] = ''
                    expanded_row['MATCH_TYPE'] = 'NO_COA_MATCH'
                    expanded_rows.append(expanded_row)
            else:
                # Non-account mapping or ELSE - keep as-is
                expanded_row = row.copy()
                expanded_row['EXPANDED_ACCOUNT_CODE'] = ''
                expanded_row['EXPANDED_ACCOUNT_ID'] = ''
                expanded_row['EXPANDED_ACCOUNT_NAME'] = ''
                expanded_row['EXPANDED_ACCOUNT_CLASS'] = ''
                expanded_row['EXPANDED_ACCOUNT_MAJOR'] = ''
                expanded_row['EXPANDED_ACCOUNT_MINOR'] = ''
                expanded_row['EXPANDED_ACCOUNT_HOLDER'] = ''
                expanded_row['EXPANDED_BILLING_CATEGORY_CODE'] = ''
                expanded_row['MATCH_TYPE'] = 'NON_ACCOUNT' if not is_account_mapping else 'ELSE_CLAUSE'
                expanded_rows.append(expanded_row)

    stats['expanded_rows'] = len(expanded_rows)

    # Write expanded file
    if expanded_rows:
        base_name = Path(mapping_path).stem
        output_path = os.path.join(output_dir, f"{base_name}_EXPANDED.csv")

        # Define output fieldnames
        output_fieldnames = list(original_fieldnames) + [
            'EXPANDED_ACCOUNT_CODE',
            'EXPANDED_ACCOUNT_ID',
            'EXPANDED_ACCOUNT_NAME',
            'EXPANDED_ACCOUNT_CLASS',
            'EXPANDED_ACCOUNT_MAJOR',
            'EXPANDED_ACCOUNT_MINOR',
            'EXPANDED_ACCOUNT_HOLDER',
            'EXPANDED_BILLING_CATEGORY_CODE',
            'MATCH_TYPE',
        ]

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=output_fieldnames)
            writer.writeheader()
            writer.writerows(expanded_rows)

        stats['output_path'] = output_path

    return stats


def create_summary_by_hierarchy(expanded_files: list, output_dir: str) -> str:
    """Create a summary showing account counts by hierarchy."""
    summary_rows = []

    for file_path in expanded_files:
        if not os.path.exists(file_path):
            continue

        # Count by hierarchy
        hierarchy_counts = defaultdict(lambda: {'total': 0, 'matched': 0})

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                hier_name = row.get('HIERARCHY_NAME', 'Unknown')
                hierarchy_counts[hier_name]['total'] += 1
                if row.get('MATCH_TYPE') == 'COA_MATCH':
                    hierarchy_counts[hier_name]['matched'] += 1

        source_file = Path(file_path).stem.replace('_EXPANDED', '')

        for hier_name, counts in hierarchy_counts.items():
            summary_rows.append({
                'SOURCE_FILE': source_file,
                'HIERARCHY_NAME': hier_name,
                'TOTAL_MAPPINGS': counts['total'],
                'COA_MATCHED': counts['matched'],
                'MATCH_RATE': f"{counts['matched']/counts['total']*100:.1f}%" if counts['total'] > 0 else "0%",
            })

    # Write summary
    summary_path = os.path.join(output_dir, 'COA_EXPANSION_SUMMARY.csv')
    if summary_rows:
        with open(summary_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=summary_rows[0].keys())
            writer.writeheader()
            writer.writerows(sorted(summary_rows, key=lambda x: (x['SOURCE_FILE'], x['HIERARCHY_NAME'])))

    return summary_path


def main():
    """Main function to expand all mappings."""
    print("=" * 80)
    print("EXPANDING HIERARCHY MAPPINGS WITH CHART OF ACCOUNTS")
    print("=" * 80)

    # Paths
    coa_path = r"C:\Users\telha\Databridge_AI\Gemini\Uploads\DIM_ACCOUNT.csv"
    mapping_dir = r"C:\Users\telha\Databridge_AI\result_export"
    output_dir = r"C:\Users\telha\Databridge_AI\result_export\expanded"

    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Load COA
    print(f"\nLoading Chart of Accounts from: {coa_path}")
    coa = load_coa(coa_path)
    print(f"  -> Loaded {len(coa)} account codes")

    # Find all mapping files
    mapping_files = list(Path(mapping_dir).glob("*_MAPPING.csv"))
    print(f"\nFound {len(mapping_files)} mapping files to process")

    # Process each mapping file
    all_stats = []
    expanded_files = []

    for mapping_path in mapping_files:
        print(f"\n{'='*60}")
        print(f"Processing: {mapping_path.name}")
        print("-" * 60)

        stats = expand_mapping_file(str(mapping_path), coa, output_dir)
        all_stats.append({
            'file': mapping_path.name,
            **stats
        })

        if 'output_path' in stats:
            expanded_files.append(stats['output_path'])

        print(f"  Original rows:     {stats['original_rows']:,}")
        print(f"  Expanded rows:     {stats['expanded_rows']:,}")
        print(f"  COA matches:       {stats['matched_accounts']:,}")
        print(f"  Hierarchies:       {len(stats['hierarchies_processed'])}")

        if stats['unmatched_patterns'][:3]:
            print(f"  Unmatched samples: {stats['unmatched_patterns'][:3]}")

    # Create summary
    print(f"\n{'='*80}")
    print("CREATING SUMMARY")
    print("=" * 80)

    summary_path = create_summary_by_hierarchy(expanded_files, output_dir)
    print(f"  -> Summary written to: {summary_path}")

    # Final statistics
    print(f"\n{'='*80}")
    print("EXPANSION COMPLETE")
    print("=" * 80)

    total_original = sum(s['original_rows'] for s in all_stats)
    total_expanded = sum(s['expanded_rows'] for s in all_stats)
    total_matched = sum(s['matched_accounts'] for s in all_stats)

    print(f"\n  Total files processed:  {len(mapping_files)}")
    print(f"  Total original rows:    {total_original:,}")
    print(f"  Total expanded rows:    {total_expanded:,}")
    print(f"  Total COA matches:      {total_matched:,}")
    print(f"  Expansion factor:       {total_expanded/total_original:.1f}x" if total_original > 0 else "")
    print(f"\n  Output directory:       {output_dir}")

    return all_stats


if __name__ == "__main__":
    main()
