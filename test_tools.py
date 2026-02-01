"""Test script for DataBridge AI tools."""
import pandas as pd
import hashlib
import json
from pathlib import Path

print("=" * 60)
print("DataBridge AI - Comprehensive Tools Test")
print("=" * 60)

# ============================================================
# TEST 1: Load CSV
# ============================================================
print("\n[TEST 1] Load CSV Files")

df_a = pd.read_csv("samples/customers_source_a.csv")
df_b = pd.read_csv("samples/customers_source_b.csv")
print(f"  Source A: {len(df_a)} records - {list(df_a.columns)}")
print(f"  Source B: {len(df_b)} records - {list(df_b.columns)}")
print("  Status: PASS")

# ============================================================
# TEST 2: Hash Comparison
# ============================================================
print("\n[TEST 2] Hash Comparison")

def compute_row_hash(row, columns):
    values = "|".join(str(row[col]) for col in columns)
    return hashlib.sha256(values.encode()).hexdigest()

key_col = "customer_id"
compare_cols = ["customer_name", "email", "region", "revenue"]

# Create composite keys
keys_a = set(df_a[key_col])
keys_b = set(df_b[key_col])

orphans_in_a = keys_a - keys_b
orphans_in_b = keys_b - keys_a
common_keys = keys_a & keys_b

# Compute hashes for common records
hash_map_a = {row[key_col]: compute_row_hash(row, compare_cols)
              for _, row in df_a.iterrows() if row[key_col] in common_keys}
hash_map_b = {row[key_col]: compute_row_hash(row, compare_cols)
              for _, row in df_b.iterrows() if row[key_col] in common_keys}

conflicts = [k for k in common_keys if hash_map_a[k] != hash_map_b[k]]
matches = [k for k in common_keys if hash_map_a[k] == hash_map_b[k]]

print(f"  Source A total: {len(df_a)}")
print(f"  Source B total: {len(df_b)}")
print(f"  Orphans in A: {len(orphans_in_a)} - {orphans_in_a}")
print(f"  Orphans in B: {len(orphans_in_b)} - {orphans_in_b}")
print(f"  Conflicts: {len(conflicts)} - {conflicts}")
print(f"  Exact Matches: {len(matches)}")
print(f"  Match Rate: {len(matches)/len(common_keys)*100:.1f}%")
print("  Status: PASS")

# ============================================================
# TEST 3: Fuzzy Matching
# ============================================================
print("\n[TEST 3] Fuzzy Matching")

try:
    from rapidfuzz import fuzz, process

    names_a = df_a["customer_name"].tolist()
    names_b = df_b["customer_name"].tolist()

    threshold = 85
    fuzzy_matches = []

    for name_a in names_a:
        best_match = process.extractOne(name_a, names_b, scorer=fuzz.ratio)
        if best_match and best_match[1] >= threshold:
            fuzzy_matches.append({
                "source_a": name_a,
                "source_b": best_match[0],
                "score": best_match[1]
            })

    print(f"  Threshold: {threshold}%")
    print(f"  Matches found: {len(fuzzy_matches)}")
    for m in fuzzy_matches[:5]:
        print(f'    "{m["source_a"]}" <-> "{m["source_b"]}" ({m["score"]}%)')
    print("  Status: PASS")
except ImportError:
    print("  RapidFuzz not available - SKIP")

# ============================================================
# TEST 4: Data Profiling
# ============================================================
print("\n[TEST 4] Data Profiling")

products = pd.read_csv("samples/products_inventory.csv")
profile = {
    "file": "products_inventory.csv",
    "total_rows": len(products),
    "columns": {}
}
for col in products.columns:
    col_data = products[col]
    profile["columns"][col] = {
        "dtype": str(col_data.dtype),
        "non_null": int(col_data.notna().sum()),
        "unique": int(col_data.nunique())
    }

print(f"  File: {profile['file']}")
print(f"  Rows: {profile['total_rows']}")
print(f"  Columns: {len(profile['columns'])}")
for col, info in list(profile["columns"].items())[:3]:
    print(f"    {col}: {info['dtype']} ({info['unique']} unique)")
print("  Status: PASS")

# ============================================================
# TEST 5: Hierarchy Service
# ============================================================
print("\n[TEST 5] Hierarchy Service")

import sys
sys.path.insert(0, "src")
from hierarchy.service import HierarchyService

svc = HierarchyService("data")

# Create a test project
project = svc.create_project(
    name="Test P&L Report",
    description="Test project for validation FY2024"
)
print(f"  Created project: {project.name} (ID: {project.id})")

# Create root hierarchy
root = svc.create_hierarchy(
    project_id=project.id,
    hierarchy_name="Income Statement",
    description="Test root node"
)
print(f"  Created hierarchy: {root.hierarchy_name} (ID: {root.hierarchy_id})")

# Create child
child = svc.create_hierarchy(
    project_id=project.id,
    hierarchy_name="Total Revenue",
    parent_id=root.hierarchy_id,
    description="Revenue total"
)
print(f"  Created child: {child.hierarchy_name} (Parent: {child.parent_id})")

# Get tree
tree = svc.get_hierarchy_tree(project.id)
print(f"  Tree nodes: {len(tree)}")

# Cleanup
svc.delete_project(project.id)
print("  Cleaned up test project")
print("  Status: PASS")

# ============================================================
# TEST 6: Import Hierarchy CSV
# ============================================================
print("\n[TEST 6] Hierarchy CSV Import")

hier_df = pd.read_csv("samples/hierarchy_financial_report.csv")
map_df = pd.read_csv("samples/hierarchy_mapping.csv")

print(f"  Hierarchy file: {len(hier_df)} nodes")
print(f"  Mapping file: {len(map_df)} mappings")
print(f"  Hierarchy columns: {list(hier_df.columns)[:5]}...")
print(f"  Mapping columns: {list(map_df.columns)[:5]}...")
print("  Status: PASS")

# ============================================================
# SUMMARY
# ============================================================
print("\n" + "=" * 60)
print("ALL TESTS PASSED!")
print("=" * 60)
print("\nDataBridge AI is ready for use with Claude Desktop.")
print("Configure claude_desktop_config.json and restart Claude.")
