"""
SEC EDGAR Tutorials - Setup Script
====================================
Copies all SEC financial CSV files from the sec_edgar/ folder
into the data/ folder so DataBridge can find them easily.

Usage:
    cd C:\\Users\\telha\\Databridge_AI
    python use_cases_by_claude/sec_edgar/setup.py

Run download_sec_data.py first if the CSV files don't exist yet.
"""

import os
import shutil
import subprocess
import sys


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    data_dir = os.path.join(project_root, "data")

    # Make sure the data folder exists
    os.makedirs(data_dir, exist_ok=True)

    # List of SEC CSV files to copy
    csv_files = [
        "apple_income_statement.csv",
        "apple_income_statement_tier1.csv",
        "microsoft_income_statement.csv",
        "apple_vs_microsoft_comparison.csv",
        "apple_multiyear.csv",
        "apple_income_2023.csv",
        "apple_income_2024.csv",
        "apple_balance_sheet.csv",
        "apple_balance_sheet_tier1.csv",
        "apple_full_chart_of_accounts.csv",
    ]

    print("=" * 50)
    print("  SEC EDGAR Tutorials - Setup")
    print("=" * 50)
    print()

    # Check if CSV files exist; if not, run the download script
    first_csv = os.path.join(script_dir, csv_files[0])
    if not os.path.exists(first_csv):
        print("  CSV files not found. Running download_sec_data.py first...")
        print()
        download_script = os.path.join(script_dir, "download_sec_data.py")
        subprocess.run([sys.executable, download_script], check=True)
        print()

    # Copy files
    copied = 0
    for filename in csv_files:
        source = os.path.join(script_dir, filename)
        destination = os.path.join(data_dir, filename)

        if not os.path.exists(source):
            print(f"  WARNING: {filename} not found, skipping")
            continue

        shutil.copy2(source, destination)
        print(f"  Copied: {filename}")
        copied += 1

    print()
    print(f"Done! {copied} files copied to the data/ folder.")
    print()
    print("You can now use these files in Claude Desktop:")
    for f in csv_files:
        print(f"  - data/{f}")
    print()
    print("Start with Use Case 5 (Apple's Money Checkup)!")


if __name__ == "__main__":
    main()
