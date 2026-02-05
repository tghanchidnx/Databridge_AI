"""
DataBridge AI - Use Cases Setup Script
======================================
Copies all sample CSV files from the use_cases_by_claude/ folder
into the data/ folder so DataBridge can find them easily.

Usage:
    cd C:\\Users\\telha\\Databridge_AI
    python use_cases_by_claude/setup.py
"""

import os
import shutil

def main():
    # Figure out where we are
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    data_dir = os.path.join(project_root, "data")

    # Make sure the data folder exists
    os.makedirs(data_dir, exist_ok=True)

    # List of all CSV files to copy
    csv_files = [
        os.path.join("01_pizza_shop_sales_check", "pizza_orders.csv"),
        os.path.join("02_find_my_friends", "class_roster_morning.csv"),
        os.path.join("02_find_my_friends", "class_roster_afternoon.csv"),
        os.path.join("03_school_report_card_hierarchy", "report_card.csv"),
        os.path.join("04_sports_league_comparison", "league_stats_official.csv"),
        os.path.join("04_sports_league_comparison", "league_stats_newspaper.csv"),
    ]

    print("=" * 50)
    print("  DataBridge AI - Use Cases Setup")
    print("=" * 50)
    print()

    copied = 0
    for csv_file in csv_files:
        source = os.path.join(script_dir, csv_file)
        filename = os.path.basename(csv_file)
        destination = os.path.join(data_dir, filename)

        if not os.path.exists(source):
            print(f"  WARNING: {csv_file} not found, skipping")
            continue

        shutil.copy2(source, destination)
        print(f"  Copied: {filename}")
        copied += 1

    print()
    print(f"Done! {copied} files copied to the data/ folder.")
    print()
    print("You can now use these files in Claude Desktop:")
    print("  - data/pizza_orders.csv")
    print("  - data/class_roster_morning.csv")
    print("  - data/class_roster_afternoon.csv")
    print("  - data/report_card.csv")
    print("  - data/league_stats_official.csv")
    print("  - data/league_stats_newspaper.csv")
    print()
    print("Have fun! Start with Use Case 1 (Pizza Shop).")
    print()
    print("For the SEC EDGAR advanced tutorials (Use Cases 5-11), run:")
    print("  python use_cases_by_claude/sec_edgar/setup.py")


if __name__ == "__main__":
    main()
