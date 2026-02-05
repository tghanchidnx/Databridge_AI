import csv
from typing import List, Dict, Any

def from_csv(file_path: str, **kwargs) -> List[Dict[str, Any]]:
    """
    Reads data from a CSV file and returns it as a list of dictionaries.

    Args:
        file_path: The path to the CSV file.
        **kwargs: Additional arguments to be passed to csv.DictReader.

    Returns:
        A list of dictionaries, where each dictionary represents a row.
    """
    with open(file_path, mode='r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile, **kwargs)
        return [row for row in reader]
