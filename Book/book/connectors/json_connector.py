import json
from typing import Any

def from_json(file_path: str, **kwargs) -> Any:
    """
    Reads data from a JSON file.

    Args:
        file_path: The path to the JSON file.
        **kwargs: Additional arguments to be passed to json.load.

    Returns:
        The JSON data.
    """
    with open(file_path, 'r', encoding='utf-8') as infile:
        return json.load(infile, **kwargs)
