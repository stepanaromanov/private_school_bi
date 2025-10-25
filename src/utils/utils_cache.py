import os
import json
import logging

def update_json_cache(file_name: str, updates: dict, dir: str):
    """
    Load JSON from cache/{file_name}.json,
    update it with the given dictionary,
    and save it back.

    - Creates the file if missing.
    """
    path = os.path.join(dir, f"{file_name}.json")

    # Load existing cache (or empty)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                data = {}
    else:
        data = {}

    # Ensure it's a dict
    if not isinstance(data, dict):
        logging.info(f"File {path} is not a dict — overwriting with new one.")
        data = {}

    # Save back to file
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logging.info(f"Updated cache file: {path}")