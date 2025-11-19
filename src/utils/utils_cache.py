import os
import json
from configs.logging_config import get_logger
logger = get_logger("etl_log")

def update_json_cache(file_name: str, updates: dict, dir: str):
    """
    Load JSON from cache_folder/{file_name}.json,
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
        logger.info(f"File {path} is not a dict — overwriting with new one.")
        data = {}

    data.update(updates)

    # Save back to file
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"Updated cache file: {path}")