# pip freeze > requirements.txt
import logging
import configs.logging_config
from pathlib import Path
from datetime import datetime, timedelta

"""
def setup_logging():
    
    # Create one shared logger and configure it for the entire project.
    # All imports can use logging.getLogger('omonschool_etl') to get the same instance.
    
    log_folder = Path("logs")
    log_folder.mkdir(parents=True, exist_ok=True)

    log_file = log_folder / datetime.now().strftime("%Y_%m_%d_%H-%M-%S.log")

    # Create main logger
    logger = logging.getLogger("omonschool_etl")
    logger.setLevel(logging.INFO)

    # File handler
    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        "%Y-%m-%d %H:%M:%S"
    ))

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        "%H:%M:%S"
    ))

    # Avoid duplicate handlers
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger, log_file


__all__ = ["setup_logging"]
"""
