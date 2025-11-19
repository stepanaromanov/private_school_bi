'''
OLD LOGGING

import logging
from pathlib import Path
from datetime import datetime

# ------------------------------
# Common setup
# ------------------------------
log_folder = Path("logs")
log_folder.mkdir(parents=True, exist_ok=True)

# consistent daily log filenames
today = datetime.now().strftime("%Y_%m_%d")

# ------------------------------
# Root logger configuration
# ------------------------------
root_log_file = log_folder / f"{today}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(root_log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logging.info("Root logger initialized")

def get_logger(name: str, level=logging.INFO):
    """
    Creates a logger with consistent formatting and daily log file.
    :param name: Logger name (e.g., 'dataframe_log')
    :param level: Logging level (default: INFO)
    :return: configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    log_file = log_folder / f"{name}_{today}.log"

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s", "%H:%M:%S"
    )

    handlers = [
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]

    # Avoid duplicate handlers if logger is recreated
    if not logger.handlers:
        for h in handlers:
            h.setFormatter(formatter)
            logger.addHandler(h)

    return logger

'''

import logging
from pathlib import Path
from datetime import datetime

# prevents double logging
_is_configured = False

# Create log folder
log_folder = Path("logs")
log_folder.mkdir(parents=True, exist_ok=True)

# Daily filename
today = datetime.now().strftime("%Y_%m_%d")

def setup_logging():
    global _is_configured
    if _is_configured:
        return

    root_log_file = log_folder / f"etl_log_{today}.log"

    # Configure root logger
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.FileHandler(root_log_file, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

    logging.info("Root logger initialized")
    _is_configured = True


def get_logger(name: str, level=logging.INFO):
    """
    Creates a logger with consistent formatting and daily log file.
    :param name: Logger name (e.g., 'dataframe_log')
    :param level: Logging level (default: INFO)
    :return: configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    log_file = log_folder / f"{name}_{today}.log"

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s", "%H:%M:%S"
    )

    handlers = [
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]

    # Avoid duplicate handlers if logger is recreated
    if not logger.handlers:
        for h in handlers:
            h.setFormatter(formatter)
            logger.addHandler(h)

    return logger