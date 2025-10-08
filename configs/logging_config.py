import logging
from pathlib import Path
from datetime import datetime

# Configure root logger
log_folder = Path("logs")
log_folder.mkdir(parents=True, exist_ok=True)

log_file = log_folder / datetime.now().strftime("%Y_%m_%d.log")  # daily log file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)