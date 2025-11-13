from src.utils.utils_dataframe import *
from src.utils.utils_general import *
from src.etl.connect import *
import logging


try:
    key, token = trello_token()
    logging.info("Token successfully retrieved.")
except Exception as e:
    logging.exception(f"❌Failed to retrieve token: {e}")
    token = None

if token:
    # ---
    # --- URGANCH ---
    # ---
    # --- CLASSES ---
    try:
        classes = eduschool_fetch_classes(token)
        load_to_postgres(df=classes, dept="education", table_base_name="classes", postfix="_2526", primary_key="id")
        load_history_to_postgres(df=classes, dept="education", table_base_name="classes", postfix="_2526", primary_key="id")
        logging.info("Classes successfully fetched and loaded.")
    except Exception as e:
        logging.exception(f"❌Failed to fetch/load classes: {e}")
        classes = None