from src.etl.connect import eduschool_token
from src.etl.extract_finance import *
from src.etl.load import *
import logging
import datetime

logging.info("FINANCE DEPARTMENT ETL has started.")

try:
    token = eduschool_token()
    logging.info("Token successfully retrieved.")
except Exception as e:
    logging.exception(f"❌Failed to retrieve token: {e}")
    token = None

if token:
    # --- TRANSACTIONS ---
    try:
        transactions = finance_fetch_all_transactions(token)
        load_to_postgres(df=transactions, dept="finance", table_base_name="transactions", postfix="_2526", primary_key="id")
        logging.info("Transactions successfully fetched and loaded.")
    except Exception as e:
        logging.exception(f"❌Failed to fetch/load transactions: {e}")
        classes = None

logging.info("FINANCE DEPARTMENT ETL run completed.")
