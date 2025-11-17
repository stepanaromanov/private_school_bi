from src.etl.connect import eduschool_token
from src.etl.extract_finance import *
from src.etl.load import *
import datetime
from configs.logging_config import get_logger
logger = get_logger(__name__)

logger.info("FINANCE DEPARTMENT ETL has started.")

try:
    token = eduschool_token()
    logger.info("Token successfully retrieved.")
except Exception as e:
    logger.exception(f"❌Failed to retrieve token: {e}")
    token = None

if token:
    # --- TRANSACTIONS URGANCH OMON SCHOOL ---
    try:
        transactions = finance_fetch_all_transactions(token)
        load_to_postgres(df=transactions, dept="finance", table_base_name="transactions", postfix="_2526", primary_key="id")
        logger.info("Transactions successfully fetched and loaded.")
    except Exception as e:
        logger.exception(f"❌Failed to fetch/load Urganch school transactions: {e}")
        classes = None

    # --- TRANSACTIONS GURLAN OMON SCHOOL ---
    try:
        transactions = finance_fetch_all_transactions(token, branch="684d1fc04921a1211f725ec4")
        load_to_postgres(df=transactions, dept="finance", table_base_name="transactions", postfix="_2526", primary_key="id")
        logger.info("Transactions successfully fetched and loaded.")
    except Exception as e:
        logger.exception(f"❌Failed to fetch/load Gurlan school transactions: {e}")
        classes = None

logger.info("FINANCE DEPARTMENT ETL run completed.")
