from src.etl.connect import *
from src.etl.extract_marketing import *
from src.etl.extract_marketing import fetch_marketing_facebook_pages_data
from src.etl.load import *
from configs.logging_config import get_logger
logger = get_logger("etl_log")

logger.info(f"{'>' * 10} MARKETING DEPARTMENT ETL has started.")

try:
    access_token, ad_account_ids = marketing_facebook_token()
    logger.info("Marketing facebook token successfully retrieved.")
except Exception as e:
    logger.exception(f"❌Failed to retrieve marketing facebook token: {e}")

if access_token and ad_account_ids:
    try:
        facebook_df = fetch_marketing_facebook_data(access_token, ad_account_ids, period=1)
        facebook_monthly_df = fetch_marketing_facebook_data(access_token, ad_account_ids, period='monthly')
        load_to_postgres(df=facebook_df, dept="marketing", table_base_name="facebook", postfix="25", primary_key="id")
        load_to_postgres(df=facebook_monthly_df, dept="marketing", table_base_name="facebook_monthly", postfix="25", primary_key="id")
    except Exception as e:
        logger.exception(f"❌Failed to fetch/load marketing facebook data: {e}")

    try:
        facebook_pages_df = fetch_marketing_facebook_pages_data(access_token, ad_account_ids)
        load_to_postgres(df=facebook_pages_df, dept="marketing", table_base_name="facebook_pages", postfix="25", primary_key="id")
    except Exception as e:
        logger.exception(f"❌Failed to fetch/load marketing facebook pages data: {e}")

logger.info("MARKETING DEPARTMENT ETL run completed.")
