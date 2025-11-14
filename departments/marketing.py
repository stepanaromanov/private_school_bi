from src.etl.connect import *
from src.etl.extract_marketing import *
from src.etl.load import *
import logging


try:
    access_token, ad_account_ids = marketing_facebook_token()
    logging.info("Marketing facebook token successfully retrieved.")
except Exception as e:
    logging.exception(f"❌Failed to retrieve marketing facebook token: {e}")

if access_token and ad_account_ids:
    try:
        facebook_df = fetch_marketing_facebook_data(access_token, ad_account_ids)
        load_to_postgres(df=facebook_df, dept="marketing", table_base_name="facebook", postfix="25", primary_key="id")
    except Exception as e:
        logging.exception(f"❌Failed to fetch/load marketing facebook data: {e}")