from src.etl.connect import *
from src.etl.extract_trello import *
from src.etl.load import *
from configs.logging_config import get_logger
logger = get_logger(__name__)


try:
    key, token = trello_token()
    logger.info("Trello key and token successfully retrieved.")
except Exception as e:
    logger.exception(f"❌Failed to retrieve Trello key and token: {e}")

if key and token:
    try:
        boards_df, cards_df, checklists_df, lists_df = trello_fetch_data(key=key, token=token)

        load_to_postgres(df=boards_df, dept="trello", table_base_name="boards", postfix="25", primary_key="id")

        load_to_postgres(df=cards_df, dept="trello", table_base_name="cards", postfix="25", primary_key="id")
        load_history_to_postgres(df=cards_df, dept="trello", table_base_name="cards", postfix="25", primary_key="id")

        load_to_postgres(df=lists_df, dept="trello", table_base_name="lists", postfix="25", primary_key="id")

        load_to_postgres(df=checklists_df, dept="trello", table_base_name="checklists", postfix="25", primary_key="id")
        load_history_to_postgres(df=checklists_df, dept="trello", table_base_name="checklists", postfix="25", primary_key="id")

    except Exception as e:
        logger.exception(f"❌Failed to fetch/load trello: {e}")