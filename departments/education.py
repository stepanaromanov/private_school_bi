from src.etl.connect import eduschool_token
from src.etl.extract import *
from src.etl.load import *

logger = logging.getLogger("omonschool_etl")
logger.info("EDUCATION DEPARTMENT etl started.")

try:
    token = eduschool_token()
    logger.info("Token successfully retrieved.")

    classes = eduschool_fetch_classes(token)
    logger.info(f"Fetched {len(classes)} classes.")
   # load_to_postgres(df=classes, table_base_name="classes", postfix="_2526", primary_key="id")

   # students, agg_finance = eduschool_fetch_students(token)
   # logger.info(f"Fetched {len(students)} students and {len(agg_finance)} finance records.")
   # load_to_postgres(df=students, table_base_name="students", postfix="_2526", primary_key="id")
   # load_to_postgres(df=agg_finance, table_base_name="agg_finance", postfix="_2526")

   # employees = eduschool_fetch_employees(token)
   # logger.info(f"Fetched {len(employees)} employees.")
   # load_to_postgres(df=employees, table_base_name="employees", postfix="_2526", primary_key="id")

    journals = eduschool_fetch_journals(token, classes_df=classes)
    logger.info(f"Fetched {len(journals)} journals.")
    # load_to_postgres(df=journals, table_base_name="journals", postfix="_2526", primary_key="journal_id")

    quarters = eduschool_fetch_quarters(token)
    logger.info(f"Fetched {len(quarters)} quarters.")
    # load_to_postgres(df=quarters, table_base_name="quarters", postfix="_2526", primary_key="id")

    attendance_context, attendances = eduschool_fetch_attendance_and_marks(token, classes_df=classes, quarters_df=quarters, journals_df=journals)
    logger.info(f"Fetched {len(attendances)} attendance records.")
    logger.info(f"Fetched {len(attendance_context)} attendance context records.")
    load_to_postgres(df=attendances, table_base_name="attendances", postfix="_2526", primary_key="id")
    load_to_postgres(df=attendance_context, table_base_name="attendance_context", postfix="_2526", primary_key="id")

except Exception as e:
    logger.exception(f"ETL run failed: {e}")  # <- includes traceback in log file
finally:
    logger.info("EDUCATION DEPARTMENT etl successfully completed.")
