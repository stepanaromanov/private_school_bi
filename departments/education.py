from src.etl.connect import eduschool_token
from src.etl.extract_education import *
from src.etl.load import *
import logging
import configs.logging_config
import datetime

try:
    token = eduschool_token()
    logging.info("Token successfully retrieved.")
except Exception as e:
    logging.exception(f"Failed to retrieve token: {e}")
    token = None

if token:
    # --- CLASSES ---
    try:
        classes = eduschool_fetch_classes(token)
        load_to_postgres(df=classes, dept="education", table_base_name="classes", postfix="_2526", primary_key="id")
        load_history_to_postgres(df=classes, dept="education", table_base_name="classes", postfix="_2526", primary_key="id")
        logging.info("Classes successfully fetched and loaded.")
    except Exception as e:
        logging.exception(f"Failed to fetch/load classes: {e}")
        classes = None

    # --- STUDENTS & AGG FINANCE ---
    try:
        students, agg_finance = eduschool_fetch_students(token)
        load_to_postgres(df=students, dept="education", table_base_name="students", postfix="_2526", primary_key="id")
        load_history_to_postgres(df=students, dept="education", table_base_name="students", postfix="_2526", primary_key="id")
        load_to_postgres(df=agg_finance, dept="education", table_base_name="agg_finance", postfix="_2526")
        logging.info("Students and finance data successfully fetched and loaded.")
    except Exception as e:
        logging.exception(f"Failed to fetch/load students or finance data: {e}")
        students, agg_finance = None, None

    # --- EMPLOYEES ---
    try:
        employees = eduschool_fetch_employees(token)
        load_to_postgres(df=employees, dept="education", table_base_name="employees", postfix="_2526", primary_key="id")
        load_history_to_postgres(df=employees, dept="education", table_base_name="employees", postfix="_2526", primary_key="id")
        logging.info("Employees successfully fetched and loaded.")
    except Exception as e:
        logging.exception(f"Failed to fetch/load employees: {e}")
        employees = None
"""
    # --- JOURNALS ---
    try:
        if classes is not None:
            journals = eduschool_fetch_journals(token, classes_df=classes)
            load_to_postgres(df=journals, dept="education", table_base_name="journals", postfix="_2526", primary_key="journal_id")
            logging.info("Journals successfully fetched and loaded.")
        else:
            logging.warning("Skipped journals — missing classes data.")
            journals = None
    except Exception as e:
        logging.exception(f"Failed to fetch/load journals: {e}")
        journals = None

    # --- QUARTERS ---
    try:
        quarters = eduschool_fetch_quarters(token)
        load_to_postgres(df=quarters, dept="education", table_base_name="quarters", postfix="_2526", primary_key="id")
        logging.info("Quarters successfully fetched and loaded.")
    except Exception as e:
        logging.exception(f"Failed to fetch/load quarters: {e}")
        quarters = None

    # --- ATTENDANCE & MARKS ---
    try:
        # if datetime.date.today().weekday() == 6:
        if all(x is not None for x in [classes, quarters, journals]):
            attendance_context, attendances = eduschool_fetch_attendance_and_marks(
                token, classes_df=classes, quarters_df=quarters, journals_df=journals
            )
            load_to_postgres(df=attendances, dept="education", table_base_name="attendances", postfix="_2526", primary_key="id")
            load_to_postgres(df=attendance_context, dept="education", table_base_name="attendance_context", postfix="_2526", primary_key="id")
            logging.info("Attendance and marks successfully fetched and loaded.")
        else:
            logging.warning("Skipped attendance — missing dependent data (classes, quarters, or journals).")
    except Exception as e:
        logging.exception(f"Failed to fetch/load attendance or marks: {e}")
"""
logging.info("EDUCATION DEPARTMENT ETL run completed.")
