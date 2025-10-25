from src.etl.connect import *
from src.etl.extract_sales import *
from src.etl.load import *
import logging
from configs import logging_config
import datetime

logging.info("SALES DEPARTMENT ETL has started.")

with open("credentials/amocrm.json", "r") as f:
    creds = json.load(f)

try:
    # initial_amocrm_access_token = amocrm_initial_token(auth_code='')
    amocrm_access_token = amocrm_refresh_token()
    headers = amocrm_headers(amocrm_access_token)
    logging.info("Amocrm token and headers successfully retrieved.")
except Exception as e:
    logging.exception(f"❌Failed to retrieve token and headers : {e}")
    headers = None

if headers:
    # --- LEADS ---
    try:
        leads = amocrm_get_leads(headers)
        load_to_postgres(df=leads, dept="sales", table_base_name="leads", postfix="25", primary_key="id")
        load_history_to_postgres(df=leads, dept="sales", table_base_name="leads", postfix="25", primary_key="id")
        logging.info("Leads successfully fetched and loaded.")
    except Exception as e:
        logging.exception(f"❌Failed to fetch/load leads: {e}")

    # --- CATALOGS ---
    try:
        catalogs = amocrm_get_catalogs(headers)
        load_to_postgres(df=catalogs, dept="sales", table_base_name="catalogs", postfix="25", primary_key="id")
        logging.info("Catalogs successfully fetched and loaded.")
    except Exception as e:
        logging.exception(f"❌Failed to fetch/load catalogs: {e}")

    # --- CONTACTS ---
    try:
        contacts = amocrm_get_contacts(headers)
        load_to_postgres(df=contacts, dept="sales", table_base_name="contacts", postfix="25", primary_key="id")
        logging.info("Contacts successfully fetched and loaded.")
    except Exception as e:
        logging.exception(f"❌Failed to fetch/load contacts: {e}")

    # --- COMPANIES ---
    try:
        companies = amocrm_get_companies(headers)
        load_to_postgres(df=companies, dept="sales", table_base_name="companies", postfix="25", primary_key="id")
        logging.info("Companies successfully fetched and loaded.")
    except Exception as e:
        logging.exception(f"❌Failed to fetch/load companies: {e}")

    # --- LOSS REASONS ---
    try:
        loss_reasons = amocrm_get_loss_reasons(headers)
        load_to_postgres(df=loss_reasons, dept="sales", table_base_name="loss_reasons", postfix="25", primary_key="id")
        load_history_to_postgres(df=loss_reasons, dept="sales", table_base_name="loss_reasons", postfix="25", primary_key="id")
        logging.info("Loss reasons successfully fetched and loaded.")
    except Exception as e:
        logging.exception(f"❌Failed to fetch/load loss reasons: {e}")


    # --- PIPELINES AND STATUSES ---
    try:
        pipelines_df, statuses_df = amocrm_get_pipelines_statuses(headers)

        load_to_postgres(df=pipelines_df, dept="sales", table_base_name="pipelines", postfix="25", primary_key="id")
        load_history_to_postgres(df=pipelines_df, dept="sales", table_base_name="pipelines", postfix="25", primary_key="id")

        load_to_postgres(df=statuses_df, dept="sales", table_base_name="statuses", postfix="25", primary_key="status_id")
        load_history_to_postgres(df=statuses_df, dept="sales", table_base_name="statuses", postfix="25", primary_key="status_id")

        logging.info("Pipelines and statuses successfully fetched and loaded.")
    except Exception as e:
        logging.exception(f"❌Failed to fetch/load pipelines and statuses: {e}")

    # --- TASKS ---
    try:
        tasks = amocrm_get_tasks(headers)
        load_to_postgres(df=tasks, dept="sales", table_base_name="tasks", postfix="25", primary_key="id")
        load_history_to_postgres(df=tasks, dept="sales", table_base_name="tasks", postfix="25", primary_key="id")
        logging.info("Tasks successfully fetched and loaded.")
    except Exception as e:
        logging.exception(f"❌Failed to fetch/load tasks: {e}")

    # --- TASK TYPES ---
    try:
        task_types = amocrm_get_task_types(headers)
        load_to_postgres(df=task_types, dept="sales", table_base_name="task_types", postfix="25", primary_key="id")
        logging.info("Task types successfully fetched and loaded.")
    except Exception as e:
        logging.exception(f"❌Failed to fetch/load task types: {e}")

    # --- USERS ---
    try:
        users = amocrm_get_users(headers)
        load_to_postgres(df=users, dept="sales", table_base_name="users", postfix="25", primary_key="id")
        logging.info("Users successfully fetched and loaded.")
    except Exception as e:
        logging.exception(f"❌Failed to fetch/load leads: {e}")

logging.info("SALES DEPARTMENT ETL run completed.")
