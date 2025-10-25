import requests
from src.utils.utils_dataframe import *
from src.utils.utils_cache import *
import pandas as pd
import logging
import json

with open("credentials/amocrm.json", "r") as f:
    creds = json.load(f)

AMO_DOMAIN = creds["base_domain"]
BASE_URL = f'https://{AMO_DOMAIN}/api/v4'

def amocrm_get_all_items(endpoint, headers):
    """Fetch paginated results for any endpoint"""
    items = []
    page = 1
    while True:
        url = f"{BASE_URL}/{endpoint}?page={page}&limit=250"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        embedded = data.get("_embedded", {})
        results = embedded.get("items") or embedded.get(endpoint)
        if not results:
            break
        items.extend(results)
        if len(results) < 250:
            break
        page += 1
    return items


def amocrm_get_catalogs(headers):
    logging.info("SALES: Downloading catalogs...")
    catalogs = amocrm_get_all_items("catalogs", headers)
    catalogs_df = pd.DataFrame(catalogs)
    catalogs_df.fillna(0, inplace=True)

    catalogs_df = clean_string_columns(catalogs_df)
    catalogs_df = normalize_columns(catalogs_df)

    catalogs_df["created_at"] = pd.to_datetime(catalogs_df["created_at"], unit="s")
    catalogs_df["updated_at"] = pd.to_datetime(catalogs_df["updated_at"], unit="s")
    catalogs_df = add_timestamp(catalogs_df)

    catalogs_df.attrs["name"] = "sales_catalogs"
    save_df_with_timestamp(df=catalogs_df)
    return catalogs_df


def amocrm_get_companies(headers):
    logging.info("SALES: Downloading companies...")
    companies = amocrm_get_all_items("companies", headers)
    companies_df = pd.DataFrame(companies)

    companies_df.fillna(0, inplace=True)
    companies_df = clean_string_columns(companies_df)
    companies_df = normalize_columns(companies_df)

    companies_df["created_at"] = pd.to_datetime(companies_df["created_at"], unit="s")
    companies_df["updated_at"] = pd.to_datetime(companies_df["updated_at"], unit="s")
    companies_df["closest_task_at"] = pd.to_datetime(companies_df["closest_task_at"], unit="s")

    companies_df = add_timestamp(companies_df)
    companies_df.attrs["name"] = "sales_companies"

    save_df_with_timestamp(df=companies_df)
    return companies_df


def amocrm_get_contacts(headers):
    logging.info("SALES: Downloading contacts...")
    contacts = amocrm_get_all_items("contacts", headers)
    contacts_df = pd.DataFrame(contacts)

    contacts_df.fillna(0, inplace=True)
    contacts_df = clean_string_columns(contacts_df)
    contacts_df = normalize_columns(contacts_df)

    contacts_df["created_at"] = pd.to_datetime(contacts_df["created_at"], unit="s")
    contacts_df["updated_at"] = pd.to_datetime(contacts_df["updated_at"], unit="s")
    contacts_df["closest_task_at"] = pd.to_datetime(contacts_df["closest_task_at"], unit="s")

    contacts_df = add_timestamp(contacts_df)
    contacts_df.attrs["name"] = "sales_contacts"

    save_df_with_timestamp(df=contacts_df)
    return contacts_df


def amocrm_get_leads(headers):
    logging.info("SALES: Downloading leads...")
    leads = amocrm_get_all_items("leads", headers)
    leads_df = pd.DataFrame(leads)

    leads_df.fillna(0, inplace=True)
    leads_df = clean_string_columns(leads_df)
    leads_df = normalize_columns(leads_df)

    leads_df["created_at"] = pd.to_datetime(leads_df["created_at"], unit="s")
    leads_df["updated_at"] = pd.to_datetime(leads_df["updated_at"], unit="s")
    leads_df["closed_at"] = pd.to_datetime(leads_df["closed_at"], unit="s")
    leads_df["closest_task_at"] = pd.to_datetime(leads_df["closest_task_at"], unit="s")

    leads_df = add_timestamp(leads_df)
    leads_df.attrs["name"] = "sales_leads"

    save_df_with_timestamp(df=leads_df)
    return leads_df


def amocrm_get_loss_reasons(headers):
    logging.info("SALES: Downloading loss reasons...")
    response = requests.get(f"{BASE_URL}/leads/loss_reasons", headers=headers)
    response.raise_for_status()
    loss_reasons = response.json()["_embedded"]["loss_reasons"]
    loss_reasons_df = pd.DataFrame(loss_reasons)

    loss_reasons_df.fillna(0, inplace=True)
    loss_reasons_df = clean_string_columns(loss_reasons_df)
    loss_reasons_df = normalize_columns(loss_reasons_df)

    loss_reasons_df["created_at"] = pd.to_datetime(loss_reasons_df["created_at"], unit="s")
    loss_reasons_df["updated_at"] = pd.to_datetime(loss_reasons_df["updated_at"], unit="s")
    loss_reasons_df = add_timestamp(loss_reasons_df)

    loss_reasons_dict = loss_reasons_df.set_index('id')['name'].astype(str).to_dict()
    update_json_cache("loss_reasons", loss_reasons_dict, 'amocrm_cache')

    loss_reasons_df.attrs["name"] = "sales_loss_reasons"
    save_df_with_timestamp(df=loss_reasons_df)
    return loss_reasons_df


def amocrm_get_pipelines_statuses(headers):
    logging.info("SALES: Downloading pipelines...")
    pipelines_resp = requests.get(f"{BASE_URL}/leads/pipelines", headers=headers)
    pipelines_resp.raise_for_status()
    pipelines = pipelines_resp.json()["_embedded"]["pipelines"]
    pipelines_df = pd.DataFrame(pipelines)

    pipelines_df.fillna(0, inplace=True)
    pipelines_df = clean_string_columns(pipelines_df)
    pipelines_df = normalize_columns(pipelines_df)

    pipelines_df = add_timestamp(pipelines_df)
    pipelines_df.attrs["name"] = "sales_pipelines"

    save_df_with_timestamp(df=pipelines_df)

    logging.info("SALES: Downloading statuses...")

    statuses = []
    for pipeline in pipelines:
        for status in pipeline["_embedded"].get("statuses", []):
            statuses.append({
                "pipeline_id": pipeline["id"],
                "status_id": status["id"],
                "status_name": status["name"]
            })

    statuses_df = pd.DataFrame(statuses)
    statuses_df.fillna(0, inplace=True)
    statuses_df = clean_string_columns(statuses_df)
    statuses_df = normalize_columns(statuses_df)

    statuses_df = add_timestamp(statuses_df)
    statuses_df.attrs["name"] = "sales_pipeline_statuses"

    save_df_with_timestamp(df=statuses_df)

    # update cache files
    statuses_dict = statuses_df.set_index('status_id')['status_name'].astype(str).to_dict()
    update_json_cache("statuses", statuses_dict, 'amocrm_cache')

    pipelines_dict = pipelines_df.set_index('id')['name'].astype(str).to_dict()
    update_json_cache("pipelines", pipelines_dict, 'amocrm_cache')

    return pipelines_df, statuses_df


def amocrm_get_tasks(headers):
    logging.info("SALES: Downloading tasks...")
    tasks = amocrm_get_all_items("tasks", headers)
    tasks_df = pd.DataFrame(tasks)
    tasks_df.fillna(0, inplace=True)

    tasks_df = clean_string_columns(tasks_df)
    tasks_df = normalize_columns(tasks_df)

    tasks_df["created_at"] = pd.to_datetime(tasks_df["created_at"], unit="s")
    tasks_df["updated_at"] = pd.to_datetime(tasks_df["updated_at"], unit="s")
    tasks_df["complete_till"] = pd.to_datetime(tasks_df["complete_till"], unit="s")

    tasks_df = add_timestamp(tasks_df)

    tasks_df.attrs["name"] = "sales_tasks"
    save_df_with_timestamp(df=tasks_df)
    return tasks_df


def amocrm_get_task_types(headers):
    logging.info("SALES: Downloading task types...")
    response = requests.get(f"{BASE_URL}/account?with=task_types", headers=headers)
    response.raise_for_status()
    task_types = response.json()["_embedded"]["task_types"]
    task_types_df = pd.DataFrame(task_types)

    task_types_df.fillna(0, inplace=True)
    task_types_df = clean_string_columns(task_types_df)
    task_types_df = normalize_columns(task_types_df)

    task_types_df = add_timestamp(task_types_df)
    task_types_dict = task_types_df.set_index('id')['code'].astype(str).to_dict()
    update_json_cache("task_types", task_types_dict, 'amocrm_cache')

    task_types_df.attrs["name"] = "sales_task_types"
    save_df_with_timestamp(df=task_types_df)
    return task_types_df


def amocrm_get_users(headers):
    logging.info("SALES: Downloading users...")
    users = amocrm_get_all_items("users", headers)
    users_df = pd.DataFrame(users)

    users_df.fillna(0, inplace=True)
    users_df = clean_string_columns(users_df)
    users_df = normalize_columns(users_df)

    users_df = add_timestamp(users_df)
    users_df.attrs["name"] = "sales_users"

    users_dict = users_df.set_index('id')['name'].astype(str).to_dict()
    update_json_cache("users", users_dict, 'amocrm_cache')

    save_df_with_timestamp(df=users_df)
    return users_df


'''
# Notes
logging.info("SALES: Downloading notes...")
notes = amocrm_get_all_items("notes", headers)
notes_df = pd.json_normalize(notes)

# Pipelines
logging.info("SALES: Downloading pipelines...")
pipelines_resp = requests.get(f"{BASE_URL}/pipelines", headers=headers)
pipelines_resp.raise_for_status()
pipelines = pipelines_resp.json()["_embedded"]["pipelines"]
pipelines_df = pd.json_normalize(pipelines)

# --- STATUSES ---
statuses = []
for pipeline in pipelines:
    for status in pipeline.get("statuses", []):
        statuses.append({
            "pipeline_id": pipeline["id"],
            "status_id": status["id"],
            "status_name": status["name"]
        })
statuses_df = pd.DataFrame(statuses)
'''