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

    wanted = [
        "utmcontent",
        "utmmedium",
        "utmcampaign",
        "utmsource",
        "utmterm",
        "utmreferrer",
        "referrer",
        "Kurslar",
        "Filial"
    ]

    # build mapping for case-insensitive lookup but preserve original column names
    lower_to_col = {f.lower(): f for f in wanted}

    def extract_wanted(cf_list):
        # initialize result with None for each wanted column
        res = {col: 'UNKNOWN' for col in wanted}
        if not cf_list:
            return res
        for field in cf_list:
            name = field.get("field_name")
            if not name:
                continue
            key = lower_to_col.get(name.lower())
            if not key:
                continue
            # collect values — if multiple, join with ';'
            vals = field.get("values") or []
            # sometimes values may be plain strings; handle that defensively
            extracted = []
            for v in vals:
                if isinstance(v, dict) and "value" in v:
                    extracted.append("" if v["value"] is None else str(v["value"]))
                else:
                    extracted.append(str(v))
            # set None if extracted is empty
            res[key] = ";".join(extracted) if extracted else None
        return res

    # make dataframe and expand the extracted columns
    custom_df = pd.DataFrame(leads)
    expanded = custom_df["custom_fields_values"].apply(lambda x: pd.Series(extract_wanted(x)))
    leads_df = pd.concat([leads_df.drop(columns=["custom_fields_values"]), expanded], axis=1)

    # function to extract up to 10 tags
    def extract_tags(embedded):
        tags = embedded.get("tags")
        # always return exactly 10 values (fill with None if fewer)
        if not tags:
            return ["UNKNOWN"] * 10
        tag_names = [t.get("name") for t in tags[:10]]
        return tag_names + ["UNKNOWN"] * (10 - len(tag_names))

    # apply function and expand into separate columns
    tag_cols = leads_df["_embedded"].apply(extract_tags).apply(pd.Series)
    tag_cols.columns = [f"tag_{i + 1}" for i in range(10)]

    # combine with original df
    leads_df = pd.concat([leads_df.drop(columns=["_embedded"]), tag_cols], axis=1)

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


def amocrm_get_tags_custom_fields(headers):
    logging.info("SALES: Downloading tags and custom fields...")

    tags_df = pd.DataFrame()
    custom_fields_df = pd.DataFrame()

    for entity in ["leads", "contacts", "companies"]:
        for field in ["tags", "custom_fields"]:
            response = requests.get(f"{BASE_URL}/{entity}/{field}", headers=headers)
            response.raise_for_status()

            data = response.json()["_embedded"][field]
            df = pd.DataFrame(data)
            df["entity"] = entity

            if field == "tags":
                tags_df = pd.concat([tags_df, df], ignore_index=True)
            elif field == "custom_fields":
                custom_fields_df = pd.concat([custom_fields_df, df], ignore_index=True)

    tags_df.fillna(0, inplace=True)
    tags_df = clean_string_columns(tags_df)
    tags_df = normalize_columns(tags_df)
    tags_df = add_timestamp(tags_df)
    tags_df = tags_df.rename(columns={"id": "amocrm_id"})
    # Add a new serial 'id' column starting from 1
    tags_df.insert(0, "id", range(1, len(tags_df) + 1))

    tags_dict = tags_df.set_index('id')['name'].astype(str).to_dict()
    update_json_cache("tags", tags_dict, 'amocrm_cache')

    tags_df.attrs["name"] = "sales_tags"
    save_df_with_timestamp(df=tags_df)

    custom_fields_df.fillna(0, inplace=True)
    custom_fields_df = clean_string_columns(custom_fields_df)
    custom_fields_df = normalize_columns(custom_fields_df)
    custom_fields_df = add_timestamp(custom_fields_df)
    custom_fields_df = custom_fields_df.rename(columns={"id": "amocrm_id"})
    # Add a new serial 'id' column starting from 1
    custom_fields_df.insert(0, "id", range(1, len(custom_fields_df) + 1))

    custom_fields_dict = custom_fields_df.set_index('id')['name'].astype(str).to_dict()
    update_json_cache("custom_fields", custom_fields_dict, 'amocrm_cache')

    custom_fields_df.attrs["name"] = "sales_custom_fields"
    save_df_with_timestamp(df=custom_fields_df)

    return tags_df, custom_fields_df


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