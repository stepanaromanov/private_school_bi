from src.utils.utils_dataframe import *
from src.etl.connect import *
import time
import ast
import requests
import numpy as np
import datetime
import requests
import pandas as pd
from configs.logging_config import get_logger
logger = get_logger(__name__)


# Function to fetch all transactions by paginating the API
def finance_fetch_all_transactions(token, year="6841869b8eb7901bc71c7807", branch="68417f7edbbdfc73ada6ef01"):
    url = "https://backend.eduschool.uz/moderator-api/cashbox/transaction/pagin"
    headers = eduschool_headers(token, branch=branch, year=year)

    transactions = []
    page = 1
    limit = 20
    total = None

    while True:
        params = {
            "search": "",
            "page": page,
            "limit": limit
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raises an error for bad status codes

        json_response = response.json()
        if json_response.get("code") != 0:
            raise ValueError(f"API returned non-zero code: {json_response.get('code')}")

        data = json_response["data"]
        if total is None:
            total = data["total"]

        transactions.extend(data["data"])

        if len(transactions) >= total:
            break

        page += 1


    # Create a flattened pandas DataFrame by exploding/flattening nested structures
    transactions_df = pd.json_normalize(transactions, sep="_")

    # Clean and enrich dfs
    transactions_df.fillna(0, inplace=True)
    transactions_df = clean_string_columns(transactions_df)
    transactions_df = normalize_columns(transactions_df)
    transactions_df = add_timestamp(transactions_df)

    with open("eduschool_cache/branches.json", "r") as f:
        filials  = json.load(f)

    transactions_df['filial'] = filials[branch]

    transactions_df.rename(columns={"type": "transaction_type"}, inplace=True)

    transactions_df.attrs["name"] = "finance_transactions"

    # Save df to CSV
    save_df_with_timestamp(df=transactions_df)

    return transactions_df
