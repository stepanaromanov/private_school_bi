from src.utils.utils_dataframe import *
from src.utils.utils_general import *
import time
import ast
import requests
import numpy as np
import logging
from configs import logging_config
import datetime
import requests
import pandas as pd


# Function to fetch all transactions by paginating the API
def finance_fetch_all_transactions(token, year="6841869b8eb7901bc71c7807", branch="68417f7edbbdfc73ada6ef01"):
    url = "https://backend.eduschool.uz/moderator-api/cashbox/transaction/pagin"
    headers = {
        "academicyearid": f"{year}",
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "authorization": f"Bearer {token}",
        "branch": f"{branch}",
        "connection": "keep-alive",
        "host": "backend.eduschool.uz",
        "language": "uz",
        "organization": "test",
        "origin": "https://omonschool.eduschool.uz",
        "referer": "https://omonschool.eduschool.uz/",
        "sec-ch-ua": "\"Chromium\";v=\"142\", \"Google Chrome\";v=\"142\", \"Not_A Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"macOS\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
    }

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

    transactions_df.attrs["name"] = "finance_transactions"

    # Save df to CSV
    save_df_with_timestamp(df=transactions_df)

    return transactions_df
