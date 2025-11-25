from src.utils.utils_dataframe import *
from src.etl.connect import *
import requests
import pandas as pd
from datetime import datetime, timedelta
from configs.logging_config import get_logger
logger = get_logger("etl_log")


def fetch_marketing_facebook_data(access_token, ad_account_ids, api_version="v24.0"):
    # Step 1: Get all campaigns
    def get_campaigns(ad_account_id):
        url = f"https://graph.facebook.com/{api_version}/{ad_account_id}/campaigns"
        params = {
            "access_token": access_token,
            "fields": "id,name,status,effective_status"
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get("data", [])

    # Step 2: Get insights for a single campaign
    def get_campaign_insights(campaign_id):

        today = datetime.today().date()
        since = today - timedelta(days=30)

        time_range = json.dumps({
            "since": str(since),
            "until": str(today)
        })

        url = f"https://graph.facebook.com/{api_version}/{campaign_id}/insights"

        fields = [
            "campaign_name",
            "campaign_id",
            "impressions",
            "clicks",
            "spend",
            "ctr",
            "cpc",
            "cpm",
            "reach",
            "frequency",
            "conversions",
            "actions",
            "unique_actions",
            "unique_clicks",
            "quality_ranking",
            "engagement_rate_ranking",
            "conversion_rate_ranking",
            "date_start",
            "date_stop"
        ]

        # Join as a single comma-separated string
        fields_str = ",".join(fields)

        params = {
            "access_token": access_token,
            "level": "campaign",
            "fields": fields_str,
            "time_range": time_range
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get("data", [])

    # Step 3: Collect all insights into a DataFrame
    def collect_insights_to_df(ad_account_id):
        campaigns = get_campaigns(ad_account_id)
        all_rows = []

        for camp in campaigns:
            camp_id = camp["id"]
            insights = get_campaign_insights(camp_id)

            if not insights:
                all_rows.append({
                    "campaign_id": camp_id,
                    "campaign_name": camp["name"],
                    "impressions": 0,
                    "clicks": 0,
                    "spend": 0,
                    "ctr": 0,
                    "cpc": 0,
                    "cpm": 0,
                    "reach": 0,
                    "frequency": 0,
                    "conversions": None,
                    "actions": None,
                    "unique_actions": 0,
                    "unique_clicks": 0,
                    "quality_ranking": None,
                    "engagement_rate_ranking": None,
                    "conversion_rate_ranking": None,
                    "date_start": None,
                    "date_stop": None
                })

                continue

            # Facebook always returns a list; we use the first row
            row = insights[0]
            all_rows.append(row)

        df = pd.DataFrame(all_rows)

        # Convert numeric columns (optional)
        numeric_cols = ["impressions", "clicks", "spend", "ctr", "cpc", "cpm", "reach", "frequency", "unique_actions",
                        "unique_clicks"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df['account_id'] = ad_account_id

        return df

    # Initialize an empty DataFrame
    facebook_data = pd.DataFrame()

    # Loop through all ad accounts and collect insights
    for ad_account_id in ad_account_ids:
        df = collect_insights_to_df(ad_account_id)
        facebook_data = pd.concat([facebook_data, df], ignore_index=True)

    """
    for col in ["date_start", "date_stop"]:
        # Convert to datetime, coerce errors to NaT
        facebook_data[col] = pd.to_datetime(facebook_data[col], errors="coerce")

        # Fill missing values with a default timestamp
        facebook_data[col] = facebook_data[col].fillna(pd.Timestamp("1970-01-01T00:00:00Z"))

        # Only attempt tz_localize if column is datetime-like
        if pd.api.types.is_datetime64_any_dtype(facebook_data[col]):
            # Localize to UTC if tz-naive
            if facebook_data[col].dt.tz is None:
                facebook_data[col] = facebook_data[col].dt.tz_localize("UTC")
        else:
            # Convert non-datetime column to UTC timestamp Series
            facebook_data[col] = pd.to_datetime(facebook_data[col], errors="coerce").dt.tz_localize("UTC")
    """

    for col in ["date_start", "date_stop"]:
        # Convert to datetime, coerce errors to NaT
        facebook_data[col] = pd.to_datetime(facebook_data[col], errors="coerce")

        # Standardize timezone to UTC if column is datetime-like
        if pd.api.types.is_datetime64_any_dtype(facebook_data[col]):
            if facebook_data[col].dt.tz is None:
                # Localize to UTC if tz-naive
                facebook_data[col] = facebook_data[col].dt.tz_localize("UTC")
            else:
                # Convert to UTC if already tz-aware
                facebook_data[col] = facebook_data[col].dt.tz_convert("UTC")

        # Fill missing values with a default UTC timestamp
        facebook_data[col] = facebook_data[col].fillna(pd.Timestamp("1970-01-01T00:00:00Z"))

    facebook_data.rename(columns={'campaign_id': 'id'}, inplace=True)

    facebook_data.fillna(0, inplace=True)
    facebook_data = clean_string_columns(facebook_data)
    facebook_data = normalize_columns(facebook_data)
    facebook_data.attrs["name"] = "marketing_facebook"
    facebook_data = add_timestamp(facebook_data)
    save_df_with_timestamp(df=facebook_data)

    return facebook_data
