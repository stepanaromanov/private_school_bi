from src.utils.utils_dataframe import *
from src.etl.connect import *
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from configs.logging_config import get_logger
logger = get_logger("etl_log")


def fetch_marketing_facebook_data(access_token, ad_account_ids, api_version="v24.0", period=1):
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
    def get_campaign_insights(campaign_id, period):

        today = datetime.today().date()
        since = today - timedelta(days=90)

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

        def get_params(period):
            params = {
                "access_token": access_token,
                "level": "campaign",
                "fields": fields_str,
                "time_range": time_range,
                "time_increment": period
            }
            return params

        params = get_params(period)
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get("data", [])

    # Step 3: Collect all insights into a DataFrame
    def collect_insights_to_df(ad_account_id, period):
        campaigns = get_campaigns(ad_account_id)
        all_rows = []

        for camp in campaigns:
            camp_id = camp["id"]
            insights = get_campaign_insights(camp_id, period)

            if not insights:
                # No data for campaign, create one row with zeros
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

            else:
                # Append all daily rows
                for daily_row in insights:
                    row_data = {**daily_row, **{
                        "campaign_id": camp_id,
                        "account_id": ad_account_id
                    }}
                    all_rows.append(row_data)

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
        df = collect_insights_to_df(ad_account_id, period)
        facebook_data = pd.concat([facebook_data, df], ignore_index=True)

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

    # facebook_data.rename(columns={'campaign_id': 'id'}, inplace=True)

    facebook_data['id'] = facebook_data['date_start'].astype(str).str[:10] + "_" + facebook_data['campaign_id'].astype(str)

    facebook_data.fillna(0, inplace=True)
    facebook_data = clean_string_columns(facebook_data)
    facebook_data = normalize_columns(facebook_data)
    facebook_data.attrs["name"] = "marketing_facebook"
    facebook_data = add_timestamp(facebook_data)
    save_df_with_timestamp(df=facebook_data)

    return facebook_data


def fetch_marketing_facebook_pages_data(access_token, ad_account_ids, api_version="v24.0"):
    def get_pages(ad_account_id):
        """Fetch all promote_pages associated with the ad account"""
        url = f"https://graph.facebook.com/{api_version}/{ad_account_id}/promote_pages"
        params = {
            "access_token": access_token,
            "fields": "id,name,access_token"  # You can request these fields on the Page nodes
        }
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        return resp.json().get("data", [])

    def get_page_snapshot(page_id):
        """Get current fan_count and followers_count"""
        url = f"https://graph.facebook.com/{api_version}/{page_id}"
        params = {
            "access_token": access_token,
            "fields": "fan_count,followers_count"
        }
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    # Collect all pages
    all_rows = []

    for account_id in ad_account_ids:
        pages = get_pages(account_id)

        for page in pages:
            page_id = page['id']
            page_name = page['name']
            page_token = page.get('access_token')  # This is the long-lived Page token

            if not page_token:
                print(f"No page access token for {page_name} ({page_id})")
                continue

            snapshot = get_page_snapshot(page_id)  # can still use user token here
            # daily_metrics = get_page_daily_insights(page_id, page_token)  # ← use page token

            row = {
                "date": datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=5))).date(),
                "ad_account_id": account_id,
                "page_id": page_id,
                "page_name": page['name'],
                "fan_count": snapshot.get("fan_count", 0),
                "followers_count": snapshot.get("followers_count", 0),
                # "fan_adds": daily_metrics.get("page_fan_adds", 0),
                # "fan_removes": daily_metrics.get("page_fan_removes", 0)
            }
            all_rows.append(row)

    # Convert to DataFrame
    df = pd.DataFrame(all_rows)
    df_grouped = (
        df.groupby(['date', 'page_id'], as_index=False)
        .agg({
            'page_name': 'first',
            'ad_account_id': lambda x: ",".join(sorted(set(x))),  # combine duplicates
            'fan_count': 'max',
            'followers_count': 'max',
        })
    )

    """
    # Meta business additional permissions needed 
    
    def get_page_daily_insights(page_id, page_access_token):
    url = f"https://graph.facebook.com/{api_version}/{page_id}/insights"
    
    # Tashkent time (UTC+5)
    tashkent = timezone(timedelta(hours=5))
    
    # "Since" at start of day, 3 days ago
    since_ts = (
        datetime.now(timezone.utc)
        .astimezone(tashkent)
        .replace(hour=0, minute=0, second=0, microsecond=0)
        - timedelta(days=3)
    ).timestamp()
    
    params = {
        "access_token": page_access_token,
        "period": "day",
        "metric": (
            "page_video_views,page_video_views_paid,page_video_views_organic,"
            "page_video_view_time,page_views_total,page_actions_post_reactions_total,"
            "page_negative_feedback,page_places_checkins_total,page_consumptions,"
            "page_fans_added,page_impressions,page_impressions_organic,page_impressions_paid,"
            "page_engaged_users,page_post_engagements,page_follows"
        ),
        "since": int(since_ts),
    }

    resp = requests.get(url, params=params)
    if not resp.ok:
        print(f"Insights failed for {page_id}:", resp.json())
        return {"page_fan_adds": 0}

    result = {}
    for m in resp.json().get("data", []):
        name = m["name"]
        result[name] = m["values"][-1]["value"] if m.get("values") else 0
    return result
    """

    df_grouped['id'] = df_grouped['date'].astype(str).str[:10] + df_grouped['page_id'].astype(str)

    df_grouped.fillna(0, inplace=True)
    df_grouped = clean_string_columns(df_grouped)
    df_grouped = normalize_columns(df_grouped)
    df_grouped.attrs["name"] = "marketing_facebook_pages"
    df_grouped = add_timestamp(df_grouped)
    save_df_with_timestamp(df=df_grouped)

    return df_grouped
