# pip freeze > requirements.txt
import json
import os
import logging
from configs import logging_config
import requests
import pandas as pd
from amocrm.v2 import tokens
from datetime import datetime, timedelta


def amocrm_headers(access_token):
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }


def amocrm_initial_token(auth_code):
    # Load credentials from JSON file
    with open("credentials/amocrm.json", "r") as f:
        creds = json.load(f)

    AMO_DOMAIN = creds["base_domain"]

    """Exchange authorization code for initial access and refresh tokens (run this once)"""
    url = f'https://{AMO_DOMAIN}/oauth2/access_token'
    data = {
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": creds["redirect_uri"]
    }
    response = requests.post(url, json=data)
    try:
        response.raise_for_status()
    except requests.HTTPError:
        print("Extended error details from amoCRM:", response.text)
        raise
    token_data = response.json()

    # Update credentials.json with new tokens
    # Optional: store initial access token
    creds["access_token"] = token_data["access_token"]
    creds["refresh_token"] = token_data["refresh_token"]
    creds["timestamp"] = pd.Timestamp.now(tz="Asia/Tashkent").isoformat()

    with open("credentials/amocrm.json", "w") as f:
        json.dump(creds, f, indent=4)

    return token_data["access_token"]


def amocrm_refresh_token():
    try:
        # Load credentials from JSON file
        with open("credentials/amocrm.json", "r") as f:
            creds = json.load(f)

        AMO_DOMAIN = creds["base_domain"]
        CLIENT_ID = creds["client_id"]
        CLIENT_SECRET = creds["client_secret"]
        REFRESH_TOKEN = creds["refresh_token"]
        REDIRECT_URI = creds["redirect_uri"]

        BASE_URL = f'https://{AMO_DOMAIN}/api/v4'

        # Access token refresh function
        def amocrm_get_access_token():
            """Refresh access token via OAuth2"""
            url = f'https://{AMO_DOMAIN}/oauth2/access_token'

            data = {
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "grant_type": "refresh_token",
                "refresh_token": REFRESH_TOKEN,
                "redirect_uri": REDIRECT_URI
            }

            response = requests.post(url, json=data)
            response.raise_for_status()
            token_data = response.json()

            # Optionally update local credentials.json
            creds["access_token"] = token_data["access_token"]
            creds["refresh_token"] = token_data["refresh_token"]
            creds["timestamp"] = pd.Timestamp.now(tz="Asia/Tashkent").isoformat()

            with open("credentials/amocrm.json", "w") as f:
                json.dump(creds, f, indent=4)

            return token_data["access_token"]

        access_token = amocrm_get_access_token()
        return access_token

    except Exception as e:
        print(f"❌ Failed to get token: {e}")


def eduschool_token():
    # File paths
    CREDENTIALS_FILE = 'credentials/eduschool.json'
    TOKEN_FILE = 'tokens/eduschool.json'

    # Check if token file exists and if it's still valid (less than 2 weeks old)
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'r') as f:
            token_data = json.load(f)
            stored_timestamp = datetime.fromisoformat(token_data['timestamp'])
            if datetime.now().replace(tzinfo=None) - stored_timestamp.replace(tzinfo=None) < timedelta(weeks=49):
                logging.info("Eduschool. Using existing valid token.")
                token = token_data['token']
                # Exit or use the token as needed
            else:
                logging.info("Eduschool. Token is older than 49 weeks. Deleting old file and refreshing.")
                os.remove(TOKEN_FILE)
                refresh_token = True
    else:
        logging.info("Eduschool. No token file found. Refreshing token.")
        refresh_token = True

    # Refresh the token if needed
    if 'refresh_token' in locals() and refresh_token:
        # Load credentials
        with open(CREDENTIALS_FILE, 'r') as f:
            credentials = json.load(f)
            login = credentials['login']
            password = credentials['password']

        # API endpoint
        url = 'https://backend.eduschool.uz/moderator-api/sign-in'

        # Payload
        payload = {
            'login': login,
            'password': password
        }

        # Headers based on the provided request details
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'accept-language': 'en-US,en;q=0.9',
            'authorization': 'Bearer null',
            'branch': 'null',
            'content-type': 'application/json',
            'language': 'uz',
            'organization': 'test',
            'origin': 'https://omonschool.eduschool.uz',
            'referer': 'https://omonschool.eduschool.uz/',
            'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            "Accept-Encoding": "br, gzip, deflate",
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
        }

        # Send POST request
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # Raise error if not 200

        # Extract token from response (nested under the first key in 'data')
        response_data = response.json()
        data_dict = response_data['data']
        token_key = list(data_dict.keys())[0]  # Get the dynamic key (e.g., "6841869b8eb7901bc71c7807")
        token = data_dict[token_key]['token']

        # Store new token with current timestamp
        new_token_data = {
            'token': token,
            'timestamp': datetime.now().isoformat()
        }
        os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)  # Ensure directory exists
        with open(TOKEN_FILE, 'w') as f:
            json.dump(new_token_data, f)

        logging.info("Eduschool. New token stored.")

    return token

def eduschool_headers(token, year, branch):

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

    return headers

