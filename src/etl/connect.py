#pip freeze > requirements.txt
import json
import os
import logging
import requests
from datetime import datetime, timedelta
logger = logging.getLogger("omonschool_etl")

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
                logger.info("Eduschool. Using existing valid token.")
                token = token_data['token']
                # Exit or use the token as needed
            else:
                logger.info("Eduschool. Token is older than 49 weeks. Deleting old file and refreshing.")
                os.remove(TOKEN_FILE)
                refresh_token = True
    else:
        logger.info("Eduschool. No token file found. Refreshing token.")
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

        logger.info("Eduschool. New token stored.")

    return token
