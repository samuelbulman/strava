import time
import json
import requests
import logging
from typing import Dict, Any
from ..utilities.secrets import fetch_secrets

secrets = fetch_secrets()
token_file_path = secrets.get("token_file_path")

logger = logging.getLogger(__name__)

def get_access_token(client_id:str, client_secret:str) -> str:
    """Returns a non-expired Strava Access token for API authentication."""

    # first, load locally stored strava access and refresh tokens:
    token_data = load_local_token_data()

    # next up, set the access_token. 
    # check if the most recently pulled token has expired.
    # refresh it and save locally if so. otherwise, use the current token
    # so we can make requests to strava's api endpoints.

    if token_data.get("expires_at") < time.time():
        response = requests.post(
            url="https://www.strava.com/oauth/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "refresh_token",
                "refresh_token": token_data.get("refresh_token")
            }
        )

        new_token_data = response.json()

        save_token_data_locally(
            token_file_path=token_file_path,
            token_data=new_token_data
        )

        return new_token_data.get("access_token")

    else:
        return token_data.get("access_token")


def load_local_token_data() -> Dict[str, Any]:
    """Returns a JSON object containing Strava Access and Refresh tokens for API authentication."""

    with open(token_file_path, "r") as token_file:
        return json.load(token_file)


def save_token_data_locally(token_file_path:str, token_data:dict) -> bool:
    """Save Strava Access & Refresh tokens locally so tokens persist beyond script completion."""

    with open(token_file_path, "w") as token_file:
        logger.info("Saving strava access and refresh tokens...")
        json.dump(token_data, token_file)
        logger.info("Tokens saved successfully.")
        return True