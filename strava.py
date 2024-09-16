# Standard imports
import requests
import json
import time
import os
from typing import Dict, Any

# Third party imports
import pandas as pd

# Local imports
from modules.postgres import Postgres


def fetch_strava_activities(
        client_id:str,
        client_secret:str
    ) -> pd.DataFrame:
    """Returns a Pandas DataFrame containing user strava activity data from the activities endpoint."""

    # first, load access and refresh tokens:
    token_data = fetch_strava_tokens()

    # next up, set the access_token. 
    # check if the most recently pulled token has expired. 
    # refresh it and save locally if so, otherwise, use the current token
    # and make a request to strava's activities endpoint:

    if token_data["expires_at"] < time.time():
        new_token_data = refresh_access_token(
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=token_data["refresh_token"]
        )

        save_strava_tokens(
            token_file_path=token_file_path,
            token_data=new_token_data
        )

        access_token = new_token_data["access_token"]

    else:
        access_token = token_data["access_token"]

    if access_token:
        response = requests.get(
            url="https://www.strava.com/api/v3/athlete/activities",
            headers={"Authorization": f"Bearer {access_token}"}
        )

        # if our request is successful, parse the response and
        # stage activity records as a dictionary for later use
        if response.status_code == 200:
            activities_data = response.json()
            all_activities = {
                'activity_id': [],
                'activity_name': [],
                'athlete_id': [],
                'activity_type': [],
                'activity_timestamp': [],
                'activity_distance': [],
                'activity_duration_seconds': [],
                'activity_elevation_high': [],
                'activity_elevation_low': [],
                'activity_avg_speed': [],
                'activity_max_speed': [],
                'activity_begin_latitude': [],
                'activity_begin_longitude': [],
                'activity_end_latitude': [],
                'activity_end_longitude': []
            }

            for activity in activities_data:
                all_activities['activity_id'].append(activity["id"])
                all_activities['activity_name'].append(activity["name"])
                all_activities['athlete_id'].append(activity["athlete"]["id"])
                all_activities['activity_type'].append(activity["type"])
                all_activities['activity_timestamp'].append(activity["start_date_local"])
                all_activities['activity_distance'].append(activity["distance"])
                all_activities['activity_duration_seconds'].append(activity["elapsed_time"])
                all_activities['activity_elevation_high'].append(activity["elev_high"])
                all_activities['activity_elevation_low'].append(activity["elev_low"])
                all_activities['activity_avg_speed'].append(activity["average_speed"])
                all_activities['activity_max_speed'].append(activity["max_speed"])
                all_activities['activity_begin_latitude'].append(activity["start_latlng"][0])
                all_activities['activity_begin_longitude'].append(activity["start_latlng"][1])
                all_activities['activity_end_latitude'].append(activity["end_latlng"][0])
                all_activities['activity_end_longitude'].append(activity["end_latlng"][1])

            
            return pd.DataFrame(data=all_activities, columns=[key for key in all_activities.keys()])
        

        else:
            print(f"Error fetching data: {response.status_code}, {response.json()}")
    
    else:
        print("Could not get a valid access token.")


def fetch_strava_tokens() -> Dict[str, Any]:
    """Returns a JSON object containing Strava Access and Refresh tokens for API authentication."""

    with open(token_file_path, "r") as token_file:
        return json.load(token_file)


def refresh_access_token(
        client_id:str,
        client_secret:str,
        refresh_token:str
    ) -> Dict[str, Any]:
    """Returns a JSON object containing a non-expired Strava Access token."""

    response = requests.post(
        url="https://www.strava.com/oauth/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        }
    )

    return response.json()


def save_strava_tokens(
        token_file_path:str,
        token_data:dict
    ):
    """Save Strava Access & Refresh tokens locally so tokens persist beyond script completion."""

    with open(token_file_path, "w") as token_file:
        print("Saving strava access and refresh tokens...")
        json.dump(token_data, token_file)
        print("Tokens saved successfully!")


def load_strava_data_to_postgres():
    """Loads Strava source data to target Postgres table."""

    psql = Postgres()

    print("Fetching strava activities data...")
    strava_df = fetch_strava_activities(
        client_id=client_id,
        client_secret=client_secret
    )
    print("Successfully fetched strava activities data!")

    print("Loading strava data to Postgres...")
    psql.load_dataframe_to_table(
        df=strava_df,
        destination_table="sb.strava_activities"
    )
    print("Successfully loaded strava data to Postgres!")


def fetch_secrets() -> Dict[str, Any]:
    """Returns a JSON object containing user-specific Strava secrets stored locally."""

    strava_secrets_path = "/.secrets/.strava_secrets.json"

    with open(f"{os.path.dirname(os.path.abspath(__file__))}{strava_secrets_path}") as secrets:
        return json.load(secrets)


if __name__ == "__main__":
    secrets = fetch_secrets()
    client_id = secrets["client_id"]
    client_secret = secrets["client_secret"]
    token_file_path = secrets["token_file_path"]
    load_strava_data_to_postgres()