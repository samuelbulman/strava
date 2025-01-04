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
    """Returns a Pandas DataFrame containing a strava athletes activity data from the activities endpoint(s)."""

    access_token = access_token_workflow(
        client_id=client_id,
        client_secret=client_secret
    )

    if access_token:
        activities_data = strava_api_activities_response(access_token, detailed_activities=False)
        activities = {
            "activity_id": [],
            "activity_name": [],
            "athlete_id": [],
            "activity_type": [],
            "activity_timestamp": [],
            "activity_distance": [],
            "activity_duration_seconds": [],
            "activity_elevation_high": [],
            "activity_elevation_low": [],
            "activity_avg_speed": [],
            "activity_max_speed": [],
            "calories_burned": [],
        }

        for activity in activities_data:
            activities["activity_id"].append(activity.get("id"))
            activities["activity_name"].append(activity.get("name"))
            activities["athlete_id"].append(activity.get("athlete", {}).get("id", None))
            activities["activity_type"].append(activity.get("type"))
            activities["activity_timestamp"].append(activity.get("start_date_local"))
            activities["activity_distance"].append(activity.get("distance"))
            activities["activity_duration_seconds"].append(activity.get("elapsed_time"))
            activities["activity_elevation_high"].append(activity.get("elev_high"))
            activities["activity_elevation_low"].append(activity.get("elev_low"))
            activities["activity_avg_speed"].append(activity.get("average_speed"))
            activities["activity_max_speed"].append(activity.get("max_speed"))
            # annoyingly, we have to hit a detailed activities endpoint to fetch how many calories were burned during a workout :roll-eyes:
            detailed_activity_response = strava_api_activities_response(access_token, detailed_activities=True, activity_id=activity.get("id"))
            activities["calories_burned"].append(detailed_activity_response.get("calories"))
        
        return pd.DataFrame(data=activities, columns=[key for key in activities.keys()])
            
    else:
        print("Could not get a valid access token.")
    

def strava_api_activities_response(
        access_token:str,
        detailed_activities:bool=False,
        activity_id:int=None,
    ) -> dict:
    """
    Makes a request to Strava's activities endpoint and returns a json response containing athlete activity data if successful.
    
    Parameters
    ----------
    detailed_activities (bool):
    - When set to False, a request is made to return a list of all athelete activities (https://developers.strava.com/docs/reference/#api-Activities-getLoggedInAthleteActivities).
    - When set to True, a request is made to the activities endpoint for a specific activity_id, returning detailed information about that activity (https://developers.strava.com/docs/reference/#api-models-DetailedActivity).
    """
    
    headers = {"Authorization": f"Bearer {access_token}"}

    if detailed_activities:
        url = f"https://www.strava.com/api/v3/activities/{activity_id}"
    else:
        url = "https://www.strava.com/api/v3/athlete/activities"

    response = requests.get(url=url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}, {response.json()}")


def fetch_strava_athletes(
        client_id:str,
        client_secret:str
    ) -> pd.DataFrame:
    """Returns a Pandas DataFrame containing user strava athlete data from the athlete endpoint."""

    access_token = access_token_workflow(
        client_id=client_id,
        client_secret=client_secret
    )

    if access_token:
        response = requests.get(
            url="https://www.strava.com/api/v3/athlete",
            headers={"Authorization": f"Bearer {access_token}"}
        )

         # if our request is successful, parse the response and
        # stage activity records as a dictionary for later use
        if response.status_code == 200:
            athlete = response.json()
            user_dict = {
                "athlete_id": [athlete["id"]],  #  this will need to be refactored when multiple athletes data is pulled in single run
                "athlete_full_name": [athlete["firstname"]+" "+athlete["lastname"]]
            }

        return pd.DataFrame(data=user_dict, columns=[key for key in user_dict.keys()])


def access_token_workflow(
        client_id:str,
        client_secret:str,
    ) -> str:
    """Returns a non-expired Strava Access token for API authentication."""

    # first, load locally stored strava access and refresh tokens:
    token_data = load_local_token_data()

    # next up, set the access_token. 
    # check if the most recently pulled token has expired. 
    # refresh it and save locally if so, otherwise, use the current token
    # and make a request to strava's activities endpoint:

    if token_data["expires_at"] < time.time():
        response = requests.post(
            url="https://www.strava.com/oauth/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "refresh_token",
                "refresh_token": token_data["refresh_token"]
            }
        )

        new_token_data = response.json()

        save_token_data_locally(
            token_file_path=token_file_path,
            token_data=new_token_data
        )

        return new_token_data["access_token"]

    else:
        return token_data["access_token"]


def load_local_token_data() -> Dict[str, Any]:
    """Returns a JSON object containing Strava Access and Refresh tokens for API authentication."""

    with open(token_file_path, "r") as token_file:
        return json.load(token_file)


def save_token_data_locally(
        token_file_path:str,
        token_data:dict
    ):
    """Save Strava Access & Refresh tokens locally so tokens persist beyond script completion."""

    with open(token_file_path, "w") as token_file:
        print("Saving strava access and refresh tokens...")
        json.dump(token_data, token_file)
        print("Tokens saved successfully!")


def load_strava_data_to_postgres(
        strava_client_id:str,
        strava_client_secret:str
    ):
    """Loads Strava source data to target Postgres table."""

    psql = Postgres()

    print("Fetching strava activities data...\n")
    strava_df = fetch_strava_activities(
        client_id=strava_client_id,
        client_secret=strava_client_secret
    )
    print("Successfully fetched strava activities data!\n")

    print("Loading strava activities data to Postgres...\n")
    psql.load_dataframe_to_table(
        df=strava_df,
        schema="strava",
        table="strava_activities"
    )
    print("\nSuccessfully loaded strava activities data to Postgres!\n")

    print("Fetching strava athletes data...\n")
    strava_df = fetch_strava_athletes(
        client_id=strava_client_id,
        client_secret=strava_client_secret
    )
    print("Successfully fetched strava athletes data!\n")

    print("Loading strava athletes data to Postgres...\n")
    psql.load_dataframe_to_table(
        df=strava_df,
        schema="strava",
        table="strava_athletes"
    )
    print("\nSuccessfully loaded strava athletes data to Postgres!\n")


def fetch_secrets() -> Dict[str, Any]:
    """Returns a JSON object containing user-specific Strava secrets stored locally."""

    strava_secrets_path = "/.secrets/.strava_secrets.json"

    with open(f"{os.path.dirname(os.path.abspath(__file__))}{strava_secrets_path}") as secrets:
        return json.load(secrets)


if __name__ == "__main__":
    secrets = fetch_secrets()
    strava_client_id = secrets["client_id"]
    strava_client_secret = secrets["client_secret"]
    token_file_path = secrets["token_file_path"]
    load_strava_data_to_postgres(
      strava_client_id=strava_client_id,
      strava_client_secret=strava_client_secret
    )