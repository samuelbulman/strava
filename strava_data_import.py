# Standard imports
import requests
import json
import time
import os
from typing import Dict, Any

# Third party imports
import pandas as pd
from dotenv import load_dotenv
from pprint import pprint

# Local imports
from modules.postgres import Postgres
from modules.utils import log_prefix


def inspect_response(
        client_id:str,
        client_secret:str
    ):
    """Call adhoc when you want to print/inspect the response of the Strava API"""
    try:
        access_token = access_token_workflow(
            client_id=client_id,
            client_secret=client_secret
        )

        if access_token:
            activities_data = strava_api_activities_response(access_token)
            pprint(activities_data)
    
    except Exception as e:
        print(f"Error - {e}")


def fetch_strava_activities(
        client_id:str,
        client_secret:str
    ) -> pd.DataFrame:
    """Returns a Pandas DataFrame containing a strava athletes activity data from the activities endpoint(s)."""

    try:
        access_token = access_token_workflow(
            client_id=client_id,
            client_secret=client_secret
        )

        if access_token:
            activities_data = strava_api_activities_response(access_token)
            activities = {
                "id": [],
                "name": [],
                "athlete_id": [],
                "type": [],
                "created_at": [],
                "distance": [],
                "duration_seconds": [],
                "elevation_high": [],
                "elevation_low": [],
                "avg_speed": [],
                "max_speed": [],
                "calories_burned": [],
                "average_heartrate": [],
                "max_heartrate": [],
            }

            existing_activities = fetch_existing_activity_ids()

            print(f"""{log_prefix(log_type='info')} {len(activities_data)} activities fetched from Strava API.
{log_prefix(log_type='info')} {len(exclusion_activities)} activities will be excluded from import.
{log_prefix(log_type='info')} {len(existing_activities['id'])} activity records retrieved from Postgres.
{log_prefix(log_type='info')} {len(activities_data) - len(existing_activities['id']) - len(exclusion_activities)} new records will be imported.""")

            for activity in activities_data:
                if activity.get("id") in exclusion_activities:
                    continue

                activities["id"].append(activity.get("id"))
                activities["name"].append(activity.get("name"))
                activities["athlete_id"].append(activity.get("athlete", {}).get("id", None))
                activities["type"].append(activity.get("type"))
                activities["created_at"].append(activity.get("start_date_local"))
                activities["distance"].append(activity.get("distance"))
                activities["duration_seconds"].append(activity.get("elapsed_time"))
                activities["elevation_high"].append(activity.get("elev_high"))
                activities["elevation_low"].append(activity.get("elev_low"))
                activities["avg_speed"].append(activity.get("average_speed"))
                activities["max_speed"].append(activity.get("max_speed"))
                activities["average_heartrate"].append(activity.get("average_heartrate"))
                activities["max_heartrate"].append(activity.get("max_heartrate"))

                # Annoyingly, we have to hit a separate, detailed-activities endpoint to fetch how many calories were burned during a workout :roll-eyes:
                # To avoid rate limit errors, we can grab previously fetched calorie counts from postgres. if the current activity_id in the loop has not 
                # yet been loaded to postgres, then we will hit the detailed activities endpoint to grab that information.

                if activity.get("id") in existing_activities["id"]:
                    activities["calories_burned"].append(existing_activities["calories"][existing_activities["id"].index(activity.get("id"))])

                else:
                    print(f"{log_prefix(log_type='info')} New {activity.get('type')} activity detected: {activity.get('name')} on {activity.get('start_date_local')}")
                    detailed_activity_response = strava_api_detailed_activities_response(access_token, activity_id=activity.get("id"))
                    activities["calories_burned"].append(detailed_activity_response.get("calories"))
            
            # Convert dict to Pandas dataframe and set all NaN values to None so Postgres correctly interprets these as nulls
            df = pd.DataFrame(data=activities, columns=[key for key in activities.keys()])
            df = df.astype(object) 
            df = df.where(pd.notna(df), None)

            return df
                
        else:
            print(f"{log_prefix(log_type='error')} Something went wrong - could not obtain valid access token?")
    
    except Exception as e:
        print(f"{log_prefix(log_type='error')} Error fetching Strava data:  {e}")
    

def strava_api_activities_response(access_token:str) -> dict:
    """
    Makes a request to Strava's activities endpoint and returns a json response containing athlete activity data if successful.
    
    Parameters
    ----------
    access_token (str)
    - fill
    """

    activities = []
    page = 1
    per_page = 50
    headers = {"Authorization": f"Bearer {access_token}"}
    url = "https://www.strava.com/api/v3/athlete/activities"


    while True:
        params = {'per_page': per_page, 'page': page}
        response = requests.get(url=url, headers=headers, params=params)
        json_response = response.json()

        # break out of while loop once all activities have been exhausted
        if len(json_response) == 0:
            break

        if response.status_code == 200:
            activities.extend(json_response)
            
        else:
            print(f"{log_prefix(log_type='error')} Error fetching data: {response.status_code}, {response.json()}")
            break

        page += 1
    
    return activities


def strava_api_detailed_activities_response(
        access_token:str,
        activity_id:int=None,
    ) -> dict:
    """
    Makes a request to Strava's detailed activities endpoint and returns a json response
    json response containing extrra information for an individual activity if successful.
    
    Parameters
    ----------
    activity_id (int):
    - The specific activity to fetch detailed information about (https://developers.strava.com/docs/reference/#api-models-DetailedActivity).
    """

    headers = {"Authorization": f"Bearer {access_token}"}

    url = f"https://www.strava.com/api/v3/activities/{activity_id}"

    response = requests.get(url=url, headers=headers)

    if response.status_code == 200:
        return response.json()
    
    else:
        print(f"{log_prefix(log_type='error')} Error fetching data: {response.status_code}, {response.json()}")


def fetch_existing_activity_ids() -> dict:
    """Retrieves Strava activities that have already been loaded to Postgres. Return value is a dictionary of activity id's and their respective calories count."""

    existing_activity_records = {
        "id": [],
        "calories": []
    }

    with Postgres() as psql:
        if psql.table_exists(schema="strava", table="activities"):
            existing_activity_records = {
                "id": [],
                "calories": []
            }

            _, existing_activities = psql.query_postgres(sql_query="select distinct id, calories_burned from strava.activities;")

            for row in existing_activities:
                existing_activity_records["id"].append(row[0])
                existing_activity_records["calories"].append(row[1])
            
    return existing_activity_records
        

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

        # if our request is successful, parse the response
        # and stage activity records for later reference
        if response.status_code == 200:
            athlete = response.json()
            user_dict = {
                "id": [athlete.get("id")],  #  this will need to be refactored when multiple athletes data is pulled in single run
                "first_name": [athlete.get("firstname")],
                "last_name": [athlete.get("lastname")]
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
        print(f"{log_prefix(log_type='info')} Saving strava access and refresh tokens...")
        json.dump(token_data, token_file)
        print(f"{log_prefix(log_type='info')} Tokens saved successfully!")


def load_strava_data_to_postgres(
        strava_client_id:str,
        strava_client_secret:str
    ):
    """Loads Strava source data to target Postgres table."""
    
    try:
        print(f"{log_prefix(log_type='info')} Fetching strava activities data...")
        strava_activities_df = fetch_strava_activities(
            client_id=strava_client_id,
            client_secret=strava_client_secret
        )
        print(f"{log_prefix(log_type='info')} Successfully fetched strava activities data!")
    
    except Exception as e:
        print(f"{log_prefix(log_type='error')} Error fetching strava activities data:\n\n{e}")

    try:
        print(f"{log_prefix(log_type='info')} Fetching strava athletes data...")
        strava_athletes_df = fetch_strava_athletes(
            client_id=strava_client_id,
            client_secret=strava_client_secret
        )
        print(f"{log_prefix(log_type='info')} Successfully fetched strava athletes data!")
    
    except Exception as e:
        print(f"{log_prefix(log_type='error')} Error fetching strava athletes data:\n\n{e}")

    with Postgres() as psql:
        
        try:
            print(f"{log_prefix(log_type='info')} Loading strava activities data to Postgres...")
            psql.load_dataframe_to_table(
                df=strava_activities_df,
                schema="strava",
                table="activities"
            )
            print(f"{log_prefix(log_type='info')} Successfully loaded strava activities data to Postgres!")
   
        except Exception as e:
            print(f"{log_prefix(log_type='error')} Error loading strava activities data:\n\n{e}")
        
        try:
            print(f"{log_prefix(log_type='info')} Loading strava athletes data to Postgres...")
            psql.load_dataframe_to_table(
                df=strava_athletes_df,
                schema="strava",
                table="athletes"
            )
            print(f"{log_prefix(log_type='info')} Successfully loaded strava athletes data to Postgres!")
        
        except Exception as e:
            print(f"{log_prefix(log_type='error')} Error loading strava athletes data:\n\n{e}")


def fetch_secrets() -> Dict[str, Any]:
    """Returns a JSON object containing user-specific Strava secrets stored locally."""

    strava_secrets_path = "/.secrets/.strava_secrets.json"

    with open(f"{os.path.dirname(os.path.abspath(__file__))}{strava_secrets_path}") as secrets:
        return json.load(secrets)


if __name__ == "__main__":
    load_dotenv()

    start_time = time.time()
    start_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{log_prefix(log_type='start')} IMPORT INITIATED")
    secrets = fetch_secrets()
    strava_client_id = secrets["client_id"]
    strava_client_secret = secrets["client_secret"]
    token_file_path = secrets["token_file_path"]
    exclusion_activities = [14944301862]

    # inspect_json(client_id=strava_client_id, client_secret=strava_client_secret)

    load_strava_data_to_postgres(
      strava_client_id=strava_client_id,
      strava_client_secret=strava_client_secret
    )

    end_time = time.time()
    end_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{log_prefix(log_type='end')} IMPORT COMPLETED - Finished running in {round(number=end_time - start_time, ndigits=2)} seconds.")