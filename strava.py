# Standard imports
import requests
import json
import time
import os
from typing import Dict, Any

# Third party imports
import pandas as pd
from dotenv import load_dotenv

# Local imports
from modules.postgres import Postgres
from modules.google_sheets import GoogleSheets


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
        activities_data = strava_api_activities_response(access_token)
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

        existing_activities = fetch_existing_activity_ids()

        print(f"{len(activities_data)} activities fetched from Strava API.\n{len(existing_activities['activity_id'])} activity records retrieved from Postgres.\n{len(activities_data) - len(existing_activities['activity_id'])} new records will be imported.")

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
            # annoyingly, we have to hit a separate, detailed-activities endpoint to fetch how many calories were burned during a workout :roll-eyes:
            # to avoid rate limit errors, we can grab previously fetched calorie counts from postgres. if the current activity_id in the loop has not 
            # yet been loaded to postgres, then we will hit the detailed activities endpoint to grab that information.
            if activity.get("id") in existing_activities["activity_id"]:
                activities["calories_burned"].append(existing_activities["calories"][existing_activities["activity_id"].index(activity.get("id"))])

            else:
                print(f"New Activity detected - {activity.get('name')} on {activity.get('start_date_local')}")
                detailed_activity_response = strava_api_detailed_activities_response(access_token, activity_id=activity.get("id"))
                activities["calories_burned"].append(detailed_activity_response.get("calories"))
        
        return pd.DataFrame(data=activities, columns=[key for key in activities.keys()])
            
    else:
        print("Something went wrong - could not get a valid access token.")
    

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
            print(f"Error fetching data: {response.status_code}, {response.json()}")
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
        print(f"Error fetching data: {response.status_code}, {response.json()}")


def fetch_existing_activity_ids():
    """"""
    existing_activity_records = {
        "activity_id": [],
        "calories": []
    }

    with Postgres() as psql:
        columns, existing_activities = psql.query_postgres(sql_query="select distinct activity_id, calories_burned from strava.strava_activities;")

    for row in existing_activities:
        existing_activity_records["activity_id"].append(row[0])
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
        print("Tokens saved successfully!\n")


def load_strava_data_to_postgres(
        strava_client_id:str,
        strava_client_secret:str
    ):
    """Loads Strava source data to target Postgres table."""
    
    print("Fetching strava activities data...\n")
    strava_activities_df = fetch_strava_activities(
        client_id=strava_client_id,
        client_secret=strava_client_secret
    )
    print("Successfully fetched strava activities data!\n")

    print("Fetching strava athletes data...\n")
    strava_athletes_df = fetch_strava_athletes(
        client_id=strava_client_id,
        client_secret=strava_client_secret
    )
    print("Successfully fetched strava athletes data!\n")

    with Postgres() as psql:

        print("Loading strava activities data to Postgres...\n")
        psql.load_dataframe_to_table(
            df=strava_activities_df,
            schema="strava",
            table="strava_activities"
        )
        print("\nSuccessfully loaded strava activities data to Postgres!\n")

        print("Loading strava athletes data to Postgres...\n")
        psql.load_dataframe_to_table(
            df=strava_athletes_df,
            schema="strava",
            table="strava_athletes"
        )
        print("\nSuccessfully loaded strava athletes data to Postgres!\n")


def postgres_to_google_sheets(
        sql_query:str,
        google_sheet_spreadsheet_id:str,
        google_sheet_worksheet_name:str,
    ):

    with Postgres() as psql:
        print("Querying postgres for Strava activities...")
        strava_df = psql.query_postgres(sql_query=sql_query, return_df=True)
        print("Strava activities retrieved!")

    sheet = GoogleSheets(google_sheet_spreadsheet_id=google_sheet_spreadsheet_id)

    print("Importing Strava data to google sheets...")
    sheet.import_df_to_google_sheet(
        dataframe=strava_df,
        google_sheet_worksheet_name=google_sheet_worksheet_name,
        clear_and_resize_sheet=True
    )
    print("Strava data imported!")


def fetch_secrets() -> Dict[str, Any]:
    """Returns a JSON object containing user-specific Strava secrets stored locally."""

    strava_secrets_path = "/.secrets/.strava_secrets.json"

    with open(f"{os.path.dirname(os.path.abspath(__file__))}{strava_secrets_path}") as secrets:
        return json.load(secrets)
    

def read_sql(file_path:str) -> str:
    """
    Returns the entire contents of a SQL file.

    Parameters
    ----------
    file_path (str):
    - The path to a SQL file
    """

    try:
        with open(file_path, 'r') as file:
            file_contents = file.read()
        return file_contents
    
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


if __name__ == "__main__":
    load_dotenv()

    start_time = time.time()
    start_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"""---------------------------------------------------\nScript initiated at  {start_time_str}\n---------------------------------------------------\n""")
    secrets = fetch_secrets()
    strava_client_id = secrets["client_id"]
    strava_client_secret = secrets["client_secret"]
    token_file_path = secrets["token_file_path"]

    load_strava_data_to_postgres(
      strava_client_id=strava_client_id,
      strava_client_secret=strava_client_secret
    )

    file_name = "strava_activities.sql"
    sql_query = read_sql(f"{os.getcwd()}/sql/{file_name}")

    postgres_to_google_sheets(
        sql_query=sql_query,
        google_sheet_spreadsheet_id=os.getenv("target_spreadsheet_id"),
        google_sheet_worksheet_name=os.getenv("target_worksheet_id")
    )

    end_time = time.time()
    end_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"""\n---------------------------------------------------\nScript completed at  {end_time_str}\n---------------------------------------------------\n""")
    print(f"Finished running in {round(number=end_time - start_time, ndigits=2)} seconds.")