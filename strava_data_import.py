# Standard imports
import requests
import json
import time
import os
from typing import Dict, Any
import logging
from logging.handlers import MemoryHandler
import sys

# Third party imports
import pandas as pd
from dotenv import load_dotenv

# Local imports
from modules.postgres import Postgres
from modules.send_email import send_email

# admittedly haven't spent a ton of time with the logging lib and don't plan to
# hence the detailed in-line documentation outlining what's going on at each step
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

log_formatter = logging.Formatter(
    fmt='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)

# set up console handler to actually send logs to local terminal as they're created
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# memory handler allows us to store and reference the entire list of logs
# this is purely to help me build a detailed email notification at the end of the script
memory_handler = MemoryHandler(capacity=1000)
memory_handler.setFormatter(log_formatter)
logger.addHandler(memory_handler)

logger.propagate = False # weird find - this just prevents logs from printing twice in console/terminal


def fetch_strava_activities(
    client_id:str,
    client_secret:str
) -> pd.DataFrame:
    """Returns a Pandas DataFrame containing a strava athletes activity data from the activities endpoint(s)."""
    
    new_activity_counter = 0
    
    try:
        access_token = get_access_token(
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

            # TODO: break with "nothing to do" if no new records are available to load
            new_records_to_process_count = len(activities_data) - len(existing_activities['id']) - len(exclusion_activities)
                  
            logger.info("Activity records fetched from Strava API", extra={
                "api_total_activity_count": len(activities_data),
                "exclusion_activity_count": len(exclusion_activities),
                "postgres_total_activity_count": len(existing_activities['id']),
                "new_records_to_process": new_records_to_process_count
            })

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
                # To avoid rate limit errors, we can grab calorie counts from activities that have already been loaded to postgres. 
                # if the current activity_id in the loop hasn't yet been loaded to postgres, THEN we'll hit the detailed activities endpoint to grab the new activities calories burned.

                if activity.get("id") in existing_activities["id"]:
                    activities["calories_burned"].append(existing_activities["calories"][existing_activities["id"].index(activity.get("id"))])

                else:
                    new_activity_counter += 1
                    detailed_activity_response = strava_api_detailed_activities_response(access_token, activity_id=activity.get("id"))
                    activities["calories_burned"].append(detailed_activity_response.get("calories"))
                    logger.debug("New Activity Detected.", extra={
                        "new_activity_log_formatted": f"{new_activity_counter}. {activity.get('name')}:\n - Type  ==>  {activity.get('type')}:\n - Recorded at  ==>  {activity.get('start_date_local')}:\n - Calories Burned  ==>  {detailed_activity_response.get('calories')}",
                    })
            
            # Convert dict to Pandas dataframe and set all NaN values to None so Postgres correctly interprets these as nulls
            df = pd.DataFrame(data=activities, columns=[key for key in activities.keys()])
            df = df.astype(object) 
            df = df.where(pd.notna(df), None)

            return df
                
        else:
            logger.error("Something went wrong - could not obtain valid access token?")
    
    except Exception as e:
        logger.error(f"Error fetching Strava data:  {e}")
    

def strava_api_activities_response(access_token:str) -> dict:
    """
    Makes a request to Strava's activities endpoint and returns a json response containing athlete activity data if successful.
    
    Parameters
    ----------
    access_token (str)
    - fill
    """

    # TODO: update to only pull the first n pages worth of activities rather than looping through all pages, to avoid rate limits when the total number of activities grows too large

    activities = []
    page = 1
    activities_per_page = 50
    headers = {"Authorization": f"Bearer {access_token}"}
    url = "https://www.strava.com/api/v3/athlete/activities"


    while True:
        params = {"per_page": activities_per_page, "page": page}
        response = requests.get(url=url, headers=headers, params=params)
        json_response = response.json()

        # break out of while loop once all activities have been exhausted
        if len(json_response) == 0:
            break

        if response.status_code == 200:
            activities.extend(json_response)
        else:
            logger.error(f"Error fetching data - {response.status_code} STATUS: {response.json()}")
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
        logger.error(f"Error fetching data: {response.status_code}, {response.json()}")


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

    access_token = get_access_token(
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


def get_access_token(
    client_id:str,
    client_secret:str,
) -> str:
    """Returns a non-expired Strava Access token for API authentication."""

    # first, load locally stored strava access and refresh tokens:
    token_data = load_local_token_data()

    # next up, set the access_token. 
    # check if the most recently pulled token has expired.
    # refresh it and save locally if so. otherwise, use the current token
    # so we can make requests to strava's api endpoints.

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
) -> bool:
    """Save Strava Access & Refresh tokens locally so tokens persist beyond script completion."""

    with open(token_file_path, "w") as token_file:
        logger.info("Saving strava access and refresh tokens...")
        json.dump(token_data, token_file)
        logger.info("Tokens saved successfully.")
        return True


def load_strava_activities_to_postgres(
    strava_client_id:str,
    strava_client_secret:str
) -> bool:
    """Loads Strava activities data to target Postgres table."""
    
    try:
        logger.info("Fetching strava activities data...")
        strava_activities_df = fetch_strava_activities(
            client_id=strava_client_id,
            client_secret=strava_client_secret
        )
        logger.info("Successfully fetched strava activities data!")
    
    except Exception as e:
        logger.error(f"Error fetching strava activities data:\n\n{e}")

    with Postgres() as psql:
        try:
            logger.info("Loading strava activities data to Postgres...")
            psql.load_dataframe_to_table(
                df=strava_activities_df,
                schema="strava",
                table="activities"
            )
            logger.info("Successfully loaded strava activities data to Postgres!")
            return True
   
        except Exception as e:
            logger.error(f"Error loading strava activities data:\n\n{e}")


def load_strava_athletes_to_postgres(
    strava_client_id:str,
    strava_client_secret:str
) -> bool:
    """Loads Strava athletes data to target Postgres table."""
    try:
        logger.info("Fetching strava athletes data...")
        strava_athletes_df = fetch_strava_athletes(
            client_id=strava_client_id,
            client_secret=strava_client_secret
        )
        logger.info("Successfully fetched strava athletes data!")
    
    except Exception as e:
        logger.error(f"Error fetching strava athletes data:\n\n{e}")
        
    with Postgres() as psql:
        try:
            logger.info("Loading strava athletes data to Postgres...")
            psql.load_dataframe_to_table(
                df=strava_athletes_df,
                schema="strava",
                table="athletes"
            )
            logger.info("Successfully loaded strava athletes data to Postgres!")
            return True
        
        except Exception as e:
            logger.error(f"Error loading strava athletes data:\n\n{e}")


def fetch_secrets() -> Dict[str, Any]:
    """Returns a JSON object containing user-specific Strava secrets stored locally."""

    strava_secrets_path = os.getenv("strava_secrets_path")

    with open(f"{os.path.dirname(os.path.abspath(__file__))}{strava_secrets_path}") as secrets:
        return json.load(secrets)


def inspect_response(
    client_id:str,
    client_secret:str
):
    """Call adhoc when you want to print/inspect the response of the Strava API"""
    try:
        access_token = get_access_token(
            client_id=client_id,
            client_secret=client_secret
        )

        if access_token:
            return strava_api_activities_response(access_token)
    
    except Exception as e:
        logger.error("Failed to connect to the Strava API.")


if __name__ == "__main__":
    load_dotenv()

    start_time = time.time()
    start_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
    logger.info("IMPORT INITIATED")
    secrets = fetch_secrets()
    strava_client_id = secrets["client_id"]
    strava_client_secret = secrets["client_secret"]
    token_file_path = secrets["token_file_path"]
    exclusion_activities = [14944301862]

    _activities = load_strava_activities_to_postgres(strava_client_id=strava_client_id, strava_client_secret=strava_client_secret)
    _athletes = load_strava_athletes_to_postgres(strava_client_id=strava_client_id, strava_client_secret=strava_client_secret)

    end_time = time.time()
    end_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
    run_time = round(number=end_time - start_time, ndigits=2)

    # grab the log messages we want to add to the jobs email notification, and set empty strings to concatenate append log values to
    log_records = memory_handler.buffer
    new_activities_formatted = ""
    error_logs = ""

    for record in log_records:
        if hasattr(record, "new_records_to_process"):
            new_records_to_process = record.new_records_to_process
        if hasattr(record, "new_activity_log_formatted"):
            new_activity_log = record.new_activity_log_formatted
            new_activities_formatted += (new_activity_log + "\n")
        if record.levelname == "error":
            error_logs += (" - " + memory_handler.format(record) + "\n\n")

    # if both postgres load functions run successfully (return True), construct an email subject and body outlining the successful job
    if _activities and _athletes:
        logger.info(f"IMPORT SUCCESS - Finished running in {run_time} seconds.")
        email_subject = f"Successful Strava Import 💯 // {time.strftime('%Y-%m-%d')}"
        email_body = f"""Strava import ran successfully at {time.strftime('%Y-%m-%d %H:%M:%S')}

{new_records_to_process} new activities were processed:

{new_activities_formatted}

Total runtime: {run_time} seconds 🚀"""
    
    else:
        logger.error(f"IMPORT FAILED - Finished running in {run_time} seconds.")
        email_subject = f"Failed Strava Import 😭 // {time.strftime('%Y-%m-%d')}"
        email_body = f"""The following errors were captured:

{error_logs}"""

    send_email(
        sender_email=os.getenv("shmuel_bot_email"),
        sender_password=os.getenv("shmuel_bot_email_password"),
        to_emails=[os.getenv("internal_email")],
        subject=email_subject,
        body=email_body
    )