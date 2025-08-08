# Standard imports
import time
import os

# Third party imports
import pandas as pd
from dotenv import load_dotenv

# Local imports
from modules.postgres import Postgres
from modules.google_sheets import GoogleSheets
from modules.utils import log_prefix, read_sql


def postgres_to_google_sheets(
        sql_query:str,
        google_sheet_spreadsheet_id:str,
        google_sheet_worksheet_name:str,
    ):

    try:
        with Postgres() as psql:
            print(f"{log_prefix(log_type='info')} Querying postgres for Strava data...")
            strava_df = psql.query_postgres(sql_query=sql_query, return_df=True)
            print(f"{log_prefix(log_type='info')} Strava data retrieved!")

        sheet = GoogleSheets(google_sheet_spreadsheet_id=google_sheet_spreadsheet_id)

        print(f"{log_prefix(log_type='info')} Importing Strava data to google sheets...")
        sheet.import_df_to_google_sheet(
            dataframe=strava_df,
            google_sheet_worksheet_name=google_sheet_worksheet_name,
            clear_and_resize_sheet=True
        )
        print(f"{log_prefix(log_type='info')} Strava data imported!")
    
    except Exception as e:
        print(f"{log_prefix(log_type='error')} Error importing to google sheets:\n\n{e}")


if __name__ == "__main__":
    load_dotenv()

    start_time = time.time()
    start_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{log_prefix(log_type='start')} EXPORT INITIATED")

    file_name = "calendar_activities.sql"
    sql_query = read_sql(f"{os.getcwd()}/sql/{file_name}")

    postgres_to_google_sheets(
        sql_query=sql_query,
        google_sheet_spreadsheet_id=os.getenv("target_spreadsheet_id"),
        google_sheet_worksheet_name=os.getenv("target_worksheet_id")
    )

    end_time = time.time()
    end_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{log_prefix(log_type='end')} EXPORT COMPLETED - Finished running in {round(number=end_time - start_time, ndigits=2)} seconds.")