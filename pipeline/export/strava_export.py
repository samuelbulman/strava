# Standard imports
import time
import os
import sys

# Third party imports
from dotenv import load_dotenv
import logging
from logging.handlers import MemoryHandler

# Local imports
from ..utilities.postgres import Postgres
from ..utilities.google_sheets import GoogleSheets
from ..utilities.common import read_sql

# admittedly haven't spent a ton of time with the logging lib and don't plan to go super deep
# on it, hence the detailed in-line documentation outlining what's going on at each step
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

# weird find - this just prevents logs from printing twice in console/terminal
logger.propagate = False

def postgres_to_google_sheets(
        sql_query:str,
        google_sheet_spreadsheet_id:str,
        google_sheet_worksheet_name:str,
    ):

    try:
        with Postgres() as psql:
            strava_df = psql.query_postgres(sql_query=sql_query, return_df=True)
            logger.info("Strava data retrieved from Postgres")

        sheet = GoogleSheets(google_sheet_spreadsheet_id=google_sheet_spreadsheet_id)

        sheet.import_df_to_google_sheet(
            dataframe=strava_df,
            google_sheet_worksheet_name=google_sheet_worksheet_name,
            clear_and_resize_sheet=True
        )
        logger.info("Strava data imported to Google Sheets")
    
    except Exception as e:
        logger.error(f"Error importing to google sheets:\n\n{e}")


if __name__ == "__main__":
    load_dotenv()

    start_time = time.time()
    start_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
    logger.info("EXPORT INITIATED")

    file_name = "calendar_activities.sql"
    sql_query = read_sql(f"{os.getcwd()}/pipeline/export/sql/{file_name}")

    postgres_to_google_sheets(
        sql_query=sql_query,
        google_sheet_spreadsheet_id=os.getenv("target_spreadsheet_id"),
        google_sheet_worksheet_name=os.getenv("target_worksheet_id")
    )

    end_time = time.time()
    end_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"EXPORT COMPLETED - Finished running in {round(number=end_time - start_time, ndigits=2)} seconds.")