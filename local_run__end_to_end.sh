#!/bin/bash

start_time=$(date +%s)

# force system to enter the directory this script located in
cd "$(dirname "$0")"

echo "------------------------------------------------------------------------------------------------------"
echo "  Executing Strava import ..."
echo "------------------------------------------------------------------------------------------------------"

# ingest data to postgres
source venv/bin/activate && python3 strava_data_import.py
deactivate

echo "------------------------------------------------------------------------------------------------------"
echo "  Strava import finished. Executing dbt run ..."
echo "------------------------------------------------------------------------------------------------------"

# run lightweight data transformations
cd dbt && source venv/bin/activate
dbt run
deactivate

echo "------------------------------------------------------------------------------------------------------"
echo "  dbt run finished. Executing Strava export ..."
echo "------------------------------------------------------------------------------------------------------"

# export refreshed analytics data set to google sheets
cd .. && source venv/bin/activate
python3 strava_data_export.py

end_time=$(date +%s)
elapsed=$(( end_time - start_time ))
echo "------------------------------------------------------------------------------------------------------"
printf "  All tasks complete. Finished running in %d seconds.\n" "$elapsed"
echo "------------------------------------------------------------------------------------------------------"