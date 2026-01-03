#!/bin/bash

start_time=$(date +%s)

# force system to enter the directory this script located in
cd "$(dirname "$0")"

git checkout main && git pull

echo "------------------------------------------------------------------------------------------------------"
echo "  Executing Strava import ..."
echo "------------------------------------------------------------------------------------------------------"

# ingest data to postgres
source pipeline/venv/bin/activate && python3 -m pipeline.ingest.strava_activities
deactivate

echo "------------------------------------------------------------------------------------------------------"
echo "  Strava import finished. Executing dbt run ..."
echo "------------------------------------------------------------------------------------------------------"

# run lightweight data transformations
cd dbt && source venv/bin/activate
dbt run
dbt snapshot
deactivate

echo "------------------------------------------------------------------------------------------------------"
echo "  dbt run finished. Executing Strava export ..."
echo "------------------------------------------------------------------------------------------------------"

# export refreshed analytics data set to google sheets
cd .. && source pipeline/venv/bin/activate && python3 -m pipeline.export.strava_export

end_time=$(date +%s)
elapsed=$(( end_time - start_time ))
echo "------------------------------------------------------------------------------------------------------"
printf "  All tasks complete! Total runtime: %d seconds.\n" "$elapsed"
echo "------------------------------------------------------------------------------------------------------"