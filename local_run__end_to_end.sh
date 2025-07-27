#!/bin/bash

# ingest data to postgres
source venv/bin/activate && python3 strava_ingest.py
deactivate

# run lightweight data transformations
cd dbt && source venv/bin/activate
dbt run
deactivate

# export refreshed analytics data set to google sheets
cd .. && source venv/bin/activate
python3 strava_export.py