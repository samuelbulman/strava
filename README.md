# Want access to your strava activities data?

## Description
This project is a tool to help Strava athletes programmatically access and store their personal activity data.

## Pre-requisites
### Strava/Account Specific
All calls to the Strava API require an `access_token` that defines the athlete and application making the API call. Registered Strava athletes can obtain an `access_token` by creating a Strava application [here](https://www.strava.com/settings/api). Please refer to the [Strava Developer Docs](https://developers.strava.com/docs/getting-started/) to learn about Strava's API, creating a Strava App, authenticating with OAuth 2.0, etc.

### Storage
This project was designed to leverage Postgres and Google Sheets as storage solutions. You will ideally have access to a user that has write privilegs in a Postgres db instance, but feel free to fork and start tailoring this to whatever you have at your personal disposal!

### Packages
This project leverages third party Python packages. To prime your local environment _after_ cloning a local copy of this repository, it is recommended to create a virtual environment, activate it, and install package dependencies:
1. Create a virtual environment: `python3 -m venv venv`
2. Activate virtual environment: `source venv/bin/activate`
3. Install package dependencies: `pip install -r requirements.txt`

In order to leverage the Google Sheets functionality, you will need to create a personal [Google Project](https://cloud.google.com/free?utm_source=google&utm_medium=cpc&utm_campaign=na-US-all-en-dr-bkws-all-all-trial-b-dr-1710134&utm_content=text-ad-none-any-DEV_c-CRE_665665924741-ADGP_Hybrid+%7C+BKWS+-+MIX+%7C+Txt-Google+Cloud-Google+Cloud+Free-KWID_43700081235769755-aud-2232802565252:kwd-299377062137&utm_term=KW_google+cloud+platform+free-ST_google+cloud+platform+free&gad_source=1&gclid=CjwKCAiA5Ka9BhB5EiwA1ZVtvADHUMj870DMiev5WFHJ1uVytuOJjTn3z1zulGEOll36koGiGKRCLxoCRlkQAvD_BwE&gclsrc=aw.ds&hl=en) for free and [create a service account](https://cloud.google.com/iam/docs/service-accounts-create) associated with your project, that can read from & write to the [Google Sheets API](https://developers.google.com/sheets/api/guides/concepts).

If you do leverage a database or google sheets as a storage solution, you'll need to setup a .env file with your database connection details and/or google service account key details to authenticate to each service:

```text
host=
port=
db_name=
user=
pass=
sa_path=
target_spreadsheet_id=
target_worksheet_id=
```

---

## Project Structure

```text
.
├── dbt/                     # dbt transformation project
│   ├── models/              # dbt models (intermediate & marts)
│   ├── macros/              # dbt macros (e.g. intensity logic)
│   └── dbt_project.yml      # dbt configuration
├── pipeline/
│   ├── utilities/           # Internal python packages
│   ├── strava_export.py     # Load data to Google Sheets
│   └── strava_ingest.py     # Load flat file data into database/warehouse
└── local_run__end_to_end.sh
```

---

## dbt

Once your raw data is loaded into your warehouse, use [dbt](https://docs.getdbt.com) to build transformations.

### Setting Up dbt

If you haven’t already:

1. Install dbt:
   ```bash
   pip install dbt-core dbt-postgres  # specify your adapter of choice
   ```

2. Navigate to the dbt directory:
   ```bash
   cd dbt
   ```

3. To create your own dbt project from scratch (if starting fresh):
   ```bash
   dbt init my_project
   ```
   Follow [this guide](https://docs.getdbt.com/docs/building-a-dbt-project) for setup details.

4. dbt run!:
   ```bash
   dbt deps      # install any dbt packages
   dbt run       # build models
   ```

---

### End-to-End Execution

If you've successfully set up a database instance, connected to the Strava API, and established a working Google Sheets connection, you can run this data pipeline end-to-end by running the `local_run__end_to_end.sh` shell script available in the repo's root directory:

```bash
./local_run__end_to_end.sh
```

This shell script runs the ingestion script, executes dbt transformations, and will export your newly refreshed and report-ready data to google sheets for reference in your BI layer!

---

## Contact

Made with 🏃 💦 🏋️ by [@samuelbulman](https://github.com/samuelbulman)
