# Want access to your strava activities data? You're in the right repo.

## Description
This project is a tool to help Strava athletes programmatically access and store their personal strava activity data.

## Pre-requisites
### Strava/Account Specific
All calls to the Strava API require an `access_token` that defines the athlete and application making the API call. Registered Strava athletes can obtain an `access_token` by creating a Strava application [here](https://www.strava.com/settings/api). To learn about Stravas API, creating a Strava App, authenticating with OAuth 2.0, etc. please reference the [Strava Developer Docs](https://developers.strava.com/docs/getting-started/)

### Storage
This project was designed to leverage Postgres and Google Sheets as storage solutions. You will ideally have access to a user that has write privilegs in a Postgres db instance. If you do not, please contribute/request a new db module to work with.

### Packages
This project leverages third party Python packages. To prime your local environment _after_ cloning a local copy of this repository, it is recommended to create a virtual environment, activate it, and install package dependencies:
1. Create a virtual environment: `python3 -m venv venv`
2. Activate virtual environment: `source venv/bin/activate`
3. Install package dependencies: `pip install -r requirements.txt`

In order to leverage the Google Sheets functionality, you will need to create a personal [Google Project](https://cloud.google.com/free?utm_source=google&utm_medium=cpc&utm_campaign=na-US-all-en-dr-bkws-all-all-trial-b-dr-1710134&utm_content=text-ad-none-any-DEV_c-CRE_665665924741-ADGP_Hybrid+%7C+BKWS+-+MIX+%7C+Txt-Google+Cloud-Google+Cloud+Free-KWID_43700081235769755-aud-2232802565252:kwd-299377062137&utm_term=KW_google+cloud+platform+free-ST_google+cloud+platform+free&gad_source=1&gclid=CjwKCAiA5Ka9BhB5EiwA1ZVtvADHUMj870DMiev5WFHJ1uVytuOJjTn3z1zulGEOll36koGiGKRCLxoCRlkQAvD_BwE&gclsrc=aw.ds&hl=en) for free and [create a service account](https://cloud.google.com/iam/docs/service-accounts-create) associated with your project, that can read from & write to the [Google Sheets API](https://developers.google.com/sheets/api/guides/concepts).