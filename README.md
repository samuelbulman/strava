# Want access to your strava activities data? You're in the right repo.

## Description
This project is a tool to help Strava athletes programmatically access and store their personal strava activity data.

## Pre-requisites
### Strava/Account Specific
All calls to the Strava API require an a`ccess_token` that defines the athlete and application making the API call. Registered Strava athletes can obtain an `access_token` by creating a Strava application [here](https://www.strava.com/settings/api). To learn about Stravas API, creating a Strava App, authenticating with OAuth 2.0, etc. please reference the [Strava Developer Docs](https://developers.strava.com/docs/getting-started/)

### Storage
This project was designed to leverage Postgres as a database storage solution. You will ideally have access to a user that has write privilegs in a Postgres db instance. If you do not, please contribute/request a new db module to work with.

### Packages
This project leverages a few third party libraries. To prime your local environment _after_ cloning a local copy of this repository, it is recommended to create a virtual environment, activate it, and run the following command: `pip install -r requirements.txt`
