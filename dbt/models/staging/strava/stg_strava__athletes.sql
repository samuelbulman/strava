select
    id as athlete_id
    ,first_name as athlete_first_name
    ,last_name as athlete_last_name
    ,first_name || ' ' || last_name as athlete_full_name
from {{ source('strava','athletes') }}