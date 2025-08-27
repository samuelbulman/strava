select
    id as activity_id
    ,name as activity_name
    ,athlete_id
    ,type as activity_type
    ,replace(replace(created_at, 'T', ' '), 'Z', '')::timestamp as activity_timestamp_ct
	,replace(replace(created_at, 'T', ' '), 'Z', '')::date as activity_date
    ,distance as activity_distance
    ,duration_seconds as activity_duration_seconds
    ,elevation_high as activity_elevation_low
    ,elevation_low as activity_elevation_high
    ,avg_speed as activity_avg_speed
    ,max_speed as activity_max_speed
    ,calories_burned
    ,average_heartrate
    ,max_heartrate
from {{ source('strava','activities') }}