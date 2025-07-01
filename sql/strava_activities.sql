select
    activity_id
    ,activity_name
    ,case
        when activity_type = 'WeightTraining' then (
            case
                when activity_name ~* 'chest' then 'Chest'
                when activity_name ~* 'back' then 'Back'
                when activity_name ~* 'leg' then 'Legs'
                when activity_name ~* 'arm' then 'Arms'
                when activity_name ~* 'shoulder' then 'Shoulders'
            end
        )
        else activity_type
    end as bro_split_bucket
    ,replace(replace(activity_timestamp, 'T', ' '), 'Z', '')::timestamp as activity_timestamp_ct
    ,activity_distance
    ,activity_duration_seconds
    ,round(activity_duration_seconds / 60.00, 2) as activity_duration_minutes
    ,activity_elevation_low
    ,activity_elevation_high
    ,activity_avg_speed
    ,activity_max_speed
    ,average_heartrate
    ,max_heartrate
    ,calories_burned
    ,calories_burned / (activity_duration_seconds / 60) as calories_burned_per_minute 
from strava.strava_activities;