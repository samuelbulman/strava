/* define the activity intensity calculation once to dry up query */
/* it calculate the final intensity score for an activity, paying weighted respects to the individual components of the activity */
{% set activity_intensity = "round(((0.5 * scaled_calories_per_min) + (0.15 * scaled_max_hr) + (0.3 * scaled_total_calories) + (0.05 * scaled_duration))::int * activity_type_multiplier, 1)" %}

{% set is_milestone_activity = "activity_number in (10,50,100,150,200,250,300,300,400,500,750,1000,1500,2000)" %}

with activities as (
    select
        activity_id
        ,activity_name
        ,athlete_full_name
        ,bro_split_bucket
    /* sadly can't rely on mapped_activity_type = 'WeightTraining' for all weight training activities */
        ,bro_split_bucket in ('Multiple muscle groups','Chest','Back','Legs','Arms','Shoulders','Body Weight') as is_weight_training_activity
        ,activity_timestamp_ct
        ,activity_date
        ,activity_start_hour_scaled
        ,activity_end_timestamp_ct
        ,activity_end_hour
        ,activity_end_hour_scaled - activity_start_hour_scaled as activity_duration_hours_scaled
        ,activity_distance
        ,activity_duration_seconds
        ,activity_duration_minutes
        ,activity_elevation_low
        ,activity_elevation_high
        ,activity_avg_speed
        ,activity_max_speed
        ,calories_burned
        ,calories_burned_per_minute
        ,max_heartrate
        ,average_heartrate
        ,{{ activity_intensity }} as activity_intensity_score
        ,dense_rank() over (order by {{ activity_intensity }} desc)  as activity_intensity_rank
        ,dense_rank() over (partition by bro_split_bucket order by {{ activity_intensity }} desc) as activity_grouping_intensity_rank
        ,case when {{ is_milestone_activity }} then activity_number||'th activity logged!' end as milestone_activity
    from {{ ref('dim_activities') }}
)
select
    cal.date_day
    ,cal.day_name
    ,cal.day_of_year
    ,activities.*
from {{ ref('calendar') }} cal
left join activities
    on cal.date_day = activities.activity_date