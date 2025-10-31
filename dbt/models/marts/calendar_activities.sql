{# Define individual component weights #}
{% set weight_cal_per_minute = 0.5 %}
{% set weight_max_hr = 0.15 %}
{% set weight_total_calories = 0.3 %}
{% set weight_duration = 0.05 %}

{% set is_milestone_activity = "activity_number in (10,50,100,150,200,250,300,300,400,500,750,1000,1500,2000)" %}

with activities as (
    select
        activity_id
        ,activity_name
        ,athlete_full_name
        ,bro_split_bucket
        ,is_weight_training_activity
        ,activity_timestamp_ct
        ,activity_date
        ,activity_start_hour_scaled
        ,activity_end_timestamp_ct
        ,activity_end_hour
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
    /* postgres should really let us reference calculated columns to dry this query up */
    /* calculate the final intensity score for an activity, paying weighted respects to the individual components of an activity */
        ,round(
            (({{weight_cal_per_minute}} * scaled_calories_per_min)
            + ({{weight_max_hr}} * scaled_max_hr)
            + ({{weight_total_calories}} * scaled_total_calories)
            + ({{weight_duration}} * scaled_duration))::int
        * activity_type_multiplier, 1) as activity_intensity_score
        ,dense_rank() over (
            order by round(
                (({{weight_cal_per_minute}} * scaled_calories_per_min)
            + ({{weight_max_hr}} * scaled_max_hr)
            + ({{weight_total_calories}} * scaled_total_calories)
            + ({{weight_duration}} * scaled_duration))::int
            * activity_type_multiplier, 1) desc
        )  as activity_intensity_rank
        ,dense_rank() over (
            partition by bro_split_bucket
            order by round(
                (({{weight_cal_per_minute}} * scaled_calories_per_min)
            + ({{weight_max_hr}} * scaled_max_hr)
            + ({{weight_total_calories}} * scaled_total_calories)
            + ({{weight_duration}} * scaled_duration))::int
            * activity_type_multiplier, 1) desc
        ) as activity_grouping_intensity_rank
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