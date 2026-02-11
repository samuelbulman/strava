{% set activity_end_timestamp_ct = "activity_timestamp_ct + (round(activity_duration_seconds / 60.00, 2) * (interval '1 minute'))" %}

select
	activity_id
	,activity_name
	,athlete_full_name
	,case
		when mapped_activity_type ~* 'WeightTraining|Workout' then (
			case
				when activity_name ~* ' and ' then 'Multiple muscle groups'
				when activity_name ~* 'chest' then 'Chest'
				when activity_name ~* 'back' then 'Back'
				when activity_name ~* 'leg' then 'Legs'
				when activity_name ~* 'arm' then 'Arms'
				when activity_name ~* 'shoulder' then 'Shoulders'
				when activity_name ~* 'push up|pull up' then 'Body Weight'
				when activity_name ~* 'padel|pickleball|tennis' then 'Racket Sports'
				when activity_name ~* 'golf|driving range' then 'Golf'
			end
		)
		else activity_type
	end as bro_split_bucket
	,activity_timestamp_ct
	,activity_date
	,date_part('hour', activity_timestamp_ct)::int + ((100::float/60::float) * date_part('minute', activity_timestamp_ct) * .01) as activity_start_hour_scaled
	,{{activity_end_timestamp_ct}} as activity_end_timestamp_ct
	,date_part('hour', (activity_timestamp_ct + (round(activity_duration_seconds / 60.00, 2) * (interval '1 minute')))) as activity_end_hour
	,date_part('hour', {{activity_end_timestamp_ct}})::int + ((100::float/60::float) * date_part('minute', {{activity_end_timestamp_ct}}) * .01) as activity_end_hour_scaled
	,activity_distance
	,activity_duration_seconds
	,round(activity_duration_seconds / 60.00, 2) as activity_duration_minutes
	,round((activity_duration_seconds / 60.00) / 24) as activity_duration_hours
	,activity_elevation_low
	,activity_elevation_high
	,activity_avg_speed
	,activity_max_speed
	,calories_burned
	,calories_burned / (activity_duration_seconds / 60) as calories_burned_per_minute
	,max_heartrate
	,average_heartrate
    ,current_timestamp::timestamp as refreshed_at_ct
	,case
		when mapped_activity_type in ('Swim', 'Surfing') then 1.4
		when mapped_activity_type = 'WeightTraining' then 1.2
		when mapped_activity_type = 'Run' then 1.1
		when mapped_activity_type ~* 'padel|pickleball|tennis' then 0.9
		when mapped_activity_type = 'Yoga' then 0.8
		when mapped_activity_type in ('Ride','Walk') then 0.7
		when mapped_activity_type = 'Golf' then 0.4
		else 1.0
	end as activity_type_multiplier
	-- scale individual workout components (capped to somewhat expected limits)
    ,least(calories_burned / (activity_duration_seconds / 60) / 20.0, 1.0) * 100 as scaled_calories_per_min
  	,least(greatest((max_heartrate - 100) / 100.0, 0), 1.0) * 100 as scaled_max_hr
  	,least(round(activity_duration_seconds / 60.00, 2) / 90.0, 1.0) * 100 as scaled_duration
  	,least(calories_burned / 1000.0, 1.0) * 100 as scaled_total_calories
	,row_number() over (partition by athletes.athlete_id order by activities.activity_timestamp_ct) as activity_number
from {{ ref('src_strava__activities') }} activities
inner join {{ ref('src_strava__athletes') }} athletes
	on activities.athlete_id = athletes.athlete_id