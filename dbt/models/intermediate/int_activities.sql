select
	activity_id
	,activity_name
	,athlete_full_name
	,case
		when activity_type = 'WeightTraining' then (
			case
				when activity_name ~* 'chest' then 'Chest'
				when activity_name ~* 'back' then 'Back'
				when activity_name ~* 'leg' then 'Legs'
				when activity_name ~* 'arm' then 'Arms'
				when activity_name ~* 'shoulder' then 'Shoulders'
				when activity_name ~* 'push up|pull up' then 'Body Weight'
			end
		)
		when activity_name ~* 'padel|pickleball|tennis' then 'Racket Sports'
		when activity_name ~* 'golf' then 'Golf'
		else activity_type
	end as bro_split_bucket
	,activity_type = 'WeightTraining' as is_weight_training_activity
	,replace(replace(activity_created_at, 'T', ' '), 'Z', '')::timestamp as activity_timestamp_ct
	,replace(replace(activity_created_at, 'T', ' '), 'Z', '')::date as activity_date
	,activity_distance
	,activity_duration_seconds
	,round(activity_duration_seconds / 60.00, 2) as activity_duration_minutes
	,activity_elevation_low
	,activity_elevation_high
	,activity_avg_speed
	,activity_max_speed
	,calories_burned
	,calories_burned / (activity_duration_seconds / 60) as calories_burned_per_minute
	,max_heartrate
	,average_heartrate
    ,current_timestamp as refreshed_at
	,case
		when activity_type in ('Swim', 'Surfing') then 1.4
		when activity_type = 'Run' then 1.3
		when activity_type = 'WeightTraining' then 1.2
		when activity_name ~* 'padel|pickleball|tennis' then 0.9
		when activity_type = 'Yoga' then 0.8
		when activity_type in ('Ride','Walk') then 0.7
		when activity_type = 'Golf' then 0.4
		else 1.0
	end as activity_type_multiplier
	-- scale individual workout components (capped to somewhat expected limits)
    ,least(calories_burned / (activity_duration_seconds / 60) / 20.0, 1.0) * 100 as scaled_calories_per_min
  	,least(greatest((max_heartrate - 100) / 100.0, 0), 1.0) * 100 as scaled_max_hr
  	,least(round(activity_duration_seconds / 60.00, 2) / 90.0, 1.0) * 100 as scaled_duration
  	,least(calories_burned / 1000.0, 1.0) * 100 as scaled_total_calories
from {{ ref('stg_strava__activities') }} activities
inner join {{ ref('stg_strava__athletes') }} athletes
	on activities.athlete_id = athletes.athlete_id