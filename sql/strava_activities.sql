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
				when activity_name ~* 'push up|pull up' then 'Body Weight'
			end
		)
		when activity_name ~* 'padel' then 'Padel'
		when activity_name ~* 'golf' then 'Golf'
		else activity_type
	end as bro_split_bucket
	,activity_type = 'WeightTraining' as is_weight_training_activity
	,replace(replace(activity_timestamp, 'T', ' '), 'Z', '')::timestamp as activity_timestamp_ct
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
    	-- unfortunately we can't reference the activity type multiplier or scaled individual workout components directly
	-- in the final weighted score calculation but I want to include them all in the final result set just for reference
	-- so the activity intensity score is not a complete black box
	,case
		when activity_type in ('Swim', 'Surfing') then 1.4
		when activity_type = 'Run' then 1.3
		when activity_type = 'WeightTraining' then 1.2
		when activity_type = 'Workout' then 0.9
		when activity_type = 'Yoga' then 0.8
		when activity_type in ('Ride','Walk') then 0.7
		when activity_type = 'Golf' then 0.4
		else 1.0
	end as activity_type_multiplier
	-- scale individual workout components (capped to somewhat expected limits)
    ,least(calories_burned / (activity_duration_seconds / 60) / 20.0, 1.0) * 100 as scaled_calories_per_min
  	,least(greatest((max_heartrate - 100) / 100.0, 0), 1.0) * 100 as scaled_max_hr
  	,least(round(activity_duration_seconds / 60.00, 2) / 90.0, 1.0) * 100 as scaled_duration
  	-- calculate the final intensity score for an activity, paying weighted respects to the individual components of an activity 
  	,round(
    	((0.4 * least(calories_burned / (activity_duration_seconds / 60) / 20.0, 1.0) * 100) +
    	(0.3 * least(greatest((max_heartrate - 100) / 100.0, 0), 1.0) * 100) +
    	(0.3 * least(round(activity_duration_seconds / 60.00, 2) / 90.0, 1.0) * 100))::int
    	* case
			when activity_type in ('Swim', 'Surfing') then 1.4
			when activity_type = 'Run' then 1.3
			when activity_type = 'WeightTraining' then 1.2
			when activity_type = 'Workout' then 0.9
			when activity_type = 'Yoga' then 0.8
			when activity_type in ('Ride','Walk') then 0.7
			when activity_type = 'Golf' then 0.4
			else 1.0
		  end
  	, 1) as activity_intensity_score
from strava.strava_activities;