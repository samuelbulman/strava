{% macro calculate_activity_intensity(weight_cal_per_minute, scaled_calories_per_min, weight_max_hr, scaled_max_hr, weight_total_calories, scaled_total_calories, weight_duration, scaled_duration) -%}
    
    round((
          (weight_cal_per_minute * scaled_calories_per_min)
        + (weight_max_hr * scaled_max_hr)
        + (weight_total_calories * scaled_total_calories)
        + (weight_duration * scaled_duration)
    )::int * activity_type_multiplier, 1)

{%- endmacro %}