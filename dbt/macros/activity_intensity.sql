{% macro calculate_activity_intensity(
    calories_per_minute,
    max_heartrate,
    total_calories,
    duration_minutes
) %}

    -- Define component weights
    {% set weight_cal_per_min = 0.5 %}
    {% set weight_max_hr = 0.3 %}
    {% set weight_total_cal = 0.15 %}
    {% set weight_duration = 0.05 %}

    round(
        (
            {{ weight_cal_per_min }} * LEAST({{ calories_per_minute }} / 20.0, 1.0) * 100 +
            {{ weight_max_hr }} * LEAST(GREATEST(({{ max_heartrate }} - 100) / 100.0, 0), 1.0) * 100 +
            {{ weight_total_cal }} * LEAST({{ total_calories }} / 1000.0, 1.0) * 100 +
            {{ weight_duration }} * LEAST({{ duration_minutes }} / 90.0, 1.0) * 100
        )
    , 1)

{% endmacro %}