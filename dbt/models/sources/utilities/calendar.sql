
with date_spine as (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="'2022-01-01'::date",
            end_date="(date_trunc('year', current_date) + interval '12 months')::date"
        )
    }}
)

select
	date_day::date as date_day
	,to_char(date_day, 'Day') as day_name
	,date_part('doy', date_day) as day_of_year
	,date_trunc('month', date_day)::date as month_start
	,(date_trunc('month', date_day) + interval '1 month' - interval '1 day')::date as month_end
	,date_part('month', date_day) as month_int
	,to_char(date_day, 'Month') as month_name
	,date_part('year', date_day) as year_start
	,(date_trunc('year', date_day) + interval '12 months' - interval '1 day')::date as year_end
	,date_part('year', date_day) as year_int
from date_spine