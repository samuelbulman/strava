select *, current_timestamp::timestamp as refreshed_at_ct
from analytics.calendar_activities
order by date_day, activity_timestamp_ct;