select
    data_point_id,
    platform,
    exercise_type,
    display_name,
    cast(start_time as timestamp) as start_ts,
    cast(end_time as timestamp)   as end_ts,
    active_duration_s / 60.0      as duration_minutes,
    calories_kcal,
    distance_m / 1000.0           as distance_km,
    steps,
    avg_heart_rate,
    active_zone_minutes
from {{ source('bronze', 'google_health_exercise') }}
order by start_ts desc
