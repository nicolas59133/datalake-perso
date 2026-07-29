select
    workout_id,
    workout_type,
    source_name,
    strptime(start_date, '%Y-%m-%d %H:%M:%S %z') as start_ts,
    strptime(end_date, '%Y-%m-%d %H:%M:%S %z')   as end_ts,
    date_diff(
        'minute',
        strptime(start_date, '%Y-%m-%d %H:%M:%S %z'),
        strptime(end_date, '%Y-%m-%d %H:%M:%S %z')
    ) as duration_minutes,
    total_distance_km,
    total_energy_kcal
from {{ source('bronze', 'health_workouts') }}
order by start_ts desc
