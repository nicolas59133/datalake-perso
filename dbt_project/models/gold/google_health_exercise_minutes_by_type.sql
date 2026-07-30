select
    exercise_type,
    sum(duration_minutes) as total_minutes
from {{ ref('google_health_exercise') }}
group by exercise_type
order by total_minutes desc
