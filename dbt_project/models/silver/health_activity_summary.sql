select
    cast(date as date) as date,
    active_energy_kcal,
    active_energy_goal_kcal,
    exercise_minutes,
    exercise_goal_minutes,
    stand_hours,
    stand_goal_hours
from {{ source('bronze', 'health_activity_summary') }}
order by date
