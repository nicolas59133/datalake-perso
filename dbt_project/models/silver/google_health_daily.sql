-- Pivot Fitbit Air / Google Health par jour. Comme pour health_daily (Apple),
-- le jour est calculé dans le fuseau local d'enregistrement (timestamp UTC +
-- décalage fourni par l'API), pas celui de la session DuckDB.
with steps as (
    select
        cast(cast(start_time as timestamp) + interval (coalesce(start_utc_offset_s, 0)) second as date) as date,
        sum(count) as steps
    from {{ source('bronze', 'google_health_steps') }}
    group by 1
),

heart_rate as (
    -- Déjà agrégée par jour côté API (endpoint dailyRollUp) : pas besoin de
    -- regrouper ici, une ligne = un jour.
    select
        cast(date as date) as date,
        avg_bpm as avg_heart_rate,
        min_bpm as min_heart_rate,
        max_bpm as max_heart_rate
    from {{ source('bronze', 'google_health_heart_rate_daily') }}
),

sleep as (
    select
        cast(cast(start_time as timestamp) + interval (coalesce(start_utc_offset_s, 0)) second as date) as date,
        sum(minutes_asleep) / 60.0 as sleep_hours
    from {{ source('bronze', 'google_health_sleep') }}
    group by 1
),

weight as (
    select
        cast(cast(sample_time as timestamp) + interval (coalesce(sample_utc_offset_s, 0)) second as date) as date,
        avg(weight_kg) as weight_kg
    from {{ source('bronze', 'google_health_weight') }}
    group by 1
)

select
    date,
    steps.steps,
    heart_rate.avg_heart_rate,
    heart_rate.min_heart_rate,
    heart_rate.max_heart_rate,
    sleep.sleep_hours,
    weight.weight_kg
from steps
full outer join heart_rate using (date)
full outer join sleep using (date)
full outer join weight using (date)
order by date
