-- Pivot Fitbit Air / Google Health par jour : tous les types "continus" ou
-- déjà quotidiens. Les séances de sport (événements discrets, plusieurs par
-- jour possibles) vivent à part dans silver.google_health_exercise, même
-- logique que health_daily / health_workouts pour Apple Health.
--
-- Comme pour les autres sources horodatées de ce projet, le jour est calculé
-- dans le fuseau local d'enregistrement (timestamp UTC + décalage fourni par
-- l'API), pas celui de la session DuckDB.
with steps as (
    select
        cast(cast(start_time as timestamp) + interval (coalesce(start_utc_offset_s, 0)) second as date) as date,
        sum(count) as steps
    from {{ source('bronze', 'google_health_steps') }}
    group by 1
),

distance as (
    select
        cast(cast(start_time as timestamp) + interval (coalesce(start_utc_offset_s, 0)) second as date) as date,
        sum(value) / 1000000.0 as distance_km  -- value en millimètres
    from {{ source('bronze', 'google_health_distance') }}
    group by 1
),

active_zone_minutes as (
    select
        cast(cast(start_time as timestamp) + interval (coalesce(start_utc_offset_s, 0)) second as date) as date,
        sum(value) as active_zone_minutes
    from {{ source('bronze', 'google_health_active_zone_minutes') }}
    group by 1
),

sedentary as (
    select
        cast(cast(start_time as timestamp) + interval (coalesce(start_utc_offset_s, 0)) second as date) as date,
        count(*) as sedentary_periods_count,
        sum(date_diff('minute', cast(start_time as timestamp), cast(end_time as timestamp))) as sedentary_minutes
    from {{ source('bronze', 'google_health_sedentary_periods') }}
    group by 1
),

activity_level as (
    select
        cast(cast(start_time as timestamp) + interval (coalesce(start_utc_offset_s, 0)) second as date) as date,
        sum(date_diff('minute', cast(start_time as timestamp), cast(end_time as timestamp)))
            filter (where category = 'LIGHTLY_ACTIVE') as lightly_active_minutes,
        sum(date_diff('minute', cast(start_time as timestamp), cast(end_time as timestamp)))
            filter (where category = 'MODERATELY_ACTIVE') as moderately_active_minutes,
        sum(date_diff('minute', cast(start_time as timestamp), cast(end_time as timestamp)))
            filter (where category = 'VERY_ACTIVE') as very_active_minutes
    from {{ source('bronze', 'google_health_activity_level') }}
    group by 1
),

heart_rate as (
    -- Déjà agrégée par jour côté API (endpoint dailyRollUp) : pas besoin de
    -- regrouper ici, une ligne = un jour.
    select cast(date as date) as date, avg_bpm as avg_heart_rate, min_bpm as min_heart_rate, max_bpm as max_heart_rate
    from {{ source('bronze', 'google_health_heart_rate_daily') }}
),

resting_heart_rate as (
    -- daily-native (déjà une ligne par jour côté API).
    select cast(date as date) as date, resting_bpm as resting_heart_rate
    from {{ source('bronze', 'google_health_resting_heart_rate_daily') }}
),

hrv as (
    -- daily-native. On ne garde que la métrique la plus lisible
    -- (avg_hrv_ms) ; entropy/deep_sleep_rmssd restent dispo en bronze pour
    -- qui en a besoin.
    select cast(date as date) as date, avg_hrv_ms
    from {{ source('bronze', 'google_health_hrv_daily') }}
),

oxygen_saturation as (
    -- daily-native. Idem, on garde la moyenne ; bornes/écart-type en bronze.
    select cast(date as date) as date, avg_pct as spo2_avg_pct
    from {{ source('bronze', 'google_health_oxygen_saturation_daily') }}
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
),

body_fat as (
    select
        cast(cast(sample_time as timestamp) + interval (coalesce(sample_utc_offset_s, 0)) second as date) as date,
        avg(value) as body_fat_pct
    from {{ source('bronze', 'google_health_body_fat') }}
    group by 1
)

select
    date,
    steps.steps,
    distance.distance_km,
    active_zone_minutes.active_zone_minutes,
    sedentary.sedentary_periods_count,
    sedentary.sedentary_minutes,
    activity_level.lightly_active_minutes,
    activity_level.moderately_active_minutes,
    activity_level.very_active_minutes,
    heart_rate.avg_heart_rate,
    heart_rate.min_heart_rate,
    heart_rate.max_heart_rate,
    resting_heart_rate.resting_heart_rate,
    hrv.avg_hrv_ms,
    oxygen_saturation.spo2_avg_pct,
    sleep.sleep_hours,
    weight.weight_kg,
    body_fat.body_fat_pct
from steps
full outer join distance using (date)
full outer join active_zone_minutes using (date)
full outer join sedentary using (date)
full outer join activity_level using (date)
full outer join heart_rate using (date)
full outer join resting_heart_rate using (date)
full outer join hrv using (date)
full outer join oxygen_saturation using (date)
full outer join sleep using (date)
full outer join weight using (date)
full outer join body_fat using (date)
order by date
