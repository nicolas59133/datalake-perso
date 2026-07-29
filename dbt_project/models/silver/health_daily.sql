-- Pivot des types HealthKit les plus courants en une ligne par jour. Les
-- dizaines d'autres types (tension, VO2max, oxygène du sang...) restent
-- consultables tels quels dans bronze.health_records.
--
-- Le jour est extrait des 10 premiers caractères de start_date (déjà au
-- format YYYY-MM-DD dans le fuseau local d'enregistrement) plutôt que via un
-- cast/strptime : reconvertir au fuseau de la session DuckDB décalerait la
-- date pour les mesures prises près de minuit.
with steps as (
    select cast(left(start_date, 10) as date) as date, sum(value) as steps
    from {{ source('bronze', 'health_records') }}
    where type = 'HKQuantityTypeIdentifierStepCount'
    group by 1
),

distance as (
    select cast(left(start_date, 10) as date) as date, sum(value) as distance_km
    from {{ source('bronze', 'health_records') }}
    where type = 'HKQuantityTypeIdentifierDistanceWalkingRunning'
    group by 1
),

heart_rate as (
    select
        cast(left(start_date, 10) as date) as date,
        avg(value) as avg_heart_rate,
        min(value) as min_heart_rate,
        max(value) as max_heart_rate
    from {{ source('bronze', 'health_records') }}
    where type = 'HKQuantityTypeIdentifierHeartRate'
    group by 1
),

resting_heart_rate as (
    select cast(left(start_date, 10) as date) as date, avg(value) as resting_heart_rate
    from {{ source('bronze', 'health_records') }}
    where type = 'HKQuantityTypeIdentifierRestingHeartRate'
    group by 1
),

sleep as (
    select
        cast(left(start_date, 10) as date) as date,
        sum(
            date_diff(
                'minute',
                strptime(start_date, '%Y-%m-%d %H:%M:%S %z'),
                strptime(end_date, '%Y-%m-%d %H:%M:%S %z')
            )
        ) / 60.0 as sleep_hours
    from {{ source('bronze', 'health_records') }}
    where type = 'HKCategoryTypeIdentifierSleepAnalysis'
      and value_text like '%Asleep%'
    group by 1
)

select
    date,
    steps.steps,
    distance.distance_km,
    heart_rate.avg_heart_rate,
    heart_rate.min_heart_rate,
    heart_rate.max_heart_rate,
    resting_heart_rate.resting_heart_rate,
    sleep.sleep_hours
from steps
full outer join distance using (date)
full outer join heart_rate using (date)
full outer join resting_heart_rate using (date)
full outer join sleep using (date)
order by date
