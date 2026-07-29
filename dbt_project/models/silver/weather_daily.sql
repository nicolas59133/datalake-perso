select
    cast(date as date)            as date,
    temp_max,
    temp_min,
    round(temp_max - temp_min, 1) as temp_range,
    precipitation,
    latitude,
    longitude
from {{ source('bronze', 'weather_daily') }}
order by date
