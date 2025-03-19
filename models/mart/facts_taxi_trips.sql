{{
    config(
        materialized='table'
    )
}}

with base as (
    select
        datediff(
            minute, tpep_pickup_datetime, tpep_dropoff_datetime
        ) as trip_duration_minutes,
        extract(hour from tpep_pickup_datetime) as hour_of_day,
        extract(dayofweek from tpep_pickup_datetime) as day_of_week
    from {{ ref("stg_raw_taxi_trips") }}
)

select
    day_of_week,
    hour_of_day,
    avg(trip_duration_minutes) as avg_trip_duration_minutes
from base
group by hour_of_day, day_of_week
order by day_of_week, hour_of_day