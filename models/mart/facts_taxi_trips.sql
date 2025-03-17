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
),

aggregated as (
    select
        hour_of_day,
        day_of_week,
        avg(trip_duration_minutes) as avg_trip_duration_minutes
    from base
    group by hour_of_day, day_of_week
)

select
    day_of_week,
    hour_of_day,
    avg_trip_duration_minutes
from aggregated
order by day_of_week, hour_of_day