with
    source as (
        select *
        from {{ source("raw_yellow_taxi", "raw_taxi_trips") }}
        where "vendorID" is not null
    ),

    renamed as (
        select
            {{
                dbt_utils.generate_surrogate_key(
                    ['"vendorID"', '"tpepPickupDateTime"']
                )
            }} as trip_id,
            cast("tpepPickupDateTime" as timestamp) as tpep_pickup_datetime,
            cast("tpepDropoffDateTime" as timestamp) as tpep_dropoff_datetime,
            "tollsAmount" as tolls_amount,
            "storeAndFwdFlag" as store_and_fwd_flag,
            "paymentType" as payment_type,
            "rateCodeId" as rate_code_id,
            "vendorID" as vendor_id,
            "doLocationId" as do_location_id,
            "startLon" as start_lon,
            "endLat" as end_lat,
            "totalAmount" as total_amount,
            "extra",
            "improvementSurcharge" as improvement_surcharge,
            "mtaTax" as mta_tax,
            "tipAmount" as tip_amount,
            "fareAmount" as fare_amount,
            "startLat" as start_lat,
            "endLon" as end_lon,
            "tripDistance" as trip_distance,
            "passengerCount" as passenger_count,
            "puLocationId" as pu_location_id
        from source
    ),

    deduped as (
        select
            *,
            row_number() over (
                partition by trip_id order by tpep_pickup_datetime
            ) as row_num
        from renamed
    )

select *
from deduped
where row_num = 1
