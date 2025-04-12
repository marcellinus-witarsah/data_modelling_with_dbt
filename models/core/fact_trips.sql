{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select *, 'Green' as service_type from {{ref("stg_green_tripdata")}}
), 
yellow_tripdata as (
    select *, 'Yellow' as service_type from {{ref("stg_yellow_tripdata")}}
),
tripdata as (
    select * from green_tripdata
    union all
    select * from yellow_tripdata
),
dim_zones as (
    select * from {{ref("dim_zones")}}
    where borough != 'Unknown'
)

select
    tripdata.trip_id,
    tripdata.filename,
    tripdata.vendorid,
    tripdata.ratecodeid,
    pickup_dim_zones.zone as pickup_zone,
    dropoff_dim_zones.zone as dropoff_zone,

    -- timestamps
    tripdata.pickup_datetime,
    tripdata.dropoff_datetime,

    -- trip information
    tripdata.store_and_fwd_flag,
    tripdata.passenger_count,
    tripdata.trip_distance,
    tripdata.trip_type,


    -- payment info
    tripdata.fare_amount,
    tripdata.extra,
    tripdata.mta_tax,
    tripdata.tip_amount,
    tripdata.tolls_amount,
    tripdata.ehail_fee,
    tripdata.improvement_surcharge,
    tripdata.total_amount,
    tripdata.payment_type,
    tripdata.payment_type_description

from tripdata
inner join dim_zones as pickup_dim_zones
on tripdata.pickup_location_id = pickup_dim_zones.location_id
inner join dim_zones as dropoff_dim_zones
on tripdata.dropoff_location_id = dropoff_dim_zones.location_id
