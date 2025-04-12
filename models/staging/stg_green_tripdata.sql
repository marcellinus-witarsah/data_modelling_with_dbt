{{
    config(
        materialized='view'
    )
}}

with tripdata as (

    select 
        *,
        row_number() over(partition by filename, vendorid, lpep_pickup_datetime, lpep_dropoff_datetime) as rn 
    from {{ source('staging', 'green_tripdata') }}
    where vendorid is not null

)


select

    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['filename', 'vendorid', 'lpep_pickup_datetime', 'lpep_dropoff_datetime']) }} as trip_id,
    filename,
    vendorid,

    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip information
    store_and_fwd_flag,
    ratecodeid,
    pulocationid,
    dolocationid,
    passenger_count,
    trip_distance,

    -- price information
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description, 
    trip_type,
    congestion_surcharge

from tripdata
where rn=1

{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}