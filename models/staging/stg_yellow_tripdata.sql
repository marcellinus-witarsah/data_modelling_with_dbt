{{
    config(
        materialized='view'
    )
}}

with tripdata as (

    select 
        *,
        row_number() over(partition by filename, vendorid, tpep_pickup_datetime, tpep_dropoff_datetime) as rn 
    from {{ source('staging', 'yellow_tripdata') }}
    where vendorid is not null
)

select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['filename', 'vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']) }} as trip_id,
    filename,
    vendorid,
    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- triap information
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,

    -- payment information
    payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge

from tripdata
where rn=1

{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}
