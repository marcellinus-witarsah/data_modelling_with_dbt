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
    cast(filename as string) as filename,
    {{ dbt.safe_cast("vendorid", api.Column.translate_type("integer")) }}as vendorid,
    {{ dbt.safe_cast("ratecodeid", api.Column.translate_type("integer")) }}as ratecodeid,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }}as pickup_location_id,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }}as dropoff_location_id,

    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- triap information
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    -- yellow cabs are always street-hail
    1 as trip_type,

    -- payment information
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(0 as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }},0) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description

from tripdata
where rn=1

{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}
