{{
    config(
        materialized='table'
    )
}}

select 
    locationid as location_id,
    borough,
    zone,
    replace(service_zone, 'Boro', 'Green') as service_zone
from {{ ref("taxi_zone_lookup") }}