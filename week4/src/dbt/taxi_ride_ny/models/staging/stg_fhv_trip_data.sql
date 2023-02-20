{{ config(
    materialized='view'
)}}

with tripdata as 
(
  select *,
    -- row_number() over(partition by Affiliated_base_number, pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata') }}
)

select 
    -- identifiers
    {{ dbt_utils.surrogate_key(['Affiliated_base_number', 'pickup_datetime']) }} as tripid,
    CAST(dispatching_base_num AS string) AS dispatching_base_num,
    CAST(pickup_datetime AS timestamp) AS pickup_datetime,
    CAST(dropOff_datetime AS timestamp) AS dropOff_datetime,
    CAST(PUlocationID AS integer) AS PUlocationID,
    CAST(DOlocationID AS integer) AS DOlocationID,
    CAST(SR_Flag AS integer) AS SR_Flag,
    CAST(Affiliated_base_number AS string) AS Affiliated_base_number
from tripdata
-- where rn = 1

-- dbt run --m stg_green_trip_data --var "is_test_run: false"
{% if var('is_test_run', default=true) %}

    limit 100

{% endif %}