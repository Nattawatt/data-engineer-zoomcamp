{{ config(
    materialized='table',
    partition_by={
      "field": "tpep_pickup_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    }
)}}

select * from {{ source('staging','green_tripdata')}}