{{ config(
    materialized='view'
)}}

select 
  road_name,
  EXTRACT(YEAR FROM currenct_datetime) AS year,
  EXTRACT(HOUR FROM currenct_datetime) AS HOUROFDAY,
  AVG(speed) AS avg_speed,
from {{ source('express_way','express_raw') }}
group by 1, 2, 3