{{ config(
    materialized='view'
)}}

select 
  road_name,
  EXTRACT(YEAR FROM currenct_datetime) AS year,
  EXTRACT(DAYOFWEEK FROM currenct_datetime) AS DAYOFWEEK,
  AVG(speed) AS avg_speed,
from {{ source('express_way','express_raw') }}
group by 1, 2, 3