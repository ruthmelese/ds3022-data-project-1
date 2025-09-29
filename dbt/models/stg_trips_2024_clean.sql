{{ config(materialized='view') }}

select *
from trips_2024_clean

