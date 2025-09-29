{{ config(materialized='table') }}

with base as (
  select
    color, pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
    vendor_id, pu_location_id, do_location_id, total_amount,
    date_diff('second', pickup_datetime, dropoff_datetime)/3600.0 as duration_hours
  from {{ ref('stg_trips_2024_clean') }}
)
select
  color, pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
  vendor_id, pu_location_id, do_location_id, total_amount,
  trip_distance * 404 / 1000.0 as co2_kg,
  case when duration_hours > 0 then trip_distance / duration_hours end as avg_mph,
  extract(hour from pickup_datetime)::integer              as trip_hour,
  cast(strftime('%w', pickup_datetime) as integer)         as trip_dow,
  cast(strftime('%V', pickup_datetime) as integer)         as week_number,
  cast(strftime('%m', pickup_datetime) as integer)         as month
from base
