{{ config(materialized='view') }}

select
  color, pickup_datetime, dropoff_datetime, passenger_count,
  trip_distance, vendor_id, pu_location_id, do_location_id, total_amount
from main.trips_2024_clean

