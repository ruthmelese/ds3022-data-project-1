
  
    
    

    create  table
      "transform"."main"."trips_features__dbt_tmp"
  
    as (
      

-- Use the seeded lookup to get CO₂ rate (grams per mile).
-- avg() makes this robust even if the seed has multiple rows.
with rate as (
  select avg(co2_grams_per_mile)::double as co2_grams_per_mile
  from "transform"."main"."vehicle_emissions"
),

-- Base trips with duration in hours
base as (
  select
    t.*,
    date_diff('second', t.pickup_datetime, t.dropoff_datetime) / 3600.0 as duration_hours
  from "transform"."main"."stg_trips_2024_clean" as t
)

select
  color,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  vendor_id,
  pu_location_id,
  do_location_id,
  total_amount,

  -- REQUIRED column names
  trip_distance * rate.co2_grams_per_mile / 1000.0 as trip_co2_kgs,
  case when duration_hours > 0 then trip_distance / duration_hours end as avg_mph,
  extract(hour from pickup_datetime)::integer        as hour_of_day,
  cast(strftime('%w', pickup_datetime) as integer)   as day_of_week,    -- 0=Sun..6=Sat
  cast(strftime('%V', pickup_datetime) as integer)   as week_of_year,   -- ISO week
  cast(strftime('%m', pickup_datetime) as integer)   as month_of_year   -- 1..12

from base
cross join rate
    );
  
  