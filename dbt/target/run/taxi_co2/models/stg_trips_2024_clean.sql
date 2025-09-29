
  
  create view "transform"."main"."stg_trips_2024_clean__dbt_tmp" as (
    

select *
from trips_2024_clean
  );
