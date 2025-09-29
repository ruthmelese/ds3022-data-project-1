import duckdb
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="clean.log",
    filemode="w",
)
logging.getLogger().addHandler(logging.StreamHandler())
log = logging.getLogger(__name__)

DB_FILE = "transform.duckdb"         
ENFORCE_POSITIVE_ZONES = False     

def make_yellow_sql(zone_clause: str) -> str:
    return f"""
CREATE OR REPLACE TABLE yellow_trips_2024_clean AS
WITH base AS (
  SELECT
    tpep_pickup_datetime  AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    CAST(passenger_count AS INTEGER) AS passenger_count,
    CAST(trip_distance   AS DOUBLE)  AS trip_distance,
    CAST(VendorID        AS INTEGER) AS vendor_id,
    CAST(PULocationID    AS INTEGER) AS pu_location_id,
    CAST(DOLocationID    AS INTEGER) AS do_location_id,
    CAST(total_amount    AS DOUBLE)  AS total_amount
  FROM yellow_trips_2024
  WHERE tpep_pickup_datetime >= TIMESTAMP '2024-01-01'
    AND tpep_pickup_datetime <  TIMESTAMP '2025-01-01'
),
filtered AS (
  SELECT *
  FROM base
  WHERE pickup_datetime <= dropoff_datetime
    AND (dropoff_datetime - pickup_datetime) BETWEEN INTERVAL 0 MINUTE AND INTERVAL 24 HOUR
    AND trip_distance > 0 AND trip_distance <= 100
    AND passenger_count BETWEEN 1 AND 6
    AND total_amount BETWEEN 0 AND 1000
    {zone_clause}
)
SELECT *
FROM filtered
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY pickup_datetime, dropoff_datetime, trip_distance,
               pu_location_id, do_location_id, vendor_id, total_amount, passenger_count
  ORDER BY pickup_datetime
) = 1;
"""

def make_green_sql(zone_clause: str) -> str:
    return f"""
CREATE OR REPLACE TABLE green_trips_2024_clean AS
WITH base AS (
  SELECT
    lpep_pickup_datetime  AS pickup_datetime,
    lpep_dropoff_datetime AS dropoff_datetime,
    CAST(passenger_count AS INTEGER) AS passenger_count,
    CAST(trip_distance   AS DOUBLE)  AS trip_distance,
    CAST(VendorID        AS INTEGER) AS vendor_id,
    CAST(PULocationID    AS INTEGER) AS pu_location_id,
    CAST(DOLocationID    AS INTEGER) AS do_location_id,
    CAST(total_amount    AS DOUBLE)  AS total_amount
  FROM green_trips_2024
  WHERE lpep_pickup_datetime >= TIMESTAMP '2024-01-01'
    AND lpep_pickup_datetime <  TIMESTAMP '2025-01-01'
),
filtered AS (
  SELECT *
  FROM base
  WHERE pickup_datetime <= dropoff_datetime
    AND (dropoff_datetime - pickup_datetime) BETWEEN INTERVAL 0 MINUTE AND INTERVAL 24 HOUR
    AND trip_distance > 0 AND trip_distance <= 100
    AND passenger_count BETWEEN 1 AND 6
    AND total_amount BETWEEN 0 AND 1000
    {zone_clause}
)
SELECT *
FROM filtered
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY pickup_datetime, dropoff_datetime, trip_distance,
               pu_location_id, do_location_id, vendor_id, total_amount, passenger_count
  ORDER BY pickup_datetime
) = 1;
"""

COMBINE_SQL = """
CREATE OR REPLACE TABLE trips_2024_clean AS
SELECT 'yellow' AS color, * FROM yellow_trips_2024_clean
UNION ALL
SELECT 'green'  AS color, * FROM green_trips_2024_clean;
"""

def main():
    con = None
    try:
        con = duckdb.connect(DB_FILE, read_only=False)
        log.info("Connected to %s", DB_FILE)

        zone_clause = "AND pu_location_id > 0 AND do_location_id > 0" if ENFORCE_POSITIVE_ZONES else ""

        # Yellow clean
        con.execute(make_yellow_sql(zone_clause))
        yc = con.execute("SELECT COUNT(*) FROM yellow_trips_2024_clean").fetchone()[0]
        log.info("yellow_trips_2024_clean rows: %s", f"{yc:,}")

        # Green clean
        con.execute(make_green_sql(zone_clause))
        gc = con.execute("SELECT COUNT(*) FROM green_trips_2024_clean").fetchone()[0]
        log.info("green_trips_2024_clean rows: %s", f"{gc:,}")

        # Combine
        con.execute(COMBINE_SQL)
        rows_by_color = con.execute("""
            SELECT color, COUNT(*) AS rows
            FROM trips_2024_clean
            GROUP BY 1 ORDER BY 1;
        """).fetchall()
        log.info("trips_2024_clean rows by color: %s", rows_by_color)

        span = con.execute("""
            SELECT MIN(pickup_datetime), MAX(pickup_datetime) FROM trips_2024_clean;
        """).fetchone()
        log.info("pickup_datetime range: %s", span)

        # 
        bads = con.execute("""
          SELECT
            COUNT(*) FILTER (WHERE passenger_count = 0) AS zero_passengers,
            COUNT(*) FILTER (WHERE trip_distance <= 0) AS zero_or_neg_distance,
            COUNT(*) FILTER (WHERE trip_distance  > 100) AS over_100_miles,
            COUNT(*) FILTER (WHERE dropoff_datetime < pickup_datetime) AS negative_duration,
            COUNT(*) FILTER (WHERE (dropoff_datetime - pickup_datetime) > INTERVAL 24 HOUR) AS over_24h
          FROM trips_2024_clean;
        """).fetchone()
        log.info("bad-value counts: %s", bads)

        dupes = con.execute("""
          WITH g AS (
            SELECT color, pickup_datetime, dropoff_datetime, passenger_count,
                   trip_distance, vendor_id, pu_location_id, do_location_id, total_amount,
                   COUNT(*) AS c
            FROM trips_2024_clean
            GROUP BY 1,2,3,4,5,6,7,8,9
          )
          SELECT COALESCE(SUM(c-1),0) FROM g WHERE c>1;
        """).fetchone()[0]
        log.info("duplicates remaining: %s", dupes)

    except Exception as e:
        log.error("Cleaning failed: %s", e)
    finally:
        if con is not None:
            con.close()
            log.info("Closed DuckDB connection")

if __name__ == "__main__":
    main()