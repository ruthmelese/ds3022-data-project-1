import duckdb
import logging

# Configure logging to file (clean.log) + console output
logging.basicConfig(
    level=logging.INFO,  # Log INFO and above (INFO, WARNING, ERROR)
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="clean.log",   # Write logs to this file
    filemode="w",           # Overwrite log file each run
)
# Also stream logs to the console
logging.getLogger().addHandler(logging.StreamHandler())
log = logging.getLogger(__name__)

# Path to DuckDB file created earlier in load stage
DB_FILE = "transform.duckdb"

# Flag to enforce only positive zone IDs (exclude invalid PULocationID / DOLocationID = 0)
ENFORCE_POSITIVE_ZONES = False


# ---------- SQL TEMPLATE FUNCTIONS ----------

def make_yellow_sql(zone_clause: str) -> str:
    """
    Build SQL query to clean Yellow Taxi trips:
      - Keep only 2024 trips.
      - Enforce valid times (pickup <= dropoff, duration <= 24h).
      - Keep only realistic trip distances, fares, passenger counts.
      - Optionally enforce positive zone IDs.
      - Deduplicate identical rows.
    """
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
    {zone_clause}  -- optionally enforce pu/do location > 0
)
SELECT *
FROM filtered
-- Deduplication: keep only 1 row if identical across key fields
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY pickup_datetime, dropoff_datetime, trip_distance,
               pu_location_id, do_location_id, vendor_id, total_amount, passenger_count
  ORDER BY pickup_datetime
) = 1;
"""


def make_green_sql(zone_clause: str) -> str:
    """
    Same as make_yellow_sql but adapted for Green Taxi schema
    (lpep_* timestamps instead of tpep_*).
    """
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

# SQL to combine Yellow + Green into one cleaned table
COMBINE_SQL = """
CREATE OR REPLACE TABLE trips_2024_clean AS
SELECT 'yellow' AS color, * FROM yellow_trips_2024_clean
UNION ALL
SELECT 'green'  AS color, * FROM green_trips_2024_clean;
"""

# ---------- MAIN CLEANING PIPELINE ----------

def main():
    con = None
    try:
        # Connect to DuckDB file
        con = duckdb.connect(DB_FILE, read_only=False)
        log.info("Connected to %s", DB_FILE)

        # Add zone clause if enforcing positive zone IDs
        zone_clause = "AND pu_location_id > 0 AND do_location_id > 0" if ENFORCE_POSITIVE_ZONES else ""

        # Clean Yellow Taxi table
        con.execute(make_yellow_sql(zone_clause))
        yc = con.execute("SELECT COUNT(*) FROM yellow_trips_2024_clean").fetchone()[0]
        log.info("yellow_trips_2024_clean rows: %s", f"{yc:,}")

        # Clean Green Taxi table
        con.execute(make_green_sql(zone_clause))
        gc = con.execute("SELECT COUNT(*) FROM green_trips_2024_clean").fetchone()[0]
        log.info("green_trips_2024_clean rows: %s", f"{gc:,}")

        # Combine both into trips_2024_clean
        con.execute(COMBINE_SQL)

        # Row counts by color
        rows_by_color = con.execute("""
            SELECT color, COUNT(*) AS rows
            FROM trips_2024_clean
            GROUP BY 1 ORDER BY 1;
        """).fetchall()
        log.info("trips_2024_clean rows by color: %s", rows_by_color)

        # Time range of dataset
        span = con.execute("""
            SELECT MIN(pickup_datetime), MAX(pickup_datetime) FROM trips_2024_clean;
        """).fetchone()
        log.info("pickup_datetime range: %s", span)

        # Sanity checks: count bad values
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

        # Check for duplicates that slipped through deduplication
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