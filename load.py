import os
import duckdb
import logging

# Year and months we want to process
YEAR = 2024
MONTHS = range(1, 13)   # January (1) through December (12)
DB_FILE = "transform.duckdb"  # Local DuckDB file to persist data

# Columns we care about for each dataset
# Yellow taxi schema
YELLOW_COLS = """
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    VendorID,
    PULocationID,
    DOLocationID,
    total_amount
"""
# Green taxi schema (same, but with lpep instead of tpep for pickup/dropoff)
GREEN_COLS = """
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    VendorID,
    PULocationID,
    DOLocationID,
    total_amount
"""

def tlc_url(color, year, month):
    """
    Build the official TLC (Taxi & Limousine Commission) data URL
    for a given color (yellow/green), year, and month.
    Example: 
    https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
    """
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02d}.parquet"

def install_httpfs(con):
    """
    DuckDB needs the httpfs extension to read Parquet files over HTTP/HTTPS.
    This function installs and loads that extension.
    """
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

def create_from_first_available_month(con, table, select_cols, color):
    """
    Try to CREATE the DuckDB table from the first available month (Jan–Dec).
    Stops after the first success. Returns the month used (int), or None if none worked.
    """
    for m in MONTHS:
        url = tlc_url(color, YEAR, m)
        try:
            # Create (or replace) the table from a single month’s Parquet file
            con.execute(f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT {select_cols}
                FROM read_parquet('{url}');
            """)
            print(f"[{color}] created {table} from {YEAR}-{m:02d}")
            return m
        except Exception as e:
            # If this month’s file doesn’t exist or fails, skip and continue
            print(f"[{color}] create skip {YEAR}-{m:02d}: {e}")
    return None

def append_remaining_months(con, table, select_cols, color, skip_month):
    """
    Append all the other months into the table,
    skipping the one already used for CREATE.
    """
    for m in MONTHS:
        if skip_month is not None and m == skip_month:
            continue
        url = tlc_url(color, YEAR, m)
        try:
            con.execute(f"""
                INSERT INTO {table}
                SELECT {select_cols}
                FROM read_parquet('{url}');
            """)
            print(f"[{color}] inserted {YEAR}-{m:02d}")
        except Exception as e:
            # If a month file doesn’t exist or fails, log and move on
            print(f"[{color}] insert skip {YEAR}-{m:02d}: {e}")

def load_one_color(con, color):
    """
    Load all months of data for one taxi color (yellow or green).
    Process:
      1. Drop old table if it exists.
      2. Create table from the first available month.
      3. Append the rest of the months.
      4. Report the total row count.
    """
    table = f"{color}_trips_{YEAR}"
    select_cols = YELLOW_COLS if color == "yellow" else GREEN_COLS

    # Drop any old version of this table to start fresh
    con.execute(f"DROP TABLE IF EXISTS {table};")
    print(f"[{color}] dropped {table} if existed")

    # Create from the first available month
    created_month = create_from_first_available_month(con, table, select_cols, color)
    if created_month is None:
        print(f"[{color}] ERROR: could not create {table} from any 2024 month")
        return

    # Append the rest
    append_remaining_months(con, table, select_cols, color, skip_month=created_month)

    # Count rows and print summary
    cnt = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
    print(f"[{color}] {table}: {cnt:,} rows")

def duckdb_read_parquet():
    """
    Main driver function:
      - Connect to DuckDB.
      - Enable httpfs for remote reads.
      - Load Yellow and Green taxi datasets.
      - Close connection cleanly.
    """
    con = None
    try:
        # Connect (creates the DB file if not exists)
        con = duckdb.connect(database=DB_FILE, read_only=False)
        print(f"DuckDB connection established at {DB_FILE}")

        # Enable https parquet reads
        install_httpfs(con)

        # Load Yellow taxi trips for 2024
        load_one_color(con, "yellow")

        # Load Green taxi trips for 2024
        load_one_color(con, "green")

        print("Load stage complete.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if con is not None:
            con.close()
            print("Closed DuckDB connection")

if __name__ == "__main__":
    duckdb_read_parquet()