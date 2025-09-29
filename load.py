import os
import duckdb
import logging

YEAR = 2024
MONTHS = range(1, 13)
DB_FILE = "transform.duckdb"  

# Columns needed for this project
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
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02d}.parquet"

def install_httpfs(con):
    # Allow reading parquet directly over https
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

def create_from_first_available_month(con, table, select_cols, color):
    """
    Try Jan..Dec; on first success, CREATE table from that month.
    Return the month used (int) or None if none worked.
    """
    for m in MONTHS:
        url = tlc_url(color, YEAR, m)
        try:
            con.execute(f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT {select_cols}
                FROM read_parquet('{url}');
            """)
            print(f"[{color}] created {table} from {YEAR}-{m:02d}")
            return m
        except Exception as e:
            print(f"[{color}] create skip {YEAR}-{m:02d}: {e}")
    return None

def append_remaining_months(con, table, select_cols, color, skip_month):
    """
    Append the other months (except the one used for CREATE).
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
            print(f"[{color}] insert skip {YEAR}-{m:02d}: {e}")

def load_one_color(con, color):
    """
    Drop any old table, then load all 2024 months for one color into a single table.
    """
    table = f"{color}_trips_{YEAR}"
    select_cols = YELLOW_COLS if color == "yellow" else GREEN_COLS

    # 1. Drop old table if exists (clean start)
    con.execute(f"DROP TABLE IF EXISTS {table};")
    print(f"[{color}] dropped {table} if existed")

    # 2. Create from first available month
    created_month = create_from_first_available_month(con, table, select_cols, color)
    if created_month is None:
        print(f"[{color}] ERROR: could not create {table} from any 2024 month")
        return

    # 3. Append the rest
    append_remaining_months(con, table, select_cols, color, skip_month=created_month)

    # 4. Count rows
    cnt = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
    print(f"[{color}] {table}: {cnt:,} rows")

def duckdb_read_parquet():
    
    
    con = None
    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database=DB_FILE, read_only=False)
        print(f"DuckDB connection established at {DB_FILE}")

        # enable https reads
        install_httpfs(con)

        # Yellow 2024
        load_one_color(con, "yellow")

        # Green 2024
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
