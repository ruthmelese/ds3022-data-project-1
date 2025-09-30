import os
import logging
import duckdb
import pandas as pd
import matplotlib.pyplot as plt

# Resolve paths relative to this file so the script works no matter
# where it's invoked from (e.g., CLI, IDE Run button, cron, etc.).
HERE = os.path.dirname(os.path.abspath(__file__))

# Try a couple of common locations for the DuckDB file; fall back to ./transform.duckdb
CANDIDATE_DB_PATHS = [
    os.path.join(HERE, "transform.duckdb"),
    os.path.join(HERE, "..", "transform.duckdb"),
]
DB_FILE = next((p for p in CANDIDATE_DB_PATHS if os.path.exists(p)), os.path.join(HERE, "transform.duckdb"))

# Output folders/files
PLOTS_DIR = os.path.join(HERE, "plots"); os.makedirs(PLOTS_DIR, exist_ok=True)
LOG_FILE = os.path.join(HERE, "analysis.log")

# Log to file; INFO level is enough for a human-readable run log
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    filename=LOG_FILE, filemode="w")
log = logging.getLogger(__name__)


# ---- SQL QUERIES ----

# 1) For each taxi color, find the single trip with the largest CO2 (in kg).
#    We use ROW_NUMBER() partitioned by color, ordered by trip_co2_kgs DESC, and pick rn = 1.
MAX_TRIP_SQL = """
WITH ranked AS (
  SELECT
    color,
    pickup_datetime,
    dropoff_datetime,
    trip_distance,
    trip_co2_kgs,
    ROW_NUMBER() OVER (PARTITION BY color ORDER BY trip_co2_kgs DESC) AS rn
  FROM trips_features
)
SELECT color, pickup_datetime, dropoff_datetime, trip_distance, trip_co2_kgs
FROM ranked
WHERE rn = 1
ORDER BY color;
"""

# 2) Template to identify the most/least carbon-heavy "bucket" for a given time unit.
#    bucket_col is something like hour_of_day, day_of_week, week_of_year, month_of_year.
#    We compute AVG(trip_co2_kgs) per (color, bucket), rank DESC for heavy and ASC for light, and keep rnk = 1.
HEAVY_LIGHT_TEMPLATE = """
-- {bucket_name}: per-trip average CO2 across the year
WITH agg AS (
  SELECT color, {bucket_col} AS bucket, AVG(trip_co2_kgs) AS avg_co2
  FROM trips_features
  GROUP BY 1,2
),
heavy AS (
  SELECT color, bucket, avg_co2,
         ROW_NUMBER() OVER (PARTITION BY color ORDER BY avg_co2 DESC) AS rnk
  FROM agg
),
light AS (
  SELECT color, bucket, avg_co2,
         ROW_NUMBER() OVER (PARTITION BY color ORDER BY avg_co2 ASC) AS rnk
  FROM agg
)
SELECT 'heavy' AS kind, color, bucket, avg_co2 FROM heavy WHERE rnk = 1
UNION ALL
SELECT 'light' AS kind, color, bucket, avg_co2 FROM light WHERE rnk = 1
ORDER BY color, kind;
"""

# 3) Monthly totals (sum of CO2 per month per color) to drive the plot.
MONTHLY_TOTALS_SQL = """
SELECT color, month_of_year AS month, SUM(trip_co2_kgs) AS total_co2_kg
FROM trips_features
GROUP BY 1,2
ORDER BY month, color;
"""

# ---- REPORTING/FORMATTING HELPERS -----

def report_bucket(df, label, formatter):
    """
    Pretty-print the 'heavy' and 'light' buckets for each color.

    Parameters
    ----------
    df : pd.DataFrame
        Output of HEAVY_LIGHT_TEMPLATE (columns: kind, color, bucket, avg_co2)
    label : str
        Human label for the bucket dimension (e.g., 'HOUR', 'DAY-OF-WEEK')
    formatter : callable(int) -> str
        Formats the integer bucket value to a readable string (e.g., 0 -> 'Sun')
    """
    print(f"\n=== {label}: Most/Least carbon-heavy (avg CO2 per trip) ===")
    log.info("=== %s: Most/Least carbon-heavy (avg CO2 per trip) ===", label)
    for color in sorted(df["color"].unique()):
        sub = df[df["color"] == color]
        # Each color should have exactly one 'heavy' and one 'light' row
        heavy = sub[sub["kind"] == "heavy"].iloc[0]
        light = sub[sub["kind"] == "light"].iloc[0]
        msg_h = f"{color.upper()} HEAVY {label}: {formatter(int(heavy['bucket']))} (avg {heavy['avg_co2']:.3f} kg/trip)"
        msg_l = f"{color.upper()} LIGHT {label}: {formatter(int(light['bucket']))} (avg {light['avg_co2']:.3f} kg/trip)"
        print(msg_h); print(msg_l)
        log.info(msg_h); log.info(msg_l)


def make_monthly_plot(monthly_df, out_path):
    """
    Create a dual-axis line chart:
      - Left y-axis: Yellow total CO2 per month
      - Right y-axis: Green total CO2 per month
    Using a twin axis makes both series readable if they differ in magnitude.

    Saves the figure to out_path (PNG).
    """
    # Pivot to month x color matrix; ensure months 1..12 exist in the index
    pivot = monthly_df.pivot(index="month", columns="color", values="total_co2_kg").reindex(range(1,13), fill_value=0.0)
    mon_labels = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax2 = ax1.twinx()  # second y-axis for the other color

    # Plot Yellow on left axis (solid line, gold-ish color)
    if "yellow" in pivot.columns:
        ax1.plot(pivot.index, pivot["yellow"], color="#FFB000", marker="o", label="YELLOW")
        ax1.set_ylabel("YELLOW Total CO₂ (kg)", color="#FFB000")
        ax1.tick_params(axis="y", colors="#FFB000"); ax1.spines["left"].set_color("#FFB000")

    # Plot Green on right axis (dashed line, green color)
    if "green" in pivot.columns:
        ax2.plot(pivot.index, pivot["green"], color="#2E7D32", marker="o", linestyle="--", label="GREEN")
        ax2.set_ylabel("GREEN Total CO₂ (kg)", color="#2E7D32")
        ax2.tick_params(axis="y", colors="#2E7D32"); ax2.spines["right"].set_color("#2E7D32")

    # X-axis month labels and chart decorations
    ax1.set_xticks(range(1, 13)); ax1.set_xticklabels(mon_labels)
    ax1.set_xlabel("Month"); ax1.set_title("Monthly CO₂ Totals by Taxi Color (2024)")
    ax1.grid(True, alpha=0.3)

    # Build a combined legend from both axes
    h1, l1 = ax1.get_legend_handles_labels(); h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="upper left")

    # Save and close the figure
    fig.tight_layout(); fig.savefig(out_path, dpi=150); plt.close(fig)
    print(f"\nSaved plot: {out_path}"); log.info("Saved plot: %s", out_path)


# ---- MAIN ANALYSIS ROUTINE -----

def main():
    con = None
    try:
        # Connect read-only to avoid accidental writes
        con = duckdb.connect(DB_FILE, read_only=True)
        log.info("Connected to %s", DB_FILE)

        # (1) Largest CO₂ trip per color
        max_trips = con.execute(MAX_TRIP_SQL).fetchdf()
        print("\n=== Largest CO₂ trip (per color) ===")
        log.info("Largest CO₂ trip (per color):")
        for _, r in max_trips.iterrows():
            msg = (f"{r['color'].upper()}: {r['trip_co2_kgs']:.3f} kg "
                   f"(dist={r['trip_distance']:.2f} mi, "
                   f"pickup={r['pickup_datetime']}, dropoff={r['dropoff_datetime']})")
            print(msg); log.info(msg)

        # (2) Most/least carbon-heavy HOUR of day
        hour_stats = con.execute(HEAVY_LIGHT_TEMPLATE.format(
            bucket_name="hour", bucket_col="hour_of_day")).fetchdf()
        report_bucket(hour_stats, "HOUR", lambda b: f"{b:02d}:00")

        # (3) Most/least carbon-heavy DAY OF WEEK (0..6 assumed Sun..Sat)
        dow_stats = con.execute(HEAVY_LIGHT_TEMPLATE.format(
            bucket_name="day-of-week", bucket_col="day_of_week")).fetchdf()
        dow_name = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"]
        report_bucket(dow_stats, "DAY-OF-WEEK", lambda b: dow_name[b] if 0 <= b <= 6 else str(b))

        # (4) Most/least carbon-heavy WEEK of year (1..53 depending on ISO week)
        week_stats = con.execute(HEAVY_LIGHT_TEMPLATE.format(
            bucket_name="week", bucket_col="week_of_year")).fetchdf()
        report_bucket(week_stats, "WEEK", lambda b: f"Week {b}")

        # (5) Most/least carbon-heavy MONTH
        month_stats = con.execute(HEAVY_LIGHT_TEMPLATE.format(
            bucket_name="month", bucket_col="month_of_year")).fetchdf()
        mon_name = [None,"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        report_bucket(month_stats, "MONTH", lambda b: mon_name[b] if 1 <= b <= 12 else str(b))

        # (6) Plot monthly totals to PNG
        monthly = con.execute(MONTHLY_TOTALS_SQL).fetchdf()
        out_png = os.path.join(PLOTS_DIR, "monthly_co2.png")
        make_monthly_plot(monthly, out_png)

        print("\nAnalysis complete."); log.info("Analysis complete.")
    except Exception as e:
        # Mirror the error to both stdout (for CLI users) and the log file
        print(f"Analysis failed: {e}"); log.error("Analysis failed: %s", e)
    finally:
        # Always close the connection cleanly
        if con is not None:
            con.close(); log.info("Closed DuckDB connection")

if __name__ == "__main__":
    main()