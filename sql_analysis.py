"""
AgroSentry — SQL Analysis
Loads sensor data into SQLite and runs analytical queries.
Demonstrates GROUP BY, CASE WHEN, subqueries, window functions,
aggregations, and conditional filtering.

Run: python sql_analysis.py
Requires: sensor_data.csv (from simulate_data.py)
Output: sql_*.csv files + printed results
"""

import pandas as pd
import sqlite3
import json

# ─── Load & prepare ──────────────────────────────────────────────────────────
df = pd.read_csv("sensor_data.csv", parse_dates=["timestamp"])
df.columns = df.columns.str.lower()
df["date"]        = df["timestamp"].dt.date.astype(str)
df["hour"]        = df["timestamp"].dt.hour
df["month"]       = df["timestamp"].dt.month
df["day_of_week"] = df["timestamp"].dt.day_name()

df = df[
    (df["temperature_c"]     >= -5)  & (df["temperature_c"]     <= 55) &
    (df["humidity_pct"]      >= 10)  & (df["humidity_pct"]      <= 100) &
    (df["soil_moisture_pct"] >= 5)   & (df["soil_moisture_pct"] <= 100)
]

conn = sqlite3.connect("agrosentry.db")
df.to_sql("sensor_readings", conn, if_exists="replace", index=False)
print(f"Loaded {len(df):,} rows into agrosentry.db\n")

QUERIES = {

    # ── Q1: Average conditions per field ─────────────────────────────────────
    "field_averages": """
        SELECT
            field,
            ROUND(AVG(temperature_c), 2)     AS avg_temp_c,
            ROUND(AVG(humidity_pct), 2)      AS avg_humidity_pct,
            ROUND(AVG(soil_moisture_pct), 2) AS avg_soil_pct,
            ROUND(AVG(light_lux), 0)         AS avg_light_lux,
            COUNT(*)                          AS total_readings
        FROM sensor_readings
        GROUP BY field
        ORDER BY avg_temp_c DESC
    """,

    # ── Q2: Drought alerts — soil moisture below 25% ──────────────────────────
    "drought_alerts": """
        SELECT
            date, field, sensor_id,
            ROUND(soil_moisture_pct, 2) AS soil_moisture_pct,
            ROUND(temperature_c, 2)     AS temperature_c
        FROM sensor_readings
        WHERE soil_moisture_pct < 25
        ORDER BY soil_moisture_pct ASC
        LIMIT 30
    """,

    # ── Q3: Hottest hour of each day per field (subquery) ─────────────────────
    "peak_temp_by_hour": """
        SELECT
            hour,
            ROUND(AVG(temperature_c), 2)     AS avg_temp,
            ROUND(AVG(soil_moisture_pct), 2) AS avg_soil,
            ROUND(AVG(humidity_pct), 2)      AS avg_humidity,
            COUNT(*)                          AS readings
        FROM sensor_readings
        GROUP BY hour
        ORDER BY hour
    """,

    # ── Q4: Anomaly count per sensor using CASE WHEN ──────────────────────────
    "sensor_anomalies": """
        SELECT
            sensor_id,
            field,
            COUNT(*) AS total_readings,
            SUM(CASE WHEN temperature_c < 10 OR temperature_c > 45 THEN 1 ELSE 0 END)
                AS temp_anomalies,
            SUM(CASE WHEN soil_moisture_pct < 20 THEN 1 ELSE 0 END)
                AS drought_alerts,
            SUM(CASE WHEN humidity_pct > 90 THEN 1 ELSE 0 END)
                AS high_humidity_alerts,
            ROUND(
                100.0 * SUM(CASE WHEN temperature_c < 10 OR temperature_c > 45 THEN 1 ELSE 0 END)
                / COUNT(*), 2
            ) AS anomaly_rate_pct
        FROM sensor_readings
        GROUP BY sensor_id, field
        ORDER BY anomaly_rate_pct DESC
    """,

    # ── Q5: Monthly field performance comparison ──────────────────────────────
    "monthly_comparison": """
        SELECT
            month,
            field,
            ROUND(AVG(temperature_c), 2)     AS avg_temp,
            ROUND(MIN(soil_moisture_pct), 2) AS min_soil,
            ROUND(AVG(humidity_pct), 2)      AS avg_humidity,
            COUNT(*)                          AS readings
        FROM sensor_readings
        GROUP BY month, field
        ORDER BY month, field
    """,

    # ── Q6: Days ranked by soil stress risk ──────────────────────────────────
    "soil_stress_ranking": """
        SELECT
            date,
            field,
            ROUND(AVG(soil_moisture_pct), 2) AS avg_soil,
            ROUND(AVG(temperature_c), 2)     AS avg_temp,
            CASE
                WHEN AVG(soil_moisture_pct) < 20                      THEN 'Critical'
                WHEN AVG(soil_moisture_pct) < 30                      THEN 'High'
                WHEN AVG(soil_moisture_pct) < 40                      THEN 'Medium'
                ELSE 'Normal'
            END AS stress_level
        FROM sensor_readings
        GROUP BY date, field
        HAVING stress_level IN ('Critical', 'High')
        ORDER BY avg_soil ASC
        LIMIT 20
    """,
}

results = {}
for name, sql in QUERIES.items():
    result = pd.read_sql_query(sql, conn)
    result.to_csv(f"sql_{name}.csv", index=False)
    results[name] = result.to_dict(orient="records")
    print(f"── {name} ({len(result)} rows) ──")
    print(result.to_string(index=False))
    print()

conn.close()

with open("sql_results.json", "w") as f:
    json.dump(results, f, indent=2)

print("All SQL results saved to sql_results.json and individual CSVs.")
