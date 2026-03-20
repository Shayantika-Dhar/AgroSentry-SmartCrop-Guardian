"""
AgroSentry — Pandas Analysis Pipeline
Cleans raw sensor data, computes daily summaries, detects anomalies,
performs correlation analysis, and exports dashboard_data.json.

Run: python analyze.py
Requires: sensor_data.csv (from simulate_data.py)
Output: daily_summary.csv, anomalies.csv, dashboard_data.json
"""

import pandas as pd
import numpy as np
import json

# ─── 1. Load & inspect ───────────────────────────────────────────────────────
print("Loading data...")
df = pd.read_csv("sensor_data.csv", parse_dates=["timestamp"])
print(f"Shape: {df.shape}")
print(f"Nulls:\n{df.isnull().sum()}")
print(f"\nStatistics:\n{df.describe().round(2)}")

# ─── 2. Clean ────────────────────────────────────────────────────────────────
# Remove physically impossible readings
before = len(df)
df = df[
    (df["temperature_c"]    >= -5)  & (df["temperature_c"]    <= 55)  &
    (df["humidity_pct"]     >= 10)  & (df["humidity_pct"]     <= 100) &
    (df["soil_moisture_pct"] >= 5)  & (df["soil_moisture_pct"] <= 100) &
    (df["light_lux"]        >= 0)   & (df["light_lux"]        <= 1200)
]
print(f"\nRemoved {before - len(df)} out-of-range rows. Clean shape: {df.shape}")

# ─── 3. Feature engineering ──────────────────────────────────────────────────
df["date"]        = df["timestamp"].dt.date
df["hour"]        = df["timestamp"].dt.hour
df["month"]       = df["timestamp"].dt.month
df["day_of_week"] = df["timestamp"].dt.day_name()
df["is_daytime"]  = df["hour"].between(6, 18)

# ─── 4. Daily summary per field ──────────────────────────────────────────────
daily = df.groupby(["date", "field"]).agg(
    avg_temp     = ("temperature_c",    "mean"),
    max_temp     = ("temperature_c",    "max"),
    min_temp     = ("temperature_c",    "min"),
    avg_humidity = ("humidity_pct",     "mean"),
    avg_soil     = ("soil_moisture_pct","mean"),
    min_soil     = ("soil_moisture_pct","min"),
    avg_light    = ("light_lux",        "mean"),
    readings     = ("temperature_c",    "count"),
).round(2).reset_index()

daily.to_csv("daily_summary.csv", index=False)
print(f"\nSaved daily_summary.csv — {len(daily)} rows")

# ─── 5. Anomaly detection (2σ rule) ──────────────────────────────────────────
METRICS = ["temperature_c", "humidity_pct", "soil_moisture_pct"]
for col in METRICS:
    mean = df[col].mean()
    std  = df[col].std()
    df[f"{col}_anomaly"] = (df[col] - mean).abs() > 2 * std

anomaly_cols = [f"{m}_anomaly" for m in METRICS]
anomalies = df[df[anomaly_cols].any(axis=1)].copy()
anomalies.to_csv("anomalies.csv", index=False)
print(f"Found {len(anomalies)} anomalous readings -> anomalies.csv")

# ─── 6. Drought alert periods (soil moisture < 25%) ──────────────────────────
drought = df[df["soil_moisture_pct"] < 25].groupby(["date", "field"]).size()
drought = drought.reset_index(name="drought_readings")
print(f"\nDrought alert periods:\n{drought.head(10)}")

# ─── 7. Correlation matrix ───────────────────────────────────────────────────
corr = df[METRICS + ["light_lux"]].corr().round(3)
print(f"\nCorrelation matrix:\n{corr}")

# ─── 8. Hourly pattern (for dashboard heatmap) ───────────────────────────────
hourly = df.groupby("hour").agg(
    avg_temp     = ("temperature_c",    "mean"),
    avg_soil     = ("soil_moisture_pct","mean"),
    avg_humidity = ("humidity_pct",     "mean"),
    avg_light    = ("light_lux",        "mean"),
).round(2).reset_index()

# ─── 9. Field summary ────────────────────────────────────────────────────────
field_summary = df.groupby("field").agg(
    avg_temp     = ("temperature_c",    "mean"),
    avg_humidity = ("humidity_pct",     "mean"),
    avg_soil     = ("soil_moisture_pct","mean"),
    avg_light    = ("light_lux",        "mean"),
    anomaly_count = ("temperature_c_anomaly", "sum"),
).round(2).reset_index()

# ─── 10. Export dashboard JSON ───────────────────────────────────────────────
dashboard = {
    "daily":        daily.assign(date=daily["date"].astype(str)).to_dict(orient="records"),
    "hourly":       hourly.to_dict(orient="records"),
    "field_summary":field_summary.to_dict(orient="records"),
    "anomalies":    anomalies.assign(
                        timestamp=anomalies["timestamp"].astype(str),
                        date=anomalies["date"].astype(str)
                    ).head(200).to_dict(orient="records"),
    "drought":      drought.assign(date=drought["date"].astype(str)).to_dict(orient="records"),
    "meta": {
        "total_readings": len(df),
        "total_anomalies": len(anomalies),
        "fields": df["field"].unique().tolist(),
        "sensors": df["sensor_id"].unique().tolist(),
        "date_range": [str(df["timestamp"].min().date()), str(df["timestamp"].max().date())],
    }
}

with open("dashboard_data.json", "w") as f:
    json.dump(dashboard, f, indent=2)

print("\nSaved dashboard_data.json")
print(f"\nMeta: {dashboard['meta']}")
