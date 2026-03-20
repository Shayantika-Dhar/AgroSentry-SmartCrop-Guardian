"""
AgroSentry — Sensor Data Simulator
Simulates IoT sensor readings from an ARM7-based crop field monitoring system.
Generates realistic temperature, humidity, soil moisture, and light data
across 3 fields and 4 sensor nodes over 1000 hours.

Run: python simulate_data.py
Output: sensor_data.csv
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

FIELDS  = ["Field_A", "Field_B", "Field_C"]
SENSORS = ["S01", "S02", "S03", "S04"]
START   = datetime(2024, 1, 1)
HOURS   = 1000

rows = []

for i in range(HOURS):
    ts = START + timedelta(hours=i)
    hour_of_day = ts.hour

    for field in FIELDS:
        # Each field has slightly different baseline conditions
        field_temp_offset = {"Field_A": 0, "Field_B": 1.5, "Field_C": -1.0}[field]
        field_soil_offset = {"Field_A": 0, "Field_B": -5,  "Field_C": 3.0 }[field]

        for sensor in SENSORS:
            # Diurnal temperature cycle (hotter midday)
            diurnal = 4 * np.sin((hour_of_day - 6) * np.pi / 12)

            temp     = round(np.random.normal(28 + diurnal + field_temp_offset, 3), 2)
            humidity = round(np.random.normal(65 - diurnal * 0.5, 8), 2)
            soil_mst = round(np.random.normal(40 + field_soil_offset, 6), 2)
            light    = round(max(0, np.random.normal(
                600 * max(0, np.sin((hour_of_day - 6) * np.pi / 12)), 80
            )), 2)

            # Inject realistic anomalies (~3% of readings)
            if random.random() < 0.03:
                temp     += random.choice([12, -12])
            if random.random() < 0.02:
                soil_mst  = round(max(5, soil_mst - random.uniform(15, 25)), 2)  # drought spike

            # Clamp to realistic ranges
            temp     = max(-5,   min(55,   temp))
            humidity = max(10,   min(100,  humidity))
            soil_mst = max(5,    min(100,  soil_mst))
            light    = max(0,    min(1200, light))

            rows.append([ts, field, sensor, temp, humidity, soil_mst, light])

df = pd.DataFrame(rows, columns=[
    "timestamp", "field", "sensor_id",
    "temperature_c", "humidity_pct",
    "soil_moisture_pct", "light_lux"
])

df.to_csv("sensor_data.csv", index=False)
print(f"Generated {len(df):,} rows -> sensor_data.csv")
print(df.describe().round(2))
