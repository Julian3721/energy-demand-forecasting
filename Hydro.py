# Hydro reservoir, Norway example
# Water Reservoirs and Hydro Storage Plants [16.1.D], ENTSOE

# %% Import packages
from pathlib import Path
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine
import os

# %% %% Define MySQL connection parameters
db_user = "student"
db_password = "#q6a21I&OA5k"
host = "132.252.60.112"
port = 3306
dbname = "ENTSOE"

# %% Connect to database

engine = create_engine(
    f"mysql://{urllib.parse.quote_plus(db_user)}:"
    f"{urllib.parse.quote_plus(db_password)}@{host}:{port}/{dbname}"
)

spec = pd.read_sql_query("SELECT * FROM spec", engine)  # Load specification table

# %% FillingRateHydro for Norway: Control area, and bidding zones(BZN) wise

hydro_targets = spec[
    (spec["Name"] == "FillingRateHydro")
    & (spec["MapCode"].isin(["NO", "NO1", "NO2", "NO3", "NO4", "NO5"]))
]

hydro_query = f"""
SELECT *
FROM vals
WHERE TimeSeriesID IN ({", ".join(map(str, hydro_targets["TimeSeriesID"]))})
"""
hydro_vals = pd.read_sql_query(hydro_query, engine)

hydro_raw = (
    hydro_vals.merge(hydro_targets, on="TimeSeriesID")
    .loc[:, ["DateTime", "Name", "MapCode", "MapTypeCode", "Value"]]
    .sort_values("DateTime")
)

hydro_raw["Series"] = (
    hydro_raw["Name"] + "_" + hydro_raw["MapCode"] + "_" + hydro_raw["MapTypeCode"]
)

hydro_data = hydro_raw.pivot(
    index="DateTime", columns="Series", values="Value"
).sort_index()

print("\n--- Hydro reservoir Norway ---")
print(hydro_data)

# %% Save it, change to your desired directory
save_dir = Path("~/Documents/DSEE_2025").expanduser()  # <-- your directory
save_dir.mkdir(parents=True, exist_ok=True)
file_path = save_dir / "FillingRateHydro.csv"
hydro_data.to_csv(file_path, index=True, encoding="utf-8")
print(f"\nHydro reservoir data written to: {file_path}")

# %% Read the saved csv, for later use
hydro = pd.read_csv(os.path.join("~/Documents/DSEE_2025", "FillingRateHydro.csv"))

# End

# %%
