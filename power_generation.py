# %%
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path


# Define MySQL connection parameters

# login credentials, you have to be in the UDE, RUB or TuDO network (e.g. via VPN)
db_user < -"student"
db_password < -"#q6a21I&OA5k"
host = "132.252.60.112"
port = 3306
dbname = "ENTSOE"
# %%

#  %%Create MySQL engine
engine = create_engine(f"mysql://{db_user}:{db_password}@{host}:{port}/{dbname}")
# %%
# Say you want to get wind generation, actual and day-ahead for PT (portugal)
# only where the year is 2022

# Obtain specification table
spec_query = "SELECT * FROM spec"
spec = pd.read_sql_query(spec_query, engine)

# Get an overview
print(spec.head())
# %%

# %% Narrow down the spec table to get the targetTimeSeriesID's
targets = spec[
    (spec["Name"] == "Generation")
    & (spec["Type"].isin(["DayAhead", "Actual"]))
    & (spec["ProductionType"].isin(["Wind Onshore", "Wind Offshore"]))
    & (spec["MapCode"] == "PT")
    & (spec["MapTypeCode"] == "BZN")
]
# %%

# %% Obtain the forecasts table
values_query = f"""
SELECT *
FROM vals
WHERE TimeSeriesID IN ({", ".join(map(str, targets['TimeSeriesID']))})
AND YEAR(`DateTime`) = '2022'
"""
values = pd.read_sql_query(values_query, engine)
# %%

# Get the actual data
data = pd.merge(values, targets, on="TimeSeriesID")
data = data[data["DateTime"].dt.year == 2022]

# Select and wrangle even further
data = data[["DateTime", "Type", "ProductionType", "Value"]]
data = data.sort_values(by="DateTime")
data = data.pivot_table(
    index="DateTime", columns=["Type", "ProductionType"], values="Value"
)

print(data)
