# %%
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import urllib.parse

# %% Define MySQL connection parameters
db_user = "student"
db_password = "#q6a21I&OA5k"
host = "132.252.60.112"
port = 3306
dbname = "ENTSOE"


#  %%Create MySQL engine
engine = create_engine(
    f"mysql://{urllib.parse.quote_plus(db_user)}:{urllib.parse.quote_plus(db_password)}@{host}:{port}/{dbname}"
)


# %% Obtain specification table
spec_query = "SELECT * FROM spec"
spec = pd.read_sql_query(spec_query, engine)

# Get an overview
print(spec.head())
spec.MapTypeCode.unique()
spec.MapCode.unique()
spec.Name.unique()
spec.ProductionType.unique()
spec.ResolutionCode.unique()

# %% Get value by the production types, you can add more
#  Narrow down the spec table to get the targetTimeSeriesID's
targets = spec[
    (spec["Name"] == "Generation")
    & (spec["Type"].isin(["DayAhead", "Actual"]))
    & (spec["ProductionType"].isin(["Wind Onshore", "Wind Offshore", "Solar"]))
    & (spec["MapCode"] == "DE_LU")  # Put desired BZN here
    & (spec["MapTypeCode"] == "BZN")
]


# %% Obtain the forecasts table
values_query = f"""
SELECT *
FROM vals
WHERE TimeSeriesID IN ({", ".join(map(str, targets['TimeSeriesID']))})
AND YEAR(`DateTime`) >= '2014'
"""
values = pd.read_sql_query(values_query, engine)


# %%

# Get the actual data
data = pd.merge(values, targets, on="TimeSeriesID")
data = data[data["DateTime"].dt.year >= 2014]  # You can change the year here

# Select and wrangle even further
data = data[["DateTime", "Type", "ProductionType", "Value"]]
data = data.sort_values(by="DateTime")
data_generations = data.pivot_table(
    index="DateTime", columns=["Type", "ProductionType"], values="Value"
)

print(data_generations)
data_generations.describe()

# We have used the DE_LU BZN in the earlier example (data_generations). 
# You can get DE_AT_LU or any other zone data, such as FR, PT, and so on. 
# The DE_AT_LU zone was split into DE_LU and AT. 
# Please take a closer look at the specs. 
# To supplement further, you may follow the "R" codes.

# ************************************************************************
# %% Load data
# *************************************************************************

#  Narrow down the spec table to get the targetTimeSeriesID's
targets = spec[
    (spec["Name"] == "Load")
    & (spec["Type"].isin(["DayAhead", "Actual"]))
    & (spec["MapCode"] == "FR")  # Put desired BZN here
    & (spec["MapTypeCode"] == "BZN")
]


# %% Obtain the forecasts table
values_query = f"""
SELECT *
FROM vals
WHERE TimeSeriesID IN ({", ".join(map(str, targets['TimeSeriesID']))})
AND YEAR(`DateTime`) >= '2014'
"""
values = pd.read_sql_query(values_query, engine)


# %%

# Get the actual data
data = pd.merge(values, targets, on="TimeSeriesID")
data = data[data["DateTime"].dt.year >= 2014]

# Select and wrangle even further
data = data[["DateTime", "Type", "Value"]]
data = data.sort_values(by="DateTime")
data_load = data.pivot_table(index="DateTime", columns=["Type"], values="Value")

print(data_load)
data_load.describe()

# You can save the data to your desired directory, for further use

# ************************************************************************
# %% Priee data
# *************************************************************************
#  Narrow down the spec table to get the targetTimeSeriesID's
targets = spec[
    (spec["Name"] == "Price")
    & (spec["Type"].isin(["DayAhead", "Actual"]))
    # & (spec["ProductionType"].isin(["Wind Onshore", "Wind Offshore", "Solar"]))
    & (spec["MapCode"] == "NL")  # Put desired price zones (BZN)here
    & (spec["MapTypeCode"] == "BZN")
]


# %% Obtain the forecasts table
values_query = f"""
SELECT *
FROM vals
WHERE TimeSeriesID IN ({", ".join(map(str, targets['TimeSeriesID']))})
AND YEAR(`DateTime`) >= '2014'
"""
values = pd.read_sql_query(values_query, engine)


# %%

# Get the actual data
data = pd.merge(values, targets, on="TimeSeriesID")
data = data[data["DateTime"].dt.year >= 2014]

# Select and wrangle even further
data = data[["DateTime", "Type", "Value"]]
data = data.sort_values(by="DateTime")
data_price = data.pivot_table(index="DateTime", columns=["Type"], values="Value")

print(data_price)
data_price.describe()

# You can save the data to your desired directory, for further use

# %%
