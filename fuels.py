# %%
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import urllib.parse

# %% Read secrets -> db_user, db_password

db_user = "student"
db_password = "#q6a21I&OA5k"


# Define MySQL connection parameters
host = "132.252.60.112"
port = 3306
dbname = "DATASTREAM"
# %%

#  %%Create MySQL engine
engine = create_engine(
    f"mysql://{urllib.parse.quote_plus(db_user)}:{urllib.parse.quote_plus(db_password)}@{host}:{port}/{dbname}"
)

query = "SELECT * FROM datastream"
datastream_table = pd.read_sql_query(query, engine)

print(datastream_table)

datastream_table.columns

# **************************************************************************
# %% Exaple Streamlined fuel data frame
# ***************************************************************************

df = pd.read_sql_table("datastream", con=engine)

# Filter by name
target_names = [
    "coal_fM_01",  # USD
    "oil_fM_01",  # USD
    "gas_fD_01",
    "gas_fM_01",
    "gas_fQ_01",
    "gas_fY_01",
    "eua_fM_01",
    "USD_EUR",
    "EUR_GBP",
    "EUR_PLN",
    "EUR_RON",
    "EUR_UAH",
    "EUR_BGN",
]
df = df[df["name"].isin(target_names)].copy()

# Convert 'Date' to a proper datetime, then filter from '2010-01-01' onward
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df = df[df["Date"] >= "2010-01-01"].copy()

if "RIC" in df.columns:
    df.drop(columns=["RIC"], inplace=True)

# Pivot
df_wide = df.pivot(index="Date", columns="name", values="Value")

# EUR_USD = 1 / USD_EUR
df_wide["EUR_USD"] = 1.0 / df_wide["USD_EUR"]

# Convert coal_fM_01, oil_fM_01 from USD to EUR using EUR_USD
df_wide["coal_fM_01"] = df_wide["coal_fM_01"] / df_wide["EUR_USD"]
df_wide["oil_fM_01"] = df_wide["oil_fM_01"] / df_wide["EUR_USD"]

# Round coal_fM_01, oil_fM_01 to 2 decimals
df_wide["coal_fM_01"] = df_wide["coal_fM_01"].round(2)
df_wide["oil_fM_01"] = df_wide["oil_fM_01"].round(2)

df_wide.index = pd.to_datetime(df_wide.index).date

# Rename or select specific columns

df_wide = df_wide.rename(
    columns={
        "coal_fM_01": "Coal_fM",
        "gas_fD_01": "Gas_fD",
        "gas_fM_01": "Gas_fM",
        "gas_fQ_01": "Gas_fQ",
        "gas_fY_01": "Gas_fY",
        "oil_fM_01": "Oil_fM",
        "eua_fM_01": "EUA_fM",
    }
)

df_wide = df_wide.reset_index().rename(columns={"index": "Date", "Date": "Date"})

final_cols = [
    "Date",
    "Coal_fM",
    # "Coal_fQ",
    # "Coal_fY",
    "Gas_fD",
    "Gas_fM",
    "Gas_fQ",
    "Gas_fY",
    "Oil_fM",
    # "Oil_fQ",
    # "Oil_fY",
    "EUA_fM",
    "EUR_USD",
    "EUR_BGN",
    "EUR_GBP",
    "EUR_PLN",
    "EUR_RON",
    "EUR_UAH",
]

# Filter columns that actually exist in df_wide to avoid errors
final_cols = [c for c in final_cols if c in df_wide.columns]
df_wide = df_wide[final_cols]
fuels = df_wide.sort_values(by="Date")

print(fuels.head())

# Save the fuel data in your directory

# %%
