# %%
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import urllib.parse
import os

if not os.path.exists("merged.csv"):


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
    print(spec)
    spec.MapTypeCode.unique()
    spec.MapCode.unique()
    spec.Name.unique()
    spec.ProductionType.unique()
    spec.ResolutionCode.unique()
    spec.to_csv("spec.csv")

    # %% Get value by the production types, you can add more
    #  Narrow down the spec table to get the targetTimeSeriesID's
    targets = spec[
        (spec["Name"] == "Generation")
        & (spec["Type"].isin(["DayAhead", "Actual"]))
        & (spec["ProductionType"].isin(["Wind Onshore", "Wind Offshore", "Solar"]))
        & (spec["MapCode"] == "AT")  # Put desired BZN here
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
    data_generations.to_csv("data_generations.csv")

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
        & (spec["MapCode"] == "AT")  # Put desired BZN here
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
    data_load.to_csv("data_load.csv")

    # You can save the data to your desired directory, for further use

    # ************************************************************************
    # %% Priee data
    # *************************************************************************
    #  Narrow down the spec table to get the targetTimeSeriesID's
    targets = spec[
        (spec["Name"] == "Price")
        & (spec["Type"].isin(["DayAhead", "Actual"]))
        # & (spec["ProductionType"].isin(["Wind Onshore", "Wind Offshore", "Solar"]))
        & (spec["MapCode"] == "AT")  # Put desired price zones (BZN)here
        & (spec["MapTypeCode"] == "BZN")
    ]


    # %% Obtain the forecasts table
    values_query = f"""
    SELECT *
    FROM vals
    WHERE YEAR(`DateTime`) >= '2014'AND
    TimeSeriesID IN ({", ".join(map(str, targets['TimeSeriesID']))})"""


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
    data_price.to_csv("data_price.csv")

    # You can save the data to your desired directory, for further use


    # ************************************************************************

    # %% Merge the dataframes
    # Schritt 1: Zusammenführen
    # Für data_generations (2 Spaltenebenen: Type + ProductionType)
    data_generations.columns = ['_'.join(col).strip() for col in data_generations.columns.values]

    # Für data_load (nur Type als Spaltenname)
    data_load.columns = [f'Load_{col}' for col in data_load.columns]

    # Für data_price (nur Type als Spaltenname)
    data_price.columns = [f'Price_{col}' for col in data_price.columns]

    # Merge the dataframes on DateTime
    # Merge alle drei DataFrames
    merged = (
        pd.merge(data_generations, data_load, on="DateTime", how="outer")
        .merge(data_price, on="DateTime", how="outer")
    )
    # Schritt 2: Speichern
    print(merged.info())
    print(merged.head())
    # Save the merged DataFrame to a CSV file
    merged.to_csv("merged.csv")

else:
    merged = pd.read_csv("merged.csv", index_col=0, parse_dates=True)
    print(merged.info())
    print(merged.head())


