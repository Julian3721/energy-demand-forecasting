import os
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine


def get_merged_data(path: str = "merged.csv"):

    if not os.path.exists(path):
        # %% Define MySQL connection parameters
        db_user = "student"
        db_password = "#q6a21I&OA5k"
        host = "132.252.60.112"
        port = 3306
        dbname = "ENTSOE"

        #  %%Create MySQL engine
        engine = create_engine(
            f"mysql://{urllib.parse.quote_plus(db_user)}:"
            f"{urllib.parse.quote_plus(db_password)}@{host}:{port}/{dbname}"
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
        targets = spec[
            (spec["Name"] == "Generation")
            & (spec["Type"].isin(["DayAhead", "Actual"]))
            & (spec["ProductionType"].isin(["Wind Onshore", "Wind Offshore", "Solar"]))
            & (spec["MapCode"] == "AT")
            & (spec["MapTypeCode"] == "BZN")
        ]

        values_query = f"""
        SELECT *
        FROM vals
        WHERE TimeSeriesID IN ({", ".join(map(str, targets['TimeSeriesID']))})
        AND YEAR(`DateTime`) >= '2014'
        """
        values = pd.read_sql_query(values_query, engine)

        data = pd.merge(values, targets, on="TimeSeriesID")
        data = data[data["DateTime"].dt.year >= 2014]
        data = data[["DateTime", "Type", "ProductionType", "Value"]]
        data = data.sort_values(by="DateTime")
        data_generations = data.pivot_table(
            index="DateTime", columns=["Type", "ProductionType"], values="Value"
        )
        print(data_generations)
        data_generations.describe()
        data_generations.to_csv("data_generations.csv")

        # ************************************************************************
        # %% Load data
        # *************************************************************************
        targets = spec[
            (spec["Name"] == "Load")
            & (spec["Type"].isin(["DayAhead", "Actual"]))
            & (spec["MapCode"] == "AT")
            & (spec["MapTypeCode"] == "BZN")
        ]
        values_query = f"""
        SELECT *
        FROM vals
        WHERE TimeSeriesID IN ({", ".join(map(str, targets['TimeSeriesID']))})
        AND YEAR(`DateTime`) >= '2014'
        """
        values = pd.read_sql_query(values_query, engine)
        data = pd.merge(values, targets, on="TimeSeriesID")
        data = data[data["DateTime"].dt.year >= 2014]
        data = data[["DateTime", "Type", "Value"]]
        data = data.sort_values(by="DateTime")
        data_load = data.pivot_table(index="DateTime", columns=["Type"], values="Value")
        print(data_load)
        data_load.describe()
        data_load.to_csv("data_load.csv")

        # ************************************************************************
        # %% Price data
        # *************************************************************************
        targets = spec[
            (spec["Name"] == "Price")
            & (spec["Type"].isin(["DayAhead", "Actual"]))
            & (spec["MapCode"] == "AT")
            & (spec["MapTypeCode"] == "BZN")
        ]
        values_query = f"""
        SELECT *
        FROM vals
        WHERE YEAR(`DateTime`) >= '2014'AND
        TimeSeriesID IN ({", ".join(map(str, targets['TimeSeriesID']))})"""
        values = pd.read_sql_query(values_query, engine)
        data = pd.merge(values, targets, on="TimeSeriesID")
        data = data[data["DateTime"].dt.year >= 2014]
        data = data[["DateTime", "Type", "Value"]]
        data = data.sort_values(by="DateTime")
        data_price = data.pivot_table(index="DateTime", columns=["Type"], values="Value")
        print(data_price)
        data_price.describe()
        data_price.to_csv("data_price.csv")

        # Merge
        data_generations.columns = ['_'.join(col).strip() for col in data_generations.columns.values]
        data_load.columns = [f'Load_{col}' for col in data_load.columns]
        data_price.columns = [f'Price_{col}' for col in data_price.columns]
        merged = (
            pd.merge(data_generations, data_load, on="DateTime", how="outer")
            .merge(data_price, on="DateTime", how="outer")
        )
        print(merged.info())
        print(merged.head())
        merged.to_csv(path)

    else:
        merged = pd.read_csv(path, parse_dates=["DateTime"])

    return merged

