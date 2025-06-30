import numpy as np
import pandas as pd
import requests
import holidays


def engineer_features(
    df: pd.DataFrame,
    country: str = "Austria",
    latitude: float = 48.2082,
    longitude: float = 16.3738,
    holiday_cls=None,
    weather: bool = True,
) -> pd.DataFrame:
    df = df.copy()

    df["target"] = df["Load_Actual"].shift(-1)

    at_holidays = holiday_cls() if holiday_cls else holidays.country_holidays(country)
    df["is_weekend"] = (df["DateTime"].dt.weekday >= 5).astype(int)
    df["is_holiday"] = df["DateTime"].dt.date.map(at_holidays.__contains__).astype(int)
    df["day_of_week"] = df["DateTime"].dt.weekday
    df["week_of_year"] = df["DateTime"].dt.isocalendar().week.astype(int) - 1
    df["is_workday"] = (~df["is_weekend"] & ~df["is_holiday"]).astype(int)

    seconds_in_day = 86_400
    t_sec = (
        df["DateTime"].dt.hour * 3600
        + df["DateTime"].dt.minute * 60
        + df["DateTime"].dt.second
    )
    df["time_sin"] = np.sin(2 * np.pi * t_sec / seconds_in_day)
    df["time_cos"] = np.cos(2 * np.pi * t_sec / seconds_in_day)

    angle = 2 * np.pi * df["DateTime"].dt.dayofyear / 365
    df["doy_sin"] = np.sin(angle)
    df["doy_cos"] = np.cos(angle)

    df = df.drop(columns=df.filter(like="DayAhead").columns)

    cutoff = pd.Timestamp("2018-10-01", tz=df["DateTime"].dt.tz)
    df = df[df["DateTime"] >= cutoff]

    full_rows = ~df.isna().any(axis=1)
    last_full_idx = full_rows[full_rows].index[-1]
    df = df.loc[:last_full_idx]

    df["load_lag_1"] = df["Load_Actual"].shift(1)
    df["load_lag_2"] = df["Load_Actual"].shift(2)
    df["load_lag_3"] = df["Load_Actual"].shift(3)
    df["load_lag_4"] = df["Load_Actual"].shift(4)
    df["load_lag_96"] = df["Load_Actual"].shift(96)
    df["load_diff_1"] = df["load_lag_1"] - df["load_lag_2"]
    df["load_diff_4"] = df["load_lag_1"] - df["load_lag_4"]
    df["load_diff_24h"] = df["load_lag_1"] - df["load_lag_96"]

    df["load_mean_1h"] = df["Load_Actual"].rolling(4).mean()
    df["load_std_1h"] = df["Load_Actual"].rolling(4).std()
    df["load_ramp_1h"] = (
        df["Load_Actual"].rolling(4).max() - df["Load_Actual"].rolling(4).min()
    )

    if weather:
        start_date = df["DateTime"].min().date()
        end_date = df["DateTime"].max().date()
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "hourly": "temperature_2m,shortwave_radiation,wind_speed_10m",
            "timezone": "auto",
        }
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        weather_df = pd.DataFrame(r.json().get("hourly", {}))
        if "time" in weather_df.columns:
            weather_df["time"] = pd.to_datetime(weather_df["time"])

        df = pd.merge_asof(
            df.sort_values("DateTime"),
            weather_df.sort_values("time"),
            left_on="DateTime",
            right_on="time",
            direction="backward",
        ).drop(columns="time")

        p = 1.2
        df["heating_demand"] = df["temperature_2m"].apply(
            lambda t: (15 - t) ** p if t < 15 else 0
        )
        df["cooling_demand"] = df["temperature_2m"].apply(
            lambda t: (t - 22) ** p if t > 22 else 0
        )

    df = df.dropna().reset_index(drop=True)
    return df
