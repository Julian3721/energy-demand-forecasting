# 1. Bibliotheken importieren
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import requests
from datetime import datetime, timedelta



# Anzeigeoptionen
pd.set_option('display.max_columns', None)
sns.set_theme(style="whitegrid")

# 2. Daten laden
df = pd.read_csv("merged.csv", parse_dates=["DateTime"])


start_date = df["DateTime"].min().date()
end_date = df["DateTime"].max().date()

# example: coordinates for Vienna
latitude = 48.2082
longitude = 16.3738

# query weather data via api
url = "https://archive-api.open-meteo.com/v1/archive"

params = {
    "latitude": latitude,
    "longitude": longitude,
    "start_date": start_date.isoformat(),
    "end_date": end_date.isoformat(),
    "hourly": "temperature_2m,shortwave_radiation,wind_speed_10m",
    "timezone": "auto"
}

response = requests.get(url, params=params)
data = response.json()

# weather dataframe
hourly_data = data.get("hourly", {})
weather = pd.DataFrame(hourly_data)

# time column in datetime
if "time" in weather.columns:
    weather["time"] = pd.to_datetime(weather["time"])

# format and sort datatime
df = df.sort_values("DateTime")
weather = weather.sort_values("time")

# merge on datetime (left join: df ← weather)
df = pd.merge_asof(df, weather, left_on="DateTime", right_on="time", direction="backward")

# drop weather time
df = df.drop(columns=["time"])

# 3. Übersicht der Daten
print(df.head())
print(df.info())
print(df.describe())

# 4. Fehlende Werte
missing = df.isnull().sum()
print("Fehlende Werte je Spalte:")
print(missing[missing > 0])

# 5. Korrelationen
plt.figure(figsize=(12, 10))
sns.heatmap(df.corr(numeric_only=True), annot=True, fmt=".2f", cmap="coolwarm")
plt.title("Korrelationsmatrix")
plt.show()

# 6. Zeitreihe: Stromverbrauch
df.set_index("DateTime", inplace=True)
df["Load_Actual"].plot(figsize=(15, 5), title="Stromverbrauch über die Zeit")
plt.ylabel("Last [MW]")
plt.xlabel("Zeit")
plt.grid()
plt.show()

# 7. Temperaturverlauf
df["temperature_2m"].plot(figsize=(15, 5), title="Temperaturverlauf")
plt.ylabel("Temperatur [°C]")
plt.grid()
plt.show()

# 8. Load vs. Temperatur
plt.figure(figsize=(8, 6))
sns.scatterplot(data=df, x="temperature_2m", y="Load_Actual", alpha=0.3)
plt.title("Zusammenhang zwischen Temperatur und Last")
plt.xlabel("Temperatur [°C]")
plt.ylabel("Last [MW]")
plt.show()