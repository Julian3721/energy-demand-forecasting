## set language setting
Sys.setlocale(locale = "en_US.utf8") ## English US Linux
Sys.setlocale(locale = "en_US") ## English US Mac
Sys.setlocale(locale = "English_United States.1252") ## English US Windows
## eventually change working folder using setwd() or copy file into working folder using getwd()

db_user <- "student"
db_password <- "#q6a21I&OA5k"
## %%

## Load packages
## %%
library(dplyr)
library(tidyr)
if (!"RMySQL" %in% rownames(installed.packages())) {
    stop("Rymsql is needed.")
}
library(dbx) # Needs RMySQL as well...
library(purrr)
library(DBI)
library(readr)
## %%


## Weather data for e.g. FR (France) from 2015 until now
# =================================================================================================================
# %%
# Establish a connection to the database
db <- dbxConnect(
    adapter = "mysql",
    host = "132.252.60.112",
    port = 3306,
    dbname = "DWD_MOSMIX",
    user = db_user,
    password = db_password
)

# Show the list of tables in db
dbListTables(db)

# Get stations from meteostat
meteostat_stations <- read_csv("meteostat_stations.csv")

# Get stations from DWD
stations_dwd <- tbl(db, "locations") %>%
    collect()

# Stations / locations for which we have forecasts and actuals
locations <- meteostat_stations %>%
    select("wmo") %>%
    inner_join(
        stations_dwd,
        by = c("wmo" = "stationid")
    ) %>%
    select(
        "stationid" = "wmo", "stationname",
        "latitude", "longitude", "height"
    )

# Get worldcities
worldcities <- read_csv("worldcities.csv")

n_largest <- 5 # for simplification use mean of Temp in n_largest largest cities

target_cities <- worldcities %>%
    filter(iso2 == "FR") %>%
    group_by(iso2) %>% # If more countries considered
    slice_max(order_by = population, n = n_largest) %>%
    split(.$iso2) # Order changes to alphabetical therefore we sorted above ...


# Find target stations by L2-distance to chosen cities
target_stations <- purrr::map(target_cities, .f = function(x) {
    LOCID <- numeric(nrow(x))
    for (i.x in 1:nrow(x)) { ## to loop across target_cities
        L2toCity <- (locations$longitude - x$lng[i.x])^2 +
            (locations$latitude - x$lat[i.x])^2
        LOCID[i.x] <- locations$stationid[which.min(L2toCity)]
    }
    return(LOCID)
})

# Get weather actuals
meteostat_utc <- tbl(db, "meteostat_utc")

# Overview of table
glimpse(meteostat_utc)

# Get temperature (temp) and wind speed (wspd)
# check https://dev.meteostat.net/bulk/hourly.html#endpoints
# for information on meteostat variable names
weather_actuals <- meteostat_utc %>%
    select(
        stationid, year, month, day, hour, temp, wspd
    ) %>%
    filter(
        stationid %in% !!target_stations[["FR"]]
    ) %>%
    collect() %>%
    # Add leading zeros and convert to POSIXct
    mutate(
        month = sprintf("%02d", month),
        day = sprintf("%02d", day),
        hour = sprintf("%02d", hour)
    ) %>%
    unite("origin", c("year", "month", "day", "hour")) %>%
    mutate(
        origin = anytime::anytime(origin, tz = "UTC", asUTC = TRUE)
    ) %>%
    group_by(origin) %>%
    summarize(
        temp_actual = mean(temp, na.rm = TRUE),
        wspd_actual = mean(wspd, na.rm = TRUE)
    ) %>%
    select("DateTime" = origin, temp_actual, wspd_actual) %>%
    filter(
        lubridate::year(DateTime) >= 2015
    )


# Get weather forecasts for 240 hours
# usually takes a while
forecasts_utc <- tbl(db, "mosmix_s_utc")

# Overview of table
glimpse(forecasts_utc)

# Get temperature (T5cm) and wind speed (FF)
# check mosmix_elements.xlsx for information on variable names
weather_forecasts <- forecasts_utc %>%
    select(
        stationid,
        oyear, omonth, oday, ohour,
        horizon,
        fyear, fmonth, fday, fhour,
        FF,
        TTT
    ) %>%
    filter(
        stationid %in% !!target_stations[["FR"]],
    ) %>%
    collect() %>%
    # Convert origin and forecast time to POSIXct format
    mutate(
        omonth = sprintf("%02d", omonth),
        oday = sprintf("%02d", oday),
        ohour = sprintf("%02d", ohour),
        fmonth = sprintf("%02d", fmonth),
        fday = sprintf("%02d", fday),
        fhour = sprintf("%02d", fhour)
    ) %>%
    unite("origin", c("oyear", "omonth", "oday", "ohour")) %>%
    unite("forecast", c("fyear", "fmonth", "fday", "fhour")) %>%
    mutate(
        origin = anytime::anytime(origin, tz = "UTC", asUTC = TRUE),
        forecast = anytime::anytime(forecast, tz = "UTC", asUTC = TRUE)
    )

# Check the available forecasting horizons
weather_forecasts %>% distinct(horizon)

weather_forecasts <- weather_forecasts %>%
    group_by(origin, horizon) %>%
    summarize(
        temp_forecast = mean(TTT - 273.15, na.rm = TRUE), # Convert Kelvin to Celsius
        wspd_forecast = mean(FF, na.rm = TRUE)
    ) %>%
    select("DateTime" = origin, horizon, temp_forecast, wspd_forecast) %>%
    filter(
        lubridate::year(DateTime) >= 2015
    )

# Close the connection
dbDisconnect(db)
# %%
# ====



