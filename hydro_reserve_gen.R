# nolint srart
# %%
library(dplyr)
library(tidyr)
if (!"RMySQL" %in% rownames(installed.packages())) {
  stop("Rymsql is needed.")
}
library(dbx) # Needs RMySQL as well...

db_user <- "student"
db_password <- "#q6a21I&OA5k"

# Establish a connection to the database
db <- dbxConnect(
  adapter = "mysql",
  host = "132.252.60.112",
  port = 3306,
  dbname = "ENTSOE",
  user = db_user,
  password = db_password
)

# %%
# Say you want to get wind generation, actual and day-ahead

# Obtain specification table
spec <- tbl(db, "spec") %>% collect()

# Get an overview
glimpse(spec)

unique(spec$Name) # We need "Generation" here...
unique(spec$Type) # "DayAhead" and "Actual" ...
unique(spec$ProductionType) # "Wind Onshore" and "Wind Offshore" ...
unique(spec$MapCode) # We want "PT" here...
unique(spec$MapTypeCode) # We take "BZN" here ...

# Lets narrow down the spec table to get the targetTimeSeriesID's
targets <- spec %>%
  filter(
    Name == "Generation",
    Type %in% c("DayAhead", "Actual"),
    ProductionType %in% c("Wind Onshore", "Wind Offshore"),
    MapCode == "PT",
    MapTypeCode == "BZN"
  ) %>%
  # Remove empty columns
  select_if(function(x) !(all(is.na(x)) | all(x == "")))


# Obtain (a connection to) the forecasts table
values <- tbl(db, "vals")

glimpse(values)

# %% Get the actual data
data <- values %>%
  filter(TimeSeriesID %in% !!targets$TimeSeriesID) %>%
  filter( # CAPITAL WORDS will be passed to SQL 1:1 ...
    YEAR(`DateTime`) >= "2014"
  ) %>%
  collect() %>%
  left_join(spec, by = "TimeSeriesID")

# We may want to select and wrangle even further:
data %>%
  # Select the cols of interest
  select(DateTime, Type, ProductionType, Value) %>%
  arrange(DateTime) %>%
  pivot_wider(names_from = c(Type, ProductionType), values_from = Value)

# Use show_query() to check how the SQL query will look
values %>%
  filter(TimeSeriesID %in% !!targets$TimeSeriesID) %>%
  filter( # CAPITAL WORDS will be passed to SQL 1:1 ...
    YEAR(`DateTime`) >= "2014"
  ) %>%
  show_query()

View(data)

#*************************************************************************
# %% Hydro Reservoir

# Lets narrow down the spec table to get the targetTimeSeriesID's
targets <- spec %>%
  filter(
    Name == "FillingRateHydro",
    MapCode %in% c("NO", "NO1", "NO2", "NO3", "NO4", "NO5"),
  ) %>%
  # Remove empty columns
  select_if(function(x) !(all(is.na(x)) | all(x == "")))

# Obtain (a connection to) the forecasts table
values <- tbl(db, "vals")

glimpse(values)

# %%
# Get the actual data
data <- values %>%
  filter(TimeSeriesID %in% !!targets$TimeSeriesID) %>%
  collect() %>%
  left_join(spec, by = "TimeSeriesID")

# We may want to select and wrangle even further:
data %>%
  # Select the cols of interest
  select(DateTime, Name, MapCode, MapTypeCode, Value) %>%
  arrange(DateTime) %>%
  pivot_wider(
    names_from = c(Name, MapCode, MapTypeCode),
    values_from = Value
  ) %>%
  select(FillingRateHydro_NO_CTY, FillingRateHydro_NO_CTA)

# Use show_query()
values %>%
  filter(TimeSeriesID %in% !!targets$TimeSeriesID) %>%
  filter(
    YEAR(`DateTime`) >= "2014"
  ) %>%
  show_query()

# Inspect the data and determine the relevant information

View(data)

# Save it to directory, in your computer/notebook/cloud


# %% Example: filtering and extraction of data

filling_rate_hydro <- values %>%
  filter(TimeSeriesID %in% !!targets$TimeSeriesID) %>%
  collect() %>%
  left_join(spec, by = "TimeSeriesID") %>%
  select(DateTime, Name, MapCode, MapTypeCode, Value) %>%
  arrange(DateTime) %>%
  # Pivot
  pivot_wider(
    names_from  = c(Name, MapCode, MapTypeCode),
    values_from = Value
  ) %>%
  select(
    DateTime,
    # Entire Norway (country) if it exists:
    FillingRateHydro_NO_CTY,
    # Control area if it exists:
    FillingRateHydro_NO_CTA,
    # Individual bidding zones:
    FillingRateHydro_NO1_BZN,
    FillingRateHydro_NO2_BZN,
    FillingRateHydro_NO3_BZN,
    FillingRateHydro_NO4_BZN,
    FillingRateHydro_NO5_BZN
  )

# Inspect the resulting DataFrame
filling_rate_hydro
View(filling_rate_hydro)

# %% Save the data to the directory. Put your desired local directory

save(filling_rate_hydro,
  file = "~/Documents/DSEE_2025/FillingRateHydro.Rds"
)

# nolint end
