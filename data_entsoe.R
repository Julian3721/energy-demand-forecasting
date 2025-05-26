# nolint start
# %%
library(dbx) # Needs RMySQL as well...
if (!"RMySQL" %in% rownames(installed.packages())) {
  stop("Rymsql is needed.")
}
library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)

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


# %%  Functions
get_data <- function(x, values, mapcode = FALSE) {
  if (mapcode) {
    specs_new <- x %>%
      dplyr::select(
        TimeSeriesID, Name, ProductionType, Type,
        ResolutionCode, MapCode
      )

    values %>%
      filter(TimeSeriesID %in% !!specs_new$TimeSeriesID) %>%
      # Collect downloads the data from database
      collect() %>%
      select(TimeSeriesID, DateTime, Value) %>%
      # We join our spec table to get the information on the TimeSeriesID's
      left_join(specs_new, by = "TimeSeriesID") %>%
      mutate_at(vars(Type, ProductionType), str_replace_all, " ", "_") %>%
      mutate_at(vars(MapCode), str_replace_all, "-", "_") %>%
      # Spread the time series into columns
      select(-TimeSeriesID) %>%
      arrange(ProductionType, desc(Type)) %>%
      pivot_wider(
        names_from = c(
          MapCode, Name, ProductionType, Type, ResolutionCode
        ),
        values_from = Value,
      ) %>%
      mutate(
        DateTime = anytime::anytime(DateTime, tz = "UTC", asUTC = TRUE),
      ) %>%
      arrange(DateTime)
  } else {
    specs_new <- x %>%
      dplyr::select(
        TimeSeriesID, Name, ProductionType, Type,
        ResolutionCode
      )

    values %>%
      filter(TimeSeriesID %in% !!specs_new$TimeSeriesID) %>%
      # Collect downloads the data from database
      collect() %>%
      select(TimeSeriesID, DateTime, Value) %>%
      # We join our spec table to get the information on the TimeSeriesID's
      left_join(specs_new, by = "TimeSeriesID") %>%
      mutate_at(vars(Type, ProductionType), str_replace_all, " ", "_") %>%
      # Spread the time series into columns
      select(-TimeSeriesID) %>%
      arrange(ProductionType, desc(Type)) %>%
      pivot_wider(
        names_from = c(Name, ProductionType, Type, ResolutionCode),
        values_from = Value,
      ) %>%
      mutate(
        DateTime = anytime::anytime(DateTime, tz = "UTC", asUTC = TRUE),
      ) %>%
      arrange(DateTime)
  }
}


# %%
# Obtain specification table
spec <- tbl(db, "spec") %>%
  collect()

# Obtain (a connection to) the forecasts table
values <- tbl(db, "vals")


# %% Batch processing. Will take a bit of time; keep your eye on the terminal
mapcode_sub <- c(
  "BE", "BG", "CH", "CZ", "EE", "ES", "FI", "FR", "GB", "GR", "HR", "HU",
  "LT", "LV", "ME", "MK", "NL", "PL", "PT", "RO", "RS", "SI", "SK"
)

entsoe <- list()
for (mc in seq_along(mapcode_sub)) {
  specs_prices <- spec %>%
    filter(
      Name %in% c("Price"),
      Type %in% c("DayAhead"),
      MapCode %in% mapcode_sub[mc]
    )

  specs_load <- spec %>%
    filter(
      Name == c("Load"),
      Type %in% c("DayAhead", "Actual"),
      MapCode == mapcode_sub[mc],
      MapTypeCode == "BZN"
    )

  specs_generation <- spec %>%
    filter(
      Name == c("Generation"),
      Type %in% c("DayAhead", "Actual"),
      ProductionType %in% c("Wind Onshore", "Wind Offshore", "Solar"),
      MapCode == mapcode_sub[mc],
      MapTypeCode == "BZN",
      is.na(Specification) | Specification == "Output"
    )

  data_prices <- specs_prices %>%
    get_data(values)

  data_load_gen <- bind_rows(
    specs_load,
    specs_generation
  ) %>%
    get_data(values) %>%
    mutate(
      across(-DateTime, ~ round(.x / 1000, 5))
    )

  data <- full_join(data_prices, data_load_gen, by = "DateTime")

  colnames(data) <- str_remove_all(colnames(data), "_NA")

  entsoe[[mapcode_sub[mc]]] <- data

  cat(mapcode_sub[mc], "\n")
}


# %% Italy
specs_prices <- spec %>%
  filter(
    Name %in% c("Price"),
    Type %in% c("DayAhead"),
    ResolutionCode == "PT60M",
    MapCode %in% c(
      "IT-SOUTH",
      "IT-CSOUTH",
      "IT-CNORTH",
      "IT-Sicily",
      "IT-Sardinia",
      "IT-NORTH",
      "IT-Brindisi",
      "IT-Calabria",
      "IT-Foggia",
      "IT-Priolo",
      "IT-Rossano",
      "IT-SACOAC",
      "IT-SACODC"
    )
  )

# "IT"

specs_load <- spec %>%
  filter(
    Name == c("Load"),
    Type %in% c("DayAhead", "Actual"),
    MapTypeCode == "CTA",
    MapCode == "IT"
  )

specs_generation <- spec %>%
  filter(
    Name == c("Generation"),
    Type %in% c("DayAhead", "Actual"),
    ProductionType %in% c("Wind Onshore", "Wind Offshore", "Solar"),
    MapCode == "IT",
    MapTypeCode == "CTA",
    is.na(Specification) | Specification == "Output"
  )

data_prices <-
  specs_prices %>%
  get_data(values, mapcode = TRUE)

data_load_gen <- bind_rows(
  specs_load,
  specs_generation
) %>%
  get_data(values, mapcode = TRUE) %>%
  mutate(
    across(-DateTime, ~ round(.x / 1000, 5))
  )

data <- full_join(data_prices, data_load_gen, by = "DateTime") %>%
  arrange(DateTime)

colnames(data) <- str_remove_all(colnames(data), "_NA")

entsoe[["IT"]] <- data


# %% Germany
specs_list <- list()
specs_list[["prices"]] <- spec %>%
  filter(
    Name %in% c("Price"),
    Type %in% c("DayAhead"),
    ResolutionCode == "PT60M",
    MapCode %in% c("DE_AT_LU", "DE_LU")
  )

# "DE_AT_LU", "DE_LU"

specs_load <- spec %>%
  filter(
    Name == c("Load"),
    Type %in% c("DayAhead", "Actual"),
    MapCode == "DE"
  )

specs_generation <- spec %>%
  filter(
    Name == c("Generation"),
    Type %in% c("DayAhead", "Actual"),
    ProductionType %in% c("Wind Onshore", "Wind Offshore", "Solar"),
    MapCode == "DE",
    is.na(Specification) | Specification == "Output"
  )

specs_list[["load_gen"]] <- bind_rows(
  specs_load,
  specs_generation
)

data_de <- purrr::map(specs_list, get_data, values = values, mapcode = TRUE)

data_de[["load_gen"]] <-
  data_de %>%
  purrr::pluck("load_gen") %>%
  mutate(
    DateTime = anytime::anytime(DateTime, tz = "UTC", asUTC = TRUE),
    DateTime = floor_date(DateTime, "hour"),
    across(-DateTime, ~ .x / 1000)
  ) %>%
  # Group by DateTime
  group_by(DateTime) %>%
  # Sum of the groups -> Aggregate load per hour
  summarise_all(mean) %>%
  mutate(
    across(-DateTime, ~ round(.x, 5))
  )

entsoe[["DE"]] <- data_de %>%
  purrr::pluck("prices") %>%
  mutate(
    Price = case_when(
      DateTime < ymd_hms("2018-09-30 22:00:00") ~ DE_AT_LU_Price_NA_DayAhead_PT60M,
      DateTime >= ymd_hms("2018-09-30 22:00:00") ~ DE_LU_Price_NA_DayAhead_PT60M,
    )
  ) %>%
  select(
    DateTime,
    Price
  ) %>%
  full_join(data_de[["load_gen"]], by = "DateTime") %>%
  select(
    DateTime,
    Price,
    "Load_DA" = `DE_Load_NA_DayAhead_PT15M`,
    "Load_Act" = `DE_Load_NA_Actual_PT15M`,
    "Solar_DA" = `DE_Generation_Solar_DayAhead_PT15M`,
    "Solar_Act" = `DE_Generation_Solar_Actual_PT15M`,
    "WindOn_DA" = `DE_Generation_Wind_Offshore_DayAhead_PT15M`,
    "WindOn_Act" = `DE_Generation_Wind_Offshore_Actual_PT15M`,
    "WindOff_DA" = `DE_Generation_Wind_Onshore_DayAhead_PT15M`,
    "WindOff_Act" = `DE_Generation_Wind_Onshore_Actual_PT15M`
  ) %>%
  arrange(DateTime)

#***************************************************************************
# %% Norway
# Load and generation: You can get the bidding zone information by changing,
# Copy MapCodes of prices into the load and generations calls
# Change the MapTypeCode from "CTY" to "BZN"
# Do not change the Price db query

specs_prices <- spec %>%
  filter(
    Name %in% c("Price"),
    Type %in% c("DayAhead"),
    ResolutionCode == "PT60M",
    MapCode %in% c(
      "NO1",
      "NO2",
      "NO3",
      "NO4",
      "NO5"
    )
  )

# %%
# "NO"- Norway aggregated. See the instructions above if you would like bidding zone info

specs_load <- spec %>%
  filter(
    Name == c("Load"),
    Type %in% c("DayAhead", "Actual"),
    MapTypeCode == "CTY",
    MapCode %in% c(
      "NO"
    )
  )

# "NO"- Norway aggregated. See the instructions above if you would like bidding zone info

specs_generation <- spec %>%
  filter(
    Name == c("Generation"),
    Type %in% c("DayAhead", "Actual"),
    ProductionType %in% c("Wind Onshore", "Wind Offshore", "Solar"),
    MapCode %in% c(
      "NO"
    ),
    MapTypeCode == "CTY",
    is.na(Specification) | Specification == "Output"
  )

data_prices <-
  specs_prices %>%
  get_data(values, mapcode = TRUE)

data_load_gen <- bind_rows(
  specs_load,
  specs_generation
) %>%
  get_data(values, mapcode = TRUE) %>%
  mutate(
    across(-DateTime, ~ round(.x / 1000, 5))
  )

data <- full_join(data_prices, data_load_gen, by = "DateTime") %>%
  arrange(DateTime)

colnames(data) <- str_remove_all(colnames(data), "_NA")

entsoe[["NO"]] <- data


# %% Check the data, for Germany
# Check the available data, for some cases full dataset may be available from 2015
tail(entsoe[["DE"]])
summary(entsoe[["DE"]])
View(entsoe[["DE"]])

# %% Check the data, for France
head(entsoe[["FR"]])
summary(entsoe[["FR"]])
View(entsoe[["FR"]])

# %% Check the data, for Norway
head(entsoe[["NO"]])
summary(entsoe[["NO"]])
View(entsoe[["NO"]])

# %% Save ENTSOE list in your local directory

save(entsoe,
  file = "~/Documents/DSEE_2025/entsoe_CTY.Rds"
)

# nolint end
