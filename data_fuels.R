# nolint start
# %%
library(dplyr)
library(tidyr)
if (!"RMySQL" %in% rownames(installed.packages())) {
  stop("Rymsql is needed.")
}
library(dbx) # Needs RMySQL as well...

# %%
db_user <- "student"
db_password <- "#q6a21I&OA5k"

# Establish a connection to the database
db <- dbxConnect(
  adapter = "mysql",
  host = "132.252.60.112",
  port = 3306,
  dbname = "DATASTREAM",
  user = db_user,
  password = db_password
)

# %%
# Get the data for a specific product in from a specific date
fuels <- tbl(db, "datastream") %>%
  filter(name %in% c(
    "coal_fM_01", # USD
    # "coal_fQ_01",
    # "coal_fY_01",
    "oil_fM_01", # USD
    # "oil_fQ_01",
    # "oil_fY_01",
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
    "EUR_BGN"
  )) %>%
  filter(Date >= "2010-01-01") %>%
  select(-RIC) %>%
  collect() %>%
  pivot_wider(names_from = name, values_from = Value) %>%
  mutate(
    EUR_USD = 1 / USD_EUR,
    # Convert coal and oil prices to EUR
    across(c(coal_fM_01, oil_fM_01), ~ .x / EUR_USD),
    across(c(coal_fM_01, oil_fM_01), ~ round(.x, 2)),
    # across(c(coal_fM_01, coal_fQ_01, coal_fY_01, oil_fM_01, oil_fQ_01, oil_fY_01), ~ .x / EUR_USD),
    # across(c(coal_fM_01, coal_fQ_01, coal_fY_01, oil_fM_01, oil_fQ_01, oil_fY_01), ~ round(.x, 2)),
    Date = as.Date(Date) # ,
    # Date = lubridate::with_tz(Date, tz = "UTC")
  ) %>%
  select(
    DateTime = Date,
    Coal_fM = coal_fM_01,
    # Coal_fQ = coal_fQ_01,
    # Coal_fY = coal_fY_01,
    Gas_fD = gas_fD_01,
    Gas_fM = gas_fM_01,
    Gas_fQ = gas_fQ_01,
    Gas_fY = gas_fY_01,
    Oil_fM = oil_fM_01,
    # Oil_fQ = oil_fQ_01,
    # Oil_fY = oil_fY_01,
    EUA_fM = EUA_fM_01,
    EUR_USD,
    EUR_BGN,
    EUR_GBP,
    EUR_PLN,
    EUR_RON,
    EUR_UAH
  ) %>%
  arrange(DateTime)

head(fuels)
View(fuels)

# %% Save the datafile to your directory
save(fuels,
  file = "~/Documents/DSEE_2025/fuels.Rds"
)
# %%
# Nolint end
