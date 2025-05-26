# %% set language setting
Sys.setlocale(locale = "en_US.utf8") ## English US Linux
Sys.setlocale(locale = "en_US") ## English US Mac
Sys.setlocale(locale = "English_United States.1252") ## English US Windows
# %% eventually change working folder using setwd() or copy file into working folder using getwd()

# region load packges
## Load packages
library(dplyr)
library(tidyr)
if (!"RMySQL" %in% rownames(installed.packages())) {
    stop("Rymsql is needed.")
}
library(dbx) # Needs RMySQL as well...
library(purrr)
library(DBI)
library(readr)
#
# endregion

# region # Load data (actual and day-ahead) for e.g. FR (France) from 2015 until now
# login credentials, you have to be in the UDE, RUB or TuDO network (e.g. via VPN)
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

# Show the list of tables in db
dbListTables(db)

# Obtain specification table
spec <- tbl(db, "spec") %>% collect()

# Get an overview
glimpse(spec)

unique(spec$Name) # We need "Load" here...
unique(spec$Type) # "DayAhead" and "Actual" ...
unique(spec$MapCode) # We want "FR" here...
unique(spec$MapTypeCode) # We take "CTY" here ...

# Lets narrow down the spec table to get the targetTimeSeriesID's
targets <- spec %>%
    filter(
        Name == "Load",
        Type %in% c("DayAhead", "Actual"),
        MapCode == "FI",
        MapTypeCode == "CTY",
    ) %>%
    # Remove empty columns
    select_if(function(x) !(all(is.na(x)) | all(x == "")))


# Obtain (a connection to) the forecasts table
values <- tbl(db, "vals")

glimpse(values)

# Get the actual data
data.input <- values %>%
    # !! (bang-bang) to unquote the expression
    filter(TimeSeriesID %in% !!targets$TimeSeriesID) %>%
    collect() %>%
    left_join(spec, by = "TimeSeriesID") %>%
    # Filter the joined data to keep only from 2015+
    filter(
        lubridate::year(DateTime) >= 2015
    ) %>%
    # DateTime in UTC
    mutate(DateTime = as.POSIXct(DateTime, format = "%Y-%m-%d %H:%M", tz = "UTC")) %>%
    # Select the cols of interest
    select(DateTime, Type, Value, MapCode, Name) %>%
    arrange(DateTime) %>% 
    distinct(pick(DateTime, MapCode, Name, Type), .keep_all = TRUE) %>% #deal with duplicates
    pivot_wider(names_from = c(MapCode, Name, Type), values_from = Value)#

# Use show_query() to check how the SQL query will look
values %>%
    filter(TimeSeriesID %in% !!targets$TimeSeriesID) %>%
    filter(
        lubridate::year(DateTime) >= 2015
    ) %>%
    show_query()

# Close the connection
dbDisconnect(db)
# endregion
# =================================================================================================================

source("DST.trafo.R")

time.utc <- data.input$DateTime # as.POSIXct( strptime(data.input[,1], format="%Y-%m-%d %H:%M:%S", tz="UTC")  )
time.numeric <- as.numeric(time.utc) ## time as numeric
local.time.zone <- "CET" # local time zone abbrv.
time.lt <- as.POSIXct(time.numeric, origin = "1970-01-01", tz = local.time.zone)
## 'fake' local time ... with 24 hours a day.
S <- 24 * 60 * 60 / as.numeric(names(which.max(table(diff(time.numeric))))) ## frequency of the data
start.end.time.S <- strptime(format(c(time.lt[1], time.lt[length(time.lt)])), format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
time.S.numeric <- seq(as.numeric(start.end.time.S[1]), as.numeric(start.end.time.S[length(start.end.time.S)]), by = 24 * 60 * 60 / S)
time.S <- as.POSIXct(time.S.numeric, origin = "1970-01-01", tz = "UTC") ## 'fake' local time
dates.S <- unique(as.Date(time.S))

id.time<- match(time.S.numeric, time.numeric)
id.time<- id.time[!is.na(id.time)]

data <- DST.trafo(X = data.input[id.time, -1], Xtime = time.utc[id.time], Xtz = local.time.zone) # may take a while
dim(data)




load <- as.numeric(t(data[, , 1])) / 1000 ## all available load data  /1000 only for scaling issues

sum(is.na(load))

QQ <- seq(.1, .9, .1) ## target probability levels for quantiles
Horizon <- 7 * S
TT <- 365 * S ## insample length


SS <- seq(0, 300, S) ## 300 days for testing purpose



## here illustration only for one time point s in the rolling window study.
i.S <- 1
s <- SS[i.S]
index <- 1:TT + s
X <- load[index]
time <- time.S[index]
H <- Horizon
TAU <- QQ


f.quantile_ARX_normal <- function(X, time, H, TAU) {
    time.tmp.num <- as.numeric(time)
    time.ext <- c(time.tmp.num, max(time.tmp.num) + 1:H * (3600 * 24) / S)
    time.ext.utc <- as.POSIXct(time.ext, origin = "1970-01-01", tz = "UTC")

    DD <- as.numeric(format(time.ext.utc, "%w")) %in% c(0, 6) ## dummy

    LAGS <- c(1, 24, 168)
    get.lagged <- function(lag, Z) c(rep(NA, lag), Z[(1 + lag):(length(Z)) - lag])
    XLAG <- sapply(LAGS, get.lagged, Z = X)

    XLAG.ext <- rbind(XLAG, matrix(NA, H, length(LAGS)))
    XREG <- cbind(1, XLAG.ext, DD)
    p <- dim(XREG)[2] # p = number of regressors

    model <- lm(X ~ head(XREG, length(X)) - 1) ## lm can be replace by lm.fit when missing data is handled.

    betahat <- model$coef
    sigma2hat <- var(model$res)


    lagm <- max(LAGS) ## maximal memory
    M <- 10000

    ## now create large matrix with response and regression matrix
    XSIM <- array(cbind(c(tail(X, lagm), rep(NA, H)), tail(XREG, lagm + H)), dim = c(lagm + H, 1 + p, M))

    h <- 1
    EPS <- array(rnorm(H * M, sd = sqrt(sigma2hat)), dim = c(H, M))

    for (h in 1:H) {
        ## replace model specific unknown values with known ones (in the path)
        XSIM[lagm + h, 1 + 1 + 1:length(LAGS), ] <- XSIM[lagm + h - LAGS, 1, ]
        ## compute forecast:  beta'X_t + eps_t
        XSIM[lagm + h, 1, ] <- betahat %*% XSIM[lagm + h, 1 + 1:p, ] + EPS[h, ]
    } # h


    QUANTILES <- apply(XSIM[lagm + 1:H, 1, ], 1, quantile, probs = TAU)

    t(QUANTILES)
} # f.quantile_ARX_normal



### run the model and plot the results
set.seed(1234)
Qhat1 <- f.quantile_ARX_normal(X = load[index], time = time.S[index], H = Horizon, TAU = QQ)

ts.plot(Qhat1)



## now ARX model with bootstrap

f.quantile_ARX_boot <- function(X, time, H, TAU) {
    time.tmp.num <- as.numeric(time)
    time.ext <- c(time.tmp.num, max(time.tmp.num) + 1:H * (3600 * 24) / S)
    time.ext.utc <- as.POSIXct(time.ext, origin = "1970-01-01", tz = "UTC")

    DD <- as.numeric(format(time.ext.utc, "%w")) %in% c(0, 6) ## dummy

    LAGS <- c(1, 24, 168)
    get.lagged <- function(lag, Z) c(rep(NA, lag), Z[(1 + lag):(length(Z)) - lag])
    XLAG <- sapply(LAGS, get.lagged, Z = X)

    XLAG.ext <- rbind(XLAG, matrix(NA, H, length(LAGS)))
    XREG <- cbind(1, XLAG.ext, DD)
    p <- dim(XREG)[2] # p = number of regressors

    model <- lm(X ~ head(XREG, length(X)) - 1) ## lm can be replace by lm.fit when missing data is handled.

    betahat <- model$coef


    lagm <- max(LAGS) ## maximal memory
    M <- 10000

    ## now create large matrix with response and regression matrix
    XSIM <- array(cbind(c(tail(X, lagm), rep(NA, H)), tail(XREG, lagm + H)), dim = c(lagm + H, 1 + p, M))

    h <- 1
    EPS <- array(sample(model$res, H * M, replace = TRUE), dim = c(H, M)) ## now Bootstrap
    # 	EPS<- array( rnorm(H*M, sd=sqrt(sigma2hat)) , dim=c(H, M) )

    for (h in 1:H) {
        ## replace model specific unknown values with known ones (in the path)
        XSIM[lagm + h, 1 + 1 + 1:length(LAGS), ] <- XSIM[lagm + h - LAGS, 1, ]
        ## compute forecast:  beta'X_t + eps_t
        XSIM[lagm + h, 1, ] <- betahat %*% XSIM[lagm + h, 1 + 1:p, ] + EPS[h, ]
    } # h


    QUANTILES <- apply(XSIM[lagm + 1:H, 1, ], 1, quantile, probs = TAU)

    t(QUANTILES)
} # f.quantile_ARX_boot



set.seed(1234)
Qhat2 <- f.quantile_ARX_boot(X = load[index], time = time.S[index], H = Horizon, TAU = QQ)

ts.plot(Qhat2)



## quantile regression

library(quantreg)

f.quantileReg_ARX <- function(X, time, H, TAU) {
    time.tmp.num <- as.numeric(time)
    time.ext <- c(time.tmp.num, max(time.tmp.num) + 1:H * (3600 * 24) / S)
    time.ext.utc <- as.POSIXct(time.ext, origin = "1970-01-01", tz = "UTC")

    DD <- as.numeric(format(time.ext.utc, "%w")) %in% c(0, 6) ## dummy

    LAGS <- c(1, 24, 168)
    get.lagged <- function(lag, Z) c(rep(NA, lag), Z[1:(length(Z) + pmin(0, H - lag))], rep(NA, max(0, H - lag))) ## caution! modified
    XLAG.ext <- sapply(LAGS, get.lagged, Z = X)

    XREG <- cbind(1, XLAG.ext, DD)


    ## create active lag sets
    LAGS.active <- list()
    for (h in 1:H) {
        LAGS.active[[h]] <- LAGS[LAGS >= h]
    }
    LAGS.active.length <- sapply(LAGS.active, length)

    IDX <- match(unique(LAGS.active.length), LAGS.active.length)
    LAGS.used <- list()
    H.used <- list()
    IDXH <- c(IDX - 1, H)
    for (i in seq_along(IDX)) {
        LAGS.used[[i]] <- LAGS.active[[IDX[i]]]
        H.used[[i]] <- (IDXH[i] + 1):IDXH[i + 1]
    }


    QUANTILES <- matrix(, H, length(TAU))


    i.used <- 2
    for (i.used in seq_along(H.used)) {
        LAGS.used.now <- LAGS.used[[i.used]] ## lag sets

        active.set <- match(LAGS.used.now, LAGS)
        QXREG <- head(XREG, length(X))[, c(1, 1 + active.set, 1 + length(LAGS) + 1)]
        model <- rq(X ~ QXREG - 1, tau = TAU) ## lm can be replace by lm.fit when missing data is handled.

        betahat <- model$coef


        QUANTILES[H.used[[i.used]], ] <- XREG[length(X) + H.used[[i.used]], c(1, 1 + active.set, 1 + length(LAGS) + 1)] %*% betahat
    } # i.used

    QUANTILES.sorted <- t(apply(QUANTILES, 1, sort))

    QUANTILES.sorted
} # f.quantileReg_ARX_boot



Qhat3 <- f.quantileReg_ARX(X = load[index], time = time.S[index], H = Horizon, TAU = QQ)

ts.plot(Qhat3)




## distributional regression
library(mgcv)
f.distributionalReg_ARX_normal <- function(X, time, H, TAU) {
    time.tmp.num <- as.numeric(time)
    time.ext <- c(time.tmp.num, max(time.tmp.num) + 1:H * (3600 * 24) / S)
    time.ext.utc <- as.POSIXct(time.ext, origin = "1970-01-01", tz = "UTC")

    DD <- as.numeric(format(time.ext.utc, "%w")) %in% c(0, 6) ## dummy

    LAGS <- c(1, 24, 168)
    get.lagged <- function(lag, Z) c(rep(NA, lag), Z[1:(length(Z) + pmin(0, H - lag))], rep(NA, max(0, H - lag))) ## caution! modified
    XLAG.ext <- sapply(LAGS, get.lagged, Z = X)
    dimnames(XLAG.ext)[[2]] <- paste0("L", LAGS)

    XREG <- cbind(XLAG.ext, DD)


    ## create active lag sets
    LAGS.active <- list()
    for (h in 1:H) {
        LAGS.active[[h]] <- LAGS[LAGS >= h]
    }
    LAGS.active.length <- sapply(LAGS.active, length)

    IDX <- match(unique(LAGS.active.length), LAGS.active.length)
    LAGS.used <- list()
    H.used <- list()
    IDXH <- c(IDX - 1, H)
    for (i in seq_along(IDX)) {
        LAGS.used[[i]] <- LAGS.active[[IDX[i]]]
        H.used[[i]] <- (IDXH[i] + 1):IDXH[i + 1]
    }


    QUANTILES <- matrix(, H, length(TAU))


    i.used <- 2
    for (i.used in seq_along(H.used)) {
        LAGS.used.now <- LAGS.used[[i.used]] ## lag sets

        active.set <- match(LAGS.used.now, LAGS)
        Xid <- c(active.set, length(LAGS) + 1)
        DXREG <- cbind(Y = X, as.data.frame(head(XREG, length(X))[, Xid]))
        location.formula <- as.formula(paste("Y~", paste0(dimnames(DXREG)[[2]][-1], collapse = "+")))
        scale.formula <- as.formula(paste("~", paste0(dimnames(DXREG)[[2]][-1], collapse = "+")))
        model <- gam(list(location.formula, scale.formula),
            data = DXREG, family = gaulss()
        )
        # 		model <- gam(location.formula,
        #            data=DXREG,family=gaulss())

        Xout <- as.data.frame(XREG[length(X) + H.used[[i.used]], Xid, drop = FALSE])
        Xpredlss <- predict(model, newdata = Xout, type = "link") # , se.fit=TRUE)
        Xpredlss
        QUANTILES[H.used[[i.used]], ] <- qnorm(rep(TAU, each = length(H.used[[i.used]])), rep(Xpredlss[, 1], length(TAU)), exp(rep(Xpredlss[, 2], length(TAU))))

        # 	ts.plot(QUANTILES[H.used[[i.used]], ] )
    } # i.used

    QUANTILES.sorted <- t(apply(QUANTILES, 1, sort))

    QUANTILES.sorted
} # f.distributionalReg_ARX_normal



Qhat4 <- f.distributionalReg_ARX_normal(X = load[index], time = time.S[index], H = Horizon, TAU = QQ)

ts.plot(Qhat4)







## pinball loss

pinball <- function(X, y, tau) t(t(y - X) * tau) * (y - X > 0) + t(t(X - y) * (1 - tau)) * (y - X < 0)

y <- load[max(index) + 1:Horizon]

Qlist <- list(Qhat1, Qhat2, Qhat3, Qhat4)
PBlist <- lapply(Qlist, function(Qhat) pinball(Qhat, y, TAU))

lapply(PBlist, mean)

par(mfrow = c(2, 2))
lapply(Qlist, ts.plot)
