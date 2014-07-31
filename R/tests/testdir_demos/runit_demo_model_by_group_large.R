#'
#' An H2O Demo: Building Models By Group
#'
#'  
#' This demo walks through a smalldata case where a data scientist wants to build a model based
#' on groupings of her data. We use flight record data in modeling against departure delays by 
#' airport origin code.
#'
#' What this demo covers:
#'   h2o.init        -- How to establish a connection to an H2O instance.
#'   h2o.importFile  -- How to import your data relative to the directory you started H2O on a machine
#'   colnames        -- Overloaded use of colnames for H2OParsedData objects (these are pointers to data frames in the H2O cloud).
#'   h2o::ddply      -- How to use the overloaded ddply call (overloaded from plyr) 
#'                       **NB: Take note of the library() calls below: library(plyr) masks h2o's ddply, so the namespace must be specified
#'   h2o.glm         -- How to use h2o's glm with N-Fold Cross Validation
#'   `[`             -- How to slice a dataset using factors (takes place in the h2o.glm call)
#'   other           -- How to mix lapply, ldply, and other R-specific functionality to produce interesting results that _will_ scale



# Some H2O-specifc R-Unit Header Boilerplate. You may ignore this
#################################################################################
setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))          #   
source('../findNSourceUtils.R')                                                 #   
options(echo=TRUE)                                                              #   
heading("BEGIN TEST")                                                           #   
h <- new("H2OClient", ip=myIP, port=myPort)                                     #   
#################################################################################
library(h2o)
library(plyr)
#
##
### Begin the Demo 
##
#


# Let's fit logistic regressions to each subgroup of the airlines data for the `origin` variable
# Read in the data
# Path is relative to the location that I started h2o (i.e. which dir did I java -jar in?)
flights <- h2o.importFile(h, "../../../smalldata/airlines/allyears2k_headers.zip", "flights.hex")

#################################################################################
#
# Columns of the flights data
#
#colnames(flights)
# [1] "Year"              "Month"             "DayofMonth"       
# [4] "DayOfWeek"         "DepTime"           "CRSDepTime"       
# [7] "ArrTime"           "CRSArrTime"        "UniqueCarrier"    
#[10] "FlightNum"         "TailNum"           "ActualElapsedTime"
#[13] "CRSElapsedTime"    "AirTime"           "ArrDelay"         
#[16] "DepDelay"          "Origin"            "Dest"             
#[19] "Distance"          "TaxiIn"            "TaxiOut"          
#[22] "Cancelled"         "CancellationCode"  "Diverted"         
#[25] "CarrierDelay"      "WeatherDelay"      "NASDelay"         
#[28] "SecurityDelay"     "LateAircraftDelay" "IsArrDelayed"     
#[31] "IsDepDelayed"     
#################################################################################
vars <- colnames(flights)

# Suggested Explanatory Variables:
FlightDate     <- vars[1:4]        # "Year", "Month", "DayofMonth", "DayOfWeek"
ScheduledTimes <- vars[c(6,8,13)]  # "CRSDepTime", "CRSArrTime", "CRSElapsedTime"
FlightInfo     <- vars[c(9,18,19)] # "UniqueCarrier", "Dest", "Distance"

# Response
Delayed        <- vars[31]         # "IsDepDelayed"

#take a peek at the number of unique origin airport codes
counts.by.origincode <- h2o::ddply(flights, "Origin", nrow)
dim(counts.by.origincode)  # it's not so big so we can pull it down into R
#[1] 132   2

cnts <- as.data.frame(counts.by.origincode)
ordered.cnts <- cnts[with(cnts, order(-C1)),]  # ORDER BY count DESC

# Choose the 30 most frequently used airports to fly out from
frequent.origin.codes <- as.character(ordered.cnts$Origin[1:30])

# Fit logistic regression for IsDepDelayed for some origin
lr.fit <-
function(origin, dataset) {
  data <- dataset[dataset$Origin == origin,]
  t0 <- Sys.time()
  model <- h2o.glm(x = c(FlightDate, ScheduledTimes, FlightInfo), y = Delayed, data = dataset, family = "binomial", nfolds = 10)
  elapsed_seconds <- as.numeric(Sys.time() - t0)
  modelkey <- model@key
  result <- list(list(model, origin, elapsed_seconds))
  names(result) <- model@key
  return(result)
}

# Build 300 GLM models (30 airpirt codes X 10-Fold XVal for each)
models.by.airport.origin      <- lapply(frequent.origin.codes, lr.fit, flights)

##
## Now display the results in a frame sorted by AUC
##

# First unlist the outer list compiled by the lapply call
models.by.airport.origin      <- unlist(recursive = F, models.by.airport.origin)

# Use ldply to iterate over the list of models, extracting the model key, the model auc, the origin code, and the elapsed training time in seconds
models.auc.origin.frame       <- ldply(models.by.airport.origin, function(x) { c(model_key = x[[1]]@key, auc = as.numeric(x[[1]]@model$auc), origin = x[[2]], train_time = as.numeric(x[[3]])) })

# sort the models by auc from worst to best
models.sort.by.auc            <- models.auc.origin.frame[with(models.auc.origin.frame, order(auc)),-1]

# convert the `auc` and `train_time` columns into numerics
models.sort.by.auc$auc        <- as.numeric(models.sort.by.auc$auc)
models.sort.by.auc$train_time <- as.numeric(models.sort.by.auc$train_time)

# display the frame
models.sort.by.auc

# Total model building time
total_time <- sum(models.sort.by.auc$train_time)
cat("Built 300 GLM models in ", total_time, " seconds.", '\n')

#
##
### End of Demo
##
#
