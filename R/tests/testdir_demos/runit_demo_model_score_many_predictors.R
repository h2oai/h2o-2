#'
#' An H2O Demo: Building Many Models By Group And Scoring With Validation
#'
#'
#' New in this demo:
#'
#'  This demo covers additional workflows when the dataset contains several binary predictors
#'  and we want to build models for each one. As always, these demos strive to stress the
#'  flexibility and conciseness of the R language.
#'  
#' This demo walks through a smalldata case where a data scientist wants to build a model based
#' on groupings of her data. We use flight record data in modeling against departure delays by 
#' airport origin code.
#'
#'
#' This demo assumes coverage of the related demos: runit_demo_model_by_group.R, runit_demo_model_by_group_extended2.R
#'
#' What this demo covers:
#'  h2o.performance       --Useful for binary responses: computes the AUC and best thresholds for a variety of quality metrics
#'  
#' 
#'  Additionally, we demonstrate best practices in storing model quality information and other meta data.

# Some H2O-specifc R-Unit Header Boilerplate. You may ignore this################
#                                                                               #
setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))          #   
source('../findNSourceUtils.R')                                                 #   
options(echo=TRUE)                                                              #   
heading("BEGIN TEST")                                                           #   
h <- new("H2OClient", ip=myIP, port=myPort)                                     #
#################################################################################
library(h2o)

#
##
### Begin the Demo
##
#


# Read in the data
# This dataset has a number of "fake" predictors
#   They are simply the IsDepDelayed column copied 30 times
flights <- h2o.importFile(h, "../../../smalldata/airlines/allyears2k_many_predictors.csv", "flights.hex")

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
FlightInfo     <- vars[c(9,17,18,19)] # "UniqueCarrier", "Origin", "Dest", "Distance"

# Response
Delayed        <- vars[31]         # "IsDepDelayed"



# Fit logistic regression for IsDepDelayed for some origin
lr.fit<-
function(origin, dataset) {
  dataset <- dataset[dataset$Origin == origin,]
  print("Beginning GLM with 10-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.glm(x = c(FlightDate, ScheduledTimes, FlightInfo), y = Delayed, data = dataset, family = "binomial", nfolds = 10)
  elapsed_seconds <- as.numeric(Sys.time() - t0)
  modelkey <- model@key
  result <- list(list(model, origin, elapsed_seconds))
  names(result) <- model@key
  return(result)
}

rf.fit<-
function(origin, dataset) {
  dataset <- dataset[dataset$Origin == origin,]
  print("Beginning Random Forest with 50 trees, 20 depth, and 10-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.randomForest(x = c(FlightDate, ScheduledTimes, FlightInfo), y = Delayed, data = dataset, ntree = 50, depth = 20, nfolds = 10) 
  elapsed_seconds <- as.numeric(Sys.time() - t0) 
  modelkey <- model@key
  result <- list(list(model, origin, elapsed_seconds))
  names(result) <- model@key
  return(result)
}

srf.fit<-
function(origin, dataset) {
  dataset <- dataset[dataset$Origin == origin,]
  print("Beginning Speedy Random Forest with 50 trees, 20 depth, and 10-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.SpeeDRF(x = c(FlightDate, ScheduledTimes, FlightInfo), y = Delayed, data = dataset, ntree = 50, depth = 20, nfolds = 10) 
  elapsed_seconds <- as.numeric(Sys.time() - t0) 
  modelkey <- model@key
  result <- list(list(model, origin, elapsed_seconds))
  names(result) <- model@key
  return(result)
}

gbm.fit<-
function(origin, dataset) {
  dataset <- dataset[dataset$Origin == origin,]
  print("Beginning Gradient Boosted Machine with 100 trees, 5 depth, and 10-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.gbm(x = c(FlightDate, ScheduledTimes, FlightInfo), y = Delayed, data = dataset, n.trees = 100, shrinkage = 0.01, nfolds = 10) 
  elapsed_seconds <- as.numeric(Sys.time() - t0) 
  modelkey <- model@key
  result <- list(list(model, origin, elapsed_seconds))
  names(result) <- model@key
  return(result)
}

dl.fit<-
function(origin, dataset) {
  dataset <- dataset[dataset$Origin == origin,]
  print("Beginning Deep Learning with 3 hidden layers and 10-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.deeplearning(x = c(FlightDate, ScheduledTimes, FlightInfo), y = Delayed, data = dataset, hidden = c(200,200,200), activation = "RectifierWithDropout", input_dropout_ratio = 0.2, l1 = 1e-5, train_samples_per_iteration = 10000, epochs = 100, nfolds = 10)
  elapsed_seconds <- as.numeric(Sys.time() - t0) 
  modelkey <- model@key
  result <- list(list(model, origin, elapsed_seconds))
  names(result) <- model@key
  return(result)
}

all.fit<-
function(fitMethod, origins, dataset) {
  unlist(recursive = F, lapply(origins, fitMethod, dataset))
}

#iterate over the fit fcns as well
model.fit.fcns <- c(lr.fit, rf.fit, srf.fit, gbm.fit, dl.fit)
# See the Notes section below to get insight into the following one-liner
models.by.airport.origin <- unlist(recursive = F, lapply(model.fit.fcns, all.fit, frequent.origin.codes, flights))

##
## Now display the results in a frame sorted by AUC
##

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
cat("Built all models in ", total_time, " seconds.", '\n')


#
##
### End of Demo
##
#



# **Notes**:
#
# In order to save space here, make use of R's functional aspects:
# Transform:
#   models.by.airport.origin      <- unlist(recursive = F, lapply(frequent.origin.codes, lr.fit, flights))
#   models.by.airport.origin      <- c(models.by.airport.origin, unlist(recursive = F, lapply(frequent.origin.codes, rf.fit, flights)))
#   models.by.airport.origin      <- c(models.by.airport.origin,(models.by.airport.origin, unlist(recursive = F, lapply(frequent.origin.codes, srf.fit, flights)))
#   models.by.airport.origin      <- c(models.by.airport.origin,unlist(recursive = F, lapply(frequent.origin.codes, gbm.fit, flights)))
#   models.by.airport.origin      <- c(models.by.airport.origin, unlist(recursive = F, lapply(frequent.origin.codes, dl.fit, flights)))
# Into a one-liner:
#   models.by.airport.origin <- unlist(recursive = F, lapply(model.fit.fcns, all.fit, frequent.origin.codes, flights))
