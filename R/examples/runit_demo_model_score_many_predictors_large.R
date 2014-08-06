#'
#' An H2O Demo: Building Many Models And Scoring With Validation
#'
#'
#' New in this demo:
#'
#'  This demo covers additional workflows when the dataset contains several binary predictors
#'  and we want to build models for each one. As always, these demos strive to stress the
#'  flexibility and conciseness of the R language.
#'  
#'
#'
#' This demo assumes coverage of the related demos: runit_demo_model_by_group.R, runit_demo_model_by_group_extended.R
#'
#' What this demo covers:
#'  h2o.splitFrame        --How to split a data frame into train/test splits.
#'  h2o.performance       --Useful for binary responses: computes the AUC and best thresholds for a variety of quality metrics
#'  get priors            --How to get the priors from the model: @model$priorDistribution
#'  h2o.exec              --Execute an arbitrary R-like expresion in the H2O cloud (expression must contain an H2OParsedData object).
#'  invisible             --An interesting & useful base package function. See ?invisible for more
#'  write.csv             --Store all of the models into a csv grouped by the different responses and sorted by the AUC on testing data

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


# Read in the data
flights <- h2o.importFile(h, normalizePath("../../../smalldata/airlines/allyears2k_headers.zip"), "flights.hex")

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
ArrivalDelayed <- vars[30]         # "IsArrDelayed"

############################################################################################
#'   Make additional columns to score on using h2o.exec
#'
#'    h2o.exec is used to 
#'      1. speed up computation and; 
#'      2. improve memory efficiency (does not create so many Last.value.* temporary values

# Make a new column of 1s and 0s from Delayed variable
h2o.exec(flights[,"newResponse_1_0"] <- ifelse(flights[, "IsDepDelayed"] == "YES", 1, 0))

# Make a new column of 2s and 3s from Delayed variable
h2o.exec(flights$newResponse2_3 <- ifelse(flights$IsDepDelayed == "YES", 3, 2))

# Make a new column of `0` and `-1` from Delayed variable
h2o.exec(flights[, "newResponse_0_neg1"] <- ifelse(flights[,Delayed] == "YES", 0, -1))

# OK too slow to do this by hand: use lapply! Here's where `invisible` is used:
f<-
function(i, hex) {
  ints <- sample(1000, 2, replace=F)
  pos_value <- ints[1]
  neg_value <- ints[2]
  newColName <- paste("newResponse", i, sep = "")
  invisible(h2o.exec(hex[, newColName] <- ifelse(hex[,Delayed] == "YES", pos_value, neg_value)))
}

# Make 3 more "fake" response columns
invisible(lapply(1:3, f, flights))

vars <- colnames(flights)
#################################################################################
#
# Columns of the flights data
#
#colnames(flights)
# [1] "Year"               "Month"              "DayofMonth"        
# [4] "DayOfWeek"          "DepTime"            "CRSDepTime"        
# [7] "ArrTime"            "CRSArrTime"         "UniqueCarrier"     
#[10] "FlightNum"          "TailNum"            "ActualElapsedTime" 
#[13] "CRSElapsedTime"     "AirTime"            "ArrDelay"          
#[16] "DepDelay"           "Origin"             "Dest"              
#[19] "Distance"           "TaxiIn"             "TaxiOut"           
#[22] "Cancelled"          "CancellationCode"   "Diverted"          
#[25] "CarrierDelay"       "WeatherDelay"       "NASDelay"          
#[28] "SecurityDelay"      "LateAircraftDelay"  "IsArrDelayed"      
#[31] "IsDepDelayed"       "newResponse_1_0"    "newResponse2_3"    
#[34] "newResponse_0_neg1" "newResponse1"       "newResponse2"      
#[37] "newResponse3"     
############################################################################################

#factorize the new response columns
factorize<-
function(tgt, data) { 
  h2o.exec(data[,tgt] <- factor(data[,tgt])) 
}

tgts <- vars[32:37]
invisible(lapply(tgts, factorize, flights))

# Show the types of each column
str(flights)

# Finally lets split the flights data into train/test splits (80/20)

split.keys <- h2o.splitFrame(flights, ratios = .80, shuffle = FALSE)
train <- split.keys[[1]]
test  <- split.keys[[2]]

# Here's a function that takes a model and a testing dataset and calculates the AUC on the testdata...
test_performance <-
function(model, testdata, response) {
  preds <- h2o.predict(model, testdata)
  
  # p(success)  is the last column in the frame returned by h2o.predict, that's what the ncol(preds) is for below
  perf  <- h2o.performance(data = preds[, ncol(preds)], reference = testdata[, response])
  perf@model$auc
}

# Fit logistic regression for IsDepDelayed for some origin
lr.fit<-
function(response, dataset, testdata) {
  print("Beginning GLM with 2-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.glm(x = c(FlightDate, ScheduledTimes, FlightInfo), y = response, data = dataset, family = "binomial", nfolds = 2)
  elapsed_seconds <- as.numeric(Sys.time() - t0)
  modelkey <- model@key

  #perform the holdout computation  
  test_auc <- test_performance(model, testdata, response)

  priors <- c(NA, NA) # GLM doesn't compute the prior and model distributions. It has no option to 'balance classes'
  result <- list(list(model, response, elapsed_seconds, test_auc, priors))
  names(result) <- model@key
  return(result)
}

rf.fit<-
function(response, dataset, testdata) {
  print("Beginning Random Forest with 10 trees, 20 depth, and 2-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.randomForest(x = c(FlightDate, ScheduledTimes, FlightInfo), y = response, data = dataset, ntree = 10, depth = 20, nfolds = 2, balance.classes = T, type = "BigData") 
  elapsed_seconds <- as.numeric(Sys.time() - t0) 
  modelkey <- model@key
 
  #perform the holdout computation  
  test_auc <- test_performance(model, testdata, response)

  priors <- model@model$priorDistribution  # Can obtain the model distributions with model@model$modelDistribution
  result <- list(list(model, response, elapsed_seconds, test_auc, priors)) 
  names(result) <- model@key
  return(result)
}

srf.fit<-
function(response, dataset, testdata) {
  print("Beginning Speedy Random Forest with 10 trees, 20 depth, and 2-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.randomForest(x = c(FlightDate, ScheduledTimes, FlightInfo), y = response, data = dataset, ntree = 10, depth = 20, nfolds = 2, balance.classes = T, type = "fast")
  elapsed_seconds <- as.numeric(Sys.time() - t0) 
  modelkey <- model@key
 
  #perform the holdout computation  
  test_auc <- test_performance(model, testdata, response)

  priors <- model@model$priorDistribution
  result <- list(list(model, response, elapsed_seconds, test_auc, priors)) 
  names(result) <- model@key
  return(result)
}

gbm.fit<-
function(response, dataset, testdata) {
  print("Beginning Gradient Boosted Machine with 50 trees, 5 depth, and 2-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.gbm(x = c(FlightDate, ScheduledTimes, FlightInfo), y = response, data = dataset, n.trees = 50, shrinkage = 1/50, nfolds = 2, balance.classes = T) 
  elapsed_seconds <- as.numeric(Sys.time() - t0) 
  modelkey <- model@key
 
  #perform the holdout computation  
  test_auc <- test_performance(model, testdata, response)

  priors <- model@model$priorDistribution
  result <- list(list(model, response, elapsed_seconds, test_auc, priors)) 
  names(result) <- model@key
  return(result)
}

dl.fit<-
function(response, dataset, testdata) {
  print("Beginning Deep Learning with 3 hidden layers and 2-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.deeplearning(x = c(FlightDate, ScheduledTimes, FlightInfo), y = response, data = dataset, hidden = c(200,200,200), activation = "RectifierWithDropout", input_dropout_ratio = 0.2, l1 = 1e-5, train_samples_per_iteration = 10000, epochs = 100, nfolds = 2, balance_classes = T)
  elapsed_seconds <- as.numeric(Sys.time() - t0)
  modelkey <- model@key

  #perform the holdout computation  
  test_auc <- test_performance(model, testdata, response)

  priors <- model@model$priorDistribution
  result <- list(list(model, response, elapsed_seconds, test_auc, priors))
  names(result) <- model@key
  return(result)
}

all.fit<-
function(fitMethod, responses, dataset, testdata) {
  unlist(recursive = F, lapply(responses, fitMethod, dataset, testdata))
}

#iterate over the fit fcns as well as the tgts
model.fit.fcns <- c(lr.fit, rf.fit, srf.fit, gbm.fit)#, dl.fit)

# This will loop over all of the models and score for each of the responses in tgts
models.by.tgt <- unlist(recursive = F, lapply(model.fit.fcns, all.fit, tgts, train, test))

##
## Now display the results in a frame sorted by AUC
##

# Use ldply to iterate over the list of models, extracting the model key, the model auc, the response, and the elapsed training time in seconds
models.auc.response.frame <- ldply(models.by.tgt, function(x) { 
                                  c(model_key = x[[1]]@key, 
                                    train_auc = as.numeric(x[[1]]@model$auc), 
                                    test_auc = as.numeric(x[[4]]), 
                                    response = x[[2]], 
                                    train_time = as.numeric(x[[3]]),
                                    prior_neg_class = x[[5]][1], prior_pos_class = x[[5]][2]) })

# sort the models by auc from worst to best
models.sort.by.auc <- models.auc.response.frame[with(models.auc.response.frame, order(response, test_auc)),-1]

# convert the `auc` and `train_time` columns into numerics
models.sort.by.auc$train_auc       <- as.numeric(models.sort.by.auc$train_auc)
models.sort.by.auc$test_auc        <- as.numeric(models.sort.by.auc$test_auc)
models.sort.by.auc$train_time      <- as.numeric(models.sort.by.auc$train_time)
models.sort.by.auc$prior_pos_class <- as.numeric(models.sort.by.auc$prior_pos_class)
models.sort.by.auc$prior_neg_class <- as.numeric(models.sort.by.auc$prior_neg_class)

# display the frame
models.sort.by.auc

# Total model building time
total_time <- sum(models.sort.by.auc$train_time)
cat("Built all models in ", total_time, " seconds.", '\n')

# Store the results to the local directory
write.csv(models.sort.by.auc, "models_sort_by_auc.csv", row.names=F)

#
##
### End of Demo
##
#
