#'
#' An H2O Demo: Building Many Models, Validating, and Scoring
#'

#
##
### Begin the Demo
##
#
library(h2o)
library(plyr)

# Assumes h2o started elsewhere
ip <- "127.0.0.1"
port <- 54321
h <- h2o.init(ip = ip, port = port)


# **PATHS**

# For 181:
#AIRLINES_ALL_PATH <- "/home/0xdiag/datasets/airlines/airlines_all.csv"
#EXPORT_PREDS_PATH <- "/home/spencer/pp_demo/preds.csv"

# For local git:
#AIRLINES_ALL_PATH <- "/Users/spencer/master/h2o/smalldata/airlines/allyears2k_headers.zip"
#EXPORT_PREDS_PATH <- "/Users/spencer/pp_demo/preds.csv"


# Read in the data
flights <- h2o.importFile(h, AIRLINES_ALL_PATH, "flights")

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

# Combine the explanatory variables into a single variable
FlightsVars <- c(FlightDate, ScheduledTimes, FlightInfo)

# Response
Delayed        <- vars[31]         # "IsDepDelayed"
ArrivalDelayed <- vars[30]         # "IsArrDelayed"

############################################################################################

# Split the flights data into train/validation/test splits of (60/10/30)

splits <- h2o.splitFrame(flights, ratios = c(.6,.1), shuffle = TRUE)
train  <- splits[[1]]
valid  <- splits[[2]]
test   <- splits[[3]]

cat("\nTRAINING ROWS: ", nrow(train))
cat("\nVALIDATION ROWS: ", nrow(valid))
cat("\nTEST ROWS: ", nrow(test))

# Here's a function that takes a model and a testing dataset and calculates the AUC on the testdata...
test_performance <-
function(model, testdata, response) {
  preds <- h2o.predict(model, testdata)

  # p(success)  is the last column in the frame returned by h2o.predict, that's what the ncol(preds) is for below
  perf  <- h2o.performance(data = preds[, ncol(preds)], reference = testdata[, response])
  perf@model$auc
}

coda<-
function(t0, model, testdata, response) {
  elapsed_seconds <- as.numeric(Sys.time() - t0)
  modelkey <- model@key
  #perform the holdout computation
  test_auc <- test_performance(model, testdata, response)

  result <- list(list(model, response, elapsed_seconds, test_auc))
  names(result) <- model@key
  return(result)
}

# Fit logistic regression for IsDepDelayed for some origin
lr.fit<-
function(response, dataset, testdata) {
  print("Beginning GLM with 2-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.glm(x = FlightsVars, y = response, data = dataset, family = "binomial", nfolds = 2)
  coda(t0, model, testdata, response)
}

rf.fit<-
function(response, dataset, testdata) {
  print("Beginning Random Forest with 10 trees, 20 depth, and 2-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.randomForest(x = FlightsVars, y = response, data = dataset, ntree = 10, depth = 20, nfolds = 2, balance.classes = T, type = "BigData")
  coda(t0, model, testdata, response)
}

srf.fit<-
function(response, dataset, testdata) {
  print("Beginning Speedy Random Forest with 10 trees, 20 depth, and 2-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.randomForest(x = FlightsVars, y = response, data = dataset, ntree = 10, depth = 20, nfolds = 2, balance.classes = T, type = "fast")
  coda(t0, model, testdata, response)
}

gbm.fit<-
function(response, dataset, testdata) {
  print("Beginning Gradient Boosted Machine with 50 trees, 5 depth, and 2-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.gbm(x = FlightsVars, y = response, data = dataset, n.trees = 50, shrinkage = 1/50, nfolds = 2, balance.classes = T)
  coda(t0, model, testdata, response)
}

dl.fit<-
function(response, dataset, testdata) {
  print("Beginning Deep Learning with 3 hidden layers and 2-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.deeplearning(x = FlightsVars, y = response, data = dataset,
                            hidden = c(200,200,200),
                            activation = "RectifierWithDropout",
                            input_dropout_ratio = 0.2,
                            l1 = 1e-5,
                            train_samples_per_iteration = 10000,
                            epochs = 100,
                            nfolds = 2,
                            balance_classes = T)
  coda(t0, model, testdata, response)
}

all.fit <- function(fitMethod, response, dataset, testdata) { fitMethod(response, dataset, testdata) }

#iterate over the fit fcns
model.fit.fcns <- c(lr.fit, rf.fit, srf.fit, gbm.fit)#, dl.fit)

models <- unlist(recursive = F, lapply(model.fit.fcns, all.fit, Delayed, train, valid))

##
### Now display the results in a frame sorted by AUC
##

# Use ldply to iterate over the list of models, extracting the model key, the model auc, the response, and the elapsed training time in seconds
models.auc.response.frame <- ldply(models, function(x) {
                                  c(model_key = x[[1]]@key,
                                    train_auc = as.numeric(x[[1]]@model$auc),
                                    validation_auc = as.numeric(x[[4]]),
                                    response = x[[2]],
                                    train_time = as.numeric(x[[3]]))})

# sort the models by auc from worst to best
models.sort.by.auc <- models.auc.response.frame[with(models.auc.response.frame, order(response, validation_auc)),-1]
models.sort.by.auc <- models.sort.by.auc[rev(rownames(models.sort.by.auc)),]

# convert the `auc` and `train_time` columns into numerics
models.sort.by.auc$train_auc       <- as.numeric(models.sort.by.auc$train_auc)
models.sort.by.auc$validation_auc  <- as.numeric(models.sort.by.auc$validation_auc)
models.sort.by.auc$train_time      <- as.numeric(models.sort.by.auc$train_time)

# display the frame
print(models.sort.by.auc)

# score the best model on the test data
best_model <- h2o.getModel(h, models.sort.by.auc[1,1])
test_auc <- test_performance(best_model, test, Delayed)
cat("\n-------------------------------\n")
cat("\n Best Model Performance On Final Testing Data:\n")
cat("\n AUC = ", test_auc, "\n")
cat("\n-------------------------------\n")

# save the predictions
preds <- h2o.predict(best_model, test)
h2o.exportFile(preds, EXPORT_PREDS_PATH, force = TRUE)

# save the model
h2o.saveModel(best_model, dir = "/home/spencer/pp_demo/", name = "best_model", force = TRUE)

# load model: h2o.loadModel(h, PATH_TO_SAVED_MODEL)

#
##
### End of Demo
##
#
