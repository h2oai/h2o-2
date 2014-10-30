#'
#' An H2O Demo: Building Many Models, Validating, and Scoring
#'


#######################
# Scraps & Extras:
#
# h2o.exportFile(preds, EXPORT_PREDS_PATH, force = TRUE)
#
# save the model
# h2o.saveModel(best_model, dir = "/home/spencer/pp_demo/", name = "best_model", force = TRUE)
#
# load model: h2o.loadModel(h, PATH_TO_SAVED_MODEL)
#
# **PATHS**
#
# For 181:
# AIRLINES_ALL_PATH <- "/home/0xdiag/datasets/airlines/airlines_all.csv"
# EXPORT_PREDS_PATH <- "/home/spencer/pp_demo/preds.csv"
# For local git:
AIRLINES_ALL_PATH <-"https://github.com/0xdata/h2o/blob/master/smalldata/airlines/allyears2k_headers.zip?raw=true"
# EXPORT_PREDS_PATH <- "/Users/spencer/pp_demo/preds.csv"
# library(plyr)
#######################

#
##
### Begin the Demo
##
#
library(h2o)

# Assumes h2o started elsewhere
ip <- "127.0.0.1"
port <- 54321
h <- h2o.init(ip = ip, port = port)

#################################################
##### IMPORTANT VARIABLES TO SET HERE !!!! ######
#################################################
#AIRLINES_ALL_PATH <- ""   # set this to the path to the airlines dataset
NUM_FEATURES <- 9        # set this to toggle the number of features to collect
#################################################
#################################################

if (AIRLINES_ALL_PATH == "") stop("AIRLINES_ALL_PATH must be set")
if (NUM_FEATURES <= 0) stop("NUM_FEATURES must be > 0")

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
s <- h2o.runif(flights, seed = 123456789)
train <- flights[s < .6,]
valid <- flights[s >= .6 & s < .7,]
test  <- flights[s >= .7,]

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
function(t0, model, modeltype, testdata, response, top4) {
  elapsed_seconds <- as.numeric(Sys.time() - t0)
  modelkey <- model@key
  type <- modeltype
  #perform the holdout computation
  test_auc <- test_performance(model, testdata, response)

  result <- list(list(model, modeltype, response, elapsed_seconds, test_auc, top4))
  names(result) <- model@key
  return(result)
}

# Fit logistic regression for IsDepDelayed for some origin
lr.fit<-
function(response, dataset, testdata) {
  print("Beginning GLM with 2-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.glm(x = FlightsVars, y = response, data = dataset, family = "binomial", nfolds = 2, variable_importances = TRUE)
  top4 <- paste(names(sort(model@model$coefficients, T))[1:NUM_FEATURES], collapse = ",", sep = ",")
  coda(t0, model, "glm", testdata, response, top4)
}

rf.fit<-
function(response, dataset, testdata) {
  print("Beginning Random Forest with 10 trees, 20 depth, and 2-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.randomForest(x = FlightsVars, y = response, data = dataset, ntree = 10, depth = 20, nfolds = 2, balance.classes = T, type = "BigData", importance = TRUE)
  top4 <- paste(names(sort(model@model$varimp[1,]))[1:NUM_FEATURES], collapse = ",", sep = ",")
  coda(t0, model, "BigData_random_forest", testdata, response, top4)
}

srf.fit<-
function(response, dataset, testdata) {
  print("Beginning Speedy Random Forest with 10 trees, 20 depth, and 2-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.randomForest(x = FlightsVars, y = response, data = dataset, ntree = 10, depth = 20, nfolds = 2, balance.classes = T, type = "fast", importance = TRUE)
  top4 <- paste(names(sort(model@model$varimp[1,]))[1:NUM_FEATURES], collapse = ",", sep = ",")
  coda(t0, model, "fast_random_forest", testdata, response, top4)
}

gbm.fit<-
function(response, dataset, testdata) {
  print("Beginning Gradient Boosted Machine with 50 trees, 5 depth, and 2-fold Cross Validation\n")
  t0 <- Sys.time()
  model <- h2o.gbm(x = FlightsVars, y = response, data = dataset, n.trees = 50, shrinkage = 1/50, nfolds = 2, balance.classes = T, importance = TRUE)
  top4 <- paste(rownames(model@model$varimp)[1:NUM_FEATURES], collapse = ",", sep = ",")
  coda(t0, model, "gbm", testdata, response, top4)
}

#dl.fit<-
#function(response, dataset, testdata) {
#  print("Beginning Deep Learning with 3 hidden layers and 2-fold Cross Validation\n")
#  t0 <- Sys.time()
#  model <- h2o.deeplearning(x = FlightsVars, y = response, data = dataset,
#                            hidden = c(200,200,200),
#                            activation = "RectifierWithDropout",
#                            input_dropout_ratio = 0.2,
#                            l1 = 1e-5,
#                            train_samples_per_iteration = 10000,
#                            epochs = 100,
#                            nfolds = 2,
#                            balance_classes = T, importance = TRUE)
#  coda(t0, model, "deeplearning", testdata, response)
#}

all.fit <- function(fitMethod, response, dataset, testdata) { fitMethod(response, dataset, testdata) }

#iterate over the fit fcns
model.fit.fcns <- c(lr.fit, rf.fit, srf.fit, gbm.fit)#, dl.fit)

models <- unlist(recursive = F, lapply(model.fit.fcns, all.fit, Delayed, train, valid))

##
### Now display the results in a frame sorted by AUC
##

# Use ldply to iterate over the list of models, extracting the model key, the model auc, the response, and the elapsed training time in seconds
#models.auc.response.frame <- ldply(models, function(x) {
#                                  c(model_key = x[[1]]@key,
#                                    model_type = x[[2]],
#                                    train_auc = as.numeric(x[[1]]@model$auc),
#                                    validation_auc = as.numeric(x[[5]]),
#                                    important_feat = x[[6]],
#                                    response = x[[3]],
#                                    train_time = as.numeric(x[[4]]))})

#Alternative to ldply:

selectModel <- function(x) {
  c(model_key = x[[1]]@key,
  model_type = x[[2]],
  train_auc = as.numeric(x[[1]]@model$auc),
  validation_auc = as.numeric(x[[5]]),
  important_feat = x[[6]],
  response = x[[3]],
  train_time_s = as.numeric(as.character(x[[4]])))
}

models.auc.response.frame <- as.data.frame(t(as.data.frame(lapply(models, selectModel))))
#t(lapply(models.auc.response.frame, class))

models.auc.response.frame$train_auc <- as.numeric(as.character(models.auc.response.frame$train_auc))
models.auc.response.frame$validation_auc <- as.numeric(as.character(models.auc.response.frame$validation_auc))

# sort the models by auc from worst to best
models.sort.by.auc <- models.auc.response.frame[with(models.auc.response.frame, order(response, validation_auc)),-1]
models.sort.by.auc <- models.sort.by.auc[rev(rownames(models.sort.by.auc)),]

# convert the `auc` and `train_time` columns into numerics
models.sort.by.auc$train_auc       <- as.numeric(as.character(models.sort.by.auc$train_auc))
models.sort.by.auc$validation_auc  <- as.numeric(as.character(models.sort.by.auc$validation_auc))
models.sort.by.auc$train_time      <- as.numeric(as.character(models.sort.by.auc$train_time))

# display the frame
print(models.sort.by.auc)

# score the best model on the test data
best_model <- h2o.getModel(h, rownames(models.sort.by.auc)[1])
test_auc <- test_performance(best_model, test, Delayed)  # Swap out test to any datset to do the final scoring on.
cat(paste(" -------------------------------\n",
          "Best Model Performance On Final Testing Data:", "\n",
          "AUC = ", test_auc, "\n",
          "--------------------------------\n"))

# save the predictions
preds <- h2o.predict(best_model, test)


cat(paste(" =---------Summary------------=\n",
            "Best model type: ", models.sort.by.auc[1,]$model_type, "\n",
            "Best model auc on test: ", test_auc, "\n",
            "Top", NUM_FEATURES, "important features: ", models.sort.by.auc[1,]$important_feat, "\n",
            "Model training time: ", models.sort.by.auc[1,]$train_time_s, "\n",
            "Training data rows: ", nrow(train), "\n",
            "Training data cols: ", ncol(train), "\n",
            "Validation data rows: ", nrow(valid), "\n",
           "=----------------------------=\n"))

#
##
### End of Demo
##
#
