source('./Utils/h2oR.R')

logging("\n======================== Begin Test ===========================\n")
serverH2O = new("H2OClient", ip=myIP, port=myPort)
grabRemote <- function(myURL, myFile) {
  temp <- tempfile()
  download.file(myURL, temp, method = "curl")
  aap.file <- read.csv(file = unz(description = temp, filename = myFile), as.is = TRUE)
  unlink(temp)
  return(aap.file)
}

checkGBMModel <- function(myGBM.h2o, myGBM.r,serverH2O) {
  # Check GBM model against R
  cat("\nMSE by tree in H2O:")
  print(myGBM.h2o@model$err)
  cat("\nGaussian Deviance by tree in R (i.e. the per tree 'train error'): \n")
  print(myGBM.r$train.error)
  cat("Expect these to be close... mean of the absolute differences is < .5, and sd < 0.1")
  errDiff <- abs(myGBM.r$train.error - myGBM.h2o@model$err)
  cat("\nMean of the absolute difference is: ", mean(errDiff))
  cat("\nStandard Deviation of the absolute difference is: ", sd(errDiff))
  expect_true(mean(errDiff) < 0.5)
  expect_true(sd(errDiff) < 0.1)
 
  #TODO(spencer): checkGBMModel should be a general fcn
 
  # Compare GBM models on out-of-sample data
  cat("\nUploading ecology testing data...\n")
  ecologyTest.hex <- h2o.uploadFile(serverH2O, "../../smalldata/gbm_test/ecology_eval.csv")
  ecologyTest.data <- read.csv("../../smalldata/gbm_test/ecology_eval.csv")
  actual <- ecologyTest.data[,1]
  cat("\nPerforming the predictions on h2o GBM model: ")
  #TODO: Building CM in R instead of in H2O
  h2ogbm.predict <- h2o.predict(myGBM.h2o, ecologyTest.hex)
  h2o.preds <- head(h2ogbm.predict,nrow(h2ogbm.predict))[,1]
  h2oCM <- table(actual,h2o.preds)
  cat("\nH2O CM is: \n")
  print(h2oCM)
  cat("\nPerforming the predictions of R GBM model: ")
  R.preds <- ifelse(predict.gbm(myGBM.r, ecologyTest.data,n.trees=100,type="response") < 0.5, 0,1)
  cat("\nR CM is: \n")
  RCM <- table(actual,R.preds)
  print(RCM)
  cat("\nCompare AUC from R and H2O:\n")
  cat("\nH2O AUC: ")
  print(gbm.roc.area(actual,h2o.preds))
  cat("\nR AUC: ")
  print(gbm.roc.area(actual,R.preds))
}

test.GBM.ecology <- function(serverH2O) {
  cat("\nImporting ecology_model.csv data...\n")
  ecology.hex = h2o.uploadFile(serverH2O, "../../smalldata/gbm_test/ecology_model.csv")
  ecology.sum = summary(ecology.hex)
  cat("\nSummary of the ecology data from h2o: \n") 
  print(ecology.sum)
  
  #import csv data for R to use
  ecology.data = read.csv("../../smalldata/gbm_test/ecology_model.csv", header = TRUE)
  ecology.data = na.omit(ecology.data) #this omits NAs... does GBM do this? Perhaps better to model w/o doing this?
  
  cat("\nH2O GBM with parameters:\nntrees = 100, max_depth = 5, min_rows = 10, learn_rate = 0.1\n")
  #Train H2O GBM Model:
  ecology.h2o = h2o.gbm(x = 3:13, y = "Angaus", data = ecology.hex, n.trees = 100, interaction.depth = 5, n.minobsinnode = 10, shrinkage = 0.1)
  print(ecology.h2o)
  
  #Train R GBM Model: Using Gaussian loss function for binary outcome OK... Also more comparable to H2O, which uses MSE
  ecology.r = gbm(Angaus ~ ., data = ecology.data[,-1], distribution = "gaussian", 
                  n.trees = 100,
                  interaction.depth = 5, 
                  n.minobsinnode = 10, 
                  shrinkage = 0.1,
                  bag.fraction=1)

  checkGBMModel(ecology.h2o, ecology.r, serverH2O)
}

test.GBM.airlines <- function() {
  # allyears.data = grabRemote("https://raw.github.com/0xdata/h2o/master/smalldata/airlines/allyears2k.zip", "ecology.csv")
  # allyears.data = na.omit(allyears.data)
  # allyears.data = data.frame(rapply(allyears.data, as.factor, classes = "character", how = "replace"))
  # allyears.hex = h2o.importFile(serverH2O, "../../smalldata/airlines/allyears2k.zip")
  
  # ignoreNum = sapply(ignoreFeat, function(x) { which(allXCol == x) })
  # ignoreFeat = c("CRSDepTime", "CRSArrTime", "ActualElapsedTime", "CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "TaxiIn", "TaxiOut", "Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
  # myX = setdiff(colnames(allyears.hex), c(ignoreFeat, "IsArrDelayed"))
  # allyears.h2o = h2o.gbm(x = myX, y = "IsArrDelayed", data = allyears.hex, n.trees = 100, interaction.depth = 5, n.minobsinnode = 10, shrinkage = 0.1)
  
  # allyears.x = allyears.data[,-which(colnames(allyears.data) == "IsArrDelayed")]
  # allyears.x = subset(allyears.x, select = -ignoreNum)
  # allyears.gbm = gbm.fit(y = allyears.data$IsArrDelayed, x = allyears.x, distribution = "bernoulli", n.trees = 100, interaction.depth = 5, n.minobsinnode = 10, shrinkage = 0.1)
}

test.GBM.ecology(serverH2O)
