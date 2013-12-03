source('./findNSourceUtils.R')

Log.info("======================== Begin Test ===========================\n")

grabRemote <- function(myURL, myFile) {
  temp <- tempfile()
  download.file(myURL, temp, method = "curl")
  aap.file <- read.csv(file = unz(description = temp, filename = myFile), as.is = TRUE)
  unlink(temp)
  return(aap.file)
}

Log.info("==============================")
Log.info("H2O GBM Params: ")
Log.info("x = 3:13")
Log.info("y = Angaus")
Log.info("data = ecology.hex,")
Log.info("n.trees = 100") 
Log.info("interaction.depth = 5")
Log.info("n.minobsinnode = 10") 
Log.info("shrinkage = 0.1")
Log.info("==============================")
Log.info("==============================")
Log.info("R GBM Params: ")
Log.info("Formula: Angaus ~ ., data = ecology.data[,-1]")
Log.info("distribution =  gaussian")
Log.info("ntrees = 100")
Log.info("interaction.depth = 5")
Log.info("n.minobsinnode = 10")
Log.info("shrinkage = 0.1")
Log.info("bag.fraction = 1")
Log.info("==============================")
n.trees <- 100
interaction.depth <- 5
n.minobsinnode <- 10
shrinkage <- 1

checkGBMModel <- function(myGBM.h2o, myGBM.r,conn) {
  # Check GBM model against R
  Log.info("MSE by tree in H2O:")
  print(myGBM.h2o@model$err)
  expect_true(length(myGBM.h2o@model$err) == n.trees) #100 is ntrees
  Log.info("Gaussian Deviance by tree in R (i.e. the per tree 'train error'): \n")
  print(myGBM.r$train.error)
  Log.info("Expect these to be close... mean of the absolute differences is < .5, and sd < 0.1")
  errDiff <- abs(myGBM.r$train.error - myGBM.h2o@model$err)
  Log.info(cat("Mean of the absolute difference is: ", mean(errDiff)))
  Log.info(cat("Standard Deviation of the absolute difference is: ", sd(errDiff)))
  expect_true(mean(errDiff) < 0.5)
  expect_true(sd(errDiff) < 0.1)
 
  #TODO(spencer): checkGBMModel should be a general fcn
 
  # Compare GBM models on out-of-sample data
  Log.info("Uploading ecology testing data...\n")
  ecologyTest.hex <- h2o.uploadFile(conn, locate("smalldata/gbm_test/ecology_eval.csv"))
  ecologyTest.data <- read.csv(locate("smalldata/gbm_test/ecology_eval.csv"))
  actual <- ecologyTest.data[,1]
  Log.info("Performing the predictions on h2o GBM model: ")
  #TODO: Building CM in R instead of in H2O
  h2ogbm.predict <- h2o.predict(myGBM.h2o, ecologyTest.hex)
  h2o.preds <- head(h2ogbm.predict,nrow(h2ogbm.predict))[,1]
  h2oCM <- table(actual,h2o.preds)
  Log.info("H2O CM is: \n")
  print(h2oCM)
  Log.info("Performing the predictions of R GBM model: ")
  R.preds <- ifelse(predict.gbm(myGBM.r, ecologyTest.data,n.trees=100,type="response") < 0.5, 0,1)
  Log.info("R CM is: \n")
  RCM <- table(actual,R.preds)
  print(RCM)
  Log.info("Compare AUC from R and H2O:\n")
  Log.info("H2O AUC: ")
  print(gbm.roc.area(actual,h2o.preds))
  Log.info("R AUC: ")
  print(gbm.roc.area(actual,R.preds))
}

test.GBM.ecology <- function(conn) {
  Log.info("Importing ecology_model.csv data...\n")
  print("=============================")
  print(locate("smalldata/gbm_test/ecology_model.csv"))
  print("=============================")
  ecology.hex <- h2o.uploadFile(conn, locate("smalldata/gbm_test/ecology_model.csv", schema="put"))
  ecology.sum <- summary(ecology.hex)
  Log.info("Summary of the ecology data from h2o: \n") 
  print(ecology.sum)
  
  #import csv data for R to use
  ecology.data <- read.csv(locate("smalldata/gbm_test/ecology_model.csv"), header = TRUE)
  ecology.data <- na.omit(ecology.data) #this omits NAs... does GBM do this? Perhaps better to model w/o doing this?
  
  Log.info("H2O GBM with parameters:\nntrees = 100, max_depth = 5, min_rows = 10, learn_rate = 0.1\n")
  #Train H2O GBM Model:
  ecology.h2o <- h2o.gbm(x = 3:13, 
                        y = "Angaus", 
                     data = ecology.hex, 
                  n.trees = 100, 
        interaction.depth = 5, 
           n.minobsinnode = 10, 
                shrinkage = 0.1)

  print(ecology.h2o)
  
  #Train R GBM Model: Using Gaussian loss function for binary outcome OK... Also more comparable to H2O, which uses MSE
  ecology.r <- gbm(Angaus ~ ., data = ecology.data[,-1], distribution = "gaussian", 
                  n.trees = 100,
                  interaction.depth = 5, 
                  n.minobsinnode = 10, 
                  shrinkage = 0.1,
                  bag.fraction=1)

  checkGBMModel(ecology.h2o, ecology.r, conn)
  Log.info("End of test.")
  PASSS <<- TRUE
}

PASSS <- FALSE
conn <- new("H2OClient", ip=myIP, port=myPort)
tryCatch(test_that("GBM Test: Ecology", test.GBM.ecology(conn)), warning = function(w) WARN(w), error = function(e) FAIL(e))
if (!PASSS) FAIL("Did not reach the end of test. Check Rsandbox/errors.log for warnings and errors.")
PASS()
