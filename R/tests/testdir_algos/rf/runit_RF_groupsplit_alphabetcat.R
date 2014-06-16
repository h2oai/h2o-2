setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')
library(randomForest)

test.DRF.smallcat <- function(conn) {
  # Training set has 26 categories from A to Z
  Log.info("Importing alphabet_cattest.csv data...\n")
  alphabet.hex <- h2o.uploadFile(conn, locate("smalldata/histogram_test/alphabet_cattest.csv"), key = "alphabet.hex")
  alphabet.hex$y <- as.factor(alphabet.hex$y)
  Log.info("Summary of alphabet_cattest.csv from H2O:\n")
  print(summary(alphabet.hex))
  
  # Import CSV data for R to use in comparison
  alphabet.data <- read.csv(locate("smalldata/histogram_test/alphabet_cattest.csv"), header = TRUE)
  alphabet.data$y <- as.factor(alphabet.data$y)
  Log.info("Summary of alphabet_cattest.csv from R:\n")
  print(summary(alphabet.data))
  
  # Train H2O DRF Model:
  Log.info("H2O DRF with parameters:\nclassification = TRUE, ntree = 50, depth = 20, nbins = 500\n")
  drfmodel.h2o <- h2o.randomForest(x = c("x1", "x2"), y = "y", data = alphabet.hex, classification = TRUE, ntree = 50, depth = 20, nbins = 100)
  
  # Train R DRF Model:
  Log.info("R DRF with same parameters:")
  drfmodel.r <- randomForest(y ~ ., data = alphabet.data, ntree = 50, nodesize = 1)
  drfmodel.r.pred <- predict(drfmodel.r, alphabet.data, type = "response")
  
  # Compute confusion matrices
  Log.info("R Confusion Matrix:")
  print(drfmodel.r$confusion)
  Log.info("H2O Confusion Matrix:")
  print(drfmodel.h2o@model$confusion)
  
  # Compute the AUC - need to convert factors back to numeric
  actual <- ifelse(alphabet.data$y == "0", 0, 1)
  pred <- ifelse(drfmodel.r.pred == "0", 0, 1)
  R.auc = gbm.roc.area(actual, pred)
  Log.info(paste("R AUC:", R.auc, "\tH2O AUC:", drfmodel.h2o@model$auc))
  testEnd()
}

doTest("DRF Test: Classification with 26 categorical level predictor", test.DRF.smallcat)