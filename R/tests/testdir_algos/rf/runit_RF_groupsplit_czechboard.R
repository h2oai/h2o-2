setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.DRF.Czechboard <- function(conn) {
  # Training set has checkerboard pattern
  Log.info("Importing czechboard_300x300.csv data...\n")
  board.hex <- h2o.uploadFile(conn, locate("smalldata/histogram_test/czechboard_300x300.csv"), key = "board.hex", header = FALSE)
  board.hex[,3] <- as.factor(board.hex[,3])
  Log.info("Summary of czechboard_300x300.csv from H2O:\n")
  print(summary(board.hex))
  
  # Train H2O DRF Model:
  Log.info("H2O DRF with parameters:\nclassification = TRUE, ntree = 50, depth = 20, nbins = 500\n")
  drfmodel.h2o <- h2o.randomForest(x = c("C1", "C2"), y = "C3", data = board.hex, classification = TRUE, ntree = 50, depth = 20, nbins = 100)
  print(drfmodel.h2o)
  
  testEnd()
}

doTest("DRF Test: Classification with Checkboard Group Split", test.DRF.Czechboard)