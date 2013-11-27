source('./findNSourceUtils.R')
Log.info("Loading R.utils package\n")
if(!"R.utils" %in% rownames(installed.packages())) install.packages("R.utils")
require(R.utils)
Log.info("======================= Begin Test ===========================\n")

test.mnist.manyCols <- function(conn) {
  Log.info("Importing mnist train data...\n")
  train.hex = h2o.uploadFile(conn, locate("../../../smalldata/mnist/train.csv.gz"), "train.hex")
  Log.info("Check that tail works...")
  tail(train.hex)
  tail_ <- tail(train.hex)
  Log.info("Doing gbm on mnist training data.... \n")
  gbm.mnist <- h2o.gbm(x= 1:784, y = 785, data = train.hex, n.trees = 5, interaction.depth = 5, n.minobsinnode = 10, shrinkage = 0.01)
  print(gbm.mnist)
  
  Log.info("End of test.")
}

conn <- new("H2OClient", ip=myIP, port=myPort)
tryCatch(test_that("manyCols",test.mnist.manyCols(conn)), warning = function(w) WARN(w), error = function(e) FAIL(e))
PASS()
