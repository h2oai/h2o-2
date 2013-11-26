source('../Utils/h2oR.R')
logging("\nLoading R.utils package\n")
if(!"R.utils" %in% rownames(installed.packages())) install.packages("R.utils")
require(R.utils)
logging("\n======================== Begin Test ===========================\n")

H2Ocon <- new("H2OClient", ip=myIP, port=myPort)

test.mnist.manyCols <- function(con) {
  cat("\nImporting mnist train data...\n")
  train.hex = h2o.uploadFile(H2Ocon, "../../../smalldata/mnist/train.csv.gz", "train.hex")
  cat("\nCheck that tail works...")
  tail(train.hex)
  tail_ <- tail(train.hex)
  cat(" OK, yes tail works!\n")
  cat("Doing gbm on mnist training data.... \n")
  cat("This may take a while if the package is freshly loaded.\n")
  cat("The reason for this is the S4 method selection process by the\n") 
  cat("R evaluator is taking forever checking out the inheritances of\n")
  cat("all of the classes of the arguments given in h2o.gbm.\n")
  gbm.mnist <- h2o.gbm(x= 0:783, y = 784, data = train.hex, n.trees = 5, interaction.depth = 5, n.minobsinnode = 10, shrinkage = 0.01)
  print(gbm.mnist)
  
  print("End of test.")
}

test_that("manyCols",test.mnist.manyCols(H2Ocon))

