setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

check.nn_grid <- function(conn) {
  Log.info("Test checks if NN grid search works")
  
  iris.hex <- h2o.uploadFile(conn, locate("smalldata/iris/iris_wheader.csv"))
  iris.nn <- h2o.nn(x = 1:4, y = 5, data = iris.hex, rate = seq(0.01, 0.05, 0.01), l2_reg = c(0.0010, 0.0050))
  print(iris.nn)
  
  testEnd()
}

doTest("NN Grid Search Test", check.nn_grid)