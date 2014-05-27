setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

check.deeplearning_grid <- function(conn) {
  Log.info("Test checks if Deep Learning grid search works")
  
  iris.hex <- h2o.uploadFile(conn, locate("smalldata/iris/iris_wheader.csv"))
  iris.deeplearning <- h2o.deeplearning(x = 1:4, y = 5, data = iris.hex, rate = seq(0.01, 0.05, 0.01), l2_reg = c(0.0010, 0.0050))
  print(iris.deeplearning)
  
  testEnd()
}

doTest("Deep Learning Grid Search Test", check.deeplearning_grid)
