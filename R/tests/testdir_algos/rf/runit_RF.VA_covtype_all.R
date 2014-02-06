setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.RF.VA.covtype_class <- function(conn) {
  covtype.hex = h2o.uploadFile.VA(conn, locate( "smalldata/covtype/covtype.20k.data"), "covtype.hex")
  covtype.rf = h2o.randomForest.VA(y = 55, x = seq(1,54), data = covtype.hex, ntree = 50, depth = 100) 
  print(covtype.rf)
  testEnd()
}

doTest("RF1 VA test covtype all", test.RF.VA.covtype_class)

