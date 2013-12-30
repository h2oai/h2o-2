setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../../findNSourceUtils.R')

check.nn_multi <- function(conn) {
  Log.info("Test checks if NN works fine with a multiclass training and test dataset")
  
  prostate = h2o.uploadFile(conn, locate("smalldata/logreg/prostate.csv"))
  hh=h2o.nn(x=c(1,2,3),y=5,data=prostate,validation=prostate)
  print(hh)

  testEnd()
}

doTest("NN MultiClass Test", check.nn_multi)

