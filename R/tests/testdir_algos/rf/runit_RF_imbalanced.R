setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.rf.imbalanced <- function(conn) {
  prostate = h2o.uploadFile(conn, locate("smalldata/logreg/prostate.csv"))

  hh_imbalanced=h2o.randomForest(x=c(1,2,3,5),y=4,ntree=5,data=prostate,balance.classes=F,nfolds=10)
  print(hh_imbalanced)
  hh_balanced=h2o.randomForest(x=c(1,2,3,5),y=4,ntree=5,data=prostate,balance.classes=T,nfolds=10)
  print(hh_balanced)

  # test that it improves the overall classification error...
  checkTrue(hh_imbalanced@model$confusion[4,4] > hh_balanced@model$confusion[4,4], "balance_classes makes it worse!")

  testEnd()
}

doTest("rf imbalanced", test.rf.imbalanced)
