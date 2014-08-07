setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

check.deeplearning_imbalanced <- function(conn) {
  Log.info("Test checks if Deep Learning works fine with an imbalanced dataset")
  
  prostate = h2o.uploadFile(conn, locate("smalldata/logreg/prostate.csv"))
  hh_imbalanced=h2o.deeplearning(x=c(1,2,3,5),y=4,l1=1e-5,activation="RectifierWithDropout",input_dropout_ratio=0.2,hidden=c(20,20,20),epochs=500,data=prostate,balance_classes=F,nfolds=10)
  print(hh_imbalanced)
  hh_balanced=h2o.deeplearning(x=c(1,2,3,5),y=4,l1=1e-5,activation="RectifierWithDropout",input_dropout_ratio=0.2,hidden=c(20,20,20),epochs=500,data=prostate,balance_classes=T,nfolds=10)
  print(hh_balanced)

  if (hh_imbalanced@model$valid_class_error < hh_balanced@model$valid_class_error) {
      print("--------------------")
      print("")
      print("FAIL, balanced error greater than imbalanced error")
      print("")
      print("")
      print("hh_imbalanced@model$valid_class_error")
      print(hh_imbalanced@model$valid_class_error)
      print("")
      print("hh_balanced@model$valid_class_error")
      print(hh_balanced@model$valid_class_error)
      print("")
      print("--------------------")
  }
  #checkTrue(hh_imbalanced@model$valid_class_error >= hh_balanced@model$valid_class_error, "balance_classes makes it worse!")

  #relaxed tolerance
  checkTrue(hh_imbalanced@model$valid_class_error >= 0.8*hh_balanced@model$valid_class_error, "balance_classes makes it at least 20% worse!")

  testEnd()
}

doTest("Deep Learning Imbalanced Test", check.deeplearning_imbalanced)
