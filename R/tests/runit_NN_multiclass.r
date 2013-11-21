source('./Utils/h2oR.R')
Log.info("\n=============This test checks if NN works fine with a multiclass training and test dataset========================\n")

Log.info("\n========================== Begin Tests ==============================\n")

serverH2O = new("H2OClient", ip=myIP, port=myPort)
check.nn_multi <- function() {

#prostate = h2o.uploadFile(serverH2O, "/home/infinity/h2o/smalldata/logreg/prostate.csv")

prostate = h2o.uploadFile(serverH2O, "../../smalldata/logreg/prostate.csv")

hh=h2o.nn(x=c(1,2,3),y=5,data=prostate,validation=p.hex)
print(hh)
}

test_that("nn multiclass test", check.nn_multi())
