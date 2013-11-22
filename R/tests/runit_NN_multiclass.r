source('./Utils/h2oR.R')

Log.info("=============This test checks if NN works fine with a multiclass training and test dataset========================\n")

Log.info("========================== Begin Tests ==============================\n")

check.nn_multi <- function() {

#prostate = h2o.uploadFile(conn, "/home/infinity/h2o/smalldata/logreg/prostate.csv")

prostate = h2o.uploadFile(conn, "../../smalldata/logreg/prostate.csv")

hh=h2o.nn(x=c(1,2,3),y=5,data=prostate,validation=p.hex)
print(hh)
}

conn = new("H2OClient", ip=myIP, port=myPort)
tryCatch(test_that("nn multiclass test", check.nn_multi()), error = function(e) FAIL(e))
PASS()
