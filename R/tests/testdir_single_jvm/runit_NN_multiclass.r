source('./findNSourceUtils.R')

Log.info("=============This test checks if NN works fine with a multiclass training and test dataset========================\n")

Log.info("========================== Begin Tests ==============================\n")

check.nn_multi <- function() {

  #prostate = h2o.uploadFile(conn, "/home/infinity/h2o/smalldata/logreg/prostate.csv")

  prostate = h2o.uploadFile(conn, locate("smalldata/logreg/prostate.csv") )

  hh=h2o.nn(x=c(1,2,3),y=5,data=prostate,validation=p.hex)
  print(hh)
  Log.info("End of test..")
  PASSS <<- TRUE
}
PASSS <- FALSE
conn = new("H2OClient", ip=myIP, port=myPort)
tryCatch(test_that("nn multiclass test", check.nn_multi()), warning = function(w) WARN(w), error = function(e) FAIL(e))
if (!PASSS) FAIL("Did not reach the end of test. Check Rsandbox/errors.log for warnings and errors.")
PASS()
