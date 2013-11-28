source('./findNSourceUtils.R')

Log.info("#------------------------------ Begin Tests ------------------------------#")

check.nn_basic <- function(conn) {
	iris.hex <- h2o.uploadFile(conn, locate("smalldata/iris/iris.csv"), "iris.hex")
	hh=h2o.nn(x=c(1,2,3,4),y=5,data=iris.hex)
	print(hh)
    Log.info("End of test")
    PASSS <<- TRUE
}

PASSS <- FALSE
conn = new("H2OClient", ip=myIP, port=myPort)

tryCatch(test_that("nn test", check.nn_basic(conn)), warning = function(w) WARN(w), error = function(e) FAIL(e))
if (!PASSS) FAIL("Did not reach the end of test. Check Rsandbox/errors.log for warnings and errors.")
PASS()
