source('../Utils/h2oR.R')

Log.info("#------------------------------ Begin Tests ------------------------------#")

check.nn_basic <- function(conn) {
	iris.hex <- h2o.uploadFile(conn, "../../../smalldata/iris/iris.csv", "iris.hex")
	hh=h2o.nn(x=c(1,2,3,4),y=5,data=iris.hex)
	print(hh)
}

conn = new("H2OClient", ip=myIP, port=myPort)

tryCatch(test_that("nn test", check.nn_basic(conn)), error = function(e) FAIL(e))
PASS()
