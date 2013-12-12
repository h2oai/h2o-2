source('./findNSourceUtils.R')

check.nn_basic <- function(conn) {
	iris.hex <- h2o.uploadFile(conn, locate("smalldata/iris/iris.csv"), "iris.hex")
	hh=h2o.nn(x=c(1,2,3,4),y=5,data=iris.hex)
	print(hh)
    testEnd()
}

doTest("NN Test: Iris", check.nn_basic)

