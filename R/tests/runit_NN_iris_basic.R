source('./Utils/h2oR.R')


#------------------------------ Begin Tests ------------------------------#

serverH2O = new("H2OClient", ip=myIP, port=myPort)
check.nn_basic <- function() {
	iris.hex <- h2o.uploadFile(serverH2O, "../../smalldata/iris/iris.csv")
	hh=h2o.nn(x=c(1,2,3,4),y=5,data=iris.hex)
	print(hh)
}

test_that("nn test", check.nn_basic())
#stop("NNNNNN")
