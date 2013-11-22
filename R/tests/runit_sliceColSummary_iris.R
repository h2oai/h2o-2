source('./Utils/h2oR.R')

Log.info("\n======================== Begin Test ===========================\n")
view_max <- 10000 #maximum returned by Inspect.java


test.slice.colSummary <- function(conn) {
  Log.info("Importing iris.csv data...\n")
  iris.hex = h2o.importFile(conn, "./smalldata/iris/iris_wheader.csv", "iris.hex")
  Log.info("Check that summary works...")
  
  summary(iris.hex)
  summary_ <- summary(iris.hex)
  iris_nrows <- nrow(iris.hex)
  iris_ncols <- ncol(iris.hex)

  Log.info("Check that iris is 150x5")
  Log.info(paste("Got: nrows = ", iris_nrows, sep =""))
  Log.info(paste("Got: ncols = ", iris_ncols, sep =""))
  
  expect_that(iris_nrows, equals(150))
  expect_that(iris_ncols, equals(5))
  
  sepalLength <- iris.hex[,1]
  Log.info("Summary on the first column:\n")
  expect_that(sepalLength, is_a("H2OParsedData2"))
  
  summary(sepalLength)
    
  tryCatch(summary(sepalLength), error = function(e) print(paste("Cannot perform summary: ",e)))

  Log.info("Try mean, min, max, and compare to actual:\n")
  stats_ <- list("mean"=mean(sepalLength), "min"=min(sepalLength), "max"=max(sepalLength))
  stats <- list("mean"=mean(iris[,1]), "min"=min(iris[,1]), "max"=max(iris[,1]))
  
  Log.info("Actual mean, min, max:\n")
  Log.info(stats)
  cat("\n")
  Log.info("H2O-R's mean, min, max: \n")
  Log.info(stats_)
  cat("\n")
  tryCatch(expect_that(unlist(stats),equals(unlist(stats_))), error = function(e) e)
  Log.info("Check standard deviation and variance: ")
  tryCatch(sd(sepalLength),error = function(e) print(paste("Cannot perform standard deviation: ",e,sep="")))
  
  Log.info("End of test.")
}

conn <- new("H2OClient", ip=myIP, port=myPort)
tryCatch(test_that("sliceTestsColSummary", test.slice.colSummary(conn)), error = function(e) FAIL(e))
PASS()
