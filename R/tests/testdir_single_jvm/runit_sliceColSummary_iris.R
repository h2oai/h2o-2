source('./findNSourceUtils.R')

Log.info("======================== Begin Test ===========================\n")
view_max <- 10000 #maximum returned by Inspect.java

test.slice.colSummary <- function(conn) {
  Log.info("Importing iris.csv data...\n")
  iris.hex = h2o.importFile(conn, locate("../smalldata/iris/iris_wheader.csv",schema = "local"), "iris.hex")
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
  expect_that(sepalLength, is_a("H2OParsedData"))
  
  print(summary(sepalLength))
  
  Log.info("try mean")
  m <- mean(sepalLength)
  expect(m, equals(mean(iris$sepalLength)))
  Log.info("Try mean, min, max, and compare to actual:\n")
  stats_ <- list("mean"=mean(sepalLength), "min"=min(sepalLength), "max"=max(sepalLength))
  stats <- list("mean"=mean(iris[,1]), "min"=min(iris[,1]), "max"=max(iris[,1]))
  
  Log.info("Actual mean, min, max:\n")
  Log.info(stats)
  cat("\n")
  Log.info("H2O-R's mean, min, max: \n")
  Log.info(stats_)
  cat("\n")
  expect_that(unlist(stats),equals(unlist(stats_)))
  #tryCatch(expect_that(unlist(stats),equals(unlist(stats_))), error = function(e) e)
  Log.info("Check standard deviation and variance: ")
  #tryCatch(sd(sepalLength),error = function(e) print(paste("Cannot perform standard deviation: ",e,sep="")))
  
  Log.info("End of test.")
  PASS <<- TRUE
}

PASS <- FALSE
conn <- new("H2OClient", ip=myIP, port=myPort)
tryCatch(test_that("sliceTestsColSummary", test.slice.colSummary(conn)), warning = function(w) WARN(w), error = function(e) FAIL(e))
if (!PASS) FAIL("Did not reach the end of test. Check Rsandbox/errors.log for warnings and errors.")
PASS()
