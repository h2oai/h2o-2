source('./findNSourceUtils.R')

Log.info("======================== Begin Test ===========================")

test.summary2 <- function(con) {
  Log.info("\nImporting iris.csv data...\n")
  iris.hex <- h2o.importFile(con, locate("smalldata/iris/iris_wheader.csv", schema="local"))
  
  Log.info("\nCheck that summary works...")
  print(summary(iris.hex)) 

  Log.info("Summary from R's iris data: ")
  summary(iris)
  Log.info("End of test.")
  PASSS <<- TRUE
}

conn <- new("H2OClient", ip=myIP, port=myPort)

PASSS <- FALSE
tryCatch(test_that("summary2",test.summary2(conn)), error = function(e) FAIL(e))
if (!PASSS) FAIL("Did not reach the end of test. Check Rsandbox/errors.log for warnings and errors.")
PASS()
