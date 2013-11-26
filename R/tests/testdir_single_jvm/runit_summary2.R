source('../Utils/h2oR.R')

Log.info("\n======================== Begin Test ===========================\n")

test.summary2 <- function(con) {
  Log.info("\nImporting iris.csv data...\n")
  iris.hex <- h2o.importFile(con, "smalldata/iris/iris_wheader.csv")
  
  Log.info("\nCheck that summary works...")
  print(summary(iris.hex)) 

  Log.info("\nSummary from R's iris data: \n")
  data(iris); print(summary(iris))
  
  Log.info("End of test.")
}

conn <- new("H2OClient", ip=myIP, port=myPort)

tryCatch(test_that("summary2",test.summary2(conn)), error = function(e) FAIL(e))
PASS()
