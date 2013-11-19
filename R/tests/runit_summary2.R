source('./Utils/h2oR.R')

logging("\n======================== Begin Test ===========================\n")

con <- new("H2OClient", ip=myIP, port=myPort)

test.slice.colTail <- function(con) {
  cat("\nImporting iris.csv data...\n")
  iris.hex <- h2o.importFile.FV(con, "./smalldata/iris/iris_wheader.csv")
  
  cat("\nCheck that summary works...")
  summary(iris.hex) 

  cat("\nSummary from R's iris data: \n")
  summary(iris)
  
  print("End of test.")
}

test_that("sliceTestsColTail",test.slice.colTail(con))
