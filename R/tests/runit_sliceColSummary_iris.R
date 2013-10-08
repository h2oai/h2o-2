source('./Utils/h2oR.R')

logging("\n======================== Begin Test ===========================\n")
view_max <- 10000 #maximum returned by Inspect.java

H2Ocon <- new("H2OClient", ip=myIP, port=myPort)

test.slice.colSummary <- function(con) {
  cat("\nImporting iris.csv data...\n")
  iris.hex = h2o.uploadFile(H2Ocon, "../../smalldata/iris/iris_wheader.csv", "iris.hex")
  cat("\nCheck that summary works...")
  summary(iris.hex)
  summary_ <- summary(iris.hex) #keep the summary around
  iris_nrows <- nrow(iris.hex)
  iris_ncols <- ncol(iris.hex)
  cat("\nCheck that iris is 150x5")
  cat("\nGot:\n","nrows =",iris_nrows, "ncols =",iris_ncols)
  expect_that(iris_nrows, equals(150))
  expect_that(iris_ncols, equals(5))
  cat(" Good!")
  sepalLength <- iris.hex[,1]
  cat("\nSummary on the first column:\n")
  expect_that(sepalLength, is_a("H2OParsedData"))
  tryCatch(summary(sepalLength), error = function(e) print(paste("Cannot perform summary: ",e)))
  cat("\nTry mean, min, max, and compare to actual:\n")
  stats_ <- list("mean"=mean(sepalLength), "min"=min(sepalLength), "max"=max(sepalLength))
  stats <- list("mean"=mean(iris[,1]), "min"=min(iris[,1]), "max"=max(iris[,1]))
  cat("Actual mean, min, max:\n")
  print(stats)
  cat("\nH2O-R's mean, min, max: \n")
  print(stats_)
  tryCatch(expect_that(unlist(stats),equals(unlist(stats_))), error = function(e) e)
  cat("\nCheck standard deviation and variance: ")
  tryCatch(sd(sepalLength),error = function(e) print(paste("Cannot perform standard deviation: ",e,sep="")))
  print("End of test.")
}

test_that("sliceTestsColSummary", test.slice.colSummary(H2Ocon))
