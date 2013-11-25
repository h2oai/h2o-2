source('../Utils/h2oR.R')

Log.info("======================== Begin Test ===========================\n")
view_max <- 10000 #maximum returned by Inspect.java

test.slice.colTypes <- function(conn) {
  Log.info("Importing iris.csv data...")
  iris.hex = h2o.uploadFile.VA(conn, "../../../smalldata/iris/iris_wheader.csv", "iris.hex")
  Log.info("Check that summary works...")
  summary(iris.hex)
  summary_ <- summary(iris.hex) #keep the summary around
  iris_nrows <- nrow(iris.hex)
  iris_ncols <- ncol(iris.hex)
  Log.info("Check that iris is 150x5")
  Log.info("Got:\n",iris_nrows, iris_ncols)
  expect_that(iris_nrows, equals(150))
  expect_that(iris_ncols, equals(5))
  Log.info("Check the column data types: \nExpect 'double, double, double, double, integer'")

  col1_type <- typeof(head(iris.hex[,1],nrow(iris.hex))[,1])
  col2_type <- typeof(head(iris.hex[,2],nrow(iris.hex))[,1])
  col3_type <- typeof(head(iris.hex[,3],nrow(iris.hex))[,1])
  col4_type <- typeof(head(iris.hex[,4],nrow(iris.hex))[,1])
  col5_type <- typeof(head(iris.hex[,5],nrow(iris.hex))[,1])
  Log.info("Got:\n",col1_type,col2_type,col3_type,col4_type,col5_type)
  
  expect_that(col1_type, equals("double"))
  expect_that(col2_type, equals("double"))
  expect_that(col3_type, equals("double"))
  expect_that(col4_type, equals("double"))
  expect_that(col5_type, equals("integer"))
  Log.info("End of test.")
}

conn <- new("H2OClient", ip=myIP, port=myPort)
tryCatch(test_that("sliceTestsColTypes",test.slice.colTypes(conn)), error = function(e) FAIL(e))
PASS()
