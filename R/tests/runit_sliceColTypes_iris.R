source('./Utils/h2oR.R')

logging("\n======================== Begin Test ===========================\n")
view_max <- 10000 #maximum returned by Inspect.java

H2Ocon <- new("H2OClient", ip=myIP, port=myPort)

test.slice.colTypes <- function(con) {
  cat("\nImporting iris.csv data...\n")
  iris.hex = h2o.uploadFile(H2Ocon, "../../smalldata/iris/iris_wheader.csv", "iris.hex")
  cat("\nCheck that summary works...")
  summary(iris.hex)
  summary_ <- summary(iris.hex) #keep the summary around
  iris_nrows <- nrow(iris.hex)
  iris_ncols <- ncol(iris.hex)
  cat("\nCheck that iris is 150x5")
  cat("\nGot:\n",iris_nrows, iris_ncols)
  expect_that(iris_nrows, equals(150))
  expect_that(iris_ncols, equals(5))
  cat(" Good!")
  cat("\nCheck the column data types: \nExpect 'double, double, double, double, integer'")
  #TODO: Once head(hex) returns proper class (numeric), fix this col extraction
  col1_type <- typeof(head(iris.hex[,1],nrow(iris.hex))[,1])
  col2_type <- typeof(head(iris.hex[,2],nrow(iris.hex))[,1])
  col3_type <- typeof(head(iris.hex[,3],nrow(iris.hex))[,1])
  col4_type <- typeof(head(iris.hex[,4],nrow(iris.hex))[,1])
  col5_type <- typeof(head(iris.hex[,5],nrow(iris.hex))[,1])
  cat("\nGot:\n",col1_type,col2_type,col3_type,col4_type,col5_type)
  expect_that(col1_type, equals("double"))
  expect_that(col2_type, equals("double"))
  expect_that(col3_type, equals("double"))
  expect_that(col4_type, equals("double"))
  expect_that(col5_type, equals("integer"))
  print("End of test.")
}

test_that("sliceTestsColTypes",test.slice.colTypes(H2Ocon))
