source('./findNSourceUtils.R')

test.as.factor.basic <- function(conn) {
  hex <- h2o.uploadFile(conn, "~/master/h2o/smalldata/cars.csv", "cars.hex")
  hex[,"cylinders"] <- as.factor(hex[,"cylinders"])
  expect_true(is.factor(hex[,"cylinders"]))
  testEnd()
}

doTest("Test the as.factor unary operator", test.as.factor.basic)

