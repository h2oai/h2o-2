##
# Testing creation of random data frame in H2O 
##

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.createFrame <- function(conn) {
  Log.info("Create a data frame with rows = 10000, cols = 100")
  hex <- h2o.createFrame(conn, "hex", rows = 10000, cols = 100, categorical_fraction = 0.1, factors = 5, integer_fraction = 0.5, integer_range = 1)
  expect_equal(dim(hex), c(10000, 100))
  expect_equal(length(colnames(hex)), 100)
  
  Log.info("Check that 0.1 * 100 = 10 columns are categorical")
  fac_col <- sapply(1:100, function(i) is.factor(hex[,i]))
  num_fac <- sum(fac_col)
  expect_equal(num_fac/100, 0.1)
  
  Log.info("Create a data frame with rows = 100, cols = 10")
  hex2 <- h2o.createFrame(conn, "hex2", rows = 100, cols = 10, randomize = FALSE, value = 5, categorical_fraction = 0, integer_fraction = 0, missing_fraction = 0, has_response = TRUE)
  print(summary(hex2))
  expect_equal(dim(hex2), c(100, 11))
  expect_equal(length(colnames(hex2)), 11)
  
  Log.info("Check that all data entries are equal to 5")
  cons_col <- sapply(1:10, function(i) { min(hex2[,i]) == 5 && max(hex2[,i]) == 5 })
  expect_true(all(cons_col))
  
  testEnd()
}

doTest("Create a random data frame in H2O", test.createFrame)