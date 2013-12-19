source('./findNSourceUtils.R')

#setupRandomSeed(1193410486)
test.slice.rows <- function(conn) {
  Log.info("Importing cars.csv data...\n")
  H <- h2o.uploadFile(conn, locate("smalldata/cars.csv"), "cars.hex")
#  R <- read.csv(locate("smalldata/cars.csv"))
  R <- as.data.frame(H)
  
  Log.info("Compare H[I,] and R[I,],  range of I is included in range of the data frame.")
  I <- sample(1:nrow(R), 1000, replace=T)
  Log.info("head(H[I,])")
  print(head(H[I,2:6]))
  Log.info("head(R[I,])")
  print(head(R[I,2:6]))
  expect_that(as.data.frame(H[I,2:6]), equals(R[I,2:6]))

  Log.info("Compare H[I,] and R[I,],  range of I goes beyond the range of the data frame.")
  I <- c(nrow(R) + 1, nrow(R) + 2, I)
  Log.info("head(H[I,])")
  print(head(H[I,]))
  Log.info("head(R[I,])")
  print(head(R[I,]))  
  expect_that(as.data.frame(H[I,2:6]), equals(R[I,2:6]))
  
  testEnd()
}

doTest("Slice Tests: Row slice using R index", test.slice.rows)
