setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.nested_ifelse <- function(conn) {
  a <- as.h2o(conn, 2)
  b <- as.h2o(conn, 2)
  d <- as.h2o(conn, 2)
  e <- ifelse(a == b, b, ifelse(b == b, ifelse(a == d, 3.1415, 0), a))
  print(e)

  testEnd()
}

doTest("Test frame add.", test.nested_ifelse )
