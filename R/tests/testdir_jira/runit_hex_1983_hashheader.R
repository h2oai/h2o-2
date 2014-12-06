setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.headers <- function(conn) {

  hex <- h2o.uploadFile(conn, locate("smalldata/jira/hex_1983.csv"), header_with_hash=TRUE)
  expect_that(hex@nrows, equals(3)) 
  expect_that(hex@col_names[1], equals("#col1")) 
  

  hex <- h2o.uploadFile(conn, locate("smalldata/jira/hex_1983.csv"))
  expect_that(hex@nrows, equals(3)) 
  expect_that(hex@col_names[1], equals("C1")) 

  testEnd()
}

doTest("Import a dataset with a header H2OParsedData Object", test.headers)
