######################################################################
# Test for PUB-169
# apply should work across columns
######################################################################

# setwd("/Users/tomk/0xdata/ws/h2o/R/tests/testdir_jira")

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
options(echo=TRUE)
source('../findNSourceUtils.R')

test.colapply <- function(conn) {
  Log.info('Importing test_uuid.csv to H2O...')
  df <- h2o.importFile(conn, normalizePath(locate('smalldata/test/test_uuid.csv')))
  colnames(df) = c("AA", "UUID", "CC")
  
  Log.info("Slice a subset of columns")
  df.train = df[df$CC == 1,]
  
  testEnd()
}

doTest("PUB-169 Test: Apply scale over columns", test.colapply)