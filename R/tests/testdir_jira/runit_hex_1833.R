setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')


test <- function(conn) {
  json_file <- locate("smalldata/jira/hex-1833.json")

  print(fromJSON(file=json_file))
 
  testEnd()
}

doTest("testing JSON parse", test)
