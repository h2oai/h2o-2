##
##

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.columndereference <- function(conn) {

  hex <- as.h2o(conn, iris)

  hex.sub <- hex[hex$Species != "setosa",]

  print(dim(hex.sub))


  testEnd()
}

doTest("test column dereference and assignment", test.columndereference)

