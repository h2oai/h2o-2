setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test.checkclient.golden <- function(H2Oserver) {
	
#Example from checkclient R doc - it just needs to run

h2o.checkClient(H2Oserver)

testEnd()
}

doTest("R Doc check client", test.checkclient.golden)

