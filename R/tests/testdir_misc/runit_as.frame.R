##
# Testing number of rows in as.data.frame 
##

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test <- function(conn) {
	Log.info("Reading prostate into R")	
	x = read.csv("../../../smalldata/logreg/prostate.csv", header=F)
	Log.info("Parsing prostate into H2O")	
	hex = h2o.uploadFile(conn, locate("../../../smalldata/logreg/prostate.csv"), "hex")
	Nhex = as.data.frame(hex)
	
	Log.info("Expect that number of rows in as.data.frame is same as the original file")
	expect_that(nrow(Nhex), equals(nrow(x)))
      
        testEnd()
}
doTest("Test data frame", test)

