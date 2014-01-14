setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test.rdocuploadfile.golden <- function(H2Oserver) {
	
prostate.hex = h2o.uploadFile(H2Oserver, path = system.file("extdata", "prostate.csv", package="h2oRClient"), key = "prostate.hex")
summary(prostate.hex)

testEnd()
}

doTest("R Doc as.data.frame", test.rdocuploadfile.golden)

