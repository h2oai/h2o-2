
setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.rdocuploadfileVA.golden <- function(H2Oserver) {



prostate.hex = h2o.uploadFile.VA(H2Oserver, path = system.file("extdata", "prostate.csv", package="h2o"), key = "prostate.hex")
summary(prostate.hex)

testEnd()
}

doTest("R Doc Upload File VA", test.rdocuploadfileVA.golden)

