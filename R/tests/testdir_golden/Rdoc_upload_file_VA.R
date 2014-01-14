
setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test.rdocuploadfileVA.golden <- function(H2Oserver) {


library(h2o)
localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)
prostate.hex = h2o.uploadFile.VA(localH2O, path = system.file("extdata", "prostate.csv", package="h2oRClient"), key = "prostate.hex")
summary(prostate.hex)

testEnd()
}

doTest("R Doc Upload File VA", test.rdocuploadfileVA.golden)

