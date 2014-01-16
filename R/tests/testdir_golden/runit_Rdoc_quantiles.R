
setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test.rdocquantiles.golden <- function(H2Oserver) {

prosPath = system.file("extdata", "prostate.csv", package="h2oRClient")
prostate.hex = h2o.importFile(H2Oserver, path = prosPath)
quantile(prostate.hex)


quantile(prostate.hex[,3:5])


testEnd()
}

doTest("R Doc Quantiles", test.rdocquantiles.golden)

