setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.rdocasimportURL.golden <- function(H2Oserver) {

prostate.hex = h2o.importURL.VA(H2Oserver, path = "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", key = "prostate.hex")
summary(prostate.hex)

testEnd()
}

doTest("R Doc Import URL", test.rdocasimportURL.golden)

