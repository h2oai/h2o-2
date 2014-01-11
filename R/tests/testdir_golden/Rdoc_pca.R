setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test.RdocPCA.golden <- function(H2Oserver) {
	

prostate.hex = h2o.importFile(H2Oserver, path = "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", key = "prostate.hex")
h2o.pcr(x = c("AGE","RACE","PSA","DCAPS"), y = "CAPSULE", data = prostate.hex, family = "binomial", 
nfolds = 10, alpha = 0.5, ncomp = 3)

testEnd()
}

doTest("R Doc PCA", test.RdocPCA.golden)

