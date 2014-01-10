setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test.assign.golden <- function(H2Oserver) {
	
#Example from as.data.frame R doc
# Test should run example - which is all that is required for this to pass
prosPath = system.file("extdata", "prostate.csv", package="h2oRClient")
prostate.hex = h2o.importFile(H2Oserver, path = prosPath)
prostate.qs = quantile(prostate.hex$PSA)
PSA.outliers = prostate.hex[prostate.hex$PSA <= prostate.qs[2] | prostate.hex$PSA >= prostate.qs[10],]
PSA.outliers = h2o.assign(PSA.outliers, "PSA.outliers")
sum1<- summary(PSA.outliers)
PSA.outliers.df = as.data.frame(PSA.outliers)
sum2<- summary(PSA.outliers.df)
head(prostate.hex)
head(PSA.outliers)


testEnd()
}

doTest("R Doc as.data.frame", test.assign.golden)