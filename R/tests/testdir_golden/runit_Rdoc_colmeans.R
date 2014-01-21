setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test.rdoccolmeans.golden <- function(H2Oserver) {
	

irisPath = system.file("extdata", "iris.csv", package="h2oRClient")
iris.hex = h2o.importFile(H2Oserver, path = irisPath, key = "iris.hex")
colMeans(iris.hex[,2:4])

testEnd()
}

doTest("R Doc Col Means", test.rdoccolmeans.golden)

