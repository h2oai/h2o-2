setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.rdoc_cut.golden <- function(H2Oserver) {
	
irisPath = system.file("extdata", "iris_wheader.csv", package="h2o")
iris.hex = h2o.importFile(H2Oserver, path = irisPath, key = "iris.hex")
summary(iris.hex)

testEnd()
}

doTest("R Doc Cut Status", test.rdoc_cut.golden)

