setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test.rdocimportfile.golden <- function(H2Oserver) {
	
#Example from as.data.frame R doc

irisPath = system.file("extdata", "iris.csv", package="h2oRClient")
iris.hex = h2o.importFile(H2Oserver, path = irisPath, key = "iris.hex")
summary(iris.hex)

testEnd()
}

doTest("R Doc as.data.frame", test.rdocimportfile.golden)


