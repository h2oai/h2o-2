setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.rdocKmeans.golden <- function(H2Oserver) {
	

prosPath = system.file("extdata", "prostate.csv", package="h2o")
prostate.hex = h2o.importFile(H2Oserver, path = prosPath)
h2o.kmeans(data = prostate.hex, centers = 10, cols = c("AGE", "RACE", "VOL", "GLEASON"))
covPath = system.file("extdata", "covtype.csv", package="h2o")
covtype.hex = h2o.importFile(H2Oserver, path = covPath)
covtype.km = h2o.kmeans(data = covtype.hex, centers = 5, cols = c(1, 2, 3))
print(covtype.km)

testEnd()
}

doTest("R Doc K Means", test.rdocKmeans.golden)

