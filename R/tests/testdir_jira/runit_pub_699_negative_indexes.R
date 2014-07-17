setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.pub_699_negative_indexes <- function(H2Oserver) {

prostatePath = system.file("extdata", "prostate.csv", package="h2o")
prostate.hex = h2o.importFile(H2Oserver, path = prostatePath, key = "prostate.hex")

prostate.local = as.data.frame(prostate.hex)

# Are we in the right universe?
expect_equal(380, dim(prostate.local)[1])
expect_equal(9, dim(prostate.local)[2])

# simple row exclusion
expect_equal(100, dim(prostate.local[-101:-380,])[1])
expect_equal(100, dim(prostate.hex[-101:-380,])[1])

simple column exclusion
expect_equal(7, dim(prostate.local[,-8:-9,])[2])
expect_equal(7, dim(prostate.hex[,-8:-9,])[2])

# list row exclusion
expect_equal(378, dim(prostate.local[c(-101, -110),])[1])
expect_equal(378, dim(prostate.hex[c(-101, -110),])[1])

# list column exclusion
expect_equal(6, dim(prostate.local[,c(-1, -3, -5),])[2])
expect_equal(6, dim(prostate.hex[,c(-1, -3, -5),])[2])

testEnd()

}

doTest("PUB-699 negative indexes should work for both rows and columns", test.pub_699_negative_indexes)

