setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.summary2dists.golden <- function(H2Oserver) {

#Import data: (the data are 20000 observations pulled from known distributions - parameters given at end of test)
Log.info("Importing MAKE data...") 
makeH2O<- h2o.uploadFile.FV(H2Oserver, locate("../../smalldata/makedata.csv"), key="makeH2O")
makeR<- read.csv(locate("smalldata/makedata.csv"), header=T)


#Obtain summary for both: 
sumH2O<- summary(makeH2O)
sumR<- summary(makeR)

Log.info("Print summary for H2O and R... \n")
Log.info(paste("H2O summary :",  sumH2O[,2]))
Log.info(paste("R summary :",  sumR[,2]))
Log.info(paste("H2O summary :",  sumH2O[,3]))
Log.info(paste("R summary :",  sumR[,3]))
Log.info(paste("H2O summary :",  sumH2O[,4]))
Log.info(paste("R summary :",  sumR[,4]))
Log.info(paste("H2O summary :",  sumH2O[,5]))
Log.info(paste("R summary :",  sumR[,5]))
Log.info(paste("H2O summary :",  sumH2O[,6]))
Log.info(paste("R summary :",  sumR[,6]))
Log.info(paste("H2O summary :",  sumH2O[,7]))
Log.info(paste("R summary :",  sumR[,7]))
Log.info(paste("H2O summary :",  sumR[,8]))
Log.info(paste("R summary :",  sumR[,8]))
Log.info(paste("H2O summary :",  sumR[,9]))
Log.info(paste("R summary :",  sumR[,9]))



Log.info("Compare H2O summary to R summary... \n")
expect_equal(sumH2O[,2], sumR[,2], tolerance=.01)
expect_equal(sumH2O[,3], sumR[,3], tolerance=.01)
expect_equal(sumH2O[,4], sumR[,4], tolerance=.01)
expect_equal(sumH2O[,5], sumR[,5], tolerance=.01)
expect_equal(sumH2O[,6], sumR[,6], tolerance=.01)
expect_equal(sumH2O[,7], sumR[,7], tolerance=.01)
expect_equal(sumH2O[,8], sumR[,8], tolerance=.01)
expect_equal(sumH2O[,9], sumR[,9], tolerance=.01)
testEnd()
}

doTest("Summary 2 Known Dists", test.summary2dists.golden)

#A: normal, mean: -100, sd = 50
#B: uniform, min: -5000, max: 2000
#C: poisson, lambda: 5
#D: cauchy, location: 50, scale: 500
#E: binom, size=100, prob=.1
#F: binom, size=100, prob=.02
#G: binom, size=10, prob=.01
#H: exponential: rate= .4
