setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.summary2runif.golden <- function(H2Oserver) {

#Import data: (the data are 20000 observations pulled from known distributions - parameters given at end of test)
Log.info("Importing MAKE data...") 
AH2O<- h2o.uploadFile.FV(H2Oserver, locate("../../smalldata/runifA.csv"), key="AH2O")
AR<- read.csv(locate("smalldata/runifA.csv"), header=T)
BH2O<- h2o.uploadFile.FV(H2Oserver, locate("../../smalldata/runifB.csv"), key="BH2O")
BR<- read.csv(locate("smalldata/runifB.csv"), header=T)
CH2O<- h2o.uploadFile.FV(H2Oserver, locate("../../smalldata/runifC.csv"), key="CH2O")
CR<- read.csv(locate("smalldata/runifC.csv"), header=T)
runifH2O<- h2o.uploadFile.FV(H2Oserver, locate("../../smalldata/runif.csv"), key="CH2O")
runifR<- read.csv(locate("smalldata/runif.csv"), header=T)



#Obtain summary for both: 
sumAH2O<- summary(AH2O)
sumAR<- summary(AR)
sumBH2O<- summary(BH2O)
sumBR<- summary(BR)
sumCH2O<- summary(CH2O)
sumCR<- summary(CR)
sumrunifH2O<- summary(runifH2O)
sumrunifR<- summary(runifR)

Log.info("Print summary for H2O and R... \n")
Log.info(paste("H2O summary :",  sumAH2O[,2]))
Log.info(paste("R summary :",  sumAR[,2]))
Log.info(paste("H2O summary :",  sumBH2O[,2]))
Log.info(paste("R summary :",  sumBR[,2]))
Log.info(paste("H2O summary :",  sumCH2O[,2]))
Log.info(paste("R summary :",  sumCR[,2]))
Log.info(paste("H2O summary :",  sumrunifH2O[,2]))
Log.info(paste("R summary :",  sumrunifR[,2]))
Log.info(paste("H2O summary :",  sumrunifH2O[,3]))
Log.info(paste("R summary :",  sumrunifR[,3]))
Log.info(paste("H2O summary :",  sumrunifH2O[,4]))
Log.info(paste("R summary :",  sumrunifR[,4]))



Log.info("Compare H2O summary to R summary... \n")
expect_equal(sumAH2O[,2], sumAR[,2], tolerance=.01)
expect_equal(sumBH2O[,2], sumBR[,2], tolerance=.01)
expect_equal(sumCH2O[,2], sumCR[,2], tolerance=.01)
expect_equal(sumrunifH2O[,2], sumrunifR[,2], tolerance=.01)
expect_equal(sumrunifH2O[,3], sumrunifR[,3], tolerance=.01)
expect_equal(sumrunifH2O[,4], sumrunifR[,4], tolerance=.01)

testEnd()
}

doTest("Summary2 on Random Uniform Dist", test.summary2runif.golden)

#A: normal, mean: -100, sd = 50
#B: uniform, min: -5000, max: 2000
#C: poisson, lambda: 5
#D: cauchy, location: 50, scale: 500
#E: binom, size=100, prob=.1
#F: binom, size=100, prob=.02
#G: binom, size=10, prob=.01
#H: exponential: rate= .4
