setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.quantiles.golden <- function(H2Oserver) {

#Import data: (the data are 20000 observations pulled from known distributions - parameters given at end of test)
Log.info("Importing MAKE and RUNIF data...") 
makeH2O<- h2o.uploadFile(H2Oserver, locate("../../smalldata/makedata.csv"), key="makeH2O")
makeR<- read.csv(locate("smalldata/makedata.csv"), header=T)
runifH2O<- h2o.uploadFile(H2Oserver, locate("../../smalldata/runif.csv"), key="runifH2O")
runifR<- read.csv(locate("smalldata/runif.csv"), header=T)

#generate quantiles from R
qrR2<- as.data.frame(quantile(runifR[,2]))
qrR3<- as.data.frame(quantile(runifR[,3]))
qrR4<- as.data.frame(quantile(runifR[,4]))
makeR2<- as.data.frame(quantile(makeR[,2]))
makeR3<- as.data.frame(quantile(makeR[,3]))
makeR4<- as.data.frame(quantile(makeR[,4]))
makeR5<- as.data.frame(quantile(makeR[,5]))
makeR6<- as.data.frame(quantile(makeR[,6]))
makeR7<- as.data.frame(quantile(makeR[,7]))
makeR8<- as.data.frame(quantile(makeR[,8]))
makeR9<- as.data.frame(quantile(makeR[,9]))

#generate quantiles from H2O
qrH2<- as.data.frame(quantile(runifH2O[,2]))
qrH3<- as.data.frame(quantile(runifH2O[,3]))
qrH4<- as.data.frame(quantile(runifH2O[,4]))
makeH2<- as.data.frame(quantile(makeH2O[,2]))
makeH3<- as.data.frame(quantile(makeH2O[,3]))
makeH4<- as.data.frame(quantile(makeH2O[,4]))
makeH5<- as.data.frame(quantile(makeH2O[,5]))
makeH6<- as.data.frame(quantile(makeH2O[,6]))
makeH7<- as.data.frame(quantile(makeH2O[,7]))
makeH8<- as.data.frame(quantile(makeH2O[,8]))
makeH9<- as.data.frame(quantile(makeH2O[,9]))


#Print Quantiles for Both: (note that makeH2 corresponds to the H2O quantiles for the second col of make dataframe, where makeR2 is the #quantiles produced by R for the second column of make data frame) 

Log.info("Print summary for H2O and R... \n")

Log.info(paste("H2O Make 2  : ", makeH2,      "\t\t", "R Make 2   : ", makeR2))
Log.info(paste("H2O Make 2  : ", makeH2,      "\t\t", "R Make 2   : ", makeR2))
Log.info(paste("H2O Make 3  : ", makeH3,      "\t\t", "R Make 3   : ", makeR3))
Log.info(paste("H2O Make 4  : ", makeH2,      "\t\t", "R Make 4   : ", makeR4))
Log.info(paste("H2O Make 5  : ", makeH2,      "\t\t", "R Make 5   : ", makeR5))
Log.info(paste("H2O Make 6  : ", makeH2,      "\t\t", "R Make 6   : ", makeR2))
Log.info(paste("H2O Make 7  : ", makeH2,      "\t\t", "R Make 7   : ", makeR2))
Log.info(paste("H2O Make 8  : ", makeH2,      "\t\t", "R Make 8   : ", makeR2))
Log.info(paste("H2O Make 9  : ", makeH2,      "\t\t", "R Make 9   : ", makeR2))
Log.info(paste("H2O Runif 2  : ", qrH2,      "\t\t", "R Runif 2   : ", qrR2))
Log.info(paste("H2O Runif 3  : ", qrH3,      "\t\t", "R Runif 3   : ", qrR3))
Log.info(paste("H2O Runif 4  : ", qrH2,      "\t\t", "R Runif 4   : ", qrR4))




Log.info("Compare H2O summary to R summary... \n")
expect_equal(makeH2[,1], makeR2[,1], tolerance=.01)
#expect_equal(sumBH2O[,2], sumBR[,2], tolerance=.01)
#expect_equal(sumCH2O[,2], sumCR[,2], tolerance=.01)
#expect_equal(sumrunifH2O[,2], sumrunifR[,2], tolerance=.01)
#expect_equal(sumrunifH2O[,3], sumrunifR[,3], tolerance=.01)
#expect_equal(sumrunifH2O[,4], sumrunifR[,4], tolerance=.01)

testEnd()
}

doTest("Summary on Random Uniform Dist", test.quantiles.golden)

#NOTES ON MAKEDATA 
#A: normal, mean: -100, sd = 50
#B: uniform, min: -5000, max: 2000
#C: poisson, lambda: 5
#D: cauchy, location: 50, scale: 500
#E: binom, size=100, prob=.1
#F: binom, size=100, prob=.02
#G: binom, size=10, prob=.01
#H: exponential: rate= .4
