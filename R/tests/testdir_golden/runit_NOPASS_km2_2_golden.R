setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test.km2vanilla.golden <- function(H2Oserver) {

#Import Data:
dummyH2O<- h2o.uploadFile(H2Oserver, locate("../../smalldata/dummydata.csv"), key="dummyH2O")
dummyR<- read.csv(locate("smalldata/dummydata.csv"), header=T)

#Remove unneeded cols
dataR<- dataR[,-1]
dataH2O<- dataH2O[,-1]

#Fit matching R and H2O models for k=2 on simple data
fitR<- kmeans(dataR, centers=2)
fitH2O<- h2o.kmeans(dataH2O, centers=2)
fit2H2O<- h2o.kmeans(dataH2O, centers=1)

Log.info("Print model statistics for R and H2O... \n")
#Note that there are two "total" statistics: total within ss, and total ss. Total ss is the total variance in the whole data set, is computed as the sum of the vector norms between each point and the data mean, and is equal to within cluster sum of squares when k=1. As of Dec 21 K means in H2O does not produce total ss as an accessible metric in R, and does not model k=1 (known to jira).
Log.info(paste("H2O WithinSS  : ", fitH2O@model$withinss,      "\t\t", "R WithinSS   : ", fitR$withinss))
Log.info(paste("H2O TotalSS  : ", fit2H2O@model$withinss, "\t\t", "R Total SS   : ", fitR$totss))

Log.info("Compare model descriptives in R to model statistics in H2O")
expect_equal(fitH2O@model$withinss, fitR$withinss, tolerance = 0.01)
expect_equal(fit2H2O@model$withinss, fitR$totss, tolerance = 0.01)

testEnd()
}

doTest("K Means test on well separated dummy data example", test.km2vanilla.golden)
