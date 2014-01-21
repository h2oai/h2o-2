setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test.km2vanilla.golden <- function(H2Oserver) {
#within ss addressed in JIRA 1489
#Import Data:
#dummyH2O<- h2o.uploadFile.FV(H2Oserver, locate("../../smalldata/dummydata.csv"), key="dummyH2O")
#dummyR<- read.csv(locate("smalldata/dummydata.csv"), header=T)

#Remove unneeded cols
#dataR<- dummyR[,-1]
#dataH2O<- dummyH2O[,-1]

#Fit matching R and H2O models for k=2 on simple data
#fitR<- kmeans(dataR, centers=2)
#fitH2O<- h2o.kmeans.FV(dataH2O, centers=2)

# Not sure why building a kmeans cluster with one center is useful.
# But it found an interesting bug in summary2, so I'll leave it.
#fit2H2O<- h2o.kmeans.FV(dataH2O, centers=1)

# Sanity check to make sure required fields are actually present in the model that gets returned.
#if (! ('withinss' %in% names(fitH2O@model))) {
 # stop("H2O model has no component 'withinss'")
}

#if (! ('totss' %in% names(fitH2O@model))) {
 # stop("H2O model has no component 'totss'")
}

#Log.info("Print model statistics for R and H2O... \n")
#Note that there are two "total" statistics: total within ss, and total ss. Total ss is the total variance in the whole data set, #is computed as the sum of the vector norms between each point and the data mean, and is equal to within cluster sum of squares #when k=1. As of Dec 21 K means in H2O does not produce total ss as an accessible metric in R, and does not model k=1 (known to #jira).
#Log.info(paste("H2O WithinSS  : ", fitH2O@model$withinss, "\t\t", "R WithinSS   : ", fitR$withinss))
#Log.info(paste("H2O TotalSS   : ", fitH2O@model$totss,    "\t\t", "R TotalSS    : ", fitR$totss))

#Log.info("Compare model descriptives in R to model statistics in H2O")
#expect_equal(fitH2O@model$withinss, fitR$withinss, tolerance = 0.01)
#expect_equal(fitH2O@model$totss,    fitR$totss,    tolerance = 0.01)

#testEnd()
}

#doTest("K Means test on well separated dummy data example", test.km2vanilla.golden)
