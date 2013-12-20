setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test.glm2asfactorpoisson.golden <- function(H2Oserver) {
	
#Import data: 
Log.info("Importing AWARDS data...") 
awardH2O<- h2o.uploadFile(H2Oserver, locate("../../smalldata/award.csv"), key="awardH2O")
awardR<- read.csv(locate("smalldata/award.csv"), header=T)

Log.info("Test H2O conversion using as.factor")
awardR$prog<- as.factor(awardR$prog)
awardH2O[,3]<- as.factor(awardH2O[,3])
a<- is.factor(awardR$prog)
b<- is.factor(awardH2O[,3])
expect_equal(a,b)

Log.info("Run matching models in R and H2O")
fitR<- glm(num_awards ~ prog + math, family=poisson, data=awardR)
fitH2O<- h2o.glm.FV(x=c("prog", "math"), y="num_awards", nfolds=0, alpha=0, lambda=0, family="poisson", data=awardH2O)


Log.info("Print model statistics for R and H2O... \n")
Log.info(paste("H2O Deviance  : ", fitH2O@model$deviance,      "\t\t", "R Deviance   : ", fitR$deviance))
Log.info(paste("H2O Null Dev  : ", fitH2O@model$null.deviance, "\t\t", "R Null Dev   : ", fitR$null.deviance))
Log.info(paste("H2O residul df: ", fitH2O@model$df.residual,    "\t\t\t\t", "R residual df: ", fitR$df.residual))
Log.info(paste("H2O null df   : ", fitH2O@model$df.null,       "\t\t\t\t", "R null df    : ", fitR$df.null))
Log.info(paste("H2O aic       : ", fitH2O@model$aic,           "\t\t", "R aic        : ", fitR$aic))

Log.info("Compare model statistics in R to model statistics in H2O")
expect_equal(fitH2O@model$null.deviance, fitR$null.deviance, tolerance = 0.01)
expect_equal(fitH2O@model$deviance, fitR$deviance, tolerance = 0.01)
expect_equal(fitH2O@model$df.residual, fitR$df.residual, tolerance = 0.01)
expect_equal(fitH2O@model$df.null, fitR$df.null, tolerance = 0.01)
expect_equal(fitH2O@model$aic, fitR$aic, tolerance = 0.01)

testEnd()
}

doTest("GLM Test: Golden GLM2 - as.factor in Poisson", test.glm2asfactorpoisson.golden)

