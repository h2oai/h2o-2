setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.glm2collinearfeatures.golden <- function(H2Oserver) {
	
#Import data: 
Log.info("Importing CUSE data...") 
cuseH2O<- h2o.uploadFile.FV(H2Oserver, locate("../../smalldata/cuseexpanded.csv"), key="cuseH2O")
cuseR<- read.csv(locate("smalldata/cuseexpanded.csv"), header=T)

#Build matching models in R and H2O - All levels of factors included
#Including all factor levels means that the model will be specified on a non-spd matrix. We should return an error, or drop a column, but BECAUSE REGULARIZATION IS SET TO 0 we should produce the model as it is specified. R drops a collinear column. Alpha and Lambda regularization are not produed as part of the R output for the FV model, so there's no way for the user to know if regularization was applied. 
Log.info("Test H2O treatment of collinear columns - expect to drop col when regularization is 0")
Log.info("Run matching models in R and H2O")
fitH2O<- h2o.glm.FV(x=c("AgeA", "AgeB", "AgeC", "AgeD", "LowEd", "HighEd", "MoreYes", "MoreNo"), y="UsingBinom", lambda=0, alpha=0, nfolds=0, data=cuseH2O, family="binomial")
fitR<- glm(UsingBinom ~ AgeA + AgeB + AgeC + AgeD + LowEd + HighEd + MoreYes + MoreNo, data=cuseR, family="binomial")


Log.info("Print model coefficients, and test that number of params returned match for R and H2O... \n")
H2Ocoeffs<- sort(t(fitH2O@model$coefficients))
Rcoeffs<- sort(t(fitR$coefficients))
Log.info(paste("H2O Coeffs  : ", H2Ocoeffs,      "\t\t", "R Coeffs   : ", Rcoeffs))
expect_equal(H2Ocoeffs, Rcoeffs, tolerance = 0.01)

Log.info("Print model statistics for R and H2O... \n")
Log.info(paste("H2O Deviance  : ", fitH2O@model$deviance,      "\t\t", "R Deviance   : ", fitR$deviance))
Log.info(paste("H2O Null Dev  : ", fitH2O@model$null.deviance, "\t\t", "R Null Dev   : ", fitR$null.deviance))
Log.info(paste("H2O residul df: ", fitH2O@model$df.residual,   "\t\t\t\t", "R residual df: ", fitR$df.residual))
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

doTest("GLM Test: Golden GLM2 - Treatment of Collinear Cols", test.glm2collinearfeatures.golden)

