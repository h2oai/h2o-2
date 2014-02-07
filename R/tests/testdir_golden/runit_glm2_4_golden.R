setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.glm2vanilla.golden <- function(H2Oserver) {
	
#Import data: 
Log.info("Importing Swiss data...") 
swissH2O<- h2o.uploadFile.FV(H2Oserver, locate("../smalldata/swiss.csv"), key="swissH2O")
swissR<- read.csv(locate("../smalldata/swiss.csv"), header=T)

Log.info("Test H2O treatment vanilla GLM - continuious real predictors, gaussian family")
Log.info("Run matching models in R and H2O")
fitH2O<- h2o.glm.FV(x=c("Agriculture", "Examination", "Education", "Catholic", "Infant.Mortality"), y="Fertility", lambda=0, alpha=0, nfolds=0, family="gaussian", data=swissH2O)
fitR<- glm(Fertility ~ Agriculture + Examination + Education + Catholic + Infant.Mortality, family="gaussian", data=swissR)


Log.info("Print model coefficients, and test that number of params returned match for R and H2O... \n")
H2Ocoeffs<- sort(t(fitH2O@model$coefficients))
Rcoeffs<- sort(t(fitR$coefficients))
Log.info(paste("H2O Coeffs  : ", H2Ocoeffs,      "\t\t", "R Coeffs   : ", Rcoeffs))
expect_equal(H2Ocoeffs, Rcoeffs, tolerance = 0.01)

Log.info("Print model statistics for R and H2O... \n")
Log.info(paste("H2O Deviance  : ", fitH2O@model$deviance,      "\t\t", "R Deviance   : ", fitR$deviance))
Log.info(paste("H2O Null Dev  : ", fitH2O@model$null.deviance, "\t\t", "R Null Dev   : ", fitR$null.deviance))
Log.info(paste("H2O residul df: ", fitH2O@model$df.residual,    "\t\t\t\t", "R residual df: ", fitR$df.residual))
Log.info(paste("H2O null df   : ", fitH2O@model$df.null,       "\t\t\t\t", "R null df    : ", fitR$df.null))
Log.info(paste("H2O aic       : ", fitH2O@model$aic,           "\t\t", "R aic        : ", fitR$aic))


Log.info("Compare model coefficients in R to model statistics in H2O")
expect_equal(fitH2O@model$null.deviance, fitR$null.deviance, tolerance = 0.01)
expect_equal(fitH2O@model$deviance, fitR$deviance, tolerance = 0.01)
expect_equal(fitH2O@model$df.residual, fitR$df.residual, tolerance = 0.01)
expect_equal(fitH2O@model$df.null, fitR$df.null, tolerance = 0.01)
expect_equal(fitH2O@model$aic, fitR$aic, tolerance = 0.01)
   testEnd()
}

doTest("GLM Test: Golden GLM2 - Vanilla Gaussian", test.glm2vanilla.golden)

