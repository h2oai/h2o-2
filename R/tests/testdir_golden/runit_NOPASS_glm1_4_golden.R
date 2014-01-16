setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test.glm1vanillafactors.golden <- function(H2Oserver) {
	
#Import data: 
Log.info("Importing CUSE data...") 
cuseH2O<- h2o.uploadFile.VA(H2Oserver, locate("../smalldata/cuseexpanded.csv"))
cuseR<- read.csv(locate("../smalldata/cuseexpanded.csv"), header=T)

Log.info("Test H2O treatment factors in gaussian family")
Log.info("Run matching models in R and H2O")
fitH2O<- h2o.glm(x=c("Age", "Ed"), y="Percentuse", family="gaussian", nfolds=0, lambda=0, alpha=0, data=cuseH2O)
expect_error(h2o.glm(x=c("Age", "Ed"), y="Percentuse", family="gaussian", nfolds=0, lambda=0, alpha=0, data=cuseH2O)



fitR<- fitR<- lm(Percentuse ~ AgeA + AgeB + AgeC + AgeD, data=cuseR, singular.ok=F)


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
expect_equal(fitH2O@model$df.residual, fitR$df.residual)
expect_equal(fitH2O@model$df.null, fitR$df.null, tolerance = 0.01)
expect_equal(fitH2O@model$aic, fitR$aic, tolerance = 0.01)
   
   testEnd()
}

doTest("GLM Test: Golden GLM2 - Vanilla factors", test.glm1vanillafactors.golden)

