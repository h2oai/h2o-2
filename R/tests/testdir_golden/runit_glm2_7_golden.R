setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.glm2tweedie.golden <- function(H2Oserver) {
	
#Import data: (data are imported from URL as part of test, but are also in small data if needed)
Log.info("Importing ZINAB data...") 
zinabH2O<- h2o.importFile.FV(H2Oserver, path="http://www.ats.ucla.edu/stat/data/fish.csv", key="zinabH2O")
zinabR<- read.csv("http://www.ats.ucla.edu/stat/data/fish.csv")

#Build matching models in R and H2O - Tweedie
Log.info("Test H2O treatment vanilla GLM - continuious real predictors, gaussian family")
Log.info("Run matching models in R and H2O")
fitH2O<- h2o.glm.FV(x=c("nofish","livebait","camper"), y="count", family="tweedie", tweedie.p=1.5, nfolds=0, alpha=0, lambda=0, data=zinabH2O)
fitR<- glm(count ~ nofish + livebait+ camper, family=tweedie(var.power=1.5), data=zinabR)
H2Ocoeffs<- sort(t(fitH2O@model$coefficients))
Rcoeffs<- sort(t(fitR$coefficients))

Log.info("Print model statistics for R and H2O... \n")
#Log.info(paste("H2O Deviance  : ", fitH2O@model$deviance,      "\t\t", "R Deviance   : ", fitR$deviance))
#Log.info(paste("H2O Null Dev  : ", fitH2O@model$null.deviance, "\t\t", "R Null Dev   : ", fitR$null.deviance))
#Log.info(paste("H2O residul df: ", fitH2O@model$df.residual,    "\t\t\t\t", "R residual df: ", fitR$df.residual))
#Log.info(paste("H2O null df   : ", fitH2O@model$df.null,       "\t\t\t\t", "R null df    : ", fitR$df.null))
Log.info(paste("H2O coefficients  : ", H2Ocoeffs,      "\t\t", "R coefficients   : ", Rcoeffs))


Log.info("Compare model coefficients in R to model statistics in H2O")

#expect_equal(fitH2O@model$null.deviance, fitR$null.deviance, tolerance = 0.01)
#expect_equal(fitH2O@model$deviance, fitR$deviance, tolerance = 0.01)
#expect_equal(fitH2O@model$df.residual, fitR$df.residual, tolerance = 0.01)
#expect_equal(fitH2O@model$df.null, fitR$df.null, tolerance = 0.01)
expect_equal(H2Ocoeffs, Rcoeffs, tolerance = 0.01)


  testEnd()
}

doTest("GLM Test: Golden GLM2 - Tweedie zero inf poisson", test.glm2tweedie.golden)




