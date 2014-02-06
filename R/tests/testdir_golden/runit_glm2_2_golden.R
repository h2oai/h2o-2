setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.glm2binregression2.golden <- function(H2Oserver) {
	
#Import data: 
Log.info("Importing CUSE data...") 
cuseH2O<- h2o.uploadFile.FV(H2Oserver, locate("../../smalldata/cuseexpanded.csv"), key="cuseH2O")
cuseR<- read.csv(locate("smalldata/cuseexpanded.csv"), header=T)

#Build matching models in R and H2O - R model specified to match H2O behavior for reference factor levels
Log.info("Test H2O treatment of FACTORS AS PREDICTORS - Coefficients")
Log.info("Run matching models in R and H2O")
fitH2O<- h2o.glm.FV(y="UsingBinom", x=c("Age", "Ed", "Wantsmore"), data=cuseH2O, family="binomial", lambda=0, alpha=0, nfolds=0)
fitR<- glm(UsingBinom ~ AgeA + AgeC + AgeD + LowEd + MoreYes, family=binomial, data=cuseR)


Log.info("Print model coefficients for R and H2O... \n")
H2Ocoeffs<- sort(t(fitH2O@model$coefficients))
Rcoeffs<- sort(t(fitR$coefficients))
Log.info(paste("H2O coefficients  : ", H2Ocoeffs,      "\t\t", "R coefficients   : ", Rcoeffs))


Log.info("Compare model coefficients in R to coefficients in H2O")
H2Ocoeffs<- sort(t(fitH2O@model$coefficients))
Rcoeffs<- sort(t(fitR$coefficients))
expect_equal(H2Ocoeffs, Rcoeffs, tolerance = 0.01)

  testEnd()
}

doTest("GLM Test: Golden GLM2 - Coefficients for factors as predictors", test.glm2binregression2.golden)

