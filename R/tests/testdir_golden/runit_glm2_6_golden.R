setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.glm2asfactor.golden <- function(H2Oserver) {
	
#Import data: 
Log.info("Importing DRUGS data...") 
drugsH2O<- h2o.uploadFile.FV(H2Oserver, locate("../../smalldata/drugs.csv"), key="drugsH2O")
drugsR<- read.csv(locate("smalldata/drugs.csv"), header=T)

Log.info("Test H2O data treatment: as.factor and derived column")
drugsR[,1]<- as.factor(drugsR[,1])
drugsR[,2]<- as.factor(drugsR[,2])
drugsR[,6]<- drugsR[,4]-drugsR[,5]
drugsH2O[,1]<- as.factor(drugsH2O[,1])
drugsH2O[,2]<- as.factor(drugsH2O[,2])
drugsH2O[,6]<- drugsH2O[,4]-drugsH2O[,5]


Log.info("Run matching models in R and H2O")
fitH2O<- h2o.glm.FV(x=c("V1", "V2", "V3"), y="C6", data=drugsH2O, family="gaussian", alpha=0, lambda=0, nfolds=0)
fitR<- glm(V6 ~ V1 + V2 + V3, family=gaussian, data=drugsR)


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

doTest("GLM Test: Golden GLM2 - as.factor and derived col", test.glm2asfactor.golden)

