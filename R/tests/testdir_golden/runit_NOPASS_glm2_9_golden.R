setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.glm2glmnetregSIMPLECASE.golden <- function(H2Oserver) {
	
#Import data: 
Log.info("Importing TREES data...") 
treesH2O<- h2o.uploadFile(H2Oserver, locate("../smalldata/trees.csv"), key="treesH2O")
treesR<- read.csv(locate("../smalldata/trees.csv"), header=T)



#fit R model in glmnet with regularization
x<- as.matrix(treesR[,3:4])
y<- as.matrix(treesR[,2])
fitRglmnet<- glmnet(x, y, family="gaussian", nlambda=1, alpha=0, lambda=0.04010316)

#fit R model in penalized with regularization
library(penalized)
fitRpenalized<- penalized(response=y, penalized=x, lambda1=0, lambda2=2, fusedl=FALSE, model="linear")


#fit corresponding H2O model

fitH2O<- h2o.glm.FV(x=c("Height", "Volume"), y="Girth", family="gaussian", nfolds=0, alpha=0, lambda=0.04010316, data=treesH2O)

#test that R coefficients and basic descriptives are equal
Rcoeffsglmnet<- sort(as.matrix(coefficients(fitRglmnet)))
Rcoeffspenalized<- sort(as.matrix(coefficients(fitRpenalized)))
H2Ocoeffs<- sort(fitH2O@model$coefficients)
H2Ocoeffs<- as.data.frame(H2Ocoeffs)

Log.info(paste("H2O Coeffs  : ", H2Ocoeffs,  "\t\t\t", "R GLMNET Coeffs  :", Rcoeffsglmnet))
expect_equal(H2Ocoeffs[1,1], Rcoeffsglmnet[1], tolerance = 0.1)
expect_equal(H2Ocoeffs[2,1], Rcoeffsglmnet[2], tolerance = 0.1)
expect_equal(H2Ocoeffs[3,1], Rcoeffsglmnet[3], tolerance = 0.03)
expect_equal(H2Ocoeffs[1,1], Rcoeffspenalized[1], tolerance = 0.1)
expect_equal(H2Ocoeffs[2,1], Rcoeffspenalized[2], tolerance = 0.1)
expect_equal(H2Ocoeffs[3,1], Rcoeffspenalized[3], tolerance = 0.03)
H2Oratio<- 1-(fitH2O@model$deviance/fitH2O@model$null.deviance)
Log.info(paste("H2O Deviance  : ", fitH2O@model$deviance,      "\t\t\t", "R Deviance   : ", deviance(fitR)))
Log.info(paste("H2O Null Dev  : ", fitH2O@model$null.deviance, "\t\t\t", "R Null Dev   : ", fitR$nulldev))
Log.info(paste("H2O Dev Ratio  : ", H2Oratio, "\t\t", "R Dev Ratio   : ", fitR$dev.ratio))
expect_equal(fitH2O@model$deviance, deviance(fitRglmnet), tolerance = 0.01)
expect_equal(fitH2O@model$null.deviance, fitRglmnet$nulldev, tolerance = 0.01)
expect_equal(H2Oratio, fitRglmnet$dev.ratio, tolerance = 0.01)


   testEnd()
}

doTest("GLM Test: Regularization: Alpha=0, verif with penalized", test.glm2glmnetregSIMPLECASE.golden)

