setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.glm1glmnetreg4.golden <- function(H2Oserver) {
	
#Import data: 
Log.info("Importing TREES data...") 
treesH2O<- h2o.uploadFile.VA(H2Oserver, locate("../smalldata/trees.csv"), key="treesH2O")
treesR<- read.csv(locate("../smalldata/trees.csv"), header=T)



#fit R model in glmnet with regularization
x<- as.matrix(treesR[,3:4])
y<- as.matrix(treesR[,2])
fitRglmnet<- glmnet(x, y, family="gaussian", nlambda=1, alpha=.7, lambda=.15)


#fit corresponding H2O model

fitH2O<- h2o.glm.VA(x=c("Height", "Volume"), y="Girth", family="gaussian", nfolds=0, alpha=.7, lambda=.15, data=treesH2O)

#test that R coefficients and basic descriptives are equal
Rcoeffsglmnet<- sort(as.matrix(coefficients(fitRglmnet)))
H2Ocoeffs<- sort(fitH2O@model$coefficients)
H2Ocoeffs<- as.data.frame(H2Ocoeffs)

#Log.info(paste("H2O Coeffs  : ", H2Ocoeffs,  "\t\t\t", "R GLMNET Coeffs  :", Rcoeffsglmnet))
expect_equal(H2Ocoeffs[1,1], Rcoeffsglmnet[1], tolerance = 0.1)
expect_equal(H2Ocoeffs[2,1], Rcoeffsglmnet[2], tolerance = 0.1)
expect_equal(H2Ocoeffs[3,1], Rcoeffsglmnet[3], tolerance = 0.1)
H2Oratio<- 1-(fitH2O@model$deviance/fitH2O@model$null.deviance)
Log.info(paste("H2O Deviance  : ", fitH2O@model$deviance,      "\t\t\t", "R Deviance   : ", deviance(fitRglmnet)))
Log.info(paste("H2O Null Dev  : ", fitH2O@model$null.deviance, "\t\t\t", "R Null Dev   : ", fitRglmnet$nulldev))
Log.info(paste("H2O Dev Ratio  : ", H2Oratio, "\t\t", "R Dev Ratio   : ", fitRglmnet$dev.ratio))
expect_equal(fitH2O@model$deviance, deviance(fitRglmnet), tolerance = 0.01)
expect_equal(fitH2O@model$null.deviance, fitRglmnet$nulldev, tolerance = 0.01)
expect_equal(H2Oratio, fitRglmnet$dev.ratio, tolerance = 0.01)


   testEnd()
}

doTest("GLM Test: Regularization: Alpha=.7, verif with penalized", test.glm1glmnetreg4.golden)

