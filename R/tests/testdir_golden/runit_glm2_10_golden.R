setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test.glm2glmnet.golden <- function(H2Oserver) {
	
#Import data: 
Log.info("Importing TREES data...") 
treesH2O<- h2o.uploadFile.VA(H2Oserver, locate("../smalldata/trees.csv"), key="treesH2O")
treesR<- read.csv(locate("../smalldata/trees.csv"), header=T)


#fit R models 1R:standard GLM, 2R:glmnet with no regularization
fit1R<- glm(Girth ~ Height + Volume, family=gaussian, data=treesR)
x<- as.matrix(treesR[,3:4])
y<- as.matrix(treesR[,2])
fit2R<- glmnet(x, y, family="gaussian", nlambda=1, lambda=0)

#test that R coefficients and basic descriptives are equal
R1coeffs<- sort(t(fit1R$coefficients))
R2coeffs<- sort(as.matrix(coefficients(fit2R)))
Log.info(paste("R1 Coeffs  : ", R1coeffs,      "\t\t", "R2 Coeffs   : ", R2coeffs))
expect_equal(R1coeffs, R2coeffs, tolerance = 0.01)
expect_equal(fit1R$null.deviance, fit2R$nulldev, tolerance = 0.01)
expect_equal(fit1R$deviance, deviance(fit2R), tolerance = 0.01)

#fit corresponding non-regularized H2O model
fitH2O<- h2o.glm(x=c("Height", "Volume"), y="Girth", family="gaussian", nfolds=0, alpha=0, lambda=0, data=treesH2O)

#Compare H2O non reglarized model to glmnet non regularized model
#H2Oratio<- 1-(fitH2O@model$deviance/fitH2O@model$null.deviance)
#Log.info(paste("H2O Deviance  : ", fitH2O@model$deviance,      "\t\t\t", "R Deviance   : ", deviance(fit2R)))
#Log.info(paste("H2O Null Dev  : ", fitH2O@model$null.deviance, "\t\t\t", "R Null Dev   : ", fit2R$nulldev))
#Log.info(paste("H2O Dev Ratio  : ", H2Oratio, "\t\t", "R Dev Ratio   : ", fit2R$dev.ratio))



#Compare H2O and GLMnet coeffs
#H2Ocoeffs<- sort(fitH2O@model$coefficients)
#Log.info(paste("H2O Coeffs  : ", H2Ocoeffs,  "\t\t\t", "R Coeffs  :", R2coeffs))


   testEnd()
}

doTest("GLM Test: Regularization", test.glm2glmnet.golden)

