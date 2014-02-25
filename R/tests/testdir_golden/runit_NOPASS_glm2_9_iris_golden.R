setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.glm2glmnetreg.golden <- function(H2Oserver) {
	
#Import data: 
Log.info("Importing iris data...") 
irisH2O<- h2o.uploadFile(H2Oserver, locate("../smalldata/iris/iris2.csv"), key="irisH2O")
irisR<- read.csv(locate("../smalldata/iris/iris.csv"), header=T)



#fit R model in glmnet with regularization
x<- as.matrix(irisR[,1:4])
y<- as.matrix(irisR[,5])
fitR<- glmnet(x, y, family="gaussian", nlambda=1, alpha=1, lambda=2)

#fit corresponding H2O model
fitH2O<- h2o.glm.FV(x=c("Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width"), y="response", family="gaussian", nfolds=0, alpha=1, lambda=2, data=irisH2O)

#test that R coefficients and basic descriptives are equal
Rcoeffs<- sort(as.matrix(coefficients(fitR)))
H2Ocoeffs<- sort(fitH2O@model$coefficients)
Log.info(paste("H2O Coeffs  : ", H2Ocoeffs,  "\t\t\t", "R Coeffs  :", Rcoeffs))
expect_equal(H2Ocoeffs, Rcoeffs, tolerance = 0.03)
H2Oratio<- 1-(fitH2O@model$deviance/fitH2O@model$null.deviance)
Log.info(paste("H2O Deviance  : ", fitH2O@model$deviance,      "\t\t\t", "R Deviance   : ", deviance(fitR)))
Log.info(paste("H2O Null Dev  : ", fitH2O@model$null.deviance, "\t\t\t", "R Null Dev   : ", fitR$nulldev))
Log.info(paste("H2O Dev Ratio  : ", H2Oratio, "\t\t", "R Dev Ratio   : ", fitR$dev.ratio))
expect_equal(fitH2O@model$deviance, deviance(fitR), tolerance = 0.01)
expect_equal(fitH2O@model$null.deviance, fitR$nulldev, tolerance = 0.01)
expect_equal(H2Oratio, fitR$dev.ratio, tolerance = 0.01)


   testEnd()
}

doTest("GLM Test: Regularization", test.glm2glmnetreg.golden)

