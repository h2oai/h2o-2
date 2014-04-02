setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.glm2Ridge.golden <- function(H2Oserver) {
	
#Import data: 
Log.info("Importing HANDMADE data...") 
hmH2O<- h2o.uploadFile.VA(H2Oserver, locate("../smalldata/handmade.csv"))
hmR<- read.csv(locate("../smalldata/handmade.csv"), header=T)


#fit R model in glmnet with regularization
hmR[,8]<- hmR[,2]-mean(hmR[,2])
hmR[,9]<- hmR[,3]-mean(hmR[,3])
hmR[,10]<- hmR[,4]-mean(hmR[,4])
hmR[,11]<- hmR[,5]-mean(hmR[,5])
hmR[,12]<- hmR[,6]-mean(hmR[,6])
hmR[,13]<- hmR[,7]-mean(hmR[,7])

x<- as.matrix(hmR[,8:12])
y<- as.matrix(hmR[,13])
L=10/nrow(hmR)
fitRglmnet<- glmnet(x, y, family="gaussian", nlambda=1, alpha=0, lambda=L)


#fit corresponding H2O model

fitH2O<- h2o.glm.FV(x=c("V8", "V9", "V10", "V11", "V12"), y="V13", family="gaussian", nfolds=0, alpha=0, lambda=0.01, data=hmH2O)

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

doTest("GLM2 SimpleRidge", test.glm2Ridge.golden)