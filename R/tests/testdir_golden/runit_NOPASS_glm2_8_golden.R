setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test.glm2regularizedvanilla.golden <- function(H2Oserver) {
	
#Import data: 
Log.info("Importing mtcars data...") 
mtcarsH2O<- h2o.uploadFile(H2Oserver, locate("../smalldata/mtcars.csv"), key="mtcarsH2O")
mtcarsR<- read.csv(locate("../smalldata/mtcars.csv"), header=T)


#fit matching R and H2O regularized models
library(glmnet)
regfitH2O<- h2o.glm.FV(x=c("cyl", "disp", "hp", "drat", "wt", "qsec", "vs", "am", "gear", "carb"), y="mpg", data=mtcarsH2O, nfolds=0, family="gaussian", alpha=0, lambda=1.23)
y<- as.matrix(mtcarsR[,1])
x<- as.matrix(mtcarsR[,2:11])
regfitR<- glmnet(x, y, family="gaussian", alpha=0, nlambda=1, lambda=1.23)



#Print Model Coefficents and Descriptive Statistics
H2Oregcoeffs<- sort(t(regfitH2O@model$coefficients))
Rregcoeffs<- sort(t(as.matrix(coefficients(regfitR))))
H2Oratio<- 1-(regfitH2O@model$deviance/regfitH2O@model$null.deviance)
Log.info(paste("H2O Coeffs  : ", H2Oregcoeffs,      "\t\t", "R Coeffs   : ", Rregcoeffs))
Log.info(paste("H2O Deviance  : ", regfitH2O@model$deviance,      "\t\t", "R Deviance   : ", deviance(regfitR)))
Log.info(paste("H2O Null Dev  : ", regfitH2O@model$null.deviance, "\t\t\t", "R Null Dev   : ", regfitR$nulldev))
Log.info(paste("H2O Dev Ratio  : ", H2Oratio, "\t\t", "R Dev Ratio   : ", regfitR$dev.ratio))

#Compare descriptive statistics
Log.info("Compare model coefficients in R to model statistics in H2O")
expect_equal(regfitH2O@model$null.deviance, regfitR$nulldev, tolerance = 0.01)
expect_equal(regfitH2O@model$deviance, deviance(regfitR), tolerance = 0.01)
expect_equal(H2Oratio, regfitR$dev.ratio, tolerance = 0.01)
expect_equal(H2Oregcoeffs, Rregcoeffs, tolerance = 0.01)

   testEnd()
}

doTest("GLM Test: Regularized Vanilla", test.glm2regularizedvanilla.golden)

