setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"-f")))
source('../findNSourceUtils.R')

test.glm2vanilla.golden <- function(H2Oserver) {
	
#Import data: 
Log.info("Importing Swiss data...") 
swissH2O.VA<- h2o.uploadFile.VA(H2Oserver, locate("../smalldata/swiss.csv"), key="swissH2O")
swissH2O.FV<- h2o.uploadFile.FV(H2Oserver, locate("../smalldata/swiss.csv"), key="swissH2OFV")

Log.info("Test H2O treatment vanilla GLM - continuious real predictors, gaussian family between GLM1 and GLM2")
Log.info("Run matching models plain vanilla")
fitH2O.VA<- h2o.glm(x=c("Agriculture", "Examination", "Education", "Catholic", "Infant.Mortality"), y="Fertility", lambda=0, alpha=0, nfolds=0, family="gaussian", data=swissH2O.VA)
fitH2O.FV<- h2o.glm.FV(x=c("Agriculture", "Examination", "Education", "Catholic", "Infant.Mortality"), y="Fertility", lambda=0, alpha=0, nfolds=0, family="gaussian", data=swissH2O.FV)

swissH2O.VA.R<- swissH2O.VA[,-1]
swissH2O.FV.R<- swissH2O.FV[,-1]

fitH2O.VA.R<- h2o.glm(x=c("Agriculture", "Examination", "Education", "Catholic", "Infant.Mortality"), y="Fertility", lambda=0, alpha=0, nfolds=0, family="gaussian", data=swissH2O.VA.R)
fitH2O.FV.R<- h2o.glm.FV(x=c("Agriculture", "Examination", "Education", "Catholic", "Infant.Mortality"), y="Fertility", lambda=0, alpha=0, nfolds=0, family="gaussian", data=swissH2O.FV.R)



   testEnd()
}

doTest("GLM Test: Golden GLM2 - Vanilla Gaussian", test.glm2vanilla.golden)

