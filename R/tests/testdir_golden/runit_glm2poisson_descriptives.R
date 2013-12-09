source('./findNSourceUtils.R')

test.glm2poisson.descriptives <- function(H2Oserver) {
  Log.info("Importing cuse data...")
  h2ocuse<- h2o.importFile(H2Oserver, locate("./smalldata/cuse.data.csv", schema = "local")
  Rcuse<- read.csv(locate("../../smalldata/cuse.data.csv"), header=T)

#Specify model in R
  Log.info("Run Poisson family GLM in R...")
  Rfit<- glm(using ~ agelt25 + age25to29 + age30to39 + edlow + wantsmoreyes, family="poisson", data=cuse)
  print(swissR.glm)
  
#Specify model in H2O
  Log.info("Run H2O GLM2 on CUSE data...")
  H2Ofit<- h2o.glm.FV(x=c("agelt25", "age25to29", "age30to39", "edlow", "wantsmoreyes"), y="using", data= h2ocuse, alpha=0, lambda=0, nfolds=0, family="poisson")
  
#Check descriptive statistics from H2Omodel
  Log.info("Check that the descriptives from H2O matches known good values... \n")
  Log.info(paste("H2O Deviance  : ", H2Ofit@model$deviance,      "\t\t", "R Deviance   : ", Rfit$deviance))
  Log.info(paste("H2O Null Dev  : ", H2Ofit@model$null.deviance, "\t\t", "R Null Dev   : ", Rfit$null.deviance))
  Log.info(paste("H2O residul df: ", H2Ofit@model$df.residual,    "\t\t\t\t", "R residual df: ", Rfit$df.residual))
  Log.info(paste("H2O null df   : ", H2Ofit@model$df.null,       "\t\t\t\t", "R null df    : ", Rfit$df.null))
  Log.info(paste("H2O aic       : ", H2Ofit@model$aic,           "\t\t", "R aic        : ", Rfit$aic))

  expect_equal(H2Ofit@model$deviance, Rfit$deviance, tolerance = 0.01)
  expect_equal(H2Ofit@model$null.deviance, Rfit$null.deviance, tolerance = 0.01)
  expect_equal(H2Ofit@model$df.residual, Rfit$df.residual, tolerance = 0.01)
  expect_equal(H2Ofit@model$df.null, Rfit$df.null, tolerance = 0.01)
  expect_equal(H2Ofit@model$aic, Rfit$aic, tolerance = 0.01)
  
  testEnd()
}

doTest("GLM2 Numeric Golden Test", test.glm2poisson.descriptives)
