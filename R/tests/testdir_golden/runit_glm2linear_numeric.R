source('./findNSourceUtils.R')

#import swiss data set to R; import to H2O and parse as FV
test.glm2linear.numeric <- function(H2Oserver) {
  Log.info("Importing swiss.csv data...")
  swiss.hex<- h2o.importFile(H2Oserver, locate("./smalldata/swiss.csv", schema = "local"))
  swiss.df<- read.csv(locate("../../smalldata/swiss.csv"), header=T)
  
  #run vanilla glm on swiss.df for comparrison
  Log.info("Run GLM in R...")
  swissR.glm<- glm(Fertility ~ Agriculture + Examination + Education + Catholic    + Infant.Mortality, family=gaussian, data=swiss.df)
  print(swissR.glm)
 
  #run h2o GLM2 on swiss data
  Log.info("Run H2O GLM2 on swiss data...")
  swissH2O.glm<- h2o.glm.FV(x=c("Agriculture", "Examination", "Education",  "Catholic", "Infant.Mortality"), y="Fertility", data=swiss.hex,  family="gaussian", nfolds=0, alpha=0, lambda=0)
  print(swissH2O.glm)
  
  #check produced values against known values
  Log.info("Check that the descriptives from H2O matches known good values... \n")
  Log.info(paste("H2O Deviance  : ", swissH2O.glm@model$deviance,      "\t\t", "R Deviance   : ", swissR.glm$deviance))
  Log.info(paste("H2O Null Dev  : ", swissH2O.glm@model$null.deviance, "\t\t", "R Null Dev   : ", swissR.glm$null.deviance))
  Log.info(paste("H2O residul df: ",swissH2O.glm@model$df.residual,    "\t\t\t\t", "R residual df: ", swissR.glm$df.residual))
  Log.info(paste("H2O null df   : ", swissH2O.glm@model$df.null,       "\t\t\t\t", "R null df    : ", swissR.glm$df.null))
  Log.info(paste("H2O aic       : ", swissH2O.glm@model$aic,           "\t\t", "R aic        : ", swissR.glm$aic))

  expect_equal(swissH2O.glm@model$deviance, swissR.glm$deviance, tolerance = 0.01)
  expect_equal(swissH2O.glm@model$null.deviance, swissR.glm$null.deviance, tolerance = 0.01)
  expect_equal(swissH2O.glm@model$df.residual, swissR.glm$df.residual, tolerance = 0.01)
  expect_equal(swissH2O.glm@model$df.null, swissR.glm$df.null, tolerance = 0.01)
  expect_equal(swissH2O.glm@model$aic, swissR.glm$aic, tolerance = 0.01)

#  expect_true(abs(swissH2O.glm@model$deviance-7178)<(1))
#  expect_true(abs(swissH2O.glm@model$null.deviance-2105)<(1))
#  expect_equal(object=swissH2O.glm@model$df.residual, expected=41, tolerance=.01, scale=41)
#  expect_equal(object=swissH2O.glm@model$df.null, expected=46, tolerance=.01, scale=46)
#  expect_equal(object=swissH2O.glm@model$aic, expected=326, tolerance=.01, scale=326) 
 
  testEnd()
}

doTest("GLM2 Numeric Golden Test", test.glm2linear.numeric)

