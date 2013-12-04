source('./Utils/h2oR.R')

Log.info("\n======================== Begin Test ===========================\n")


#import swiss data set to R; import to H2O and parse as FV
test.glm2linear.numeric <- function(H2Oserver) {
  Log.info("Importing swiss.csv data...")
  swiss.hex<- h2o.importFile(H2Oserver, "./smalldata/wonkysummary.csv")
  swiss.df<- read.csv("../../smalldata/wonkysummary.csv", header=T)
  
#run vanilla glm on swiss.df for comparrison
  Log.info("Run GLM in R...")
  swissR.glm<- glm(Fertility ~ Agriculture + Examination + Education + Catholic    + Infant.Mortality, family=gaussian, data=swiss.df)
  print(swissR.glm)
 
#run h2o GLM2 on swiss data

  Log.info("Run H2O GLM2 on swiss data...")
  swissH2O.glm<- h2o.glm.FV(x=c("Agriculture", "Examination", "Education",  "Catholic", "Infant.Mortality"), y="Fertility", data=swiss.hex,  family="gaussian", nfolds=0, alpha=0, lambda=0)
  print(swissH2O.glm)
  

#check produced values against known values
  Log.info("Check that the descriptives from H2O matches known good values... ")
  library(testthat)
  print(swiss.h2o@model$deviance)
  print(swiss.h2o@model$null.deviance)
  print(swiss.h2o@model$df.residual)
  print(swiss.h2o@model$df.null)  
  
  expect_true(abs(swiss.h2o@model$deviance-7178)<(1))
  expect_true(abs(swiss.h2o@model$null.deviance-2105)<(1))
  expect_equal(object=swiss.h2o@model$df.residual, expected=41, tolerance=.01, scale=41)
  expect_equal(object=swiss.h2o@model$df.null, expected=46, tolerance=.01, scale=46)
  expect_equal(object=swiss.h2o@model$aic, expected=326, tolerance=.01, scale=326) 
  
 
  print("End of test.")
}

H2Oserver <- new("H2OClient", ip=myIP, port=myPort)
tryCatch(test_that("glm2tests",test.glm2linear.numeric(H2Oserver)), error = function(e) FAIL(e))
PASS()




