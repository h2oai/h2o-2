source('./Utils/h2oR.R')

Log.info("\n======================== Begin Test ===========================\n")


#import cuseexpanded data set to R; import to H2O and parse as FV
test.glm2factors.numeric <- function(H2Oserver) {
  
  Log.info("Importing cuseexpanded.csv data...")
  cuse.hex<- h2o.importFile(H2Oserver, "./smalldata/cuseexpanded.csv")
  cuse<- read.csv("../../smalldata/cuseexpanded.csv", header=T)
  
#run binomial regression on data in R for baseline. Expanded data set has binomial expansions, and factors. Factors are Age, Ed, Wantsmore.
  
  Log.info("Run GLM Binomial in R...")
  cusefactors.glm<- glm(UsingBinom ~ Age + Ed + Wantsmore, family=binomial, data=cuse)
  print(cusefactors.glm)
 
#run h2o GLM2 binomial with no CV, no regularization

  Log.info("Run H2O GLM2 on cuse data...")
  cuseH2O.glm<- h2o.glm.FV(x=c("Age", "Ed"  "Wantsmore"), y="UsingBinom", data=cuse.hex, family="binomial", nfolds=0, alpha=0, lambda=0)
  print(cuseH2O.glm)
  

#check produced values against known values
  
  Log.info("Check that the descriptives from H2O matches known good values... ")
  
#check that number of coeffs produced match expected number (3 levels of age, 1 level of education, 1 level of wants more, 1 intercept = 6)

  expect_that(length(cuseH2O.glm@model$coefficients), equals(6))

#check that descriptive statistics match known good values  
expected.df.resid<- 1602
expected.df.null<- 1607
expected.dev.null<- 2006
expected.dev.resid<-1870
expected.aic<- 1882

  expect_that(cuseH2O.glm@model$df.residual, equals(expected.df.resid))
  expect_that(cuseH2O.glm@model$df.null, equals(expected.df.null))  
  expect_equal(object=cuseH2O.glm@model$deviance, expected=expected.dev.null<- 2006, tolerance=.02, scale=expected.dev.null)
  expect_equal(object=cuseH2O.glm@model$null.deviance, expected=expected.dev.null<- 2006, tolerance=.02, scale=expected.dev.null)
  expect_equal(object=cuseH2O.glm@model$deviance, expected=expected.dev.null<- 2006, tolerance=.02, scale=expected.dev.null)
  expect_equal(object=cuseH2O.glm@model$null.deviance, expected=expected.dev.resid, tolerance=.01, scale=expected.dev.resid) 
  expect_equal(object=cuseH2O.glm@model$aic, expected=expected.aic, tolerance=.01, scale=expected.aic)  
 
  
 
  print("End of test.")
}

H2Oserver <- new("H2OClient", ip=myIP, port=myPort)
tryCatch(test_that("glm2tests",test.glm2factors.numeric(H2Oserver)), error = function(e) FAIL(e))
PASS()




