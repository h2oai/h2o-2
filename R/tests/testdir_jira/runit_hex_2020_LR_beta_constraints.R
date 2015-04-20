## This test is to check the beta contraint argument for GLM
## The test will import the prostate data set,
## runs glm with and without beta contraints which will be checked
## against glmnet's results.

#setwd("/Users/amy/h2o/R/tests/testdir_jira")
setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.LR.betaConstraints <- function(conn) {
  
  Log.info("Importing prostate dataset...")
  prostate.hex = h2o.importFile(
    object = conn,system.file("extdata", "prostate.csv", package = "h2o"))
  
  Log.info("Create beta constraints frame...")
  myX =  c("AGE","RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
  lowerbound = rep(-1, times = length(myX))
  upperbound = rep(1, times = length(myX))
  betaConstraints = data.frame(names = myX, lower_bounds = lowerbound, upper_bounds = upperbound)
  prostate.csv = as.data.frame(prostate.hex)
  
  ######## Single variable CAPSULE ~ AGE in H2O and then R
  ## actual coeff for Age without constraints = -.00823
  Log.info("Run a Linear Regression with CAPSULE ~ AGE with bound beta->[0,1] in H2O...")
  beta_age = betaConstraints[betaConstraints$names == "AGE",]
  beta_age$lower_bounds = 0
  beta_age$upper_bounds = 1
  lr.h2o = h2o.glm(x = "AGE", y = "CAPSULE", data = prostate.hex, family = "gaussian", alpha = 0, beta_constraints = beta_age, standardize = T)
  lambda = lr.h2o@model$lambda
  
  Log.info("Run a Linear Regression with CAPSULE ~ AGE with bound beta->[0,1] in R...")
  intercept = rep(0, times = nrow(prostate.hex))
  xDataFrame = data.frame(AGE = prostate.csv[,"AGE"], Intercept = intercept)
  xMatrix_age = as.matrix(xDataFrame)
  lr.R = glmnet(x = xMatrix_age, alpha = 0., lambda = lr.h2o@model$lambda, standardize = T,
                y = prostate.csv[,"CAPSULE"], family = "gaussian", lower.limits = 0., upper.limits = 1.)
  checkGLMModel2(lr.h2o, lr.R)
  
  #### shift AGE coefficient by 0.002
  run_glm <- function(family_type) {
    Log.info("Test Beta Constraints with negative upper bound in H2O...")
    lower_bound = -0.008
    upper_bound = -0.002
    beta_age$lower_bounds = lower_bound
    beta_age$upper_bounds = upper_bound
    nrow_prior = nrow(prostate.hex)
    lr_negativeUpper.h2o = h2o.glm(x = "AGE", y = "CAPSULE", data = prostate.hex, family = family_type, alpha = 0, beta_constraints = beta_age, standardize = T)
    nrow_after = nrow(prostate.hex)
    if(!nrow_prior == nrow_after) stop("H2OParsedData object is being overwritten.")
    
    Log.info("Shift AGE column to reflect negative upperbound...")
    xDataFrame = data.frame(AGE = prostate.csv[,"AGE"]*(1+upper_bound), Intercept = intercept)
    xMatrix_age = as.matrix(xDataFrame)
    lr_negativeUpper.R = glmnet(x = xMatrix_age, alpha = 0., lambda = lr.h2o@model$lambda, standardize = T,
                                y = prostate.csv[,"CAPSULE"], family = family_type, lower.limits = lower_bound, upper.limits = 0.)  
    checkGLMModel2(lr_negativeUpper.h2o, lr_negativeUpper.R)
  }
  
  full_test <- sapply(c("binomial", "gaussian"), run_glm)
  print(full_test)
  testEnd()
}

doTest("GLM Test: LR w/ Beta Constraints", test.LR.betaConstraints)

