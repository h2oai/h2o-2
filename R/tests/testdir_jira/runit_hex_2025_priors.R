###############################################################
######## Test Priors Probability for a Class for GLM  #########
###############################################################
#setwd("/Users/amy/h2o/R/tests/testdir_jira")

setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.priors <- function(conn) {
  Log.info("Import modelStack data into H2O...")
  ## Import data
  homeDir = "/mnt/0xcustomer-datasets/c27/"
  pathToFile = paste0(homeDir, "data.csv")
  pathToConstraints <- paste0(homeDir, "constraints_indices.csv")
  modelStack = h2o.importFile(conn, pathToFile)
  betaConstraints.hex = h2o.importFile(conn, pathToConstraints)
  
  ## Set Parameters (default standardization = T)
  betaConstraints = as.data.frame(betaConstraints.hex)
  indVars =  as.character(betaConstraints$names[1:nrow(betaConstraints)-1])
  depVars = "C3"
  totRealProb=0.002912744
  higherAccuracy = TRUE
  lambda = 0
  alpha = 0
  family_type = "binomial"
  
  ## Take subset of data
  Log.info("Subset dataset to only predictor and response variables...")
  data.hex = modelStack[,c(indVars, depVars)]
  summary(data.hex)
    
  ## Run full H2O GLM with and without priors
  Log.info("Run a logistic regression with no regularization and alpha = 0 and beta constraints without priors. ")
  glm_nopriors.h2o = h2o.glm(x = indVars, y = depVars, data = data.hex, family = family_type, use_all_factor_levels = T,
                    lambda = 0, higher_accuracy = T,
                    alpha = alpha, beta_constraints = betaConstraints.hex)
  Log.info("Run a logistic regression with no regularization and alpha = 0 and beta constraints with prior = total real probability. ")
  glm_priors.h2o = h2o.glm(x = indVars, y = depVars, data = data.hex, family = family_type, prior = totRealProb, use_all_factor_levels = T,
                           lambda = 0, higher_accuracy = T,
                           alpha = alpha, beta_constraints = betaConstraints.hex)
  
  
  ## Check coefficients remained the same and the intercept is adjusted
  coeff1 = glm_priors.h2o@model$coefficients[-ncol(data.hex)]
  coeff2 = glm_nopriors.h2o@model$coefficients[-ncol(data.hex)]
  intercept1 = glm_priors.h2o@model$coefficients["Intercept"]
  intercept2 = glm_nopriors.h2o@model$coefficients["Intercept"]
  print("Coefficients from GLM ran with priors: ")
  print(coeff1)
  print("Coefficients from GLM ran without priors: ")
  print(coeff2)
  ymean = mean(data.hex[,depVars])
  adjustment = -log(ymean*(1-totRealProb)/(totRealProb*(1-ymean)))
  intercept2adj = intercept1-adjustment
  checkEqualsNumeric(coeff1, coeff2, tolerance = 0)
  checkEqualsNumeric(intercept2, intercept2adj, tolerance = 1E-10)  
  testEnd()
}

doTest("GLM Test: Beta Constraints with Priors", test.priors)




