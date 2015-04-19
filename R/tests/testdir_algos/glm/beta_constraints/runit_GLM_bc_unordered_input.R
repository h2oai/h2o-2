###############################################################
#### Test Order of input features for Beta Constraints  #######
###############################################################
setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../../findNSourceUtils.R')

test.priors <- function(conn) {
  Log.info("Import modelStack data into H2O...")
  ## Import data
  homeDir = "/mnt/0xcustomer-datasets/c27/"
  pathToFile = paste0(homeDir, "data.csv")
  pathToConstraints <- paste0(homeDir, "constraints_indices.csv")
  modelStack = h2o.importFile(conn, pathToFile)
  bc = h2o.importFile(conn, pathToConstraints)
  
  ## Set Parameters (default standardization = T)
  indVars =  setdiff(as.matrix(bc$names)[,"names"], "Intercept")
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
    
  ## Run GLM
  run_glm <- function(data, beta_constraints){
    h2o.glm(x = indVars, y = depVars, data = data, family = family_type, prior = totRealProb, use_all_factor_levels = T,
            lambda = 0, higher_accuracy = T, alpha = alpha, beta_constraints = beta_constraints)
  }
    
  Log.info("Run GLM with original data and original constraints.")
  a = run_glm(data = modelStack, beta_constraints = bc)
  
  Log.info("Run GLM with reordered data and original constraints.")
  b = run_glm(data = data.hex, beta_constraints = bc)
  
  Log.info("Run GLM with reordered data and reordered beta constraints ")
  bc2 = rbind(bc[6:nrow(bc)], bc[1:5])
  c = run_glm(data = data.hex, beta_constraints = bc2)
  
  checkEqualsNumeric(sort(a@model$coefficients),sort(b@model$coefficients))
  checkEqualsNumeric(sort(b@model$coefficients),sort(c@model$coefficients))
  testEnd()
}

doTest("GLM Test: Beta Constraints with Priors", test.priors)




