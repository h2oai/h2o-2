###############################################################
###### Catch illegal input for GLM w/ Beta Constraints  #######
###############################################################
setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
# setwd("/Users/amy/h2o/R/tests/testdir_jira")
# source('../findNSourceUtils.R')
source('../../../findNSourceUtils.R')
conn <- h2o.init()
test.bc.illegal <- function(conn) {
  Log.info("Import modelStack data into H2O...")
  ## Import data
  homeDir = "/mnt/0xcustomer-datasets/c27/"
  pathToFile = paste0(homeDir, "data.csv")
  pathToConstraints <- paste0(homeDir, "constraints_indices.csv")
  modelStack = h2o.importFile(conn, pathToFile)
  betaConstraints.hex = h2o.importFile(conn, pathToConstraints)
  
  ## Set Parameters (default standardization = T)
  bc <- as.data.frame(betaConstraints.hex)
  indVars =  as.character(bc[1:nrow(bc)-1, "names"])
  depVars = "C3"
  totRealProb=0.002912744
  higherAccuracy = TRUE
  lambda = 0
  alpha = 0
  family_type = "binomial"
  
  ## Function to run GLM with specific beta_constraints
  run_glm <- function(bc) {
    h2o.glm(x = indVars, y = depVars, data = modelStack, family = family_type, use_all_factor_levels = T, key = "a",
            lambda = 0, higher_accuracy = T,
            alpha = alpha, beta_constraints = bc)
  }
    
  Log.info("Use beta constraints with same feature listed twice: ")  
  a <- rbind(bc[1,],bc)
  # Illegal Argument Exception: got duplicate constraints for 'C64'
  checkException(run_glm(a), "Did not catch duplicate constraints.", silent = T)
  
  Log.info("Use beta constraints with feature not in the feature set: ")
  b <- data.frame(names = "fakeFeature", lower_bounds = -10000, upper_bounds = 10000, beta_given = 1, rho =1)
  b <-  rbind(bc, b)
  # Illegal Argument Exception: uknown predictor name 'fakeFeature'
  checkException(run_glm(b), "Did not catch fake feature in file.", silent = T)

#   Log.info("Used empty frame for beta constraints: ")
#   empty <- betaConstraints.hex[betaConstraints.hex$names == "fake"]
#   checkException(run_glm(empty), "Did not reject empty frame.", silent = T)

#   Log.info("Typo in one of column names.")
#   c <- bc
#   names(c) <- gsub("lower_bounds", replacement = "lowerbounds", x = names(bc))
#   checkException(run_glm(c), "Did not detect one of the columns had a typo, GLM ran without lower bounds.", silent = T)

  testEnd()
}

doTest("GLM Test: Beta Constraints Illegal Argument Exceptions", test.bc.illegal)




