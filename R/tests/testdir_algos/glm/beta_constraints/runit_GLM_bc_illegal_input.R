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
  run_glm <- function(bc, data = modelStack) {
    h2o.glm(x = indVars, y = depVars, data = modelStack, family = family_type, use_all_factor_levels = T, key = "a",
            lambda = 0, higher_accuracy = T, alpha = alpha, beta_constraints = bc)
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

  Log.info("Used empty frame for beta constraints: ")
  empty <- betaConstraints.hex[betaConstraints.hex$names == "fake"]
  m1 <- run_glm(empty)
  m2 <- run_glm(NULL)
  checkEqualsNumeric(m1@model$deviance, m2@model$deviance)

#   Log.info("Typo in one of column names.")
#   c <- bc
#   names(c) <- gsub("lower_bounds", replacement = "lowerbounds", x = names(bc))
#   checkException(run_glm(c), "Did not detect one of the columns had a typo, GLM ran without lower bounds.", silent = T)

  Log.info("Change column to enum and try to input first level as beta constraints with use_all_factors = F.")
  # Choose column to use as categorical and convert column to enum column.
  cat_col = "C217"
  a = modelStack[,c(indVars,depVars)]
  a[,cat_col] = as.factor(a[,cat_col])

  bc_cat <- data.frame( names =  c( "C217.0","C217.1", "C217.2", "C217.3", "C217.6"), 
                      lower_bounds = rep(-10000,5), upper_bounds = rep(10000,5),
                      beta_given = c(0.1, -1, .5, 2.4, 1.5), 
                      rho = rep( 1, 5))
  bc_cat <- rbind(bc_cat, bc[!(bc$names == cat_col),])
  checkException(run_glm(bc_cat, data = a), "Did not block user from using first factor level in beta constraints.", silent = T)
#   Log.info("Bound was not expanded out for enum column, should reject.")
#   checkException(run_glm(bc, data = a), "Fail to reject name C217 even though column is enum.", silent = T) 

  testEnd()
}

doTest("GLM Test: Beta Constraints Illegal Argument Exceptions", test.bc.illegal)




