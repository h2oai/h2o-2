## This test is to check the beta contraint argument for GLM
## The test will import the prostate data set,
## runs glm with and without beta contraints which will be checked
## against glmnet's results.

#setwd("/Users/amy/h2o/R/tests/testdir_jira")
setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../findNSourceUtils.R')

test.GLM.betaConstraints <- function(conn) {
  
  Log.info("Importing prostate dataset...")
  data.hex = h2o.importFile(
    object = conn,system.file("extdata", "prostate.csv", package = "h2o"))
  
  myX =  c("AGE","RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
  myY = "CAPSULE"
  Log.info("Create default beta constraints frame...")
  lowerbound = rep(-1, times = length(myX))
  upperbound = rep(1, times = length(myX))
  betaConstraints = data.frame(names = myX, lower_bounds = lowerbound, upper_bounds = upperbound)
  betaConstraints.hex = as.h2o(conn, betaConstraints, key = "betaConstraints.hex")  

  Log.info("Pull data frame into R to run GLMnet...")
  data = as.data.frame(data.hex)
  Log.info("Prep Data Frame for run in GLMnet, includes categorical expansions...")
  xDataFrame = cbind(data[,myX], rep(0, times = nrow(data.hex)))
  names(xDataFrame) = c(myX, "Intercept")
  
  
  ########### run_glm function to run glm over different parameters
  ########### we want to vary family, alpha, standardization, beta constraint bounds  
  run_glm <- function(  family_type = "gaussian",
                        alpha = 0.5,
                        standardization = T,
                        lower_bound,
                        upper_bound
                        ) {
    Log.info(paste("Set Beta Constraints :", "lower bound =", lower_bound,"and upper bound =", upper_bound, "..."))
    betaConstraints.hex = as.h2o(conn, betaConstraints, key = "betaConstraints.hex")
    betaConstraints.hex$upper_bounds = upper_bound
    betaConstraints.hex$lower_bounds = lower_bound
    
    Log.info(paste("Run H2O's GLM with :", "family =", family_type, ", alpha =", alpha, ", standardization =", standardization, "..."))
    glm_constraints.h2o = h2o.glm(x = myX, y = myY, data = data.hex, standardize = standardization, higher_accuracy = T,
                                  family = family_type, alpha = alpha , beta_constraints = betaConstraints.hex)
    lambda = glm_constraints.h2o@model$lambda
    
    Log.info(paste("Run GLMnet with the same parameters, using lambda =", lambda))
    glm_constraints.r = glmnet(x = as.matrix(xDataFrame), alpha = alpha, lambda = lambda, standardize = standardization,
                               y = data[,myY], family = family_type, lower.limits = lower_bound, upper.limits = upper_bound)
    checkGLMModel2(glm_constraints.h2o, glm_constraints.r)
  }
  
  families = c("gaussian", "binomial", "poisson")
  alpha = c(0,0.5,1.0)
  standard = c(T, F)

  grid = expand.grid(families, alpha, standard)
  names(grid) = c("Family", "Alpha", "Standardize")
  
  a <- mapply(run_glm, as.character(grid[,1]), grid[,2], grid[,3], rep(-1,nrow(grid)), rep(1, nrow(grid)))
  testResults <- cbind(grid,Passed = a)
  print("TEST RESULTS FOR PROSTATE DATA SET:")
  print(testResults)  
  
#   b <- mapply(run_glm, as.character(grid[,1]), grid[,2], grid[,3], rep(-1,nrow(grid)), rep(0, nrow(grid)))
#   t <- cbind(grid,Passed = b)
#   print("TEST RESULTS FOR PROSTATE DATA SET with bounds [-1,0] : ")
#   print(t)  
# 
#   c <- mapply(run_glm, as.character(grid[,1]), grid[,2], grid[,3], rep(0,nrow(grid)), rep(1, nrow(grid)))
#   t <- cbind(grid,Passed = c)
#   print("TEST RESULTS FOR PROSTATE DATA SET with bounds [0,1] : ")
#   print(t)  
#   
#   d <- mapply(run_glm, as.character(grid[,1]), grid[,2], grid[,3], rep(-0.1,nrow(grid)), rep(0.1, nrow(grid)))
#   t <- cbind(grid,Passed = d)
#   print("TEST RESULTS FOR PROSTATE DATA SET with bounds [-0.1,0.1] : ")
#   print(t)  
  
  Log.info("Run same test over different dataset.")
  Log.info("Import modelStack data into H2O...")
  ## Import data
  homeDir = "/mnt/0xcustomer-datasets/c27/"
  pathToFile = paste0(homeDir, "data.csv")
  pathToConstraints <- paste0(homeDir, "constraints_indices.csv")
  data.hex = h2o.importFile(conn, pathToFile)
  betaConstraints.hex = h2o.importFile(conn, pathToConstraints)[1:22, 1:3]
  
  ## Set Parameters (default standardization = T)
  betaConstraints = as.data.frame(betaConstraints.hex)
  myX =  as.character(betaConstraints$names)
  myY = "C3"
  totRealProb=0.002912744  
  
  
  Log.info("Pull data frame into R to run GLMnet...")
  data = as.data.frame(data.hex)
  Log.info("Prep Data Frame for run in GLMnet, includes categorical expansions...")
  xDataFrame = cbind(data[,myX], rep(0, times = nrow(data)))
  names(xDataFrame) = c(myX, "Intercept")
  
  families = c("gaussian", "binomial", "poisson")
  alpha = c(0,0.5,1.0)
  standard = c(T, F)
  
  grid = expand.grid(families, alpha, standard)
  names(grid) = c("Family", "Alpha", "Standardize")
  
  fullTest <- mapply(run_glm, as.character(grid[,1]), grid[,2], grid[,3], rep(-10,nrow(grid)), rep(10, nrow(grid)))
  testResults <- cbind(grid,Passed = fullTest)
  print("RESULTS FOR RUNS ON PROSTATE DATASET :")
  print(testResults)    

  testEnd()
}

doTest("GLM Test: GLM w/ Beta Constraints", test.GLM.betaConstraints)

