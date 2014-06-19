setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.GLM.lambda.search <- function(conn) {
  Log.info("Importing prostate.csv data...\n")
  prostate.hex = h2o.uploadFile(conn, locate("smalldata/logreg/prostate.csv"), key = "prostate.hex")
  prostate.sum = summary(prostate.hex)
  print(prostate.sum)
  
  # GLM without lambda search, lambda is single user-provided value
  Log.info("H2O GLM (binomial) with parameters: lambda_search = TRUE, nfolds: 2\n")
  prostate.nosearch = h2o.glm(x = 3:9, y = 2, data = prostate.hex, family = "binomial", nlambda = 50, lambda_search = FALSE, nfolds = 2)
  params.nosearch = prostate.nosearch@model$params
  expect_equal(length(params.nosearch$lambda_all), 1)
  expect_equal(params.nosearch$lambda, params.nosearch$lambda_all)
  expect_error(h2o.getGLMLambdaModel(prostate.nosearch, 0.5))
  
  # GLM with lambda search, return only model corresponding to best lambda as determined by H2O
  Log.info("H2O GLM (binomial) with parameters: lambda_search: TRUE, return_all_lambda: FALSE, nfolds: 2\n")
  prostate.bestlambda = h2o.glm(x = 3:9, y = 2, data = prostate.hex, family = "binomial", nlambda = 50, lambda_search = TRUE, nfolds = 2)
  params.bestlambda = prostate.bestlambda@model$params
  
  random_lambda = sample(params.bestlambda$lambda_all, 1)
  Log.info(cat("Retrieving model corresponding to randomly chosen lambda", random_lambda, "\n"))
  random_model = h2o.getGLMLambdaModel(prostate.bestlambda, random_lambda)
  expect_equal(random_model@model$params$lambda, random_lambda)
  
  Log.info(cat("Retrieving model corresponding to best lambda", params.bestlambda$best_lambda, "\n"))
  best_model = h2o.getGLMLambdaModel(prostate.bestlambda, params.bestlambda$best_lambda)
  expect_equal(best_model@model, prostate.bestlambda@model)
  
  # GLM with lambda search, return models corresponding to all lambda searched over
  Log.info("H2O GLM (binomial) with parameters: lambda_search: TRUE, return_all_lambda: TRUE, nfolds: 2\n")
  prostate.search = h2o.glm(x = 3:9, y = 2, data = prostate.hex, family = "binomial", nlambda = 50, lambda_search = TRUE, return_all_lambda = TRUE, nfolds = 2)
  
  # lambda_all = prostate.search[[1]]@model$params$lambda_all
  lambda_all = sapply(prostate.search, function(x) { x@model$params$lambda })
  lambda_idx = sample(1:length(lambda_all), 1)
  random_lambda = lambda_all[[lambda_idx]]
  
  Log.info(cat("Retrieving model corresponding to randomly chosen lambda", random_lambda, "\n"))
  random_model = h2o.getGLMLambdaModel(prostate.search, random_lambda)
  expect_equal(random_model@model$params$lambda, random_lambda)
  expect_equal(random_model@model, prostate.search[[lambda_idx]]@model)
  
  testEnd()
}

doTest("GLM Lambda Search Test: Prostate", test.GLM.lambda.search)