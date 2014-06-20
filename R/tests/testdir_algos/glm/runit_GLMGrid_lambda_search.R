setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

test.GLMGrid.lambda.search <- function(conn) {
  Log.info("Importing prostate.csv data...\n")
  prostate.hex = h2o.uploadFile(conn, locate("smalldata/logreg/prostate.csv"), key = "prostate.hex")
  prostate.sum = summary(prostate.hex)
  print(prostate.sum)
  
  Log.info("H2O GLM (binomial) with parameters: alpha = c(0.25, 0.5), nlambda = 20, lambda_search = TRUE, nfolds: 2\n")
  prostate.bestlambda = h2o.glm(x = 3:9, y = 2, data = prostate.hex, family = "binomial", alpha = c(0.25, 0.5), nlambda = 20, lambda_search = TRUE, nfolds = 2)
  model_idx = ifelse(runif(1) <= 0.5, 1, 2)
  model.bestlambda = prostate.bestlambda@model[[model_idx]]
  params.bestlambda = model.bestlambda@model$params
  
  # Log.info(cat("All lambda values searched over:\n", params.bestlambda$lambda_all))
  # expect_equal(length(params.bestlambda$lambda_all), 20)
  
  # random_lambda = sample(params.bestlambda$lambda_all, 1)
  # Log.info(cat("Retrieving model corresponding to alpha =", params.bestlambda$alpha, "and randomly chosen lambda", random_lambda, "\n"))
  # random_model = h2o.getGLMLambdaModel(model.bestlambda, random_lambda)
  # expect_equal(random_model@model$params$lambda, random_lambda)
  
  Log.info(cat("Retrieving model corresponding to alpha =", params.bestlambda$alpha, "and best lambda", params.bestlambda$best_lambda, "\n"))
  best_model = h2o.getGLMLambdaModel(model.bestlambda, params.bestlambda$lambda_best)
  expect_equal(best_model@model, model.bestlambda@model)
  
  Log.info("H2O GLM (binomial) with parameters: alpha = c(0.25, 0.5), nlambda = 20, lambda_search = TRUE, return_all_lambda = TRUE, nfolds: 2\n")
  prostate.search = h2o.glm(x = 3:9, y = 2, data = prostate.hex, family = "binomial", alpha = c(0.25, 0.5), nlambda = 20, lambda_search = TRUE, return_all_lambda = TRUE, nfolds = 2)
  model.search = prostate.search@model[[model_idx]][[1]]
  params.search = model.search@model$params
  
  # lambda_all = sapply(model.search, function(x) { x@model$params$lambda })
  # expect_equal(lambda_all, 20)
  # lambda_idx = sample(1:length(lambda_all), 1)
  # random_lambda = lambda_all[[lambda_idx]]
  
  # Log.info(cat("Retrieving model corresponding to randomly chosen lambda", random_lambda, "\n"))
  # random_model = h2o.getGLMLambdaModel(model.search, random_lambda)
  # expect_equal(random_model@model$params$lambda, random_lambda)
  # expect_equal(random_model@model, model.search[[lambda_idx]]@model)
  testEnd()
}

doTest("GLM Grid Lambda Search Test: Prostate", test.GLMGrid.lambda.search)