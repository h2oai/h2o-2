## This script demonstrates how to compute MSE for classification problems
## All the data manipulations happen in H2O
## For simplicity, models are trained with default options, without validation data, and the training MSE is computed

## Start H2O
library(h2o)
h2oServer = h2o.init(nthreads = -1)

## Import data
iris_hex <- as.h2o(h2oServer,iris) # this upload iris from R to H2O, use h2o.importFile() for big data!
response <- 5
predictors <- colnames(iris_hex[,-response])
classes <- levels(iris_hex[,response])
print(classes)

## Compute actual per-row class probabilities (1 or 0)
resp_hex <- iris_hex[,response]
actual_hex <- resp_hex == classes[1]
for (level in levels(resp_hex)[2:length(classes)]) {
  actual_hex <- cbind(actual_hex, resp_hex==level)
}
summary(actual_hex)

## Train H2O classifiers
models <- list()
models <- c(models, h2o.deeplearning(x = predictors, y = response, data = iris_hex))
models <- c(models, h2o.randomForest(x = predictors, y = response, data = iris_hex))
models <- c(models, h2o.randomForest(x = predictors, y = response, data = iris_hex, type="BigData"))
models <- c(models, h2o.gbm(x = predictors, y = response, data = iris_hex))
models <- c(models, h2o.naiveBayes(x = predictors, y = response, data = iris_hex))

## Report Training MSE for all models
for (model in models) {
  ## Make predictions, extract probabilities
  train_preds_hex <- h2o.predict(model, iris_hex)[,-1] # h2o.predict returns N+1 columns: label + N probabilities
  
  mse <- 0
  for (i in 1:length(classes)) {
    mse <- mse + mean((train_preds_hex[,i] - actual_hex[,i])^2)
  }
  print(paste0(model@key, " <---- MSE: ", mse))
}

