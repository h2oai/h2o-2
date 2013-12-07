library(gbm)
library(h2o)
source("H2O_FV.R")

# Some helper functions to make code cleaner
# macH2OPath = "/Users/anqi_fu/Documents/workspace/h2o"
# getDataPath <- function(subPath) { normalizePath(paste(macH2OPath, subPath, sep="/")) }
predClass <- function(myData) { as.factor(colnames(myData)[apply(myData, 1, which.max)]) }
getConfusionMatrix <- function(Actual, Predicted) {
  if(length(Predicted) != length(Actual))
    stop("Mismatched dimensions between actual and predicted values")
  temp = table(Actual, Predicted)
  totals = apply(temp, 2, sum)                    # Sum total predicted of each class
  errors = 1-diag(temp)/apply(temp, 1, sum)       # Error rate for each actual class
  toterr = sum(totals - diag(temp))/sum(totals)   # Overall error rate
  
  temp = cbind(rbind(temp, Totals=totals), Errors=c(errors, toterr))
  dimnames(temp) = list(Actual=rownames(temp), Predicted=colnames(temp))
  temp
}
localH2O = new("H2OClient")

# Run GBM on Trulia training dataset on H2O
trulia.hex = h2o.FV.importFile(localH2O, path="/Users/anqi_fu/Documents/trulia_datasets/classification1Train.txt")
trulia.sum = h2o.FV.inspect(trulia.hex)
print(trulia.sum)
trulia.gbm = h2o.FV.GBM(y="outcome", data=trulia.hex, ntree=10, learning.rate=0.1, max.depth=8)
print(trulia.gbm)

# Run GBM on Trulia training dataset in R
trulia.data = read.csv("/Users/anqi_fu/Documents/trulia_datasets/classification1Train.txt", header = TRUE)
summary(trulia.data)
trulia.rgbm = gbm(outcome ~ .-hasBeenCalled, data=trulia.data, n.trees=10, shrinkage=0.1, interaction.depth=8, distribution="bernoulli")
trulia.rgbm_sum = list()
trulia.rgbm_sum$train.err = trulia.rgbm$train.err
trulia.cls = exp(trulia.rgbm$fit)/(1+exp(trulia.rgbm$fit))
trulia.rgbm_sum$confusion = getConfusionMatrix(as.numeric(trulia.cls > 0.5), trulia.data$outcome)
print(trulia.rgbm_sum)

# Predict on Trulia testing dataset in R
trulia.test = read.csv("/Users/anqi_fu/Documents/trulia_datasets/classification1Test.txt", header = TRUE)
summary(trulia.test)
trulia.rpred = predict(trulia.rgbm, newdata=trulia.test, n.trees=10, type="response")

plot(trulia.rgbm$train.err, type="l")
lines(c(0,trulia.gbm$train.err), type="l", col="red")