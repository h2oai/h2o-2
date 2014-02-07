library(gbm)
library(h2o)
source("H2O_FV.R")

# Some helper functions to make code cleaner
macH2OPath = "/Users/anqi_fu/Documents/workspace/h2o"
getDataPath <- function(subPath) { normalizePath(paste(macH2OPath, subPath, sep="/")) }
predClass <- function(myData) { as.factor(colnames(myData)[apply(myData, 1, which.max)]) }
getConfusionMatrix <- function(Actual, Predicted) {
  if(length(Predicted) != length(Actual))
    stop("Mismatched dimensions between actual and predicted values")
  table(Actual, Predicted)
}
localH2O = new("H2OClient")

# Test GBM using covtype data
covtype.hex = h2o.FV.importFile(localH2O, path=getDataPath("smalldata/covtype/covtype.20k_wheader.csv"))
covtype.sum = h2o.FV.inspect(covtype.hex)
print(covtype.sum)
covtype.gbm = h2o.FV.GBM(y="Cover_Type", data=covtype.hex, ntree=10, learning.rate=0.1, max.depth=8)
print(covtype.gbm)

covtype.data = read.csv(getDataPath("smalldata/covtype/covtype.20k_wheader.csv"), header = TRUE)
covtype.rgbm = gbm(Cover_Type ~ . -Soil_Type7 -Soil_Type15, data=covtype.data, n.trees=10, shrinkage=0.1, interaction.depth=8, distribution="multinomial")
# summary(covtype.rgbm)
covtype.rgbm_sum = list()
covtype.rgbm_sum$train.err = covtype.rgbm$train.err
covtype.rgbm_sum$confusion = getConfusionMatrix(predClass(covtype.rgbm$estimator), covtype.data$Cover_Type)
print(covtype.rgbm_sum)

# Test GBM using mnist testing data
mnist.hex = h2o.FV.importFile(localH2O, path=getPath("smalldata/mnist/test.csv"))
mnist.sum = h2o.FV.inspect(mnist.hex)
print(mnist.sum)
mnist.gbm = h2o.FV.GBM(y=785, data=mnist.hex, ntree=10, learning.rate=0.1, max.depth=8)
print(mnist.gbm)

mnist.data = read.csv(getDataPath("smalldata/mnist/test.csv"), header = FALSE)
mnist.rgbm = gbm(V785 ~ ., data=mnist.data, n.trees=10, shrinkage=0.1, interaction.depth=8, distribution="multinomial")
mnist.rgbm_sum = list()
mnist.rgbm_sum$train.err = mnist.rgbm$train.err
mnist.rgbm_sum$confusion = getConfusionMatrix(predClass(mnist.rgbm$estimator), mnist.data$V785)
print(mnist.rgbm_sum)