# Demo using R scripts to compare with H2O
# To invoke, need R 3.0.1 as of now
# R -f H2OTestComparison.R
library(randomForest)

prostate.data = read.csv("../smalldata/logreg/prostate.csv", header = TRUE)
prostate.sum2 = summary(prostate.data)
print(prostate.sum2)
prostate.glm2 = glm(CAPSULE ~ AGE + RACE + PSA + DCAPS, data = prostate.data, family = binomial)
print(prostate.glm2)
prostate.km2 = kmeans(prostate.data, centers = 5)
print(prostate.km2)

iris.data = read.csv("../smalldata/iris/iris.csv", header = FALSE)
iris.sum2 = summary(iris.data)
print(iris.sum2)
iris.rf2 = randomForest(V5 ~ ., data = iris.data, ntree = 50)
print(iris.rf2)

covtype.data = read.csv("../smalldata/covtype/covtype.20k.data", header = FALSE)
covtype.sum2 = summary(covtype.data)
print(covtype.sum2)
covtype.km2 = kmeans(covtype.data, centers = 10)
print(covtype.km2)