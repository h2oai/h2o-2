# Demo to test R functionality
# To invoke, need R 3.0.1 as of now
# R -f H2OTestDemo.R
source("H2O.R")
h2o = new("H2OClient", ip="localhost", port=54321)

# Test using prostate cancer data set
prostate.hex = importURL(h2o, "http://www.stanford.edu/~anqif/prostate.csv", "prostate.hex")
prostate.sum = summary(prostate.hex)
print(prostate.sum)
prostate.glm = h2o.glm(y = "CAPSULE", x = "AGE,RACE,PSA,DCAPS", data = prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)
print(prostate.glm)
prostate.km = h2o.kmeans(prostate.hex, 5)
print(prostate.km)

# Test of random forest using iris data set
iris.hex = importFile(h2o, "../smalldata/iris/iris.csv", "iris.hex")
iris.sum = summary(iris.hex)
print(iris.sum)
iris.rf = h2o.randomForest(y = "4", data = iris.hex, ntree = 50)
print(iris.rf)

# Test of k-means using random Gaussian data set
covtype.hex = importFile(h2o, "../smalldata/covtype/covtype.20k.data")
covtype.sum = summary(covtype.hex)
print(covtype.sum)
covtype.km = h2o.kmeans(covtype.hex, 10)
print(covtype.km)