# Demo to test R functionality
# To invoke, need R 2.15.0 or higher
# R -f H2OTestDemo.R
source("H2O.R")
# library(h2o)
h2o = new("H2OClient", ip="localhost", port=54321)

# Test using prostate cancer data set
prostate.hex = importURL(h2o, "https://raw.github.com/0xdata/h2o/master/smalldata/logreg/prostate.csv", "prostate.hex")
prostate.sum = summary(prostate.hex)
print(prostate.sum)
prostate.glm = h2o.glm(y = "CAPSULE", x = "AGE,RACE,PSA,DCAPS", data = prostate.hex, family = "binomial", nfolds = 10, alpha = 0.5)
print(prostate.glm)
prostate.km = h2o.kmeans(prostate.hex, centers = 5, cols = c("AGE,RACE,GLEASON,CAPSULE,DCAPS"))
print(prostate.km)

# Test of random forest using iris data set
iris.hex = importFile(h2o, "../smalldata/iris/iris.csv", "iris.hex")
iris.sum = summary(iris.hex)
print(iris.sum)
iris.rf = h2o.randomForest(y = "4", data = iris.hex, ntree = 50, depth = 100, classwt = c("Iris-versicolor"=20.0, "Iris-virginica"=30.0))
print(iris.rf)

# Test of k-means using random Gaussian data set
covtype.hex = importFile(h2o, "../smalldata/covtype/covtype.20k.data")
covtype.sum = summary(covtype.hex)
print(covtype.sum)
covtype.km = h2o.kmeans(covtype.hex, 10)
print(covtype.km)

# Test import folder function
glm_test.hex = importFolder(h2o, "../smalldata/glm_test")
for(i in 1:length(glm_test.hex))
  print(summary(glm_test.hex[[i]]))