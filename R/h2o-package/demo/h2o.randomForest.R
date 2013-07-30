# This is a demo of H2O's Random Forest (classification) function
# It imports a data set, parses it, and prints a summary
# Then, it runs RF with 50 trees, maximum depth of 100, using the iris class as the response
library(h2o)
h2o = new("H2OClient", ip="localhost", port=54321)

iris.hex = importFile(h2o, system.file("extdata", "iris.csv", package="h2o"), "iris.hex")
summary(iris.hex)
iris.rf = h2o.randomForest(y = "4", data = iris.hex, ntree = 50, depth = 100, classwt = c("Iris-versicolor"=20.0, "Iris-virginica"=30.0))
print(iris.rf)

prostate.hex = importFile(h2o, system.file("extdata", "prostate.csv", package="h2o"), "prostate.hex")
summary(prostate.hex)
prostate.rf = h2o.randomForest(y = "CAPSULE", x_ignore = c("ID","DPROS"), data = prostate.hex, ntree = 50, depth = 150)
print(prostate.rf)