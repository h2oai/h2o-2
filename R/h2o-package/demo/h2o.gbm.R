# This is a demo of H2O's GBM function
# It imports a data set, parses it, and prints a summary
# Then, it runs GBM on a subset of the dataset
library(h2o)
localH2O = new("H2OClient", ip="localhost", port=54321)
h2o.checkClient(localH2O)

prostate.hex = h2o.importFile(localH2O, system.file("extdata", "prostate.csv", package="h2o"), "prostate.hex")
summary(prostate.hex)
prostate.gbm = h2o.gbm(prostate.hex, "prostate.gbm", y = "CAPSULE", ntrees = 10, max_depth = 5, learn_rate = 0.1)
print(prostate.gbm)
prostate.gbm2 = h2o.gbm(prostate.hex, "prostate.gbm", y = "CAPSULE", x_ignore = c("ID", "DPROS", "DCAPS"), ntrees = 10, max_depth = 8, learn_rate = 0.2)
print(prostate.gbm2)

#This is a demo of H2O's GBM use of default parameters on iris dataset(three classes)

iris.hex = h2o.importFile(localH2O, system.file("extdata", "iris.csv", package="h2o"), "iris.hex")
summary(iris.hex)
iris.gbm=h2o.gbm(data=iris.hex,destination="iris",y="4")
print(iris.gbm)
