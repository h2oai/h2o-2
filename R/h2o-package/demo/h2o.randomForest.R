# This is a demo of H2O's Random Forest (classification) function
# It imports a data set, parses it, and prints a summary
# Then, it runs RF with 50 trees, maximum depth of 100, using the iris class as the response
# Note: This demo runs H2O on localhost:54321
library(h2oWrapper)
h2oWrapper.installDepPkgs()
localH2O = h2oWrapper.init(ip = "localhost", port = 54321, startH2O = TRUE, silentUpgrade = TRUE, promptUpgrade = FALSE)

iris.hex = h2o.importFile(localH2O, path = system.file("extdata", "iris.csv", package="h2o"), key = "iris.hex")
summary(iris.hex)
iris.rf = h2o.randomForest(y = 5, x = c(1,2,3,4), data = iris.hex, ntree = 50, depth = 100, classwt = c("Iris-versicolor" = 20.0, "Iris-virginica" = 30.0))
print(iris.rf)

invisible(readline("Hit <Return> to continue: "))

covtype.hex = h2o.importFile(localH2O, path = system.file("extdata", "covtype.csv", package="h2o"), key = "covtype.hex")
summary(covtype.hex)
covtype.rf = h2o.randomForest(y = "Cover_Type", x = setdiff(colnames(covtype.hex), c("Cover_Type", "Aspect", "Hillshade_9am")), data = covtype.hex, ntree = 50, depth = 150)
print(covtype.rf)
