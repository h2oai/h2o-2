# This is a demo of H2O's Random Forest (classification) function
# It imports a data set, parses it, and prints a summary
# Then, it runs RF with 50 trees, maximum depth of 100, using the iris class as the response
# Note: This demo runs H2O on localhost:54321
library(h2o)
myIP = readline("Enter IP address of H2O server: ")
myPort = readline("Enter port number of H2O server: ")
remoteH2O = h2o.init(ip = myIP, port = as.numeric(myPort), startH2O = FALSE)

iris.hex = h2o.uploadFile(remoteH2O, path = system.file("extdata", "iris.csv", package="h2o"), key = "iris.hex")
summary(iris.hex)
iris.rf = h2o.randomForest(y = 5, x = c(1,2,3,4), data = iris.hex, ntree = 50, depth = 100)
print(iris.rf)

invisible(readline("Hit <Return> to continue: "))

covtype.hex = h2o.uploadFile(remoteH2O, path = system.file("extdata", "covtype.csv", package="h2o"), key = "covtype.hex")
summary(covtype.hex)
covtype.rf = h2o.randomForest(y = "Cover_Type", x = setdiff(colnames(covtype.hex), c("Cover_Type", "Aspect", "Hillshade_9am")), data = covtype.hex, ntree = 50, depth = 150)
print(covtype.rf)
