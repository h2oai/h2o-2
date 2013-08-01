# Demo to test R functionality
# To invoke, need R 2.15.0 or higher

# R -f test_R_RF_diff_class.R --args Path/To/H2O.R H2OServer:Port
args <- commandArgs(trailingOnly = TRUE)
if(length(args) != 2)
	  stop("Usage: R -f test_R_RF_diff_class.R --args Path/To/H2O.R H2OServer:Port")
	  source(args[1])
	  argsplit = strsplit(args[2], ":")[[1]]
h2o = new("H2OClient", ip=argsplit[1], port=as.numeric(argsplit[2]))

# library(h2o)
h2o = new("H2OClient", ip="localhost", port=54321)

# Test of random forest using iris data set, different classes
iris.hex = importURL(h2o, "https://raw.github.com/0xdata/h2o/master/smalldata/iris/iris22.csv", "iris.hex")
iris.rf = h2o.randomForest(y = "4", data = iris.hex, ntree = 50, depth = 100, classwt = c("Iris-versicolor"=20.0, "Iris-virginica"=30.0))
print(iris.rf)
iris.rf = h2o.randomForest(y = "5", data = iris.hex, ntree = 50, depth = 100 )
print(iris.rf)