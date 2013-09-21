# Test of random forest using iris data set, different classes
test.RF.iris_class <- function() {
  serverH2O = new("H2OClient", ip=myIP, port=myPort)
  iris.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/iris/iris22.csv", "iris.hex")
  iris.rf = h2o.randomForest(y = "4", data = iris.hex, ntree = 50, depth = 100, classwt = c("Iris-versicolor"=20.0, "Iris-virginica"=30.0))
  print(iris.rf)
  iris.rf = h2o.randomForest(y = "5", data = iris.hex, ntree = 50, depth = 100 )
  print(iris.rf)
}

test.RF.iris_ignore <- function() {
  serverH2O = new("H2OClient", ip=myIP, port=myPort)
  iris.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/iris/iris22.csv", "iris.hex")
  h2o.randomForest(y = "4", data = iris.hex, ntree = 50, depth = 100)
  for(maxx in 0:3) {
    myIgnore = as.character(seq(0, maxx))
    iris.rf = h2o.randomForest(y = "4", x_ignore = myIgnore, data = iris.hex, ntree = 50, depth = 100)
    print(iris.rf)
  }
}