source('./Utils/h2oR.R')

logging("\n======================== Begin Test ===========================\n")
serverH2O = new("H2OClient", ip=myIP, port=myPort)

test.RF.iris_class <- function(serverH2O) {
  serverH2O = new("H2OClient", ip=myIP, port=myPort)
  # iris.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/iris/iris22.csv", "iris.hex")
  # iris.hex = h2o.importFile(serverH2O, normalizePath("../../smalldata/iris/iris22.csv"), "iris.hex")
  iris.hex = h2o.uploadFile(serverH2O, "../../smalldata/iris/iris22.csv", "iris.hex")
  iris.rf = h2o.randomForest(y = 5, x = seq(1,4), data = iris.hex, ntree = 50, depth = 100, classwt = c("Iris-versicolor"=20.0, "Iris-virginica"=30.0))
  print(iris.rf)
  iris.rf = h2o.randomForest(y = 6, x = seq(1,4), data = iris.hex, ntree = 50, depth = 100 )
  print(iris.rf)
}

test.RF.iris_ignore <- function(serverH2O) {
  serverH2O = new("H2OClient", ip=myIP, port=myPort)
  # iris.hex = h2o.importURL(serverH2O, "https://raw.github.com/0xdata/h2o/master/smalldata/iris/iris22.csv", "iris.hex")
  # iris.hex = h2o.importFile(serverH2O, normalizePath("../../smalldata/iris/iris22.csv"), "iris.hex")
  iris.hex = h2o.uploadFile(serverH2O, "../../smalldata/iris/iris22.csv", "iris.hex")
  h2o.randomForest(y = 5, x = seq(1,4), data = iris.hex, ntree = 50, depth = 100)
  for(maxx in 1:4) {
    # myIgnore = as.character(seq(0, maxx))
    myX = seq(1, maxx)
    iris.rf = h2o.randomForest(y = 5, x = myX, data = iris.hex, ntree = 50, depth = 100)
    print(iris.rf)
  }
}

test.RF.iris_class(serverH2O)
test.RF.iris_ignore(serverH2O)
