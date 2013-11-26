source('../Utils/h2oR.R')

Log.info("\n======================== Begin Test ===========================\n")

test.RF.iris_class <- function(conn) {
  iris.hex = h2o.uploadFile(conn, "../../../smalldata/iris/iris22.csv", "iris.hex")
  iris.rf = h2o.randomForest(y = 5, x = seq(1,4), data = iris.hex, ntree = 50, depth = 100) 
  print(iris.rf)
  iris.rf = h2o.randomForest(y = 6, x = seq(1,4), data = iris.hex, ntree = 50, depth = 100 )
  print(iris.rf)
}

test.RF.iris_ignore <- function(conn) {
  conn = new("H2OClient", ip=myIP, port=myPort)
  # iris.hex = h2o.importURL(conn, "https..//raw.github.com/0xdata/h2o/master/smalldata/iris/iris22.csv", "iris.hex")
  # iris.hex = h2o.importFile(conn, normalizePath("../../../smalldata/iris/iris22.csv"), "iris.hex")
  iris.hex = h2o.uploadFile(conn, "../../../smalldata/iris/iris22.csv", "iris.hex")
  h2o.randomForest(y = 5, x = seq(1,4), data = iris.hex, ntree = 50, depth = 100)
  for(maxx in 1:4) {
    # myIgnore = as.character(seq(0, maxx))
    myX = seq(1, maxx)
    iris.rf = h2o.randomForest(y = 5, x = myX, data = iris.hex, ntree = 50, depth = 100)
    print(iris.rf)
  }
}


conn = new("H2OClient", ip=myIP, port=myPort)

tryCatch(test_that("RF test iris all", test.RF.iris_class(conn)), error = function(e) FAIL(e))
PASS()

tryCatch(test_that("RF test iris ignore", test.RF.iris_ignore(conn)), error =function(e) FAIL(e))
PASS()
