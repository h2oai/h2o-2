source('./findNSourceUtils.R')

test.summary2 <- function(con) {
  Log.info("\nImporting iris.csv data...\n")
  iris.hex <- h2o.importFile(con, locate("smalldata/iris/iris_wheader.csv", schema="local"))
  
  Log.info("\nCheck that summary works...")
  print(summary(iris.hex)) 

  Log.info("Summary from R's iris data: ")
  summary(iris)

  testEnd()
}

doTest("Summary2 Test", test.summary2)

