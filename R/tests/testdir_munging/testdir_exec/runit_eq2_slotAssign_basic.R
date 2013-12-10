##
# Test: [<- & $<-
# Description: Select a dataset, select columns, change values in the column, re-assign col
##

source('./findNSourceUtils.R')

#setupRandomSeed(1689636624)

test.basic.slot.assignment <- function(conn) {
  Log.info("Uploading iris data...")
  hex <- h2o.uploadFile(conn, locate("smalldata/iris/iris.csv"), "iris.hex")
  oldVal <- hex[1,1]

  hex[1,1] <- 48
  hex$Sepal.Length[2] <- 90

  expect_false(hex[1,1], equals(oldVal))
  expect_that(hex[1,1], equals(48))
  expect_that(hex$Sepal.Length[2], equals(90))
  testEnd()

}

doTest("EQ2 Tests: [<- and $<-", test.basic.slot.assignment)

