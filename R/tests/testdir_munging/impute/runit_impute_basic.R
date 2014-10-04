setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')

cp <- function(this) this[1:nrow(this), 1:ncol(this)]

test.eq2.h2o.assign<-
function(conn) {
    iris.hex <- h2o.uploadFile(conn, locate("smalldata/iris/iris_missing.csv"), "iris.hex")
    dim(iris.hex)

    Log.info("Summary of the data in iris_missing.csv")
    Log.info("Each column has 50 missing observations (at random)")
    summary(iris.hex)


    Log.info("Make a copy of the original dataset to play with.")
    hex <- cp(iris.hex)
    print(hex@key)
    print(iris.hex@key)
    print(iris.hex)
    print(hex)

    Log.info("Impute a numeric column with the mean")
    h2o.impute(hex, .(Sepal.Length), method = "mean")
    expect_that(sum(is.na(hex[,"Sepal.Length"])), equals(0))
    hex <- cp(iris.hex)
    h2o.impute(hex, 1, method = "mean")
    expect_that(sum(is.na(hex[,"Sepal.Length"])), equals(0))
    hex <- cp(iris.hex)
    h2o.impute(hex, c("Sepal.Length"), method = "mean")
    expect_that(sum(is.na(hex[,"Sepal.Length"])), equals(0))
    hex <- cp(iris.hex)
    h2o.impute(hex, "Sepal.Length", method = "mean")
    expect_that(sum(is.na(hex[,"Sepal.Length"])), equals(0))
    hex <- cp(iris.hex)
 
    Log.info("Impute a numeric column with the median")
    h2o.impute(hex, .(Sepal.Length), method = "median")
    expect_that(sum(is.na(hex[,"Sepal.Length"])), equals(0))
    hex <- cp(iris.hex)
    h2o.impute(hex, 1, method = "median")
    expect_that(sum(is.na(hex[,"Sepal.Length"])), equals(0))
    hex <- cp(iris.hex)
    h2o.impute(hex, c("Sepal.Length"), method = "median")
    expect_that(sum(is.na(hex[,"Sepal.Length"])), equals(0))
    hex <- cp(iris.hex)
    h2o.impute(hex, "Sepal.Length", method = "median")
    expect_that(sum(is.na(hex[,"Sepal.Length"])), equals(0))
    hex <- cp(iris.hex)

    Log.info("Impute a factor column (uses the mode)")
    h2o.impute(hex, .(Species), method = "mode")
    expect_that(sum(is.na(hex[,"Species"])), equals(0))
    hex <- cp(iris.hex)
    h2o.impute(hex, 5, method = "mode")
    expect_that(sum(is.na(hex[,"Species"])), equals(0))
    hex <- cp(iris.hex)
    h2o.impute(hex, c("Species"), method = "mode")
    expect_that(sum(is.na(hex[,"Species"])), equals(0))
    hex <- cp(iris.hex)
    h2o.impute(hex, "Species", method = "mode")
    expect_that(sum(is.na(hex[,"Species"])), equals(0))
    hex <- cp(iris.hex)

    Log.info("Now check that imputing with column groupings works...")
    h2o.impute(hex, .(Sepal.Length), method = "mean", groupBy = c("Sepal.Width", "Petal.Width"))
    # possibly some NAs still present in the column, because of NAs in the groupBy columns
    print(hex)
    hex <- cp(iris.hex)
    h2o.impute(hex, 1, method = "median", groupBy = c("Species", "Petal.Width", "Petal.Length"))
    print(hex)
    hex <- cp(iris.hex)
    h2o.impute(hex, "Petal.Width", method = "mean", groupBy = c(1,2,5))
    print(hex)
    hex <- cp(iris.hex)
    h2o.impute(hex, "Species", method = "mode", groupBy = c(1,3,4))
    print(hex)


    testEnd()
}

doTest("Test h2o.assign(data,key)", test.eq2.h2o.assign)

