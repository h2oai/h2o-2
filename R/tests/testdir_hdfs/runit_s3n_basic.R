#----------------------------------------------------------------------
# Purpose:  This test exercises amazon s3n access from R.
#----------------------------------------------------------------------

# setwd("/Users/tomk/0xdata/ws/h2o/R/tests/testdir_hdfs")

local({r <- getOption("repos"); r["CRAN"] <- "http://cran.us.r-project.org"; options(repos = r)})
if (!"R.utils" %in% rownames(installed.packages())) install.packages("R.utils")

options(echo=TRUE)
TEST_ROOT_DIR <- ".."
source(sprintf("%s/%s", TEST_ROOT_DIR, "findNSourceUtils.R"))


heading("BEGIN TEST")
conn <- new("H2OClient", ip=myIP, port=myPort)

#----------------------------------------------------------------------
# Single file cases.
#----------------------------------------------------------------------

heading("Testing single file importHDFS S3N for VA")
s3n_iris_file <- "0xdata-public/examples/h2o/R/datasets/iris_wheader.csv"

url <- sprintf("s3n://%s", s3n_iris_file)

iris.VA.hex <- h2o.importHDFS.VA(conn, url)
head(iris.VA.hex)
tail(iris.VA.hex)
n <- nrow(iris.VA.hex)
print(n)
if (n != 150) {
    stop("VA nrows is wrong")
}
if (class(iris.VA.hex) != "H2OParsedDataVA") {
    stop("iris.VA.hex is the wrong type")
}
print ("VA import worked")

PASS_BANNER()
