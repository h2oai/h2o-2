#----------------------------------------------------------------------
# Purpose:  This test exercises HDFS operations from R.
#----------------------------------------------------------------------

# setwd("/Users/tomk/0xdata/ws/h2o/R/tests/testdir_hdfs")

options(echo=TRUE)
TEST_ROOT_DIR <- ".."
source(sprintf("%s/%s", TEST_ROOT_DIR, "findNSourceUtils.R"))


#----------------------------------------------------------------------
# Parameters for the test.
#----------------------------------------------------------------------

# Check if we are running inside the 0xdata network.
# This is checking for 192.168.1.* network, which is popular.  So this
# isn't the ideal check, but it's something.

rv = system("ifconfig | grep '192\\.168\\.1\\..*'")
# Grep returns 0 if there is a match.

running_inside_hexdata = (rv == 0)
if (running_inside_hexdata) {
    hdfs_name_node = "192.168.1.161"    
    hdfs_iris_file = "/datasets/runit/iris_wheader.csv"
    hdfs_iris_dir  = "/datasets/runit/iris_test_train"
} else {
    stop("Not running on 0xdata internal network.  No access to HDFS.")
}

#----------------------------------------------------------------------


heading("BEGIN TEST")
conn <- new("H2OClient", ip=myIP, port=myPort)

#----------------------------------------------------------------------
# Single file cases.
#----------------------------------------------------------------------

heading("Testing single file importHDFS for VA")
url <- sprintf("hdfs://%s%s", hdfs_name_node, hdfs_iris_file)
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


heading("Testing single file importHDFS for FV")
url <- sprintf("hdfs://%s%s", hdfs_name_node, hdfs_iris_file)
iris.FV.hex <- h2o.importFile(conn, url)
head(iris.FV.hex)
tail(iris.FV.hex)
n <- nrow(iris.FV.hex)
print(n)
if (n != 150) {
    stop("FV nrows is wrong")
}
if (class(iris.FV.hex) != "H2OParsedData") {
    stop("iris.FV.hex is the wrong type")
}
print ("FV import worked")

#----------------------------------------------------------------------
# Directory file cases.
#----------------------------------------------------------------------

heading("Testing directory importHDFS for VA")
url <- sprintf("hdfs://%s%s", hdfs_name_node, hdfs_iris_dir)
iris.VA.dir.hex <- h2o.importHDFS.VA(conn, url, pattern="*.csv")
head(iris.VA.dir.hex)
tail(iris.VA.dir.hex)
n <- nrow(iris.VA.dir.hex)
print(n)
if (n != 300) {
    stop("VA nrows is wrong")
}
if (class(iris.VA.dir.hex) != "H2OParsedDataVA") {
    stop("iris.VA.dir.hex is the wrong type")
}
print ("VA import worked")


heading("Testing directory importHDFS for FV")
url <- sprintf("hdfs://%s%s", hdfs_name_node, hdfs_iris_dir)
iris.FV.dir.hex <- h2o.importFolder(conn, url, pattern="*.csv")
head(iris.FV.dir.hex)
tail(iris.FV.dir.hex)
n <- nrow(iris.FV.dir.hex)
print(n)
if (n != 300) {
    stop("FV nrows is wrong")
}
if (class(iris.FV.dir.hex) != "H2OParsedData") {
    stop("iris.FV.dir.hex is the wrong type")
}
print ("FV import worked")

PASS_BANNER()
