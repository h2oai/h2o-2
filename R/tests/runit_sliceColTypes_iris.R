context("Slice Tests") #set context for test. Here we are checking the tail() functionality
# R -f runit_sliceColTypesIris.R --args H2OServer:Port
# By default, H2OServer = 127.0.0.1 and Port = 54321
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 1)
  stop("Usage: R -f runit_sliceColTypes_iris.R --args H2OServer:Port")
if(length(args) == 0) {
  myIP = "127.0.0.1"
  myPort = 54321
} else {
  argsplit = strsplit(args[1], ":")[[1]]
  myIP = argsplit[1]
  myPort = as.numeric(argsplit[2])
}
defaultPath = "../../target/R"

# Check if H2O R wrapper package is installed
if(!"h2oWrapper" %in% rownames(installed.packages())) {
  envPath = Sys.getenv("H2OWrapperDir")
  wrapDir = ifelse(envPath == "", defaultPath, envPath)
  wrapName = list.files(wrapDir, pattern="h2oWrapper")[1]
  wrapPath = paste(wrapDir, wrapName, sep="/")
  
  if(!file.exists(wrapPath))
    stop(paste("h2oWrapper package does not exist at", wrapPath))
  print(paste("Installing h2oWrapper package from", wrapPath))
  install.packages(wrapPath, repos = NULL, type = "source")
}

# Check that H2O R package matches version on server
library(h2oWrapper)
h2oWrapper.installDepPkgs()      # Install R package dependencies
h2oWrapper.init(ip=myIP, port=myPort, startH2O=FALSE, silentUpgrade = TRUE)

# Load H2O R package and run test
if(!"RUnit" %in% rownames(installed.packages())) install.packages("RUnit")
if(!"testthat" %in% rownames(installed.packages())) install.packages("testthat")

library(RUnit)
library(testthat)
library(h2o)

if(Sys.info()['sysname'] == "Windows")
  options(RCurlOptions = list(cainfo = system.file("CurlSSL", "cacert.pem", package = "RCurl")))

#------------------------------ Begin Tests ------------------------------#
view_max <- 10000 #maximum returned by Inspect.java

H2Ocon <- new("H2OClient", ip=myIP, port=myPort)

test.slice.colTypes <- function(con) {
  cat("\nImporting iris.csv data...\n")
  iris.hex = h2o.uploadFile(H2Ocon, "smalldata/iris/iris_wheader.csv", "iris.hex")
  cat("\nCheck that summary works...")
  summary(iris.hex)
  summary_ <- summary(iris.hex) #keep the summary around
  iris_nrows <- nrow(iris.hex)
  iris_ncols <- ncol(iris.hex)
  cat("\nCheck that iris is 150x5")
  cat("\nGot:\n",iris_nrows, iris_ncols)
  expect_that(iris_nrows, equals(150))
  expect_that(iris_ncols, equals(5))
  cat(" Good!")
  cat("\nCheck the column data types: \nExpect 'double, double, double, double, integer'")
  #TODO: Once head(hex) returns proper class (numeric), fix this col extraction
  col1_type <- typeof(head(iris.hex[,1],nrow(iris.hex))[,1])
  col2_type <- typeof(head(iris.hex[,2],nrow(iris.hex))[,1])
  col3_type <- typeof(head(iris.hex[,3],nrow(iris.hex))[,1])
  col4_type <- typeof(head(iris.hex[,4],nrow(iris.hex))[,1])
  col5_type <- typeof(head(iris.hex[,5],nrow(iris.hex))[,1])
  cat("\nGot:\n",col1_type,col2_type,col3_type,col4_type,col5_type)
  expect_that(col1_type, equals("double"))
  expect_that(col2_type, equals("double"))
  expect_that(col3_type, equals("double"))
  expect_that(col4_type, equals("double"))
  expect_that(col5_type, equals("integer"))
  print("End of test.")
}

test_that("sliceTestsColTypes",test.slice.colTypes(H2Ocon))
