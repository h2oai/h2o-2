if(!"testthat" %in% rownames(installed.packages())) install.packages("testthat")
library(testthat)
context("Slice Tests") #set context for test. Here we are checking the tail() functionality
# R -f runit_sliceColTail_iris.R --args H2OServer:Port
# By default, H2OServer = 127.0.0.1 and Port = 54321
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 1)
  stop("Usage: R -f runit_sliceColTail_iris.R --args H2OServer:Port")
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

library(RUnit)
library(h2o)

if(Sys.info()['sysname'] == "Windows")
  options(RCurlOptions = list(cainfo = system.file("CurlSSL", "cacert.pem", package = "RCurl")))

#------------------------------ Begin Tests ------------------------------#
H2Ocon <- new("H2OClient", ip=myIP, port=myPort)

test.slice.colTail <- function(con) {
  cat("\nImporting iris.csv data...\n")
  iris.hex = h2o.uploadFile(H2Ocon, "../../smalldata/iris/iris_wheader.csv", "iris.hex")
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
  cat("\nCheck the tail of first column (sepalLength).\n Do we get an atomic vector back? (we expect to!)")
  sepalLength <- iris.hex[,1]
  cat("\nSlicing out the first column still gives an h2o object.")
  expect_that(sepalLength, is_a("H2OParsedData"))
  cat("\nTail of sepalLength is:\n")
  print(tail(sepalLength))
  tryCatch(expect_that(is.atomic(tail(sepalLength)),equals(TRUE)),error = function(e) e)
  tryCatch(expect_that(tail(sepalLength),is_a("numeric")), error = function(e) e)
  cat("\nExamine the first 6 elements of the hex and compare to csv data: ")
  #TODO: have to get the vector of elements by using additional slice [,1]
  cat("\nhead(iris.hex[,1])[,1]")
  print(head(iris.hex[,1])[,1])
  cat("\nhead(iris[,1])")
  print(head(iris[,1]))
  tryCatch(expect_that(head(iris[,1]), equals(head(iris.hex[,1])[,1])),error=function(e) e)
  cat("\nTry to slice out a single element from a column")
  cat("\nSlicing out the first element of column1 still gives an h2o object.")
  expect_that(iris.hex[1,1], is_a("H2OParsedData"))
  cat("\nCan we perform 'head' on iris.hex[1,1]:\n ")
  cat("\n")
  tryCatch(head(iris.hex[1,1]),error=function(e) print(paste("Could not perform head(iris.hex[1,1]",e)))
  print("End of test.")
}

test_that("sliceTestsColTail",test.slice.colTail(H2Ocon))
