context("Slice Tests") #set context for test. Here we are checking the tail() functionality
# R -f runit_sliceColSummary_iris.R --args H2OServer:Port
# By default, H2OServer = 127.0.0.1 and Port = 54321
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 1)
  stop("Usage: R -f runit_sliceColSummary_iris.R --args H2OServer:Port")
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

test.slice.colSummary <- function(con) {
  cat("\nImporting iris.csv data...\n")
  iris.hex = h2o.uploadFile(H2Ocon, "smalldata/iris/iris_wheader.csv", "iris.hex")
  cat("\nCheck that summary works...")
  summary(iris.hex)
  summary_ <- summary(iris.hex) #keep the summary around
  iris_nrows <- nrow(iris.hex)
  iris_ncols <- ncol(iris.hex)
  cat("\nCheck that iris is 150x5")
  cat("\nGot:\n","nrows =",iris_nrows, "ncols =",iris_ncols)
  expect_that(iris_nrows, equals(150))
  expect_that(iris_ncols, equals(5))
  cat(" Good!")
  sepalLength <- iris.hex[,1]
  cat("\nSummary on the first column:\n")
  expect_that(sepalLength, is_a("H2OParsedData"))
  tryCatch(summary(sepalLength), error = function(e) print(paste("Cannot perform summary: ",e)))
  cat("\nTry mean, min, max, and compare to actual:\n")
  stats_ <- list("mean"=mean(sepalLength), "min"=min(sepalLength), "max"=max(sepalLength))
  stats <- list("mean"=mean(iris[,1]), "min"=min(iris[,1]), "max"=max(iris[,1]))
  cat("Actual mean, min, max:\n")
  print(stats)
  cat("\nH2O-R's mean, min, max: \n")
  print(stats_)
  tryCatch(expect_that(unlist(stats),equals(unlist(stats_))), error = function(e) e)
  cat("\nCheck standard deviation and variance: ")
  tryCatch(sd(sepalLength),error = function(e) print(paste("Cannot perform standard deviation: ",e,sep="")))
  print("End of test.")
}

test_that("sliceTestsColSummary", test.slice.colSummary(H2Ocon))
