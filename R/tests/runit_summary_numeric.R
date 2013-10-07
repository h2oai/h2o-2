if(!"testthat" %in% rownames(installed.packages())) install.packages("testthat")
library(testthat)
context("Summary Tests Numeric") #set context for test. Here we are checking the tail() functionality
# R -f runit_summary_numeric.R --args H2OServer:Port
# By default, H2OServer = 127.0.0.1 and Port = 54321
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 1)
  stop("Usage: R -f runit_summary_numeric.R --args H2OServer:Port")
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

test.summary.numeric <- function(con) {
  cat("\nImporting USArrests.csv data...\n")
  arrests.hex = h2o.uploadFile(H2Ocon, "../../smalldata/pca_test/USArrests.csv", "arrests.hex")
  cat("\nCheck that summary works...")
  summary(arrests.hex)
  summary_ <- summary(arrests.hex)
  cat("\nCheck that we get a table back from the summary(hex)")
  expect_that(summary_, is_a("table"))
  summary_2 <- summary(tail(USArrests))
  
  cat("\nCheck that the summary of the tail of the dataset is the same as what R produces: ")
  cat("\nsummary(tail(USArrests))\n")
  
  print(summary_2)
  cat("\nsummary(tail(arrests.hex))\n")
  print(summary(tail(arrests.hex)))
  expect_that(summary(tail(arrests.hex)), equals(summary_2))

  print("End of test.")
}

test_that("summaryTests",test.summary.numeric(H2Ocon))
